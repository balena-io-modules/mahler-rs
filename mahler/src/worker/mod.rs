//! Automated planning and execution of task workflows

use json_patch::Patch;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tracing::field::display;
use tracing::{
    debug, error, field, info, info_span, instrument, span, trace, warn, Instrument, Level, Span,
};

#[cfg(debug_assertions)]
mod testing;

use crate::error::{AggregateError, Error, ErrorKind};
use crate::job::Job;
use crate::json::Value;
use crate::result::Result;
use crate::runtime::{Resources, System};
use crate::serde::de::DeserializeOwned;
use crate::state::State;
use crate::sync::{channel, Interrupt, RwLock, Sender};
use crate::system_ext::SystemExt;

mod auto_interrupt;
mod domain;
mod planner;
mod workflow;
use auto_interrupt::AutoInterrupt;
use domain::Domain;
use planner::PlanningError;

pub use workflow::*;

#[derive(Debug)]
/// Exit status from [`Worker::seek_target`]
pub enum SeekStatus {
    /// The worker has reached the target state
    Success,
    /// No workflow was found for the given target
    NotFound,
    /// Worker interrupted by user request
    Interrupted,
    /// An error happened while executing the workflow.
    Aborted(Vec<Error>),
}

impl PartialEq for SeekStatus {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (SeekStatus::Success, SeekStatus::Success)
                | (SeekStatus::NotFound, SeekStatus::NotFound)
                | (SeekStatus::Interrupted, SeekStatus::Interrupted)
        )
    }
}

impl Eq for SeekStatus {}

/// Helper trait to implement the Typestate pattern for Worker
pub trait WorkerState {}

/// Initial state of a Worker
///
/// While in the `Uninitialized` state, jobs and resources may be
/// assigned to the Worker
pub struct Uninitialized {
    domain: Domain,
    resources: Resources,
}

/// Initialized worker state
///
/// This is the state where the `Worker` moves to after receiving an initial state.
///
/// At this point the Worker is ready to start seeking a target state
#[derive(Clone)]
pub struct Ready {
    domain: Domain,
    resources: Resources,
    system_rwlock: Arc<RwLock<System>>,
    update_event_channel: watch::Sender<()>,
    patch_tx: Sender<Patch>,
    writer_closed_rx: watch::Receiver<()>,
    worker_id: u64,
    generation: Arc<AtomicU64>,
}

/// Final state of a Worker
///
/// No further Worker operations can be performed after this state
pub struct Stopped {}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}
impl WorkerState for Stopped {}

pub trait WithResources {
    fn insert_resource<R: Send + Sync + 'static>(&mut self, resource: R);
}

impl WithResources for Uninitialized {
    fn insert_resource<R>(&mut self, resource: R)
    where
        R: Send + Sync + 'static,
    {
        self.resources.insert(resource);
    }
}

impl WithResources for Ready {
    fn insert_resource<R>(&mut self, resource: R)
    where
        R: Send + Sync + 'static,
    {
        self.resources.insert(resource);
    }
}

/// Core component for workflow generation and execution
///
/// Given a target to [`Worker::seek_target`], the `Worker` will look for a plan that takes the
/// system from the current state to the target, execute the plan (workflow) and re-plan if some
/// pre-condition failed at runtime.
///
/// # Worker setup
///
/// A worker may be in one of the following states
/// - `Uninitialized` is the initial state of the worker, while the worker is in this state, new
///   [jobs](`Worker::job`) and [resources](`Worker::resource`) may be configured to the worker.
/// - `Ready` this is the default state. The `Worker` goes into this state when an [initial
///   state](`Worker::initial_state`) been defined or when a `seek_target` operation terminates
///   without error.
/// - `Stopped` is the final state of the Worker. The worker will go into this state if [`Worker::stop`] is
///   called and no furher operations can be performed.
///
///
/// ```rust,no_run
/// use serde::{Deserialize, Serialize};
///
/// use mahler::result::Result;
/// use mahler::state::{State, Map};
/// use mahler::worker::{Worker, SeekStatus};
/// use mahler::task::{IO, Task};
/// use mahler::job::update;
///
/// #[derive(State, Debug, PartialEq, Eq)]
/// struct Counters(Map<String, i32>);
///
/// // A simple job to update a counter
/// fn plus_one() -> IO<i32> { todo!() }
///
/// // A composite job
/// fn plus_two() -> Vec<Task> { todo!() }
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create a new uninitialized worker
///     let mut worker = Worker::new()
///         // configure jobs
///         .job("/{counter}", update(plus_one))
///         .job("/{counter}", update(plus_two))
///         // initialize the worker moving it to the `Ready` state
///         .initial_state(Counters(Map::from([
///             ("a".to_string(), 0),
///             ("b".to_string(), 0),
///         ])))?;
///
///     // start searching for a target
///     let status = worker.seek_target(CountersTarget(Map::from([
///         ("a".to_string(), 1),
///         ("b".to_string(), 2),
///     ])))
///     // wait for a result
///     .await?;
///
///     if matches!(status, SeekStatus::Success) {
///         println!("SUCCESS!");
///     }
///
///     Ok(())
/// }
/// ```
///
/// # State type compatibility
///
/// Note that the worker requires that the internal type `<O>` implements
/// [State](`crate::state::State`), which provides a `Target` associated type for the valid target
/// state type. This is because the internal state may have additional
/// fields that may not be desirable to consider when comparing states (e.g runtime states
/// timestamps, variable data, etc.). These types must be structurally compatible to ensure the Worker
/// can convert between them without issues.
///
/// While the requirement of the `State` trait doesn't guarantee structural compatibility, if
/// implemented via the `State` derive macro, it will ensure that states remain compatible.
///
/// In the example below, `InternalState` can be serialized into `TargetState` and
/// vice-versa.
///
/// ```rust
/// use serde::{Deserialize, Serialize};
///
/// use mahler::state::State;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct InternalState {
///     pub config: String,
///     pub last_update: Option<String>,
/// }
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct TargetState {
///     pub config: String,
/// }
///
/// impl State for InternalState {
///     type Target = TargetState;
/// }
/// ```
///
/// Serialization will fail between the two states below as `other_config` is not nullable.
///
/// ```rust
/// use serde::{Deserialize, Serialize};
///
/// use mahler::state::State;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct InternalState {
///     pub config: String,
///     pub other_config: String,
/// }
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct TargetState {
///     pub config: String,
/// }
///
/// impl State for InternalState {
///     type Target = TargetState;
/// }
/// ```
///
/// This can lead to subtle bugs, for instance the types below are compatible, but serialization is
/// different which can cause issues at planning.
///
/// ```rust
/// use serde::{Deserialize, Serialize};
///
/// use mahler::state::State;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct InternalState {
///     // this will result in unexpected results during planning due to the way
///     // the property is serialized in the internal and target states
///     #[serde(skip_serializing_if = "Option::is_none")]
///     pub config: Option<String>,
///     pub other_config: String,
/// }
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct TargetState {
///     pub config: Option<String>,
/// }
///
/// impl State for InternalState {
///     type Target = TargetState;
/// }
/// ```
///
/// The best way to avoid this is to use the `State` derive macro.
///
/// ```rust
/// use mahler::state::State;
///
/// #[derive(State, Debug)]
/// struct InternalState {
///     // this will be serialized in a consistent way by mahler
///     pub config: Option<String>,
///
///     #[mahler(internal)]
///     pub other_config: String,
/// }
/// ```
///
/// Using the macro above will result in a new type `InternalStateTarget` to be generated, ensuring
/// structural compatibility.
pub struct Worker<O, S: WorkerState = Uninitialized> {
    inner: S,
    _output: std::marker::PhantomData<O>,
}

impl<O, S: WorkerState> Worker<O, S> {
    /// Create a worker from a inner [`WorkerState`]
    fn from_inner(inner: S) -> Self {
        Worker {
            inner,
            _output: std::marker::PhantomData,
        }
    }

    /// Stop following system updates
    ///
    /// This drops all internal structure for the Worker and no further
    /// operations can be performed after this is called.
    pub fn stop(self) -> Worker<O, Stopped> {
        Worker::from_inner(Stopped {})
    }
}

// Worker initialization

impl<O> Default for Worker<O, Uninitialized> {
    fn default() -> Self {
        Worker::new()
    }
}

impl<O> Worker<O, Uninitialized> {
    /// Create a new uninitialized Worker instance
    pub fn new() -> Self {
        Worker::from_inner(Uninitialized {
            domain: Domain::new(),
            resources: Resources::new(),
        })
    }
}

impl<O, S: WorkerState + WithResources> Worker<O, S> {
    /// Add a shared resource to use within tasks
    ///
    /// Resources are stored by [TypeId](`std::any::TypeId`),
    /// meaning only one resource of each type is allowed. If multiple resources of the same type
    /// are configured, only the last one will be used.
    ///
    /// This method consumes self to use when chaining calls during Worker build
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use mahler::worker::{Worker, Uninitialized};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct StateModel;
    ///
    /// // MyConnection represents a shared resource
    /// struct MyConnection;
    ///
    /// let conn = MyConnection {/* .. */};
    /// let otherconn = MyConnection { /* .. */};
    ///
    /// let worker: Worker<StateModel, Uninitialized> = Worker::new()
    ///         // add conn as resource
    ///         .resource(conn)
    ///         // only the last assignment of a resource of the same type
    ///         // will be used
    ///         .resource(otherconn);
    /// ```
    ///
    /// Resources can be accessed by jobs using the [Res extractor](`crate::extract::Res`).
    ///
    /// ```rust
    /// use mahler::extract::Res;
    ///
    /// struct MyConnection;
    ///
    /// fn job_with_resource(res: Res<MyConnection>) {
    ///     // Access the worker MyConnection instance by dereferencing `res`.
    ///     // Initializing the extractor will fail if no resource of type MyConnection
    ///     // has been assigned to the worker.
    /// }
    /// ```
    pub fn resource<R>(mut self, res: R) -> Self
    where
        R: Send + Sync + 'static,
    {
        self.inner.insert_resource(res);
        self
    }

    /// Add/update a shared resource to use within tasks
    ///
    /// Resources are stored by [TypeId](`std::any::TypeId`),
    /// meaning only one resource of each type is allowed. If multiple resources of the same type
    /// are configured, only the last one will be used.
    ///
    /// ```rust
    /// use mahler::state::State;
    /// use mahler::worker::{Worker, Uninitialized};
    ///
    /// #[derive(State)]
    /// struct StateModel;
    ///
    /// // MyConnection represents a shared resource
    /// struct MyConnection;
    ///
    /// let conn = MyConnection {/* .. */};
    /// let otherconn = MyConnection { /* .. */};
    ///
    /// let mut worker: Worker<StateModel, Uninitialized> = Worker::new()
    ///         // add conn as resource
    ///         .resource(conn);
    ///
    /// // replace the connection
    /// worker.use_resource(otherconn);
    /// ```
    ///
    /// Resources can be accessed by jobs using the [Res extractor](`crate::extract::Res`).
    ///
    /// ```rust
    /// use mahler::extract::Res;
    ///
    /// struct MyConnection;
    ///
    /// fn job_with_resource(res: Res<MyConnection>) {
    ///     // Access the worker MyConnection instance by dereferencing `res`.
    ///     // Initializing the extractor will fail if no resource of type MyConnection
    ///     // has been assigned to the worker.
    /// }
    /// ```
    pub fn use_resource<R>(&mut self, res: R)
    where
        R: Send + Sync + 'static,
    {
        self.inner.insert_resource(res);
    }
}

impl<O> Worker<O, Uninitialized> {
    /// Add a [Job](`crate::job::Job`) to the worker domain
    pub fn job(mut self, route: &'static str, job: Job) -> Self {
        self.inner.domain = self.inner.domain.job(route, job);
        self
    }

    /// Add a list if jobs linked to the same route on the worker domain
    ///
    /// This is a convenience method to simplify the configuration of multiple jobs
    /// for the same domain
    ///
    /// ```rust
    /// use mahler::state::State;
    /// use mahler::job::update;
    /// use mahler::worker::{Worker, Uninitialized};
    ///
    /// #[derive(State)]
    /// struct StateModel;
    ///
    /// fn foo() {}
    /// fn bar() {}
    ///
    /// let worker: Worker<StateModel, Uninitialized> = Worker::new()
    ///
    ///         .jobs("/{foo}", [update(foo), update(bar)]);
    /// ```
    pub fn jobs<const N: usize>(mut self, route: &'static str, list: [Job; N]) -> Self {
        self.inner.domain = self.inner.domain.jobs(route, list);
        self
    }

    /// Provide the initial worker state
    ///
    /// This moves the state of the worker to `Ready`. No further jobs may
    /// be assigned after `initial_state` has been called.
    ///
    /// Note that an additional input type `<I>` may be defined at this point in case
    /// a different model is needed for the target state. See [State Type Compatibility](#state-type-compatibility).
    ///
    /// # Errors
    /// The method will throw an [`Error`] of type [`ErrorKind::Serialization`] if the
    /// provided state cannot be converted to the internal state representation.
    pub fn initial_state(self, state: O) -> Result<Worker<O, Ready>>
    where
        O: State,
    {
        let Uninitialized {
            domain, resources, ..
        } = self.inner;

        // Create a new system with an initial state
        let system = System::try_from(state)?;

        // Shared system protected by RwLock
        let system_rwlock = Arc::new(RwLock::new(system));

        // Create the state changes synchronization channel
        // XXX: should the channel size configurable? Or should we make it unbounded at risk of running
        // out of memory
        let (patch_tx, mut patch_rx) = channel::<Patch>(100);

        // Patch error signal (notify)
        let (writer_closed_tx, writer_closed_rx) = watch::channel(());

        // Broadcast channel for state updates
        let (update_event_channel, _) = watch::channel(());

        // Generate unique worker ID using random hash
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let worker_id = RandomState::new().build_hasher().finish();

        // Spawn system writer task
        {
            let system_writer = Arc::clone(&system_rwlock);
            let update_event_tx = update_event_channel.clone();
            tokio::spawn(
                async move {
                    // If the worker is dropped, then patch_tx will get dropped and
                    // this task will terminate, causing update_event_tx to get dropped
                    // and notifying the broadcast channel followers
                    while let Some(mut msg) = patch_rx.recv().await {
                        let changes = std::mem::take(&mut msg.data);
                        trace!(received=%changes);

                        let mut system = system_writer.write().await;
                        if let Err(e) = system.patch(changes) {
                            // we need to abort on patch failure a this means the
                            // internal state may have become inconsistent and we cannot continue
                            // applying changes
                            error!("patch failed: {e}");

                            // We drop the channel to let receivers know the writer task has closed
                            drop(writer_closed_tx);
                            break;
                        }
                        trace!("patch successful");

                        // Notify worker followers, ignore errors if no receivers exist
                        let _ = update_event_tx.send(());

                        // yield back to the workflow
                        msg.ack();
                    }
                }
                .instrument(span!(Level::TRACE, "worker_writer", worker_id=%worker_id)),
            );
        }

        Ok(Worker::from_inner(Ready {
            domain,
            resources,
            system_rwlock,
            update_event_channel,
            patch_tx,
            writer_closed_rx,
            worker_id,
            generation: Arc::new(AtomicU64::new(0)),
        }))
    }
}

// -- Worker is ready to receive a target state

/// Helper type to receive Worker state changes
///
/// See [`Worker::follow`]
struct WorkerStream<T> {
    inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>,
}

impl<T> WorkerStream<T> {
    /// Create a new FollowStream from a Stream
    fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = T> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> Stream for WorkerStream<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Returns a stream of updated states after each system change.
///
/// The stream is best effort, meaning updates may be missed if the receiver lags behind.
fn follow_worker<T>(
    channel: watch::Sender<()>,
    system_rwlock: Arc<RwLock<System>>,
) -> impl Stream<Item = T>
where
    T: DeserializeOwned,
{
    let rx = channel.subscribe();
    WorkerStream::new(
        WatchStream::from_changes(rx)
            .then(move |_| {
                let sys_reader = Arc::clone(&system_rwlock);
                async move {
                    // Read the system state
                    let system = sys_reader.read().await;
                    system.state::<T>().ok()
                }
            })
            .filter_map(|opt| opt),
    )
}

impl<O: State> Worker<O, Ready> {
    /// Read the current system state from the worker
    ///
    /// The system state is behind a [RwLock](`tokio::sync::RwLock`)
    /// to allow for concurrent modification, which is why this method is `async`.
    ///
    /// # Errors
    ///
    /// The method will throw an [`Error`] of kind [`ErrorKind::Serialization`] if the
    /// internal state cannot be deserialized into the output type `<O>`
    pub async fn state(&self) -> Result<O>
    where
        O: DeserializeOwned,
    {
        let system = self.inner.system_rwlock.read().await;
        let state = system.state()?;
        Ok(state)
    }

    /// Returns a stream of updated states after each system change
    ///
    /// The stream is best effort, meaning updates may be missed if the receiver lags behind.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use serde::{Deserialize, Serialize};
    /// use mahler::worker::Worker;
    /// use mahler::state::State;
    /// use tokio_stream::StreamExt;
    ///
    /// #[derive(State, Debug)]
    /// struct MySystem;
    ///
    /// # tokio_test::block_on(async move {
    /// let mut worker = Worker::new()
    ///     // todo: configure jobs
    ///     .initial_state(MySystem {/* .. */}).unwrap();
    ///
    /// // Get a state stream
    /// let mut update_stream = worker.follow();
    /// tokio::spawn(async move {
    ///     while let Some(state) = update_stream.next().await {
    ///         println!("Updated State: {:?}", state);
    ///     }
    /// });
    ///
    /// // Start state search
    /// worker.seek_target(MySystemTarget {/* .. */})
    ///     .await
    ///     .unwrap();
    /// # })
    /// ```
    pub fn follow(&self) -> impl Stream<Item = O>
    where
        O: DeserializeOwned,
    {
        follow_worker(
            self.inner.update_event_channel.clone(),
            Arc::clone(&self.inner.system_rwlock),
        )
    }

    /// Find a workflow to reach a target state
    ///
    /// Returns a workflow that can be executed with [`run_workflow`](Self::run_workflow).
    /// The workflow remains valid while other workflows have been executed in the meantime.
    ///
    /// # Errors
    /// Returns [`PlanningError::NotFound`] if no workflow can be found
    /// Returns [`PlanningError::Aborted`] on internal errors
    pub async fn find_workflow_to_target(
        &self,
        tgt: O::Target,
    ) -> core::result::Result<Workflow, PlanningError> {
        let tgt = serde_json::to_value(tgt).map_err(Error::from)?;

        let Ready {
            domain,
            system_rwlock,
            worker_id,
            generation,
            ..
        } = &self.inner;

        let system = system_rwlock.read().await;
        let current_generation = generation.load(Ordering::Acquire);

        let mut workflow = planner::find_workflow_to_target::<O>(domain, &system, &tgt)?;

        // Attach worker metadata
        workflow.worker_id = *worker_id;
        workflow.generation = current_generation;

        Ok(workflow)
    }

    /// Execute a previously found workflow
    ///
    /// This will only succeed if:
    /// - The workflow was created by this worker (worker_id matches)
    /// - No other workflows have been executed since the workflow was created (generation matches)
    ///
    /// # Errors
    /// - [`ErrorKind::WorkflowWorkerMismatch`] if workflow was created by different worker
    /// - [`ErrorKind::WorkflowStale`] if workflow generation doesn't match current generation
    /// - [`ErrorKind::ConditionNotMet`] if workflow conditions changed at runtime (caller should re-plan)
    /// - Returns [`SeekStatus::Aborted`] if workflow execution fails with runtime errors
    #[instrument(skip_all)]
    pub async fn run_workflow(
        &mut self,
        workflow: Workflow,
        interrupt: Interrupt,
    ) -> Result<SeekStatus> {
        let Ready {
            system_rwlock,
            patch_tx,
            worker_id,
            generation,
            writer_closed_rx,
            ..
        } = &self.inner;

        // do not execute the workflow if the writer task has closed
        if writer_closed_rx.has_changed().is_err() {
            return Err(Error::new(
                ErrorKind::Internal,
                format!("worker ({worker_id}) is in an inconsistent state",),
            ));
        }

        // Validate workflow belongs to this worker
        if workflow.worker_id != *worker_id {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "workflow worker_id {} does not match current worker_id {worker_id}",
                    workflow.worker_id,
                ),
            ));
        }

        // Validate workflow is still current
        let current_generation = generation.load(Ordering::Acquire);
        if workflow.generation != current_generation {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "workflow created at generation {} but current generation is {current_generation}",
                    workflow.generation,
                ),
            ));
        }

        let mut writer_closed_rx = writer_closed_rx.clone();
        let res = select! {
            biased;
            _ = writer_closed_rx.changed() => {
                return Err(Error::internal("state patch failed, worker state possibly tainted"));
            }
            res = workflow.execute(system_rwlock, patch_tx.clone(), interrupt) => res
        };

        // Increment generation after every workflow execution as
        // the state may have been changed by the workflow
        generation.fetch_add(1, Ordering::Release);

        match res {
            Ok(status) => match status {
                WorkflowStatus::Completed => Ok(SeekStatus::Success),
                WorkflowStatus::Interrupted => Ok(SeekStatus::Interrupted),
            },
            Err(agg_err) => {
                let AggregateError(all) = agg_err;
                let mut recoverable = Vec::new();
                let mut other = Vec::new();

                for e in all.into_iter() {
                    match e.kind() {
                        ErrorKind::Runtime => recoverable.push(e),
                        ErrorKind::ConditionNotMet => recoverable.push(e),
                        _ => other.push(e),
                    }
                }

                if !other.is_empty() {
                    return Err(Error::internal(AggregateError::from(other)));
                }

                // All remaining errors are recoverable, abort
                Ok(SeekStatus::Aborted(recoverable))
            }
        }
    }

    /// Trigger system changes by providing a new target state and interrupt signal
    ///
    /// When called, this method tells the worker to look for a plan for the given
    /// target. If a plan is found, the worker then will try to execute the resulting workflow and
    /// and terminate if interrupted or a runtime error occurs. If a requirement changes between
    /// planning and runtime, the Worker triggers a re-plan.
    ///
    /// The provided `interrupt` allows external cancellation of the worker execution.
    /// When triggered, the worker will gracefully terminate and return [`SeekStatus::Interrupted`].
    ///
    /// The worker will also be interrupted if the future is dropped, providing automatic cancellation.
    ///
    /// If no plan is found, the search terminates with a [`SeekStatus::NotFound`].
    ///
    /// # Parameters
    /// - `tgt`: The target state to seek
    /// - `interrupt`: User-controlled interrupt for canceling the operation
    ///
    /// # Errors
    /// The method will result in an [`Error`] if a serialization issue occurs while converting
    /// between state types, if the worker runtime panics or there is an unexpected error during
    /// planning.
    pub async fn seek_with_interrupt(
        &mut self,
        tgt: O::Target,
        interrupt: Interrupt,
    ) -> Result<SeekStatus> {
        let tgt = serde_json::to_value(tgt)?;

        let Ready {
            resources,
            domain,
            system_rwlock,
            writer_closed_rx,
            patch_tx,
            generation,
            worker_id,
            ..
        } = self.inner.clone();

        // abort if the writer has closed
        if writer_closed_rx.has_changed().is_err() {
            return Err(Error::new(
                ErrorKind::Internal,
                format!("worker ({worker_id}) is in an inconsistent state",),
            ));
        }

        // Update system resources
        {
            let mut system = self.inner.system_rwlock.write().await;
            system.set_resources(resources);
        }

        enum InnerSeekResult {
            TargetReached,
            WorkflowCompleted,
            Interrupted,
        }

        enum InnerSeekError {
            Runtime(AggregateError<Error>),
            Planning(PlanningError),
        }

        async fn find_and_run_workflow<T: State>(
            domain: &Domain,
            sys_reader: &Arc<RwLock<System>>,
            tgt: &Value,
            patch_tx: &Sender<Patch>,
            sigint: &Interrupt,
            generation: &Arc<AtomicU64>,
        ) -> core::result::Result<InnerSeekResult, InnerSeekError> {
            info!("searching workflow");
            let now = Instant::now();

            let workflow = {
                let system = sys_reader.read().await;
                let res = planner::find_workflow_to_target::<T>(domain, &system, tgt);

                // Show pending changes at debug level
                if tracing::enabled!(tracing::Level::DEBUG) {
                    let changes = match res {
                        Ok(ref workflow) => workflow.operations(),
                        Err(PlanningError::NotFound(ref changes)) => changes.iter().collect(),
                        Err(e) => return Err(InnerSeekError::Planning(e)),
                    };

                    if !changes.is_empty() {
                        debug!("pending changes:");
                        for change in changes {
                            debug!("- {}", change);
                        }
                    }
                }

                if res.is_err() {
                    warn!(time = ?now.elapsed(), "workflow not found");
                }
                res.map_err(InnerSeekError::Planning)?

                // NOTE: there is no need to set the workflow generation here since the
                // workflow is only used internally
            };

            if workflow.is_empty() {
                debug!("nothing to do");
                return Ok(InnerSeekResult::TargetReached);
            }
            info!(time = ?now.elapsed(), "workflow found");

            if tracing::enabled!(tracing::Level::WARN) {
                warn!("the following paths were ignored during planning");
                for path in workflow.ignored() {
                    warn!("{path}");
                }
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!("will execute the following tasks:");
                for line in workflow.to_string().lines() {
                    debug!("{line}");
                }
            }

            let now = Instant::now();
            info!("executing workflow");
            let res = workflow
                .execute(sys_reader, patch_tx.clone(), sigint.clone())
                .await
                .map_err(InnerSeekError::Runtime);

            // Increment worker generation after every workflow execution as
            // state may have changed
            generation.fetch_add(1, Ordering::Release);

            // Get the status from the result
            let status = res?;
            info!(time = ?now.elapsed(), "workflow executed successfully");

            if matches!(status, WorkflowStatus::Interrupted) {
                return Ok(InnerSeekResult::Interrupted);
            }

            Ok(InnerSeekResult::WorkflowCompleted)
        }

        // Make sure dropping the future will interrupt the search
        let drop_interrupt = AutoInterrupt::from(interrupt);

        // Main seek_with_interrupt planning and execution loop
        let handle: JoinHandle<Result<SeekStatus>> = {
            let mut err_rx = writer_closed_rx;
            let interrupt = drop_interrupt.clone();
            // No writing allowed on this copy of the system lock
            let sys_reader = system_rwlock;
            let patch_tx = patch_tx;

            // We spawn a task rather than looping directly so we can catch panics happening
            // within the loop
            tokio::spawn(
                async move {
                    let seek_span = Span::current();
                    info!("applying target state");
                    loop {
                        let res = select! {
                            biased;
                            _ = err_rx.changed() => {
                                return Err(Error::internal("state patch failed, worker state possibly tainted"));
                            }
                            res = find_and_run_workflow::<O>(&domain, &sys_reader, &tgt, &patch_tx, &interrupt, &generation) => res
                        };

                        match res {
                            Ok(InnerSeekResult::TargetReached) => {
                                info!("target state applied");
                                seek_span.record("result", display("success"));
                                return Ok(SeekStatus::Success);
                            }
                            Ok(InnerSeekResult::WorkflowCompleted) => {}
                            Ok(InnerSeekResult::Interrupted) => {
                                warn!("target state apply interrupted by user request");
                                seek_span.record("result", display("interrupted"));

                                return Ok(SeekStatus::Interrupted);
                            }
                            Err(InnerSeekError::Planning(e)) => {
                                if let PlanningError::Aborted(err) = e {
                                    return Err(err);
                                } else {
                                    seek_span.record("result", display("workflow_not_found"));
                                    return Ok(SeekStatus::NotFound);
                                }
                            }
                            Err(InnerSeekError::Runtime(err)) => {
                                let mut io = Vec::new();
                                let mut other = Vec::new();
                                let AggregateError(all) = err;
                                for e in all.into_iter() {
                                    match e.kind() {
                                        ErrorKind::Runtime => io.push(e),
                                        ErrorKind::ConditionNotMet => {}
                                        _ => other.push(e),
                                    }
                                }

                                // If there are non-IO errors, there is
                                // probably a bug somewhere so we package them as an internal
                                // error
                                if !other.is_empty() {
                                    return Err(Error::internal(AggregateError::from(other)))?;
                                }

                                // Abort if there are any runtime errors as those
                                // should be recoverable
                                if !io.is_empty() {
                                    warn!("target state apply interrupted due to error");
                                    seek_span.record("result", display("aborted"));
                                    return Ok(SeekStatus::Aborted(io));
                                }

                                // If we got here, all errors were of type ConditionNotMet
                                // in which case we re-plan as the state may have changed
                                // underneath the worker
                                continue;
                            }
                        }
                    }
                }
                .instrument(info_span!("seek_target", result = field::Empty)),
            )
        };

        let status = match handle.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Error::runtime(e))?,
        }?;

        Ok(status)
    }

    /// Trigger system changes by providing a new target state for the worker
    ///
    /// When called, this method tells the worker to look for a plan for the given
    /// target. If a plan is found, the worker then will try to execute the resulting workflow and
    /// and terminate if interrupted or a runtime error occurs. If a requirement changes between
    /// planning and runtime, the Worker triggers a re-plan.
    ///
    /// The worker will be interrupted if the future is dropped, providing automatic cancellation.
    ///
    /// If no plan is found, the search terminates with a [`SeekStatus::NotFound`].
    ///
    /// # Errors
    /// The method will result in an [`Error`] if a serialization issue occurs while converting
    /// between state types, if the worker runtime panics or there is an unexpected error during
    /// planning.
    pub async fn seek_target(&mut self, tgt: O::Target) -> Result<SeekStatus> {
        self.seek_with_interrupt(tgt, Interrupt::new()).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::extract::{Target, View};
    use crate::job::*;
    use crate::task::*;
    use serde::{Deserialize, Serialize};
    use tokio::time::{sleep, timeout};
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Counters(HashMap<String, i32>);

    impl State for Counters {
        type Target = Self;
    }

    fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
        if *counter < tgt {
            // Modify the counter if we are below target
            *counter += 1;
        }

        // Return the updated counter. The I/O part of the
        // effect will only be called if the job is chosen
        // in the workflow which will only happens if there are
        // changes
        with_io(counter, |counter| async {
            sleep(Duration::from_millis(10)).await;
            Ok(counter)
        })
    }

    fn buggy_plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
        if *counter < tgt {
            // This is the wrong operation
            *counter -= 1;
        }

        counter
    }

    fn init() {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_target(false)
                    .with_thread_names(true)
                    .with_thread_ids(true)
                    .with_line_number(true)
                    .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
            )
            .with(EnvFilter::from_default_env())
            .try_init()
            .unwrap_or(());
    }

    #[tokio::test]
    async fn test_worker_complex_state() {
        init();
        let mut worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let status = worker
            .seek_target(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .await
            .unwrap();

        assert_eq!(status, SeekStatus::Success);
        let state = worker.state().await.unwrap();
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    async fn test_worker_find_and_run_separately() {
        init();
        let mut worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let workflow = worker
            .find_workflow_to_target(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .await
            .unwrap();

        let workflow_copy = workflow.clone();

        // Run the workflow
        let status = worker
            .run_workflow(workflow, Interrupt::new())
            .await
            .unwrap();

        assert_eq!(status, SeekStatus::Success);
        let state = worker.state().await.unwrap();
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );

        // Try to run the old-workflow again, this should fail
        let res = worker.run_workflow(workflow_copy, Interrupt::new()).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_worker_workflow_mismatch_should_fail() {
        init();
        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let workflow = worker
            .find_workflow_to_target(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .await
            .unwrap();

        // Create a different worker
        let mut worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        // Running the workflow should fail
        let res = worker.run_workflow(workflow, Interrupt::new()).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_worker_bug() {
        init();
        let mut worker = Worker::new()
            .job("", update(buggy_plus_one))
            .initial_state(0)
            .unwrap();

        let status = worker.seek_target(2).await.unwrap();
        assert!(matches!(status, SeekStatus::NotFound));
    }

    #[tokio::test]
    async fn test_worker_follow_updates() {
        init();
        let mut worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        // Collect all results
        let mut updates = worker.follow();
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                let mut res = results.write().await;
                // Capture two updates
                let first_update = updates.next().await;
                res.push(first_update);

                let second_update = updates.next().await;
                res.push(second_update);
            });
        }

        // Wait for worker to finish
        let status = worker.seek_target(2).await.unwrap();
        assert_eq!(status, SeekStatus::Success);

        let results = results.read().await;
        assert_eq!(*results, vec![Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn test_worker_follow_best_effort_loss() {
        init();
        let mut worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        let mut updates = worker.follow();
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                let mut res = results.write().await;

                // Consume only some of the updates to simulate slow reader
                let first = updates.next().await;
                res.push(first);

                // Sleep to let many updates be missed
                tokio::time::sleep(Duration::from_millis(200)).await;

                // Attempt to read again (might be after some lag)
                let maybe_update = updates.next().await;
                res.push(maybe_update);
            });
        }

        // Wait for worker to finish
        let status = worker.seek_target(100).await.unwrap();
        assert_eq!(status, SeekStatus::Success);

        let results = results.read().await;
        assert_eq!(
            (*results)
                .iter()
                .map(|r| r.is_some())
                .collect::<Vec<bool>>(),
            vec![true, true]
        )
    }

    #[tokio::test]
    async fn test_worker_interrupt_status() {
        init();

        fn sleepy_plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
            if *counter < tgt {
                // Modify the counter if we are below target
                *counter += 1;
            }

            // Return the updated counter. The I/O part of the
            // effect will only be called if the job is chosen
            // in the workflow which will only happens if there are
            // changes
            with_io(counter, |counter| async {
                sleep(Duration::from_millis(10)).await;
                Ok(counter)
            })
        }

        let mut worker = Worker::new()
            .job("", update(sleepy_plus_one))
            .initial_state(0)
            .unwrap();

        let mut updates = worker.follow();
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                while let Some(s) = updates.next().await {
                    let mut res = results.write().await;
                    res.push(s);
                }
            });
        }

        // Ensure a timeout happens before the end of the run
        let res = timeout(Duration::from_millis(30), worker.seek_target(10)).await;
        assert!(res.is_err());

        // dropping the worker terminates the stream early
        let results = results.read().await;
        assert!(results.len() < 3);
    }

    #[tokio::test]
    async fn test_follow_stream_closes_on_worker_end() {
        init();
        let mut worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        let mut updates = worker.follow();
        let results = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        {
            let results = Arc::clone(&results);
            tokio::spawn(async move {
                let mut res = results.write().await;

                // After worker finishes, stream should terminate
                let first = updates.next().await;
                res.push(first);

                let end = updates.next().await;
                res.push(end);
            });
        }

        let status = worker.seek_target(1).await.unwrap();

        // Wait for worker to finish
        assert_eq!(status, SeekStatus::Success);

        // Close the stream
        worker.stop();

        let results = results.read().await;
        assert_eq!(*results, vec![Some(1), None]);
    }

    #[tokio::test]
    async fn test_multiple_streams_receive_all_changes() {
        init();
        let mut worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        // Create multiple streams
        let mut stream1 = worker.follow();
        let mut stream2 = worker.follow();
        let mut stream3 = worker.follow();

        let results1 = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        let results2 = Arc::new(tokio::sync::RwLock::new(Vec::new()));
        let results3 = Arc::new(tokio::sync::RwLock::new(Vec::new()));

        // Spawn tasks to collect updates from each stream
        let task1 = {
            let results = Arc::clone(&results1);
            tokio::spawn(async move {
                let mut res = results.write().await;
                while let Some(update) = stream1.next().await {
                    res.push(update);
                }
            })
        };

        let task2 = {
            let results = Arc::clone(&results2);
            tokio::spawn(async move {
                let mut res = results.write().await;
                while let Some(update) = stream2.next().await {
                    res.push(update);
                }
            })
        };

        let task3 = {
            let results = Arc::clone(&results3);
            tokio::spawn(async move {
                let mut res = results.write().await;
                while let Some(update) = stream3.next().await {
                    res.push(update);
                }
            })
        };

        // Execute the worker to generate state changes
        let status = worker.seek_target(3).await.unwrap();
        assert_eq!(status, SeekStatus::Success);

        // Stop the worker to terminate streams
        worker.stop();

        // Wait for all tasks to complete
        let _ = tokio::join!(task1, task2, task3);

        // Verify all streams received the same updates
        let results1 = results1.read().await;
        let results2 = results2.read().await;
        let results3 = results3.read().await;

        let expected = vec![1, 2, 3];
        assert_eq!(*results1, expected);
        assert_eq!(*results2, expected);
        assert_eq!(*results3, expected);
    }
}
