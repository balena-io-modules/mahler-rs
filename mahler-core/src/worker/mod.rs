//! Automated planning and execution of task workflows

use anyhow::anyhow;
use json_patch::Patch;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio::{select, sync::RwLock};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tracing::field::display;
use tracing::{debug, error, field, info, info_span, span, trace, warn, Instrument, Level, Span};

#[cfg(debug_assertions)]
mod testing;

#[cfg(debug_assertions)]
pub use testing::*;

use crate::errors::{IOError, InternalError, SerializationError};
use crate::planner::{Domain, Error as PlannerError, Planner};
use crate::state::State;
use crate::system::{Resources, System};
use crate::task::{Error as TaskError, Job};
use crate::workflow::{channel, AggregateError, AutoInterrupt, Interrupt, Sender, WorkflowStatus};

/// A panic happened in the Worker runtime
#[derive(Debug, Error)]
#[error(transparent)]
pub struct Panicked(#[from] tokio::task::JoinError);

/// Unrecoverable error during the worker runtime
#[derive(Debug, Error)]
pub enum SeekError {
    #[error(transparent)]
    /// Failed to serialize or deserialize the Worker current/target state
    Serialization(#[from] SerializationError),

    #[error(transparent)]
    /// A panic happened in the Worker runtime
    ///
    /// This most likely means that there is an uncaught panic in a task
    Panic(#[from] Panicked),

    #[error(transparent)]
    /// An error happened with a task during planning
    ///
    /// This most likely means there is a bug in a task. This error will
    /// only be returned if `debug_assertions` are set. Otherwise
    /// task errors are ignored by the planner
    Planning(#[from] TaskError),

    #[error(transparent)]
    /// An internal error occured during the worker operation
    ///
    /// This is probably a bug with mahler and it should be reported
    Internal(#[from] InternalError),
}

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
    Aborted(Vec<IOError>),
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
    notify_writer_closed: Arc<Notify>,
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
/// use anyhow::{Context, Result};
/// use serde::{Deserialize, Serialize};
///
/// use mahler::state::{State, Map};
/// use mahler::worker::{Worker, SeekStatus};
/// use mahler::task::prelude::*;
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
///         ])))
///         // fail if something bad happens
///         .with_context(|| "failed to initialize worker")?;
///
///     // start searching for a target
///     let status = worker.seek_target(CountersTarget(Map::from([
///         ("a".to_string(), 1),
///         ("b".to_string(), 2),
///     ])))
///     // wait for a result
///     .await
///     // fail if something bad happens
///     .with_context(|| "failed to reach target state")?;
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
/// use serde::{Deserialize, Serialize};
///
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
    /// use mahler::task::prelude::*;
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
    /// use mahler::task::prelude::*;
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
    /// Add a [Job](`crate::task::Job`) to the worker domain
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
    /// use mahler::worker::{Worker, Uninitialized};
    /// use mahler::task::prelude::*;
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
    /// The method will throw a [SerializationError](`crate::errors::SerializationError`) if the
    /// provided state cannot be converted to the internal state representation.
    pub fn initial_state(self, state: O) -> Result<Worker<O, Ready>, SerializationError>
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
        let notify_writer_closed = Arc::new(Notify::new());

        // Broadcast channel for state updates
        let (update_event_channel, _) = watch::channel(());

        // Spawn system writer task
        {
            let notify_writer_closed = notify_writer_closed.clone();
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
                            notify_writer_closed.notify_one();
                            break;
                        }
                        trace!("patch successful");

                        // Notify watchers, ignore errors if no receivers
                        // exist
                        let _ = update_event_tx.send(());

                        // yield back to the workflow
                        msg.ack();
                    }
                }
                .instrument(span!(Level::TRACE, "worker_sync",)),
            );
        }

        Ok(Worker::from_inner(Ready {
            domain,
            resources,
            system_rwlock,
            update_event_channel,
            patch_tx,
            notify_writer_closed,
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
    /// The method will throw a [SerializationError](`crate::errors::SerializationError`) if the
    /// internal state cannot be deserialized into the output type `<O>`
    pub async fn state(&self) -> Result<O, SerializationError>
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
    /// The method will result in a [`SeekError`] if a serialization issue occurs while converting
    /// between state types, if the worker runtime panics or there is an unexpected error during
    /// planning.
    pub async fn seek_with_interrupt(
        &mut self,
        tgt: O::Target,
        interrupt: Interrupt,
    ) -> Result<SeekStatus, SeekError> {
        let tgt = serde_json::to_value(tgt).map_err(SerializationError::from)?;

        let Ready {
            resources,
            domain,
            system_rwlock,
            notify_writer_closed,
            patch_tx,
            ..
        } = self.inner.clone();

        // Create new planner
        let planner = Planner::new(domain);

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
            Runtime(AggregateError<TaskError>),
            Planning(PlannerError),
        }

        async fn find_and_run_workflow<T: State>(
            planner: &Planner,
            sys_reader: &Arc<RwLock<System>>,
            tgt: &Value,
            patch_tx: &Sender<Patch>,
            sigint: &Interrupt,
        ) -> Result<InnerSeekResult, InnerSeekError> {
            info!("searching workflow");
            let now = Instant::now();
            // Show pending changes at debug level
            if tracing::enabled!(tracing::Level::DEBUG) {
                let system = sys_reader.read().await;
                // We need to compare the state by first serializing the
                // current state to I, removing any internal variables.
                // This is also done by the planner to avoid comparing
                // properties that are not part of the target state model.
                // We throw a planning error immediately if this process fails as
                // it will fail in planning anyway
                let cur = system
                    .state::<T::Target>()
                    .and_then(serde_json::to_value)
                    .map_err(SerializationError::from)
                    .map_err(PlannerError::from)
                    .map_err(InnerSeekError::Planning)?;
                let changes = json_patch::diff(&cur, tgt);
                if !changes.0.is_empty() {
                    // FIXME: this dumps internal state on the logs, we
                    // might need to mask sensitive data like passwords somehow
                    debug!("pending changes:");
                    for change in &changes.0 {
                        debug!("- {}", change);
                    }
                }
            }

            let workflow = {
                let system = sys_reader.read().await;
                let res = planner.find_workflow::<T>(&system, tgt);

                if let Err(PlannerError::NotFound) = res {
                    warn!(time = ?now.elapsed(), "workflow not found");
                }
                res.map_err(InnerSeekError::Planning)?
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
            let status = workflow
                .execute(sys_reader, patch_tx.clone(), sigint.clone())
                .await
                .map_err(InnerSeekError::Runtime)?;

            info!(time = ?now.elapsed(), "workflow executed successfully");

            if matches!(status, WorkflowStatus::Interrupted) {
                return Ok(InnerSeekResult::Interrupted);
            }

            Ok(InnerSeekResult::WorkflowCompleted)
        }

        // Make sure dropping the future will interrupt the search
        let drop_interrupt = AutoInterrupt::from(interrupt);

        // Main seek_with_interrupt planning and execution loop
        let handle: JoinHandle<Result<SeekStatus, SeekError>> = {
            let err_rx = notify_writer_closed;
            let interrupt = drop_interrupt.clone();
            // No writing allowed on this copy of the system lock
            let sys_reader = system_rwlock;
            let patch_tx = patch_tx;

            // We spawn a task rather than looping directly so we can catch panics happening
            // within the loop
            tokio::spawn(async move {
                let seek_span = Span::current();
                info!("applying target state");
                loop {
                    select! {
                        biased;

                        _ = err_rx.notified() => {
                            return Err(InternalError::from(anyhow!("state patch failed, worker state possibly tainted")))?;
                        }

                        res = find_and_run_workflow::<O>(&planner, &sys_reader, &tgt, &patch_tx, &interrupt) => {
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
                                Err(InnerSeekError::Planning(PlannerError::NotFound)) =>  {
                                    seek_span.record("result", display("workflow_not_found"));
                                    return Ok(SeekStatus::NotFound);
                                }
                                Err(InnerSeekError::Planning(PlannerError::Serialization(e))) =>  return Err(e)?,
                                Err(InnerSeekError::Planning(PlannerError::Internal(e))) =>  return Err(e)?,
                                Err(InnerSeekError::Planning(PlannerError::Task(e))) => return Err(e)?,
                                Err(InnerSeekError::Runtime(err)) => {
                                    let mut io = Vec::new();
                                    let mut other = Vec::new();
                                    let AggregateError (all) = err;
                                    for e in all.into_iter() {
                                        match e {
                                            TaskError::IO(re) => io.push(re),
                                            TaskError::ConditionFailed => {},
                                            _ => other.push(e)
                                        }

                                    }

                                    // If there are non-IO errors, there is
                                    // probably a bug somewhere
                                    if !other.is_empty() {
                                        return Err(InternalError::from(anyhow!(AggregateError::from(other))))?;
                                    }

                                    // Abort if there are any runtime errors as those
                                    // should be recoverable
                                    if !io.is_empty() {
                                        warn!("target state apply interrupted due to error");
                                        seek_span.record("result", display("aborted"));
                                        return Ok(SeekStatus::Aborted(io))
                                    }

                                    // If we got here, all errors were of type ConditionNotMet
                                    // in which case we re-plan as the state may have changed
                                    // underneath the worker
                                    continue;
                                }
                            }
                        }
                    }
                }
            }.instrument(info_span!("seek_target", result=field::Empty)))
        };

        let status = match handle.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Panicked(e))?,
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
    /// The method will result in a [`SeekError`] if a serialization issue occurs while converting
    /// between state types, if the worker runtime panics or there is an unexpected error during
    /// planning.
    pub async fn seek_target(&mut self, tgt: O::Target) -> Result<SeekStatus, SeekError> {
        self.seek_with_interrupt(tgt, Interrupt::new()).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::extract::{Target, View};
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
