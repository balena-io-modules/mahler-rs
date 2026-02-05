//! Automated planning and execution of task workflows

use std::pin::pin;
use std::time::Instant;

use async_stream::try_stream;
use jsonptr::{Pointer, PointerBuf};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, info, trace, warn, Instrument, Span};

use crate::error::{AggregateError, Error, ErrorKind};
use crate::exception::Exception;
use crate::job::Job;
use crate::json::{Operation, Patch, PatchOperation, ReplaceOperation, Value};
use crate::result::Result;
use crate::runtime::{Context, Resources, System};
use crate::sensor::{Sensor, SensorBuilder, SensorRouter, SensorStream};
use crate::serde::{de::DeserializeOwned, Serialize};
use crate::state::State;
use crate::sync::{channel, rw_lock, Interrupt, Reader, WithAck, Writer};
use crate::system_ext::SystemExt;
use crate::task::Task;

mod auto_interrupt;
mod domain;
mod planner;
mod workflow;

use auto_interrupt::AutoInterrupt;
use domain::Domain;
use workflow::WorkflowStatus as InnerWorkflowStatus;

pub use workflow::Workflow;

/// Events emitted during workflow execution
///
/// Yielded by the stream returned from [`Worker::run_workflow`].
#[derive(Debug)]
pub enum WorkerEvent<S> {
    /// State was updated after a task completed
    StateUpdated(S),
    /// Workflow execution terminated
    WorkflowFinished(WorkflowStatus),
}

/// Exit status from [`Worker::seek_target`]
#[derive(Debug)]
pub enum SeekStatus {
    NotFound,
    /// The workflow was executed successfully
    Success,
    /// An error happened while executing the workflow.
    Aborted(Vec<Error>),
}

impl PartialEq for SeekStatus {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (SeekStatus::NotFound, SeekStatus::NotFound)
                | (SeekStatus::Success, SeekStatus::Success)
        )
    }
}

impl Eq for SeekStatus {}

impl From<WorkflowStatus> for SeekStatus {
    fn from(s: WorkflowStatus) -> Self {
        use WorkflowStatus::*;
        match s {
            Success => SeekStatus::Success,
            Aborted(errors) => SeekStatus::Aborted(errors),
        }
    }
}

/// Exit status from [`Worker::run_workflow`]
#[derive(Debug)]
pub enum WorkflowStatus {
    /// The workflow was executed successfully
    Success,
    /// An error happened while executing the workflow.
    Aborted(Vec<Error>),
}

impl PartialEq for WorkflowStatus {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (WorkflowStatus::Success, WorkflowStatus::Success)
        )
    }
}

impl Eq for WorkflowStatus {}

/// Helper trait to implement the Typestate pattern for Worker
pub trait WorkerState {}

/// Initial state of a Worker
///
/// While in the `Uninitialized` state, jobs and resources may be
/// assigned to the Worker
#[derive(Clone)]
pub struct Uninitialized {
    domain: Domain,
    sensors: SensorRouter,
    resources: Resources,
}

/// Initialized worker state
///
/// This is the state where the `Worker` moves to after receiving an initial state.
///
/// At this point the Worker is ready to start seeking a target state
pub struct Ready {
    domain: Domain,
    system: System,
    sensor_router: SensorRouter,
    worker_id: u64,
}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}

/// Core component for workflow generation and execution
///
/// Given a target to [`Worker::seek_target`], the `Worker` will look for a plan that takes the
/// system from the current state to the target and execute the plan (workflow).
///
/// # Worker setup
///
/// A worker may be in one of the following states
/// - `Uninitialized` is the initial state of the worker, while the worker is in this state, new
///   [jobs](`Worker::job`), [sensors](`Worker::sensor`), [exceptions](`Worker::exception`) and
///   [resources](`Worker::resource`) may be configured to the worker.
/// - `Ready` the `Worker` goes into this state when an [initial state](`Worker::initial_state`)
///   is provided.
///
/// # Ownership model
///
/// The `Worker<O, Ready>` is consumed by [`seek_target`](Worker::seek_target),
/// [`run_workflow`](Worker::run_workflow), and [`listen`](Worker::listen). This design ensures
/// that the worker's internal state remains consistent during workflow execution.
///
/// The `Worker<O, Uninitialized>` is [`Clone`], allowing a configured worker to be reused
/// with different initial states:
///
/// ```rust
/// # use mahler::state::State;
/// # use mahler::worker::{Worker, Uninitialized};
/// # use mahler::job::update;
/// # #[derive(State, Clone)] struct MyState;
/// # fn my_job() {/* .. */}
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Configure the worker once
/// let base: Worker<MyState, Uninitialized> = Worker::new()
///     .job("/", update(my_job));
///
/// // Clone and initialize with different states
/// let worker_a = base.clone().initial_state(MyState { /* .. */ })?;
/// let worker_b = base.clone().initial_state(MyState { /* .. */ })?;
/// # Ok(())
/// # }
/// ```
///
/// To perform sequential operations, create a new worker with the resulting state:
///
/// ```rust
/// # use mahler::state::State;
/// # use mahler::worker::Worker;
/// # #[derive(State, Clone)] struct MyState;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let worker = Worker::new().initial_state(MyState { /* .. */ })?;
/// let (state, _) = worker.seek_target(MyStateTarget { /* .. */ }).await?;
///
/// // Create a new worker with the updated state
/// let worker = Worker::new().initial_state(state)?;
/// let (state, _) = worker.seek_target(MyStateTarget { /* .. */ }).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Cancellation
///
/// Operations can be cancelled by dropping the returned future or stream. When dropped,
/// any in-progress workflow execution is interrupted gracefully.
///
///
/// ```rust
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
/// # async fn test_worker_example() -> Result<()> {
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
///     let (state, status) = worker.seek_target(CountersTarget(Map::from([
///         ("a".to_string(), 1),
///         ("b".to_string(), 2),
///     ])))
///     // wait for a result
///     .await?;
///
///     assert_eq!(status, SeekStatus::Success);
/// # Ok(())
/// # }
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
            sensors: SensorRouter::new(),
        })
    }
}

// Only implement Clone for uninitialized workers, as an initialized worker
// has a unique id to validate workflows
impl<O> Clone for Worker<O, Uninitialized> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Worker::from_inner(inner)
    }
}

impl<O> Worker<O, Uninitialized> {
    /// Add a [Job](`crate::job::Job`) to the worker domain
    pub fn job(mut self, route: &'static str, job: Job) -> Self {
        self.inner.domain = self.inner.domain.job(route, job);
        self
    }

    /// Add an [Exception](`crate::exception::Exception`) to the worker domain
    pub fn exception(mut self, route: &'static str, exception: Exception) -> Self {
        self.inner.domain = self.inner.domain.exception(route, exception);
        self
    }

    /// Configure a new [Sensor](`crate::sensor`) to be set up by the
    /// worker runtime whenever a state matching the sensor path is created in the system.
    ///
    /// # Example
    ///
    /// ```rust, no_run
    /// use mahler::extract::Args;
    /// use mahler::state::State;
    /// use mahler::worker::{Worker, Uninitialized};
    /// use tokio_stream::Stream;
    ///
    /// #[derive(State)]
    /// struct HomeHeating;
    ///
    /// fn temperature_monitor(Args(room): Args<String>) -> impl Stream<Item = i32> {
    ///     tokio_stream::iter(vec![20, 21, 22])
    /// }
    ///
    /// let worker: Worker<HomeHeating, Uninitialized> = Worker::new()
    ///         // add a temperature sensor to any new configured room in the home heating system
    ///         .sensor("/rooms/{room}/temperature", temperature_monitor);
    /// ```
    pub fn sensor<H, T, U>(mut self, route: &'static str, sensor: H) -> Self
    where
        H: SensorBuilder<T, U>,
        U: Serialize + 'static,
    {
        let Uninitialized {
            ref mut sensors, ..
        } = self.inner;

        // insert the sensor replacing the route
        sensors.insert(route, Sensor::new(sensor));

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
    ///         // configure multiple jobs for path `/{foo}`
    ///         .jobs("/{foo}", [update(foo), update(bar)]);
    /// ```
    pub fn jobs<const N: usize>(mut self, route: &'static str, list: [Job; N]) -> Self {
        self.inner.domain = self.inner.domain.jobs(route, list);
        self
    }

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
        self.inner.resources.insert(res);
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
        self.inner.resources.insert(res);
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
            domain,
            resources,
            sensors,
        } = self.inner;

        // Create a new system with an initial state and resources
        let mut system = System::try_from(state)?;
        system.set_resources(resources);

        // Generate unique worker ID
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let worker_id = RandomState::new().build_hasher().finish();

        Ok(Worker::from_inner(Ready {
            domain,
            system,
            sensor_router: sensors,
            worker_id,
        }))
    }
}

/// Utility trait to allow chaining operations from a workflow created
/// by calling `initial_state`
#[async_trait::async_trait]
pub trait SeekTarget<O: State> {
    async fn seek_target(self, tgt: O::Target) -> Result<(O, SeekStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static,
        O::Target: Serialize + Send;
}

#[async_trait::async_trait]
impl<O: State> SeekTarget<O> for Result<Worker<O, Ready>> {
    async fn seek_target(self, tgt: O::Target) -> Result<(O, SeekStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static,
        O::Target: Serialize + Send,
    {
        let worker = self?;
        worker.seek_target(tgt).await
    }
}

/// Utility trait to allow chaining operations from a workflow created
/// by calling `initial_state`
#[async_trait::async_trait]
pub trait RunTask<O: State> {
    async fn run_task(self, task: Task) -> Result<(O, WorkflowStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static;
}

#[async_trait::async_trait]
impl<O: State> RunTask<O> for Result<Worker<O, Ready>> {
    async fn run_task(self, task: Task) -> Result<(O, WorkflowStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static,
    {
        let worker = self?;
        worker.run_task(task).await
    }
}

/// Utility trait to allow chaining operations from a workflow created
/// by calling `initial_state`
pub trait FindWorkflow<O: State> {
    fn find_workflow(self, target: O::Target) -> Result<(Worker<O, Ready>, Option<Workflow>)>;
}

impl<O: State> FindWorkflow<O> for Result<Worker<O, Ready>> {
    fn find_workflow(self, target: O::Target) -> Result<(Worker<O, Ready>, Option<Workflow>)> {
        let worker = self?;
        let workflow = worker.find_workflow(target)?;
        Ok((worker, workflow))
    }
}

// -- Worker is ready to receive a target state

impl<O: State> Worker<O, Ready> {
    /// Read the current system state from the worker
    ///
    /// # Errors
    ///
    /// The method will throw an [`Error`] of kind [`ErrorKind::Serialization`] if the
    /// internal state cannot be deserialized into the output type `<O>`
    pub fn state(&self) -> Result<O>
    where
        O: DeserializeOwned,
    {
        self.inner.system.state()
    }

    /// Returns the distance from the current state to the target
    ///
    /// The distance is the differences between the current state and target
    /// in the form of a list of [Operations](`crate::json::Operation`).
    pub fn distance(&self, tgt: &O::Target) -> Result<Vec<Operation>> {
        let Ready { system, .. } = &self.inner;

        // the distance is calculated between target state types
        let ini = system
            .state::<O::Target>()
            .and_then(|s| serde_json::to_value(s).map_err(Error::from))?;
        let tgt = serde_json::to_value(tgt)?;

        let Patch(changes) = json_patch::diff(&ini, &tgt);

        Ok(changes.into_iter().map(Operation::from).collect())
    }

    /// Monitor the state of the system via sensors
    ///
    /// This will terminate immediately if there are no sensors defined given the current
    /// state condition
    pub fn listen(self) -> impl Stream<Item = Result<O>>
    where
        O: DeserializeOwned + Send + Unpin + 'static,
    {
        async_stream::try_stream! {
            // Setup state
            let system = self.inner.system.clone();
            let sensor_router = self.inner.sensor_router;
            let state = StreamState::new(system, sensor_router)?;

            let mut sensor_stream = pin!(listen_stream(state, Interrupt::new()));
            while let Some(local_state) = sensor_stream.next().await {
                yield local_state?;
            }
        }
    }

    /// Find a workflow to reach a target state
    ///
    /// Returns a workflow that can be executed with [`run_workflow`](Self::run_workflow).
    ///
    /// # Return values
    ///
    /// - `Ok(Some(workflow))` - A valid workflow was found
    /// - `Ok(None)` - No combination of registered jobs can reach the target state
    ///
    /// # Errors
    ///
    /// Returns an error if planning fails. In particular,
    /// [`ErrorKind::PlanningOverflow`](`crate::error::ErrorKind::PlanningOverflow`) indicates
    /// the planner exceeded the maximum search depth, typically caused by an underspecified job
    /// that doesn't make progress toward the target.
    ///
    /// See [`ErrorKind`](`crate::error::ErrorKind`) for other possible error conditions.
    pub fn find_workflow(&self, tgt: O::Target) -> Result<Option<Workflow>> {
        let tgt = serde_json::to_value(tgt).map_err(Error::from)?;

        let Ready {
            domain,
            system,
            worker_id,
            ..
        } = &self.inner;

        let mut workflow = planner::find_workflow_to_target::<O>(domain, system, &tgt)?;

        // Attach worker metadata
        if let Some(w) = workflow.as_mut() {
            w.worker_id = *worker_id;
        }

        Ok(workflow)
    }

    /// Execute a previously found workflow, returning a stream of events
    ///
    /// The returned stream yields [`WorkerEvent`] items:
    /// - [`WorkerEvent::StateUpdated`] after each task completes
    /// - [`WorkerEvent::WorkflowFinished`] when the workflow terminates.
    ///
    /// If the stream is dropped while the workflow is being executed, the workflow will be
    /// stopped.
    ///
    /// If any [sensors](`crate::sensor`) are defined for the worker, the stream will continue yielding
    /// values from sensors after the workflow completes. An interrupt while in this stage will
    /// just terminate the stream.
    ///
    /// # Panics
    ///
    /// This function will panic if the Workflow was created by a different Worker
    pub fn run_workflow(self, workflow: Workflow) -> impl Stream<Item = Result<WorkerEvent<O>>>
    where
        O: DeserializeOwned + Send + Unpin + 'static,
    {
        assert_eq!(
            workflow.worker_id, self.inner.worker_id,
            "workflow worker_id {} does not match current worker_id {}",
            workflow.worker_id, self.inner.worker_id,
        );

        workflow_stream(self, workflow)
    }

    /// Trigger system changes by providing a new target state and interrupt signal
    ///
    /// This is a convenience method that looks for a workflow for the given state and
    /// runs it waiting for the return status.
    ///
    /// It returns the updated state and the status after running the workflow.
    ///
    /// If no plan is found, the function returns [`SeekStatus::NotFound`].
    ///
    /// If any [sensors](`crate::sensor`) are defined for the worker, the stream will continue yielding
    /// values from sensors after the workflow completes. An interrupt while in this stage will
    /// just terminate the stream.
    ///
    /// If the future returned by the function is dropped while the workflow is being executed, the
    /// workflow will be interrupted.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mahler::state::State;
    /// use mahler::worker::{Worker, SeekStatus};
    ///
    /// #[derive(State, Debug, Clone)]
    /// struct MySystem;
    ///
    /// # async fn test_seek_target_example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut worker = Worker::new()
    ///     .initial_state(MySystem {})?;
    ///
    /// let (state, status) = (worker.seek_target(MySystemTarget {})).await?;
    /// assert_eq!(status, SeekStatus::Success);
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(skip_all)]
    pub async fn seek_target(self, tgt: O::Target) -> Result<(O, SeekStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static,
        O::Target: Serialize,
    {
        let mut state = self.state()?;

        info!("searching workflow");

        // Show pending changes at debug level
        if tracing::enabled!(tracing::Level::DEBUG) {
            let changes = self.distance(&tgt)?;
            if !changes.is_empty() {
                debug!("pending changes:");
                for change in changes {
                    debug!("- {}", change);
                }
            }
        }

        let now = Instant::now();
        let workflow = match self.find_workflow(tgt)? {
            Some(w) => w,
            None => {
                warn!(time = ?now.elapsed(), "workflow not found");
                return Ok((state, SeekStatus::NotFound));
            }
        };

        if workflow.is_empty() {
            debug!("nothing to do");
            return Ok((state, SeekStatus::Success));
        }
        info!(time = ?now.elapsed(), "workflow found");

        if tracing::enabled!(tracing::Level::WARN) && !workflow.exceptions().is_empty() {
            warn!("the following operations were ignored during planning");
            for path in workflow.exceptions() {
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

        let mut stream = std::pin::pin!(self.run_workflow(workflow));
        let mut final_status = SeekStatus::Success;

        while let Some(event) = stream.next().await {
            match event? {
                WorkerEvent::StateUpdated(new_state) => state = new_state,
                WorkerEvent::WorkflowFinished(status) => {
                    info!(time = ?now.elapsed(), "workflow executed successfully");
                    final_status = status.into();
                    break;
                }
            }
        }

        Ok((state, final_status))
    }

    /// Run a task within the context of the worker domain
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// use mahler::error::ErrorKind;
    /// use mahler::task::{Handler, IO, with_io, enforce};
    /// use mahler::extract::{View, Target};
    /// use mahler::worker::{Worker, Ready, WorkflowStatus, RunTask};
    /// use mahler::job::update;
    ///
    /// fn plus_one(mut counter: View<i32>, Target(target): Target<i32>) -> IO<i32> {
    ///     // modify the counter only if below the target
    ///     enforce!(*counter < target);
    ///     *counter += 1;
    ///
    ///     with_io(counter, |counter| async {
    ///         sleep(Duration::from_millis(10)).await;
    ///         Ok(counter)
    ///     })
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Setup the worker domain and resources
    /// let worker = Worker::new()
    ///     .job("", update(plus_one)).initial_state(0);
    ///
    /// // Run task emulating a target of 2 and initial state of 0
    /// let (state, status) = worker.run_task(plus_one.with_target(2)).await?;
    /// assert_eq!(status, WorkflowStatus::Success);
    /// assert_eq!(state, 1);
    ///
    /// // Run task emulating a target of 2 and initial state of 2 (condition not met)
    /// let worker = Worker::new()
    ///     .job("", update(plus_one)).initial_state(2);
    ///
    /// let err = worker.run_task(plus_one.with_target(2)).await.unwrap_err();
    /// assert_eq!(err.kind(), ErrorKind::ConditionNotMet);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_task(self, task: Task) -> Result<(O, WorkflowStatus)>
    where
        O: State + DeserializeOwned + Send + Unpin + 'static,
        O::Target: Serialize,
    {
        let mut state = self.state()?;
        let Ready {
            domain,
            system,
            worker_id,
            ..
        } = &self.inner;

        info!("searching workflow");
        let now = Instant::now();

        // look for a workflow for the task
        let mut workflow = match planner::find_workflow_for_task(task, domain, system) {
            Ok(w) => w,
            Err(e) => {
                warn!(time = ?now.elapsed(), "workflow not found");
                return Err(e);
            }
        };

        // set the worker id
        workflow.worker_id = *worker_id;

        if workflow.is_empty() {
            debug!("nothing to do");
            return Ok((state, WorkflowStatus::Success));
        }
        info!(time = ?now.elapsed(), "workflow found");

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!("will execute the following tasks:");
            for line in workflow.to_string().lines() {
                debug!("{line}");
            }
        }

        let now = Instant::now();
        info!("executing workflow");

        let mut stream = std::pin::pin!(self.run_workflow(workflow));
        let mut final_status = WorkflowStatus::Success;

        while let Some(event) = stream.next().await {
            match event? {
                WorkerEvent::StateUpdated(new_state) => state = new_state,
                WorkerEvent::WorkflowFinished(status) => {
                    info!(time = ?now.elapsed(), "workflow executed successfully");
                    final_status = status;
                    break;
                }
            }
        }

        Ok((state, final_status))
    }
}

// -- Stream implementations for workflow execution

/// Shared state for workflow stream execution
struct StreamState {
    system_reader: Reader<System>,
    system_writer: Writer<System>,
    sensor_streams: StreamMap<PointerBuf, SensorStream>,
    sensor_router: SensorRouter,
}

impl StreamState {
    /// Create stream state from a cloned system
    fn new(system: System, sensor_router: SensorRouter) -> Result<Self> {
        // Create initial sensor subscriptions
        let mut sensor_streams = StreamMap::new();
        let mut all_paths = Vec::new();
        Self::collect_paths(system.inner_state(), Pointer::root(), &mut all_paths);
        for path in all_paths {
            Self::try_subscribe(&system, &path, &sensor_router, &mut sensor_streams)?;
        }

        let (system_reader, system_writer) = rw_lock(system);

        Ok(Self {
            system_reader,
            system_writer,
            sensor_streams,
            sensor_router,
        })
    }

    /// Helper function to extract all paths from a JSON value
    fn collect_paths(value: &Value, current_path: &Pointer, paths: &mut Vec<PointerBuf>) {
        match value {
            Value::Object(map) => {
                for (key, v) in map {
                    // create a new path and add it to the list
                    let mut path = current_path.to_buf();
                    path.push_back(key);
                    paths.push(path.clone());

                    Self::collect_paths(v, &path, paths);
                }
            }
            Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    // create a new path and add it to the list
                    let mut path = current_path.to_buf().clone();
                    path.push_back(i);
                    paths.push(path.clone());

                    Self::collect_paths(v, &path, paths);
                }
            }
            _ => {}
        }
    }

    /// Try to create a sensor subscription for a path if it matches any sensor route
    fn try_subscribe(
        system: &System,
        pointer: &Pointer,
        sensor_router: &SensorRouter,
        sensor_streams: &mut StreamMap<PointerBuf, SensorStream>,
    ) -> Result<()> {
        let path = pointer.to_buf();
        if sensor_streams.contains_key(&path) {
            return Ok(());
        }

        if let Some((args, sensor)) = sensor_router.at(pointer.as_str()) {
            // try create the stream using the path and args
            let context = Context::new().with_path(pointer).with_args(args);
            let stream = sensor.create_stream(system, &context)?;

            sensor_streams.insert(path, stream);
            trace!(path = %pointer, "sensor subscription created");
        }

        Ok(())
    }

    /// Update the state subscriptions based on the last patch
    fn update(&mut self, system: &System, last_patch: &Patch) -> Result<()> {
        let sensor_streams = &mut self.sensor_streams;
        let sensor_router = &self.sensor_router;

        // Remove subscriptions for deleted paths
        let paths_to_remove: Vec<PointerBuf> = sensor_streams
            .keys()
            .filter(|path| path.resolve(system.inner_state()).is_err())
            .cloned()
            .collect();

        for path in paths_to_remove {
            sensor_streams.remove(&path);
            trace!(path = %path, "sensor subscription removed");
        }

        // Create subscriptions for paths affected by the patch
        for patch_op in last_patch.0.iter() {
            // Ignored removed paths as those should have been taken care by
            // the previous step
            if matches!(patch_op, &PatchOperation::Remove { .. }) {
                continue;
            }
            let change_path = patch_op.path();

            if let Ok(value) = change_path.resolve(system.inner_state()) {
                let mut new_paths = Vec::new();
                Self::collect_paths(value, change_path, &mut new_paths);
                new_paths.push(change_path.to_buf());

                for path in new_paths {
                    StreamState::try_subscribe(system, &path, sensor_router, sensor_streams)?;
                }
            }
        }

        Ok(())
    }
}

/// Creates a workflow stream using async-stream
///
/// After workflow completion, the stream continues to listen for sensor updates
/// until interrupted or an error occurs.
fn workflow_stream<O>(
    worker: Worker<O, Ready>,
    workflow: Workflow,
) -> impl Stream<Item = Result<WorkerEvent<O>>>
where
    O: State + DeserializeOwned + Unpin + Send + 'static,
{
    enum WorkflowEvent {
        Patch(WithAck<Patch>),
        Sensor(PointerBuf, Result<Value>),
    }

    let current_span = Span::current();

    try_stream! {
        // Setup state
        let system = worker.inner.system.clone();
        let sensor_router = worker.inner.sensor_router.clone();
        let mut state = StreamState::new(system, sensor_router)?;

        // Create patch channel and spawn workflow task
        let (patch_tx, mut patch_rx) = channel::<Patch>(100);
        let system_reader = state.system_reader.clone();
        let interrupt = Interrupt::new();
        let workflow_interrupt = interrupt.clone();
        let workflow_handle = tokio::spawn(async move {
            workflow.execute(&system_reader, patch_tx, workflow_interrupt).await
        }.instrument(current_span));

        // make sure that returning from this function or
        // dropping the stream interrupts the workflow that is running in a separate
        // task
        let drop_interrupt = AutoInterrupt::from(interrupt);
        let interrupt = drop_interrupt.clone();

        // Phase 1: Executing loop - poll patches and sensors concurrently
        loop {
            let result = tokio::select! {
                patch_opt = patch_rx.recv() => match patch_opt {
                    Some(patch) => WorkflowEvent::Patch(patch),
                    // the worklow ended once all patch senders close
                    None => break
                },
                Some((path, result)) = state.sensor_streams.next() => WorkflowEvent::Sensor(path, result),
            };

            match result {
                WorkflowEvent::Patch(mut msg) => {
                    let patch = std::mem::take(&mut msg.data);
                    trace!(received=%patch);

                    // Apply patch to system
                    let (state_result, system_copy) = {
                        let mut system = state.system_writer.write().await;
                        if let Err(e) = system.patch(&patch) {
                            return Err(e)?;
                        } else {
                            let system_copy = system.clone();
                            (system.state::<O>(), system_copy)
                        }
                    };
                    trace!("patch successful");

                    // Update sensor subscriptions
                    let update_sensors_res = state.update(&system_copy, &patch);

                    // allow the workflow to resume once the patch succeeds
                    msg.ack();


                    // Update sensor subscriptions
                    if let Err(e) = update_sensors_res {
                        return Err(e)?;
                    } else {
                        match state_result {
                            Ok(new_state) => yield WorkerEvent::StateUpdated(new_state),
                            Err(e) => {
                                return Err(e)?;
                            }
                        }
                    }
                },
                WorkflowEvent::Sensor(path, result) => {
                    match result {
                        Ok(value) => {
                            let patch = Patch(vec![PatchOperation::Replace(ReplaceOperation {
                                path: path.clone(),
                                value,
                            })]);

                            let state_result = {
                                let mut system = state.system_writer.write().await;
                                // a failure to patch the state here probably means there is a
                                // conflict between sensors (i.e. two sensors writing to the same
                                // path)
                                if let Err(e) = system.patch(&patch).map_err(|e| Error::runtime(format!("sensor conflict: {e}"))) {
                                    return Err(e)?;
                                } else {
                                    system.state::<O>()
                                }
                            };

                            match state_result {
                                Ok(new_state) => yield WorkerEvent::StateUpdated(new_state),
                                Err(e) => {
                                    return Err(e)?;
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e)?;
                        }
                    }
                },
            }
        };

        // Phase 2: Handle workflow completion
        let workflow_status = workflow_handle.await;

        match workflow_status {
            Ok(Ok(InnerWorkflowStatus::Completed)) => {
                yield WorkerEvent::WorkflowFinished(WorkflowStatus::Success);
            }
            // this is unreachable, since the workflow is only interrupted by
            // dropping the stream, in which case the status will never be evaluated
            Ok(Ok(InnerWorkflowStatus::Interrupted)) => unreachable!(),
            Ok(Err(agg_err)) => {
                let AggregateError(all) = agg_err;
                let mut recoverable = Vec::new();
                let mut other = Vec::new();

                for e in all.into_iter() {
                    match e.kind() {
                        ErrorKind::Runtime | ErrorKind::ConditionNotMet => {
                            recoverable.push(e);
                        }
                        _ => other.push(e),
                    }
                }

                // Any other errors while running the workflow mean
                // there is a bug somewhere and we need to return an internal error
                if !other.is_empty() {
                    Err(Error::internal(AggregateError::from(other)))?;
                }

                yield WorkerEvent::WorkflowFinished(WorkflowStatus::Aborted(recoverable));
            }
            Err(e) => {
                Err(Error::internal(e))?;
            }
        }


        // Phase 3: Listening loop - continue sensor updates until interrupt
        let mut sensor_stream = pin!(listen_stream(state, interrupt));
        while let Some(res) = sensor_stream.next().await {
            match res {
                Ok(new_state) => yield WorkerEvent::StateUpdated(new_state),
                Err(e) => return Err(e)?,
            }
        }
    }
}

fn listen_stream<O>(mut state: StreamState, interrupt: Interrupt) -> impl Stream<Item = Result<O>>
where
    O: State + DeserializeOwned + Unpin + Send + 'static,
{
    async_stream::try_stream! {
        loop {
            let (path, result) = tokio::select! {
                biased;
                _ = interrupt.wait() => break,
                sensor_result = state.sensor_streams.next() => match sensor_result {
                    Some((path, result)) => (path, result),
                    _ => break
                },
            };

            match result {
                Ok(value) => {
                    let patch = Patch(vec![PatchOperation::Replace(ReplaceOperation {
                        path: path.clone(),
                        value,
                    })]);

                    let state_result = {
                        let mut system = state.system_writer.write().await;
                        if let Err(e) = system
                            .patch(&patch)
                            .map_err(|e| Error::runtime(format!("sensor conflict: {e}")))
                        {
                            return Err(e)?;
                        } else {
                            system.state::<O>()
                        }
                    };

                    match state_result {
                        Ok(new_state) => yield new_state,
                        Err(e) => {
                            return Err(e)?;
                        }
                    }
                }
                Err(e) => {
                    return Err(e)?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::extract::{Target, View};
    use crate::job::*;
    use crate::task::*;
    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])));

        let (state, status) = worker
            .seek_target(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .await
            .unwrap();

        assert_eq!(status, SeekStatus::Success);
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
        use std::pin::pin;

        init();
        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let workflow = worker
            .find_workflow(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .unwrap()
            .unwrap();

        // Run the workflow stream to completion
        let mut state = worker.state().unwrap();
        let mut status = WorkflowStatus::Success;
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(new_state) => state = new_state,
                    WorkerEvent::WorkflowFinished(last_status) => {
                        status = last_status;
                        break;
                    }
                }
            }
        }

        assert_eq!(status, WorkflowStatus::Success);
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    #[should_panic]
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
            .find_workflow(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .unwrap()
            .unwrap();

        // Create a different worker
        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        // This will panic
        let _ = worker.run_workflow(workflow);
    }

    #[tokio::test]
    async fn test_worker_bug() {
        init();
        let worker = Worker::new()
            .job("", update(buggy_plus_one))
            .initial_state(0)
            .unwrap();

        let err = worker.seek_target(2).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PlanningOverflow);
    }

    #[tokio::test]
    async fn test_stream_yields_state_updates() {
        use std::pin::pin;

        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(2).unwrap().unwrap();

        // Collect all state updates from the stream
        let mut results = Vec::new();
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(state) => {
                        results.push(state);
                    }
                    WorkerEvent::WorkflowFinished(status) => {
                        assert_eq!(status, WorkflowStatus::Success);
                        break;
                    }
                }
            }
        }

        assert_eq!(results, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_stream_with_no_sensors_closes_on_completion() {
        use std::pin::pin;

        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(1).unwrap().unwrap();
        let mut event_count = 0;
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                event_count += 1;
                if let WorkerEvent::WorkflowFinished(status) = event.unwrap() {
                    assert_eq!(status, WorkflowStatus::Success);
                }
            }
        }

        // Should have: StateUpdated, WorkflowComplete
        assert_eq!(event_count, 2);
    }

    #[tokio::test]
    async fn test_view_flush_propagates_to_stream() {
        use std::pin::pin;

        init();

        // Create a job that flushes intermediate changes
        fn plus_four(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32, Error> {
            let initial = *counter;
            if tgt - *counter >= 4 {
                *counter += 4;
            }

            with_io(counter, move |mut counter| async move {
                // reset counter
                *counter = initial;

                // Simulate a long-running task with multiple flushes
                for _ in 0..3 {
                    *counter += 1;
                    counter.flush().await?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                // one more change after progress reports
                *counter += 1;
                Ok(counter)
            })
        }

        let worker = Worker::new()
            .job("", update(plus_four))
            .initial_state(0)
            .unwrap();
        let workflow = worker.find_workflow(4).unwrap().unwrap();

        let mut results = Vec::new();
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(state) => {
                        results.push(state);
                    }
                    WorkerEvent::WorkflowFinished(status) => {
                        assert_eq!(status, WorkflowStatus::Success);
                        break;
                    }
                }
            }
        }

        // We should see intermediate states from flushes: 1, 2, 3, 4
        assert!(
            results.len() >= 4,
            "Expected at least 4 updates, got {}: {:?}",
            results.len(),
            &results
        );

        assert_eq!(results[0], 1);
        assert_eq!(results[1], 2);
        assert_eq!(results[2], 3);
        assert_eq!(results[3], 4);
    }

    #[tokio::test]
    async fn test_flushed_changes_rollback_on_error() {
        use std::pin::pin;

        init();

        // Create a job that flushes changes then fails
        fn failing_after_flush(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32, Error> {
            let initial = *counter;
            if tgt - *counter >= 4 {
                *counter += 4;
            }

            with_io(counter, move |mut counter| async move {
                // reset counter
                *counter = initial;

                // Simulate a long-running task with multiple flushes
                for _ in 0..3 {
                    *counter += 1;
                    counter.flush().await?;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                // Return an error before the final flush
                Err(Error::runtime("simulated error"))
            })
        }

        let worker = Worker::new()
            .job("", update(failing_after_flush))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(4).unwrap().unwrap();
        let mut results = Vec::new();
        let mut final_status = WorkflowStatus::Success;
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(state) => {
                        results.push(state);
                    }
                    WorkerEvent::WorkflowFinished(status) => {
                        final_status = status;
                        break;
                    }
                }
            }
        }

        assert!(matches!(final_status, WorkflowStatus::Aborted(_)));

        assert!(
            results.len() >= 4,
            "Expected at least 4 updates, got {}: {:?}",
            results.len(),
            &results
        );

        // We should see the intermediate results before the roll-back
        assert_eq!(results[0], 1);
        assert_eq!(results[1], 2);
        assert_eq!(results[2], 3);
        assert_eq!(results[3], 0); // rollback
    }

    // ==================== Sensor Tests ====================
    // Note: Sensor updates during workflow execution are delivered through the stream.
    // Sensors are only polled during active workflow execution.

    #[tokio::test]
    async fn test_sensor_updates_during_workflow() {
        use std::pin::pin;

        init();

        // Simple state with just temperature
        #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
        struct TempState {
            temperature: i32,
            target_val: i32,
        }

        impl State for TempState {
            type Target = Self;
        }

        fn update_target(
            mut state: View<TempState>,
            Target(tgt): Target<TempState>,
        ) -> IO<TempState> {
            if state.target_val != tgt.target_val {
                state.target_val = tgt.target_val;
            }

            with_io(state, |state| async move {
                // Sleep to give sensor time to emit updates
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(state)
            })
        }

        let worker: Worker<TempState, _> = Worker::new()
            .job("", update(update_target))
            .sensor("/temperature", || {
                async_stream::stream! {
                    for temp in [25, 26, 27] {
                        yield temp;
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    }
                }
            })
            .initial_state(TempState {
                temperature: 20,
                target_val: 0,
            })
            .unwrap();

        let workflow = worker
            .find_workflow(TempState {
                temperature: 20,
                target_val: 1,
            })
            .unwrap()
            .unwrap();
        // Collect temperature updates during workflow execution
        let mut temp_updates = Vec::new();
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(state) => {
                        temp_updates.push(state.temperature);
                    }
                    WorkerEvent::WorkflowFinished(_) => {
                        break;
                    }
                }
            }
        }

        // We should see sensor updates interleaved with workflow execution
        assert!(
            !temp_updates.is_empty(),
            "Expected some temperature updates, got none"
        );
    }

    #[tokio::test]
    async fn test_sensor_initial_subscription_for_existing_state() {
        use std::pin::pin;

        init();

        // Tests that sensors are created for paths that exist at startup
        #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
        struct InnerClimate {
            temperature: i32,
            humidity: i32,
            target: i32,
        }

        impl State for InnerClimate {
            type Target = Self;
        }

        fn update_target(
            mut state: View<InnerClimate>,
            Target(tgt): Target<InnerClimate>,
        ) -> IO<InnerClimate> {
            if state.target != tgt.target {
                state.target = tgt.target;
            }

            with_io(state, |state| async move {
                // Sleep to give sensors time to emit
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(state)
            })
        }

        // Track which sensors were created
        let created = Arc::new(tokio::sync::RwLock::new(Vec::new()));

        let created_temp = Arc::clone(&created);
        let temp_sensor = move || {
            let created = Arc::clone(&created_temp);
            async_stream::stream! {
                {
                    let mut c = created.write().await;
                    c.push("temperature".to_string());
                }
                yield 25;
            }
        };

        let created_humid = Arc::clone(&created);
        let humidity_sensor = move || {
            let created = Arc::clone(&created_humid);
            async_stream::stream! {
                {
                    let mut c = created.write().await;
                    c.push("humidity".to_string());
                }
                yield 60;
            }
        };

        let worker: Worker<InnerClimate, _> = Worker::new()
            .job("", update(update_target))
            .sensor("/temperature", temp_sensor)
            .sensor("/humidity", humidity_sensor)
            .initial_state(InnerClimate {
                temperature: 20,
                humidity: 50,
                target: 0,
            })
            .unwrap();

        let workflow = worker
            .find_workflow(InnerClimate {
                temperature: 20,
                humidity: 50,
                target: 1,
            })
            .unwrap()
            .unwrap();

        // Trigger a workflow to activate sensors
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                if matches!(event.unwrap(), WorkerEvent::WorkflowFinished { .. }) {
                    break;
                }
            }
        }

        // Check that sensors were created
        let created = created.read().await;
        assert!(
            created.contains(&"temperature".to_string()),
            "temperature sensor not created"
        );
        assert!(
            created.contains(&"humidity".to_string()),
            "humidity sensor not created"
        );
    }

    #[tokio::test]
    async fn test_stream_continues_reporting_sensors_after_workflow_complete() {
        use std::pin::pin;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        init();

        #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
        struct SensorState {
            value: i32,
            target_val: i32,
        }

        impl State for SensorState {
            type Target = Self;
        }

        fn update_target(
            mut state: View<SensorState>,
            Target(tgt): Target<SensorState>,
        ) -> View<SensorState> {
            if state.target_val != tgt.target_val {
                state.target_val = tgt.target_val;
            }
            state
        }

        // Sensor that emits values continuously
        let sensor_emit_count = Arc::new(AtomicUsize::new(0));
        let sensor_emit_count_clone = Arc::clone(&sensor_emit_count);

        let worker: Worker<SensorState, _> = Worker::new()
            .job("", update(update_target))
            .sensor("/value", move || {
                let count = Arc::clone(&sensor_emit_count_clone);
                async_stream::stream! {
                    for i in 1..=5 {
                        count.fetch_add(1, AtomicOrdering::SeqCst);
                        yield i * 10;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            })
            .initial_state(SensorState {
                value: 0,
                target_val: 0,
            })
            .unwrap();

        let interrupt = Interrupt::new();
        let interrupt_clone = interrupt.clone();

        // Collect all state updates
        let mut updates_before_complete = Vec::new();
        let mut updates_after_complete = Vec::new();
        let mut workflow_completed = false;

        let workflow = worker
            .find_workflow(SensorState {
                value: 0,
                target_val: 1,
            })
            .unwrap()
            .unwrap();
        {
            let mut stream = pin!(worker.run_workflow(workflow));

            // Collect updates, continuing after WorkflowComplete
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(state) => {
                        if workflow_completed {
                            updates_after_complete.push(state.value);
                        } else {
                            updates_before_complete.push(state.value);
                        }
                    }
                    WorkerEvent::WorkflowFinished(status) => {
                        assert_eq!(status, WorkflowStatus::Success);
                        workflow_completed = true;
                        // Continue polling to get more sensor updates
                        // Set interrupt after getting a few more updates
                        let interrupt = interrupt_clone.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(80)).await;
                            interrupt.trigger();
                        });
                    }
                }
            }
        }

        assert!(workflow_completed, "Workflow should have completed");
        assert!(
            !updates_after_complete.is_empty(),
            "Expected sensor updates after workflow completion, got none. \
             Before: {:?}, After: {:?}",
            updates_before_complete,
            updates_after_complete
        );

        // Verify sensor continued emitting after workflow completed
        let total_emits = sensor_emit_count.load(AtomicOrdering::SeqCst);
        assert!(
            total_emits >= 2,
            "Expected sensor to emit multiple times, got {}",
            total_emits
        );
    }

    #[tokio::test]
    async fn test_workflow_interrupted_when_stream_dropped() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

        init();

        #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
        struct SlowState {
            value: i32,
        }

        impl State for SlowState {
            type Target = Self;
        }

        // Track how many tasks actually completed their IO
        let tasks_completed = Arc::new(AtomicUsize::new(0));
        let tasks_started = Arc::new(AtomicUsize::new(0));
        let workflow_was_interrupted = Arc::new(AtomicBool::new(false));

        let tasks_completed_clone = Arc::clone(&tasks_completed);
        let tasks_started_clone = Arc::clone(&tasks_started);

        fn slow_increment(
            mut state: View<SlowState>,
            Target(tgt): Target<SlowState>,
            tasks_started: Arc<AtomicUsize>,
            tasks_completed: Arc<AtomicUsize>,
        ) -> IO<SlowState> {
            if state.value < tgt.value {
                state.value += 1;
            }

            with_io(state, move |state| {
                let started = tasks_started;
                let completed = tasks_completed;
                async move {
                    started.fetch_add(1, AtomicOrdering::SeqCst);
                    // Long sleep to simulate slow IO
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    completed.fetch_add(1, AtomicOrdering::SeqCst);
                    Ok(state)
                }
            })
        }

        let worker: Worker<SlowState, _> = Worker::new()
            .job(
                "",
                update({
                    let started = Arc::clone(&tasks_started_clone);
                    let completed = Arc::clone(&tasks_completed_clone);
                    move |state: View<SlowState>, tgt: Target<SlowState>| {
                        slow_increment(state, tgt, Arc::clone(&started), Arc::clone(&completed))
                    }
                }),
            )
            .initial_state(SlowState { value: 0 })
            .unwrap();

        let workflow = worker
            .find_workflow(SlowState { value: 5 })
            .unwrap()
            .unwrap();
        // Start seeking a target that requires multiple slow tasks
        let workflow_was_interrupted_clone = Arc::clone(&workflow_was_interrupted);
        {
            use std::pin::pin;

            let mut stream = pin!(worker.run_workflow(workflow));

            // Wait for at least one task to start
            while tasks_started.load(AtomicOrdering::SeqCst) == 0 {
                // Poll the stream to start the workflow
                if let Some(event) = stream.next().await {
                    if let WorkerEvent::WorkflowFinished(status) = event.unwrap() {
                        if status == WorkflowStatus::Success {
                            workflow_was_interrupted_clone.store(true, AtomicOrdering::SeqCst);
                        }
                        break;
                    }
                }
            }

            // Drop the stream while tasks are still running
            // (stream goes out of scope here)
        }

        // Give some time for any in-flight tasks to potentially complete
        tokio::time::sleep(Duration::from_millis(50)).await;

        let started = tasks_started.load(AtomicOrdering::SeqCst);
        let completed = tasks_completed.load(AtomicOrdering::SeqCst);

        // The workflow should have been interrupted - not all 5 tasks should complete
        assert!(
            completed < 5,
            "Expected workflow to be interrupted before all tasks completed. \
             Started: {}, Completed: {}",
            started,
            completed
        );
    }

    fn plus_two(counter: View<i32>, tgt: Target<i32>) -> Vec<Task> {
        if *tgt - *counter < 2 {
            // Returning an empty result tells the planner
            // the task is not applicable to reach the target
            return vec![];
        }

        vec![plus_one.with_target(*tgt), plus_one.with_target(*tgt)]
    }

    #[tokio::test]
    async fn it_allows_running_atomic_tasks() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        impl State for Counters {
            type Target = Self;
        }

        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 1),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let task = plus_one.with_target(3).with_arg("counter", "one");
        let (state, status) = worker.run_task(task).await.unwrap();

        assert_eq!(status, WorkflowStatus::Success);
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    async fn it_allows_testing_compound_tasks() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        impl State for Counters {
            type Target = Self;
        }

        let worker: Worker<Counters, _> = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let task = plus_two.with_target(3).with_arg("counter", "one");
        let (state, status) = worker.run_task(task).await.unwrap();

        assert_eq!(status, WorkflowStatus::Success);
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }
}
