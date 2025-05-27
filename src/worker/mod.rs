//! Worker module for orchestrating system workflows toward a target state.
//!
//! This module provides a `Worker` abstraction that manages the lifecycle of reaching a desired state
//! through workflows, handling retries, failures, cancellations, and live state tracking.

use anyhow::anyhow;
use async_trait::async_trait;
use json_patch::Patch;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tokio::{select, sync::RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error, field, instrument, span, Instrument, Level, Span};

#[cfg(feature = "logging")]
mod logging;

#[cfg(debug_assertions)]
mod testing;

#[cfg(debug_assertions)]
pub use testing::*;

#[cfg(feature = "logging")]
pub use logging::init as init_logging;

use crate::errors::{IOError, InternalError, SerializationError};
use crate::planner::{Domain, Error as PlannerError, Planner};
use crate::system::{Resources, System};
use crate::task::{Error as TaskError, Job};
use crate::workflow::{channel, AggregateError, Interrupt, Sender, WorkflowStatus};

pub mod prelude {
    pub use super::SeekTarget;
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Panicked(#[from] tokio::task::JoinError);

#[derive(Debug, Error)]
pub enum FatalError {
    #[error(transparent)]
    /// An eror happened while serializing/deserializing
    /// the worker input types
    Serialization(#[from] SerializationError),

    #[error(transparent)]
    /// A panic happened within the worker execution, this
    /// most likely mean there is an uncaught error in a task
    Panic(#[from] Panicked),

    #[error(transparent)]
    /// An error happened with a task during planning. This most
    /// likely mean there is a bug in a task. This error will
    /// only be returned if debug_assertions are set. Otherwise
    /// task errors are ignored by the planner
    Planning(#[from] TaskError),

    #[error(transparent)]
    /// An internal error occured during the worker operation
    /// this is probably a bug
    Internal(#[from] InternalError),
}

#[derive(Debug)]
pub enum SeekStatus {
    /// Worker has reached the target state
    Success,
    /// Workflow not found
    NotFound,
    /// Worker interrupted by user request
    Interrupted,
    /// Worker execution was terminated due to some unexpected
    /// error during runtime
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

#[async_trait]
pub trait SeekTarget<O, I = O> {
    async fn seek_target(self, tgt: I) -> Result<Worker<O, Idle, I>, FatalError>;
}

impl Eq for SeekStatus {}

#[derive(Default)]
struct AutoInterrupt(Interrupt);

impl Drop for AutoInterrupt {
    fn drop(&mut self) {
        // Set the interrupt flag to true when the worker is dropped
        self.0.set();
    }
}

pub trait WorkerState {}

pub struct Uninitialized {
    domain: Domain,
    resources: Resources,
}

pub struct Ready {
    planner: Planner,
    system: Arc<RwLock<System>>,
    updates: broadcast::Sender<UpdateEvent>,
    patches: Sender<Patch>,
    writer_closed: Arc<Notify>,
    interrupt: AutoInterrupt,
}

#[derive(Debug, Clone)]
struct UpdateEvent;

pub struct Idle {
    planner: Planner,
    system: Arc<RwLock<System>>,
    updates: broadcast::Sender<UpdateEvent>,
    patches: Sender<Patch>,
    writer_closed: Arc<Notify>,
    status: SeekStatus,
}

pub struct Stopped {}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}
impl WorkerState for Idle {}
impl WorkerState for Stopped {}

pub struct Worker<O, S: WorkerState = Uninitialized, I = O> {
    inner: S,
    _output: std::marker::PhantomData<O>,
    _input: std::marker::PhantomData<I>,
}

impl<O, S: WorkerState, I> Worker<O, S, I> {
    fn from_inner(inner: S) -> Self {
        Worker {
            inner,
            _output: std::marker::PhantomData,
            _input: std::marker::PhantomData,
        }
    }
}

// -- Worker initialization

impl<O> Default for Worker<O, Uninitialized> {
    fn default() -> Self {
        Worker::new()
    }
}

impl<O> Worker<O, Uninitialized> {
    pub fn new() -> Self {
        Worker::from_inner(Uninitialized {
            domain: Domain::new(),
            resources: Resources::new(),
        })
    }
}

impl<O> Worker<O, Uninitialized> {
    /// Add a job to the worker domain
    pub fn job(mut self, route: &'static str, job: Job) -> Self {
        self.inner.domain = self.inner.domain.job(route, job);
        self
    }

    /// Add a list if jobs linked to a route on the worker domain
    pub fn jobs<const N: usize>(mut self, route: &'static str, list: [Job; N]) -> Self {
        self.inner.domain = self.inner.domain.jobs(route, list);
        self
    }

    /// Add a shared resource to use within tasks
    pub fn resource<R>(mut self, res: R) -> Self
    where
        R: Send + Sync + 'static,
    {
        self.inner.resources = self.inner.resources.with_res(res);
        self
    }

    /// Provide the initial worker state
    ///
    /// This moves the state of the worker to `ready`
    pub fn initial_state<I>(self, state: O) -> Result<Worker<O, Ready, I>, SerializationError>
    where
        O: Serialize,
    {
        let Uninitialized {
            domain,
            resources: env,
            ..
        } = self.inner;

        let system = System::try_from(state).map(|s| s.with_resources(env))?;

        // Shared system protected by RwLock
        let system = Arc::new(RwLock::new(system));

        // Create the messaging channel
        let (tx, mut rx) = channel::<Patch>(100);

        // Patch error signal (notify)
        let notify = Arc::new(Notify::new());
        let writer_closed = notify.clone();

        // Broadcast channel for state updates
        let (updates, _) = broadcast::channel(1);

        // Spawn system writer task
        {
            let sys_writer = Arc::clone(&system);
            let broadcast = updates.clone();
            tokio::spawn(
                async move {
                    while let Some(mut msg) = rx.recv().await {
                        let changes = std::mem::take(&mut msg.data);
                        debug!("received changes: {:?}", changes);

                        let mut system = sys_writer.write().await;
                        if let Err(e) = system.patch(changes) {
                            // we need to abort on patch failure a this means the
                            // internal state may have become inconsistent and we cannot continue
                            // applying changes
                            error!("system patch failed: {e}");
                            notify.notify_one();
                            break;
                        }

                        // Notify the change over the broadcast channel
                        let _ = broadcast.send(UpdateEvent);

                        // yield back to the workflow
                        msg.ack();
                    }
                }
                .instrument(span!(Level::DEBUG, "system_writer")),
            );
        }

        Ok(Worker::from_inner(Ready {
            planner: Planner::new(domain),
            system,
            updates,
            patches: tx,
            writer_closed,
            interrupt: AutoInterrupt::default(),
        }))
    }
}

// -- Worker is ready to receive a target state

pub struct FollowStream<T> {
    inner: Pin<Box<dyn Stream<Item = T> + Send + 'static>>,
}

impl<T> FollowStream<T> {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = T> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> Stream for FollowStream<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn follow_worker<T>(
    updates: broadcast::Sender<UpdateEvent>,
    syslock: Arc<RwLock<System>>,
) -> FollowStream<T>
where
    T: DeserializeOwned,
{
    let rx = updates.subscribe();
    FollowStream::new(
        BroadcastStream::new(rx)
            .then(move |res| {
                let sys_reader = Arc::clone(&syslock);
                async move {
                    if res.is_err() {
                        return None;
                    }
                    // Read the system state
                    let system = sys_reader.read().await;
                    system.state::<T>().ok()
                }
            })
            .filter_map(|opt| opt),
    )
}

impl<O, I> Worker<O, Ready, I> {
    /// Stop following system updates
    pub fn stop(self) -> Worker<O, Stopped, I> {
        Worker::from_inner(Stopped {})
    }

    /// Read the current system state from the worker
    pub async fn state(&self) -> Result<O, SerializationError>
    where
        O: DeserializeOwned,
    {
        let system = self.inner.system.read().await;
        let state = system.state()?;
        Ok(state)
    }

    /// Read the current system state from the worker
    /// using the target type to modify and re-insert
    /// into the worker
    pub async fn state_as_target(&self) -> Result<I, SerializationError>
    where
        I: DeserializeOwned,
    {
        let system = self.inner.system.read().await;
        let state = system.state()?;
        Ok(state)
    }

    /// Returns a stream of updated states after each system change.
    ///
    /// Best effort: updates may be missed if the receiver lags behind.
    /// Fetches the current system state at the time of notification.
    pub fn follow(&self) -> FollowStream<O>
    where
        O: DeserializeOwned,
    {
        follow_worker(self.inner.updates.clone(), Arc::clone(&self.inner.system))
    }

    #[instrument(skip_all, fields(return=field::Empty), err)]
    pub async fn seek_target(self, tgt: I) -> Result<Worker<O, Idle, I>, FatalError>
    where
        I: Serialize + DeserializeOwned,
    {
        let cur_span = Span::current();
        let tgt = serde_json::to_value(tgt).map_err(SerializationError::from)?;

        let Ready {
            planner,
            system,
            updates,
            writer_closed,
            patches,
            interrupt,
            ..
        } = self.inner;

        enum SeekResult {
            TargetReached,
            WorkflowCompleted,
            Interrupted,
        }

        enum SeekError {
            Runtime(AggregateError<TaskError>),
            Planning(PlannerError),
        }

        async fn find_and_run_workflow<I: Serialize + DeserializeOwned>(
            planner: &Planner,
            sys: &Arc<RwLock<System>>,
            tgt: &Value,
            channel: &Sender<Patch>,
            sigint: &Interrupt,
        ) -> Result<SeekResult, SeekError> {
            let workflow = {
                let system = sys.read().await;
                planner
                    .find_workflow::<I>(&system, tgt)
                    .map_err(SeekError::Planning)?
            };

            if workflow.is_empty() {
                return Ok(SeekResult::TargetReached);
            }

            let status = workflow
                .execute(sys, channel.clone(), sigint.clone())
                .await
                .map_err(SeekError::Runtime)?;

            if matches!(status, WorkflowStatus::Interrupted) {
                return Ok(SeekResult::Interrupted);
            }

            Ok(SeekResult::WorkflowCompleted)
        }

        let err_rx = writer_closed.clone();

        // Main seek_target planning and execution loop
        let handle: JoinHandle<Result<(Planner, SeekStatus), FatalError>> = {
            let task_int = interrupt.0.clone();
            let workflow_int = task_int.clone();
            let sys_reader = Arc::clone(&system);
            let changes = patches.clone();
            tokio::spawn(async move {
                loop {
                    select! {
                        biased;

                        _ = err_rx.notified() => {
                            return Err(InternalError::from(anyhow!("state patch failed, worker state possibly tainted")))?;
                        }

                        _ = workflow_int.wait() => {
                            cur_span.record("return", "interrupted");
                            return Ok((planner, SeekStatus::Interrupted));
                        }

                        res = find_and_run_workflow::<I>(&planner, &sys_reader, &tgt, &changes, &task_int) => {
                            match res {
                                Ok(SeekResult::TargetReached) => {
                                    cur_span.record("return", "success");
                                    return Ok((planner, SeekStatus::Success));
                                }
                                Ok(SeekResult::WorkflowCompleted) => {}
                                Ok(SeekResult::Interrupted) => {
                                    cur_span.record("return", "interrupted");
                                    return Ok((planner, SeekStatus::Interrupted));
                                }
                                Err(SeekError::Planning(PlannerError::NotFound)) =>  return Ok((planner, SeekStatus::NotFound)),
                                Err(SeekError::Planning(PlannerError::Serialization(e))) =>  return Err(e)?,
                                Err(SeekError::Planning(PlannerError::Internal(e))) =>  return Err(e)?,
                                Err(SeekError::Planning(PlannerError::Task(e))) => return Err(e)?,
                                Err(SeekError::Runtime(err)) => {
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
                                        cur_span.record("return", "aborted");
                                        return Ok((planner, SeekStatus::Aborted(io)));
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
            })
        };

        let (planner, status) = match handle.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(Panicked(e))?,
        }?;

        // autointerupt isc
        Ok(Worker::from_inner(Idle {
            planner,
            system,
            updates,
            patches,
            writer_closed,
            status,
        }))
    }
}

#[async_trait]
impl<O: Send, I: Serialize + DeserializeOwned + Send> SeekTarget<O, I>
    for Result<Worker<O, Ready, I>, SerializationError>
{
    async fn seek_target(self, tgt: I) -> Result<Worker<O, Idle, I>, FatalError> {
        let worker = self?;
        worker.seek_target(tgt).await
    }
}

// -- Worker is idle seek target finished

impl<O, I> Worker<O, Idle, I> {
    /// Stop following system updates
    pub fn stop(self) -> Worker<O, Stopped> {
        Worker::from_inner(Stopped {})
    }

    /// Returns a stream of updated states after each system change.
    ///
    /// Best effort: updates may be missed if the receiver lags behind.
    /// Fetches the current system state at the time of notification.
    pub fn follow(&self) -> FollowStream<O>
    where
        O: DeserializeOwned,
    {
        follow_worker(self.inner.updates.clone(), Arc::clone(&self.inner.system))
    }

    /// Read the current system state from the worker
    pub async fn state(&self) -> Result<O, SerializationError>
    where
        O: DeserializeOwned,
    {
        let system = self.inner.system.read().await;
        let state = system.state()?;
        Ok(state)
    }

    /// Read the current system state from the worker
    /// using the target type to modify and re-insert
    /// into the worker
    pub async fn state_as_target(&self) -> Result<I, SerializationError>
    where
        I: DeserializeOwned,
    {
        let system = self.inner.system.read().await;
        let state = system.state()?;
        Ok(state)
    }

    /// Return the result of the last worker run
    pub fn status(&self) -> &SeekStatus {
        &self.inner.status
    }

    pub async fn seek_target(self, tgt: I) -> Result<Worker<O, Idle, I>, FatalError>
    where
        I: Serialize + DeserializeOwned,
    {
        let Idle {
            planner,
            system,
            updates,
            writer_closed,
            patches,
            ..
        } = self.inner;

        Worker::from_inner(Ready {
            planner,
            system,
            updates,
            patches,
            writer_closed,
            interrupt: AutoInterrupt::default(),
        })
        .seek_target(tgt)
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::extract::{Target, View};
    use crate::task::*;
    use serde::Deserialize;
    use tokio::time::{sleep, timeout};
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Counters(HashMap<String, i32>);

    fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> Effect<View<i32>> {
        if *counter < tgt {
            // Modify the counter if we are below target
            *counter += 1;
        }

        // Return the updated counter. The I/O part of the
        // effect will only be called if the job is chosen
        // in the workflow which will only happens if there are
        // changes
        Effect::of(counter).with_io(|counter| async {
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
            ])))
            .seek_target(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ])))
            .await
            .unwrap();

        assert_eq!(worker.status(), &SeekStatus::Success);
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
        let res = Worker::new()
            .job("", update(buggy_plus_one))
            .initial_state(0)
            .seek_target(2)
            .await
            .unwrap();

        assert!(matches!(res.status(), &SeekStatus::NotFound))
    }

    #[tokio::test]
    async fn test_worker_follow_updates() {
        init();
        let worker = Worker::new()
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
        let worker = worker.seek_target(2).await.unwrap();
        assert_eq!(worker.status(), &SeekStatus::Success);

        let results = results.read().await;
        assert_eq!(*results, vec![Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn test_worker_follow_best_effort_loss() {
        init();
        let worker = Worker::new()
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
        let worker = worker.seek_target(100).await.unwrap();
        assert_eq!(worker.status(), &SeekStatus::Success);

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

        fn sleepy_plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> Effect<View<i32>> {
            if *counter < tgt {
                // Modify the counter if we are below target
                *counter += 1;
            }

            // Return the updated counter. The I/O part of the
            // effect will only be called if the job is chosen
            // in the workflow which will only happens if there are
            // changes
            Effect::of(counter).with_io(|counter| async {
                sleep(Duration::from_millis(10)).await;
                Ok(counter)
            })
        }

        let worker = Worker::new()
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
        let worker = Worker::new()
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

        let worker = worker.seek_target(1).await.unwrap();

        // Wait for worker to finish
        assert_eq!(worker.status(), &SeekStatus::Success);

        // Close the stream
        worker.stop();

        let results = results.read().await;
        assert_eq!(*results, vec![Some(1), None]);
    }
}
