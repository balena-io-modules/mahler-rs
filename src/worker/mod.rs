//! Worker module for orchestrating system workflows toward a target state.
//!
//! This module provides a `Worker` abstraction that manages the lifecycle of reaching a desired state
//! through workflows, handling retries, failures, cancellations, and live state tracking.

use anyhow::{anyhow, Context as AnyhowCtx};
use json_patch::Patch;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use thiserror::Error;
use tokio::{select, sync::RwLock};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tracing::{debug, error, field, span, Instrument, Level, Span};

#[cfg(feature = "logging")]
mod logging;

#[cfg(feature = "logging")]
pub use logging::init as init_logging;

use crate::system::{Resources, SerializationError, System};
use crate::workflow::WorkflowStatus;
use crate::{
    ack_channel::ack_channel,
    planner::{Domain, Error as PlannerError, Planner},
};
use crate::{ack_channel::Sender, task::Job};

pub mod prelude {
    pub use super::SeekTarget;
}

/// Worker configuration options
#[derive(Clone, Debug)]
struct WorkerOpts {
    /// The maximum number of attempts to reach the target before giving up.
    /// Defauts to infinite tries (0).
    max_retries: u32,
    /// The minimal time to wait between re-plan. Defaults to 1 second
    min_wait_ms: u64,
    /// The maximum time to wait between re-plan. Defaults to 5 minutes
    max_wait_ms: u64,
}

#[derive(Debug, Error)]
#[error("unexpected error: {0}")]
pub struct UnexpectedError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error("fatal error: {0}")]
pub struct FatalError(#[from] anyhow::Error);

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Unexpected(#[from] UnexpectedError),

    #[error(transparent)]
    Unrecoverable(#[from] FatalError),
}

#[derive(Debug)]
pub enum Status {
    // Worker is still running
    Running,
    /// Worker has reached the target state
    Success,
    /// Worker failed to reach the target state after multiple tries
    Failure,
    /// Worker interrupted by user request
    Interrupted,
    /// Worker execution terminated due to some unexpected error
    /// this should only happen during testing
    Aborted(UnexpectedError),
    // Fatal error happened, the worker cannot be recovered
    Dead(FatalError),
}

impl PartialEq for Status {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Status::Success, Status::Success)
                | (Status::Failure, Status::Failure)
                | (Status::Interrupted, Status::Interrupted)
        )
    }
}

pub trait SeekTarget<T> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error>;
}

impl Eq for Status {}

pub trait WorkerState {}

pub struct Uninitialized {
    domain: Domain,
    resources: Resources,
    opts: WorkerOpts,
}

pub struct Ready {
    planner: Planner,
    system: System,
    opts: WorkerOpts,
}

#[derive(Debug, Clone)]
struct UpdateEvent;

pub struct Running {
    opts: WorkerOpts,
    handle: Pin<Box<JoinHandle<(Planner, System, Status)>>>,
    updates: broadcast::Sender<UpdateEvent>,
    sys_reader: Arc<RwLock<System>>,
    interrupt: Arc<AtomicBool>,
}

pub struct Idle {
    planner: Planner,
    system: System,
    opts: WorkerOpts,
    status: Status,
}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}
impl WorkerState for Running {}
impl WorkerState for Idle {}

pub enum Waiting {
    Idle(Idle),
    Running(Running),
}

impl WorkerState for Waiting {}

impl Default for WorkerOpts {
    fn default() -> Self {
        WorkerOpts {
            max_retries: 0,
            min_wait_ms: 1000,
            max_wait_ms: 300_000,
        }
    }
}

impl<T, S: WorkerState> Worker<T, S> {
    fn from_inner(inner: S) -> Self {
        Worker {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

// -- Worker initialization

pub struct Worker<T, S: WorkerState = Uninitialized> {
    inner: S,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Default for Worker<T, Uninitialized> {
    fn default() -> Self {
        Worker::from_inner(Uninitialized {
            domain: Domain::new(),
            opts: WorkerOpts::default(),
            resources: Resources::new(),
        })
    }
}

impl<T> Worker<T, Uninitialized> {
    pub fn new() -> Self {
        Worker::default()
    }

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

    /// Set the maximum number of attempts to reach the target before giving up.
    /// Defaults to infinite tries (0).
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.inner.opts.max_retries = max_retries;
        self
    }

    /// Set the minimal time to wait between re-planning attempts.
    /// Defaults to 1 second
    pub fn with_min_wait_ms(mut self, min_wait_ms: u64) -> Self {
        self.inner.opts.min_wait_ms = min_wait_ms;
        self
    }

    /// Set the maximum time to wait between re-planning attempts.
    /// Defaults to 5 minutes
    pub fn with_max_wait_ms(mut self, max_wait_ms: u64) -> Self {
        self.inner.opts.max_wait_ms = max_wait_ms;
        self
    }

    /// Provide the initial worker state
    ///
    /// This moves the state of the worker to `ready`
    pub fn initial_state(self, state: T) -> Result<Worker<T, Ready>, UnexpectedError>
    where
        T: Serialize + DeserializeOwned,
    {
        let Uninitialized {
            domain,
            opts,
            resources: env,
            ..
        } = self.inner;

        let system = System::try_from(state)
            .map(|s| s.with_resources(env))
            .context("could not serialize initial state")?;
        Ok(Worker::from_inner(Ready {
            planner: Planner::new(domain),
            system,
            opts,
        }))
    }
}

// -- Worker is ready to receive a target state

impl<T> Worker<T, Ready> {
    /// Read the current system state from the worker
    pub fn state(&self) -> Result<T, UnexpectedError>
    where
        T: DeserializeOwned,
    {
        let state = self
            .inner
            .system
            .state()
            .context("could not deserialize state")?;
        Ok(state)
    }

    pub fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        let Ready {
            planner,
            system,
            opts,
            ..
        } = self.inner;

        let tgt = serde_json::to_value(tgt)
            .context("could not serialize target")
            .map_err(UnexpectedError::from)?;

        enum InternalError {
            Serialization(SerializationError),
            Runtime,
            Planning(PlannerError),
        }

        enum InternalResult {
            TargetReached,
            Completed,
            Interrupted,
        }

        async fn find_and_run_workflow<T: Serialize + DeserializeOwned>(
            planner: &Planner,
            sys: &Arc<RwLock<System>>,
            tgt: &Value,
            channel: &Sender<Patch>,
            sigint: &Arc<AtomicBool>,
        ) -> Result<InternalResult, InternalError> {
            let system = {
                let system = sys.read().await;

                // Fields in the type may be configured with skip_serialize,
                // meaning these fields should not be considered when comparing
                // the state with the target. Deserialiing and serialing the state
                // again allows the comparison to work properly
                // XXX: this may be a potential source of patching errors
                system
                    .state::<T>()
                    .and_then(System::try_from)
                    .map(|s| s.with_resources(system.resources().clone()))
                    .map_err(InternalError::Serialization)?
            };

            let workflow = planner
                .find_workflow(&system, tgt)
                .map_err(InternalError::Planning)?;

            if workflow.is_empty() {
                return Ok(InternalResult::TargetReached);
            }

            if matches!(
                workflow
                    .execute(sys, channel.clone(), sigint)
                    .await
                    .map_err(|_| InternalError::Runtime)?,
                WorkflowStatus::Interrupted
            ) {
                return Ok(InternalResult::Interrupted);
            }

            Ok(InternalResult::Completed)
        }

        // Shared system protected by RwLock
        let sys_reader = Arc::new(RwLock::new(system));

        // Create the messaging channel
        let (tx, mut rx) = ack_channel::<Patch>(100);

        // Error signal channel (one-shot)
        let (err_tx, mut err_rx) = tokio::sync::oneshot::channel::<FatalError>();

        // Broadcast channel for state updates
        let (updates, _) = tokio::sync::broadcast::channel::<UpdateEvent>(16);

        // Spawn system writer task
        {
            let sys_writer = Arc::clone(&sys_reader);
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
                            let _ = err_tx.send(FatalError(anyhow!(e)));
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

        // Cancellation flag for external interrupts
        //
        let interrupt = Arc::new(AtomicBool::new(false));
        let sigint = interrupt.clone();
        let target = tgt.clone();

        // Main seek_target planning and execution loop
        let handle = {
            let sys_reader = Arc::clone(&sys_reader);
            tokio::spawn(async move {
                let mut tries = 0;
                let mut wait_ms = opts.min_wait_ms;
                let cur_span = Span::current();

                loop {
                    select! {
                        biased;

                        maybe_err = &mut err_rx => {
                            if let Ok(err) = maybe_err {
                                cur_span.record("error", format!("failed to update worker internal state: {err}"));
                                cur_span.record("return", "aborted");
                                let system = sys_reader.read().await.clone();
                                return (planner, system, Status::Dead(err));
                            }
                        }

                        _ = async {}, if sigint.load(Ordering::Relaxed) => {
                            cur_span.record("return", "interrupted");
                            let system = sys_reader.read().await.clone();
                            return (planner, system, Status::Interrupted);
                        }

                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(wait_ms)) => {
                            match find_and_run_workflow::<T>(&planner, &sys_reader, &tgt, &tx, &sigint).await {
                                Ok(InternalResult::TargetReached) => {
                                    cur_span.record("return", "success");
                                    let system = sys_reader.read().await.clone();
                                    return (planner, system, Status::Success);
                                }
                                Ok(InternalResult::Completed) => {
                                    // made progress, reset backoff
                                    tries = 0;
                                    wait_ms = opts.min_wait_ms;
                                }
                                Ok(InternalResult::Interrupted) => {
                                    cur_span.record("return", "interrupted");
                                    let system = sys_reader.read().await.clone();
                                    return (planner, system, Status::Interrupted);
                                }
                                Err(InternalError::Serialization(err)) => {
                                    // There is an issue with the type definition
                                    // best to abort in this case
                                    cur_span.record("return", "aborted");
                                    let system = sys_reader.read().await.clone();
                                    return (planner, system, Status::Aborted(UnexpectedError(anyhow!(err))));
                                }
                                Err(InternalError::Planning(err)) => {
                                    // Abort if an unexpected error happens in planning while in
                                    // debug mode
                                    if !matches!(err, PlannerError::NotFound) && !cfg!(debug_assertions) {
                                        cur_span.record("return", "aborted");
                                        let system = sys_reader.read().await.clone();
                                        return (planner, system, Status::Aborted(UnexpectedError(anyhow!(err))));
                                    }
                                    tries += 1;

                                    // Terminate  the search if we have reached the maximum number
                                    // of retries
                                    if opts.max_retries != 0 && tries >= opts.max_retries {
                                        cur_span.record("return", "failure");
                                        let system = sys_reader.read().await.clone();
                                        return (planner, system, Status::Failure);
                                    }

                                    // Otherwise update the timeout
                                    wait_ms = std::cmp::min(wait_ms.saturating_mul(2), opts.max_wait_ms);
                                }
                                Err(_) => {
                                    tries += 1;
                                    wait_ms = std::cmp::min(wait_ms.saturating_mul(2), opts.max_wait_ms);
                                }
                            }
                        }
                    }
                }
            }
            .instrument(span!(Level::INFO, "seek_target", target = %target, return = field::Empty)))
        };

        let handle = Box::pin(handle);

        Ok(Worker::from_inner(Running {
            handle,
            interrupt,
            opts,
            sys_reader,
            updates,
        }))
    }
}

impl<T: Serialize + DeserializeOwned> SeekTarget<T> for Result<Worker<T, Ready>, UnexpectedError> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error> {
        self.map_err(Error::from)
            .and_then(|worker| worker.seek_target(tgt))
    }
}

// -- Worker is running

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

impl<T> Worker<T, Running> {
    /// Returns a stream of updated states after each system change.
    ///
    /// Best effort: updates may be missed if the receiver lags behind.
    /// Fetches the current system state at the time of notification.
    pub fn follow(&self) -> FollowStream<T>
    where
        T: DeserializeOwned,
    {
        let rx = self.inner.updates.subscribe();
        let syslock = Arc::clone(&self.inner.sys_reader);
        FollowStream::new(
            BroadcastStream::new(rx)
                .then(move |result| {
                    let sys_reader = Arc::clone(&syslock);
                    async move {
                        if result.is_err() {
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

    pub async fn cancel(self) -> Worker<T, Idle> {
        let Running {
            handle: task,
            interrupt,
            opts,
            ..
        } = self.inner;

        // Cancel the task
        interrupt.store(true, Ordering::Relaxed);

        // This should not happen
        let (planner, system, status) = task.await.expect("worker runtime panicked");
        Worker::from_inner(Idle {
            planner,
            system,
            opts,
            status,
        })
    }

    pub async fn wait(self, timeout: Option<std::time::Duration>) -> Worker<T, Waiting> {
        let Running {
            mut handle,
            opts,
            updates,
            sys_reader,
            interrupt,
        } = self.inner;

        match timeout {
            Some(duration) => {
                let mut sleeper = Box::pin(tokio::time::sleep(duration));

                select! {
                    res = handle.as_mut() => {
                        let (planner, system, status) = res.expect("worker runtime panicked");
                        Worker::from_inner(Waiting::Idle( Idle {
                            planner,
                            system,
                            opts,
                            status,
                        }))
                    }
                    _ = &mut sleeper => {
                        Worker::from_inner(Waiting::Running(Running {
                            handle,
                            opts,
                            updates,
                            sys_reader,
                            interrupt,
                        }))
                    }
                }
            }
            None => {
                let (planner, system, status) = handle.await.expect("worker runtime panicked");
                Worker::from_inner(Waiting::Idle(Idle {
                    planner,
                    system,
                    opts,
                    status,
                }))
            }
        }
    }
}

// -- Worker is idle after target state finished

impl<T> Worker<T, Idle> {
    /// Return the internal state of the worker
    pub fn state(&self) -> Result<T, UnexpectedError>
    where
        T: DeserializeOwned,
    {
        let state = self
            .inner
            .system
            .state()
            .context("could not deserialize state")?;
        Ok(state)
    }

    /// Return the result of the last worker run
    pub fn status(&self) -> &Status {
        &self.inner.status
    }

    pub fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        // A dead worker may have an inconsistent state.
        // Do not allow seeking target in that case
        if let Status::Dead(err) = self.inner.status {
            return Err(err)?;
        }

        let Idle {
            planner,
            system,
            opts,
            ..
        } = self.inner;

        Worker::from_inner(Ready {
            planner,
            system,
            opts,
        })
        .seek_target(tgt)
    }
}

// -- Worker is waiting for the target search to finish

const RUNNING_STATUS: Status = Status::Running;

impl<T> Worker<T, Waiting> {
    /// Cancel the running worker
    ///
    /// It does nothing if the worker is already idle
    pub async fn cancel(self) -> Worker<T, Idle> {
        match self.inner {
            Waiting::Running(running) => Worker::from_inner(running).cancel().await,
            Waiting::Idle(idle) => Worker::from_inner(idle),
        }
    }

    /// Return the worker runtime status
    pub fn status(&self) -> &Status {
        match &self.inner {
            Waiting::Running(_) => &RUNNING_STATUS,
            Waiting::Idle(idle) => &idle.status,
        }
    }

    /// Wait for the worker to finish
    pub async fn wait(self, timeout: Option<std::time::Duration>) -> Worker<T, Waiting> {
        match self.inner {
            Waiting::Running(running) => Worker::from_inner(running).wait(timeout).await,
            Waiting::Idle(idle) => Worker::from_inner(Waiting::Idle(idle)),
        }
    }

    /// Return true if the worker is idle
    pub fn is_idle(&self) -> bool {
        matches!(self.inner, Waiting::Idle(_))
    }

    /// Return the worker if it is idle
    pub fn idle(self) -> Option<Worker<T, Idle>> {
        if let Waiting::Idle(idle) = self.inner {
            return Some(Worker::from_inner(idle));
        }
        None
    }

    /// Unwrap the worker into an Idle worker
    ///
    /// This function will panic if the worker is still running
    pub fn unwrap_idle(self) -> Worker<T, Idle> {
        self.idle().unwrap()
    }

    /// Return true if the worker is running
    pub fn is_running(&self) -> bool {
        matches!(self.inner, Waiting::Running(_))
    }

    /// Return the worker if it is running
    pub fn running(self) -> Option<Worker<T, Running>> {
        if let Waiting::Running(running) = self.inner {
            return Some(Worker::from_inner(running));
        }
        None
    }

    /// Unwrap the worker into a Running worker
    ///
    /// This function will panic if the worker is not running
    pub fn unwrap_running(self) -> Worker<T, Running> {
        self.running().unwrap()
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
    use tokio::time::sleep;
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
            .unwrap();

        let worker = worker.wait(None).await;
        assert_eq!(worker.status(), &Status::Success);
        let state = worker.unwrap_idle().state().unwrap();
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
        let worker = Worker::new()
            .job("", update(buggy_plus_one))
            .with_max_retries(1)
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let _ = worker.wait(None).await;
    }

    #[tokio::test]
    async fn test_worker_timeout() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let worker = worker.wait(Some(Duration::from_millis(1))).await;
        assert!(!worker.is_idle());
    }

    #[tokio::test]
    async fn test_worker_follow_updates() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let mut updates = worker.follow();

        // Capture two updates
        let first_update = updates.next().await;
        assert_eq!(first_update, Some(1));
        let second_update = updates.next().await;
        assert_eq!(second_update, Some(2));

        // Wait for worker to finish
        let worker = worker.wait(None).await;
        let final_state = worker.unwrap_idle().state().unwrap();
        assert_eq!(final_state, 2);
    }

    #[tokio::test]
    async fn test_worker_follow_best_effort_loss() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(100)
            .unwrap();

        let mut updates = worker.follow();

        // Consume only some of the updates to simulate slow reader
        let first = updates.next().await;
        assert!(first.is_some(), "should receive at least one update");

        // Sleep to let many updates be missed
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Attempt to read again (might be after some lag)
        let maybe_update = updates.next().await;

        assert!(
            maybe_update.is_some(),
            "even if lagged, we eventually catch up"
        );
    }

    #[tokio::test]
    async fn test_worker_interrupt_status() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(5)
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        let worker = worker.cancel().await;

        assert_eq!(worker.status(), &Status::Interrupted);
    }

    #[tokio::test]
    async fn test_follow_stream_closes_on_worker_end() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(1)
            .unwrap();

        let mut updates = worker.follow();

        // Wait for worker to finish
        let _ = worker.wait(None).await;

        // After worker finishes, stream should terminate
        let maybe_update = updates.next().await;
        assert!(maybe_update.is_some(), "should get at least one update");

        let end = updates.next().await;
        assert!(end.is_none(), "stream should end after worker shutdown");
    }

    #[tokio::test]
    async fn test_worker_timeout_then_complete() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        // First wait with very short timeout
        let worker = worker.wait(Some(Duration::from_millis(1))).await;

        // Now wait without timeout (should finish)
        let worker = worker.wait(None).await;

        assert_eq!(worker.status(), &Status::Success);
    }
}
