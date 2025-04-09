use anyhow::{anyhow, Context as AnyhowCtx};
use json_patch::Patch;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::fmt;
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

use crate::system::System;
use crate::workflow::WorkflowStatus;
use crate::{
    ack_channel::ack_channel,
    planner::{Domain, Error as PlannerError, Planner},
};
use crate::{
    ack_channel::Sender,
    task::{self, Job},
};

#[derive(Clone, Debug)]
pub struct Opts {
    /// The maximum number of attempts to reach the target before giving up.
    /// Defauts to infinite tries (0).
    max_retries: u32,
    /// The minimal time to wait between re-plan. Defaults to 1 second
    min_wait_ms: u64,
    /// The maximum time to wait between re-plan. Defaults to 5 minutes
    max_wait_ms: u64,
}

impl Opts {
    pub fn max_retries(self, max_retries: u32) -> Self {
        let mut opts = self;
        opts.max_retries = max_retries;
        opts
    }

    pub fn min_wait_ms(self, min_wait_ms: u64) -> Self {
        let mut opts = self;
        opts.min_wait_ms = min_wait_ms;
        opts
    }

    pub fn max_wait_ms(self, max_wait_ms: u64) -> Self {
        let mut opts = self;
        opts.max_wait_ms = max_wait_ms;
        opts
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] anyhow::Error);

#[derive(Debug)]
pub enum Status {
    /// Target state reached
    Success,
    /// Failed to reach the target state after multiple tries
    Failure,
    /// Worker interrupted by user request
    Interrupted,
    /// Worker execution terminated due to some unexpected error
    Aborted(Error),
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

#[derive(Debug, Clone)]
struct UpdateEvent;

impl Eq for Status {}

pub trait WorkerState {}

pub struct Uninitialized {
    domain: Domain,
    opts: Opts,
}

pub struct Ready {
    planner: Planner,
    system: System,
    opts: Opts,
}

pub struct Running {
    opts: Opts,
    handle: Pin<Box<JoinHandle<(Planner, System, Status)>>>,
    updates: broadcast::Sender<UpdateEvent>,
    sys_reader: Arc<RwLock<System>>,
    interrupt: Arc<AtomicBool>,
}

pub struct Idle {
    planner: Planner,
    system: System,
    opts: Opts,
    status: Status,
}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}
impl WorkerState for Running {}
impl WorkerState for Idle {}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            max_retries: 0,
            min_wait_ms: 1000,
            max_wait_ms: 300_000,
        }
    }
}

pub struct Worker<T, S: WorkerState = Uninitialized> {
    inner: S,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Default for Worker<T, Uninitialized> {
    fn default() -> Self {
        Worker::from_inner(Uninitialized {
            domain: Domain::new(),
            opts: Opts::default(),
        })
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

impl<T> Worker<T, Uninitialized> {
    pub fn new() -> Self {
        Worker::default()
    }

    pub fn job(self, route: &'static str, job: Job) -> Self {
        let Self { mut inner, .. } = self;
        inner.domain = inner.domain.job(route, job);
        Worker::from_inner(inner)
    }

    pub fn with_domain(self, domain: Domain) -> Worker<T, Uninitialized> {
        let Self { mut inner, .. } = self;
        inner.domain = domain;
        Worker::from_inner(inner)
    }

    pub fn with_opts(self, opts: Opts) -> Worker<T, Uninitialized> {
        let Self { mut inner, .. } = self;
        inner.opts = opts;
        Worker::from_inner(inner)
    }

    pub fn initial_state(self, state: T) -> Result<Worker<T, Ready>, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        let Uninitialized { domain, opts, .. } = self.inner;

        // we want to panic early while setting up the worker
        let system = System::try_from(state).context("could not serialize initial state")?;
        Ok(Worker::from_inner(Ready {
            planner: Planner::new(domain),
            system,
            opts,
        }))
    }
}

pub trait SeekTarget<T> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error>;
}

impl<T: DeserializeOwned> Worker<T, Ready> {
    pub fn state(&self) -> Result<T, Error> {
        let state = self
            .inner
            .system
            .state()
            .context("could not deserialize state")?;
        Ok(state)
    }
}

impl<T: Serialize> SeekTarget<T> for Worker<T, Ready> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error> {
        let Ready {
            planner,
            system,
            opts,
            ..
        } = self.inner;

        let tgt = serde_json::to_value(tgt).context("could not serialize target")?;

        #[derive(Debug, Error)]
        enum InternalError {
            #[error(transparent)]
            Runtime(#[from] task::Error),

            #[error(transparent)]
            Planning(#[from] PlannerError),
        }

        enum InternalResult {
            TargetReached,
            Completed,
            Interrupted,
        }

        async fn find_and_run_workflow(
            planner: &Planner,
            sys: &Arc<RwLock<System>>,
            tgt: &Value,
            channel: &Sender<Patch>,
            sigint: &Arc<AtomicBool>,
        ) -> Result<InternalResult, InternalError> {
            let workflow = {
                let system = sys.read().await;
                planner.find_workflow(&system, tgt)?
            };

            if workflow.is_empty() {
                return Ok(InternalResult::TargetReached);
            }

            if matches!(
                workflow.execute(sys, channel.clone(), sigint).await?,
                WorkflowStatus::Interrupted
            ) {
                return Ok(InternalResult::Interrupted);
            }

            Ok(InternalResult::Completed)
        }

        // Shared system protected by RwLock
        let syslock = Arc::new(RwLock::new(system));

        // Create the messaging channel
        let (tx, mut rx) = ack_channel::<Patch>(100);

        // Error signal channel (one-shot)
        let (err_tx, mut err_rx) = tokio::sync::oneshot::channel::<Error>();

        // Broadcast channel for state updates
        let (updates, _) = tokio::sync::broadcast::channel::<UpdateEvent>(16);

        // Spawn system writer task
        {
            let sys_writer = Arc::clone(&syslock);
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
                            let _ = err_tx.send(Error(anyhow!(e)));
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
            let sys_reader = Arc::clone(&syslock);
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
                                return (planner, system, Status::Aborted(err));
                            }
                        }

                        _ = async {}, if sigint.load(Ordering::Relaxed) => {
                            cur_span.record("return", "interrupted");
                            let system = sys_reader.read().await.clone();
                            return (planner, system, Status::Interrupted);
                        }

                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(wait_ms)) => {
                            match find_and_run_workflow(&planner, &sys_reader, &tgt, &tx, &sigint).await {
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
                                Err(InternalError::Planning(PlannerError::NotFound)) => {
                                    tries += 1;
                                    if opts.max_retries != 0 && tries >= opts.max_retries {
                                        cur_span.record("return", "failure");
                                        let system = sys_reader.read().await.clone();
                                        return (planner, system, Status::Failure);
                                    }
                                    wait_ms = std::cmp::min(wait_ms.saturating_mul(2), opts.max_wait_ms);
                                }
                                Err(InternalError::Planning(err)) => {
                                    if cfg!(debug_assertions) {
                                        cur_span.record("return", "aborted");
                                        let system = sys_reader.read().await.clone();
                                        return (planner, system, Status::Aborted(Error(anyhow!(err))));
                                    }
                                    tries += 1;
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
            sys_reader: syslock,
            updates,
        }))
    }
}

impl<T: Serialize> SeekTarget<T> for Result<Worker<T, Ready>, Error> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error> {
        self.and_then(|worker| worker.seek_target(tgt))
    }
}

impl<T> Worker<T, Idle> {
    pub fn state(&self) -> Result<T, Error>
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

    pub fn status(&self) -> &Status {
        &self.inner.status
    }
}

impl<T: Serialize> SeekTarget<T> for Worker<T, Idle> {
    fn seek_target(self, tgt: T) -> Result<Worker<T, Running>, Error> {
        // TODO: we probably need to fail if the previous execution
        // terminated due to a patch failure as we can no longer trust
        // the internal state
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

#[derive(Error)]
#[error("timeout")]
pub struct Timeout<T>(pub Worker<T, Running>);

impl<T> Timeout<T> {
    /// Recover the running worker from the timeout.
    pub fn into_worker(self) -> Worker<T, Running> {
        self.0
    }

    /// Wait again for the worker to complete, with an optional timeout.
    ///
    /// Returns `Ok(Worker<T, Idle>)` if the worker finishes,
    /// or `Err(Timeout)` if it times out again.
    pub async fn wait(self, timeout: Option<std::time::Duration>) -> WaitResult<T> {
        self.0.wait(timeout).await
    }

    /// Cancel the worker immediately and return it in the Idle state.
    ///
    /// Useful if you decide not to wait after a timeout.
    pub async fn cancel(self) -> Worker<T, Idle> {
        self.0.cancel().await
    }
}

impl<T> fmt::Debug for Timeout<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout")
            .field("state", &"running")
            .finish()
    }
}

impl<T> From<Timeout<T>> for Worker<T, Running> {
    fn from(timeout: Timeout<T>) -> Worker<T, Running> {
        timeout.0
    }
}

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

pub type WaitResult<T> = Result<Worker<T, Idle>, Timeout<T>>;

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
            interrupt: sigint,
            opts,
            ..
        } = self.inner;

        // Cancel the task
        sigint.store(true, Ordering::Relaxed);

        // This should not happen
        let (planner, system, status) = task.await.expect("worker runtime panicked");
        Worker::from_inner(Idle {
            planner,
            system,
            opts,
            status,
        })
    }

    pub async fn wait(self, timeout: Option<std::time::Duration>) -> WaitResult<T> {
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
                        Ok(Worker::from_inner(Idle {
                            planner,
                            system,
                            opts,
                            status,
                        }))
                    }
                    _ = &mut sleeper => {
                        Err(Timeout(Worker::from_inner(Running {
                            handle,
                            opts,
                            updates,
                            sys_reader,
                            interrupt,
                        })))
                    }
                }
            }
            None => {
                let (planner, system, status) = handle.await.expect("worker runtime panicked");
                Ok(Worker::from_inner(Idle {
                    planner,
                    system,
                    opts,
                    status,
                }))
            }
        }
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

        let worker = worker.wait(None).await.unwrap();
        assert_eq!(worker.status(), &Status::Success);
        let state = worker.state().unwrap();
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
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let _ = worker.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_cancel() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        // interrupt the workflow after the first step
        sleep(Duration::from_millis(10)).await;
        let worker = worker.cancel().await;
        let state = worker.state().unwrap();
        assert_eq!(state, 1);
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
        assert!(worker.is_err());
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
        let second_update = updates.next().await;

        // Wait for worker to finish
        let worker = worker.wait(None).await.unwrap();
        let final_state = worker.state().unwrap();

        assert!(first_update.is_some(), "should receive first state update");
        assert!(
            second_update.is_some(),
            "should receive second state update"
        );

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
        let _ = worker.wait(None).await.unwrap();

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
        let res = worker.wait(Some(Duration::from_millis(1))).await;
        assert!(res.is_err(), "should timeout");

        let worker = res.err().unwrap().into_worker();

        // Now wait without timeout (should finish)
        let worker = worker.wait(None).await.unwrap();

        assert_eq!(worker.status(), &Status::Success);
    }
}
