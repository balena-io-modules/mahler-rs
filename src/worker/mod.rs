use anyhow::{anyhow, Context};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{error, field, span, Instrument, Level, Span};

#[cfg(feature = "logging")]
mod logging;

#[cfg(feature = "logging")]
pub use logging::init as init_logging;

use crate::planner::{Domain, Error as PlannerError, Planner};
use crate::system::System;
use crate::task::{self, Job};
use crate::workflow::WorkflowStatus;

#[derive(Clone)]
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
    handle: JoinHandle<(Planner, System, Status)>,
    sigint: Arc<AtomicBool>,
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
            system: &mut System,
            tgt: &Value,
            interrupted: &AtomicBool,
        ) -> Result<InternalResult, InternalError> {
            // TODO: maybe use a timeout to finding the plan
            let workflow = planner.find_workflow(system, tgt)?;
            if workflow.is_empty() {
                return Ok(InternalResult::TargetReached);
            }

            // run the plan and update the system
            if matches!(
                workflow.execute(system, interrupted).await?,
                WorkflowStatus::Interrupted
            ) {
                return Ok(InternalResult::Interrupted);
            }

            Ok(InternalResult::Completed)
        }

        // Create a flag to signal the cancellation of the planning
        let sigint = Arc::new(AtomicBool::new(false));
        let interrupted = sigint.clone();
        let target = tgt.clone();

        let handle = tokio::task::spawn(
            async move {
                let mut system = system;
                let mut tries = 0;
                let cur_span = Span::current();

                // continue to re-plan and execute while we have not reached the target state
                // or we have not been interrupted
                while !interrupted.load(Ordering::Relaxed) {
                    let found = match find_and_run_workflow(
                        &planner,
                        &mut system,
                        &tgt,
                        &interrupted,
                    )
                    .await
                    {
                        Ok(InternalResult::TargetReached) => {
                            cur_span.record("return", "success");
                            return (planner, system, Status::Success);
                        }
                        Ok(InternalResult::Completed) => true,
                        Ok(InternalResult::Interrupted) => {
                            break;
                        }
                        Err(InternalError::Planning(PlannerError::NotFound)) => false,
                        Err(InternalError::Planning(err)) => {
                            if cfg!(debug_assertions) {
                                // Abort search if a an unexpected error happens during
                                // planning
                                return (planner, system, Status::Aborted(Error(anyhow!(err))));
                            }
                            false
                        }
                        Err(_) => false,
                    };

                    if !found && tries >= opts.max_retries {
                        cur_span.record("return", "failure");
                        return (planner, system, Status::Failure);
                    }

                    // Exponential backoff
                    let wait = std::cmp::min(opts.min_wait_ms * 2u64.pow(tries), opts.max_wait_ms);
                    tokio::time::sleep(tokio::time::Duration::from_millis(wait)).await;

                    // Only backoff if we did not find the target
                    tries += if found { 0 } else { 1 };
                }

                cur_span.record("return", "interrupted");
                (planner, system, Status::Interrupted)
            }
            .instrument(span!(Level::INFO, "seek_target", target = %target, return = field::Empty)),
        );

        Ok(Worker::from_inner(Running {
            handle,
            sigint,
            opts,
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

#[derive(Debug, Error)]
#[error("timeout")]
pub struct Timeout;

impl<T> Worker<T, Running> {
    pub async fn cancel(self) -> Worker<T, Idle> {
        let Running {
            handle: task,
            sigint,
            opts,
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

    pub async fn wait(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> Result<Worker<T, Idle>, Timeout> {
        let Running { handle, opts, .. } = &mut self.inner;

        let res = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, handle).await {
                Ok(res) => res,
                Err(_) => return Err(Timeout),
            },
            None => {
                // Wait indefinitely
                handle.await
            }
        };

        let (planner, system, status) = res.expect("worker runtime panicked");
        Ok(Worker::from_inner(Idle {
            planner,
            system,
            opts: opts.clone(),
            status,
        }))
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

    #[tokio::test]
    async fn test_worker_complex_state() {
        let mut worker = Worker::new()
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
        let state = worker.state().unwrap();
        assert_eq!(worker.status(), &Status::Success);
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
        let mut worker = Worker::new()
            .job("", update(buggy_plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let _ = worker.wait(None).await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_cancel() {
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
        let mut worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2)
            .unwrap();

        let worker = worker.wait(Some(Duration::from_millis(1))).await;
        assert!(worker.is_err());
    }
}
