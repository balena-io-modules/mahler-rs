mod distance;
mod domain;
mod intent;
mod planner;
mod workflow;

use log::{debug, error, info, warn};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::{
    fmt::{self, Display},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::task::JoinHandle;

pub use domain::*;
pub use intent::*;
pub use planner::*;
pub use workflow::*;

use crate::{error::Error, system::System};

pub struct WorkerOpts {
    /// The maximum number of attempts to reach the target before giving up.
    /// Defauts to infinite tries (0).
    max_retries: u32,
    /// The minimal time to wait between re-plan. Defaults to 1 second
    min_wait_ms: u64,
    /// The maximum time to wait between re-plan. Defaults to 5 minutes
    max_wait_ms: u64,
}

impl WorkerOpts {
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

pub trait WorkerState {}

pub struct Uninitialized {
    domain: Domain,
    opts: WorkerOpts,
}

pub struct Ready {
    planner: Planner,
    system: System,
    opts: WorkerOpts,
}

pub struct Running {
    opts: WorkerOpts,
    handle: JoinHandle<(Planner, System)>,
    cancelled: Arc<AtomicBool>,
}

impl WorkerState for Uninitialized {}
impl WorkerState for Ready {}
impl WorkerState for Running {}

impl Default for WorkerOpts {
    fn default() -> Self {
        WorkerOpts {
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
            opts: WorkerOpts::default(),
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

    pub fn job(self, route: &'static str, intent: Intent) -> Self {
        let Self { mut inner, .. } = self;
        inner.domain = inner.domain.job(route, intent);
        Worker::from_inner(inner)
    }

    pub fn with_domain(self, domain: Domain) -> Worker<T, Uninitialized> {
        let Self { mut inner, .. } = self;
        inner.domain = domain;
        Worker::from_inner(inner)
    }

    pub fn with_opts(self, opts: WorkerOpts) -> Worker<T, Uninitialized> {
        let Self { mut inner, .. } = self;
        inner.opts = opts;
        Worker::from_inner(inner)
    }

    pub fn initial_state(self, state: T) -> Worker<T, Ready>
    where
        T: Serialize + DeserializeOwned,
    {
        let Uninitialized { domain, opts, .. } = self.inner;
        // this can panic
        let system = System::from(state);
        Worker::from_inner(Ready {
            planner: Planner::new(domain),
            system,
            opts,
        })
    }
}

impl<T: Serialize + DeserializeOwned> Worker<T, Ready> {
    pub fn state(self) -> T {
        self.inner.system.state().unwrap()
    }

    pub fn seek_target(self, tgt: T) -> Worker<T, Running> {
        let Ready {
            planner,
            system,
            opts,
            ..
        } = self.inner;

        // TODO: handle the error
        let tgt = serde_json::to_value(tgt).unwrap();

        async fn plan_and_execute(
            planner: &Planner,
            system: &mut System,
            tgt: &Value,
            interrupted: &AtomicBool,
        ) -> Result<bool, Error> {
            // TODO: maybe use a timeout to finding the plan
            let workflow = planner.find_workflow(system, tgt.clone())?;
            if workflow.is_empty() {
                return Ok(true);
            }

            info!("plan found");
            debug!("will execute the following tasks:\n{}", workflow);

            // run the plan and update the system
            workflow.execute(system, interrupted).await?;

            Ok(false)
        }

        // Create a flag to signal the cancellation of the planning
        let cancelled = Arc::new(AtomicBool::new(false));
        let interrupted = cancelled.clone();

        let handle = tokio::task::spawn(async move {
            let mut system = system;
            let mut tries = 0;

            // continue to re-plan and execute while we have not reached the target state
            // or we have not been interrupted
            while !interrupted.load(Ordering::Relaxed) {
                let found = match plan_and_execute(&planner, &mut system, &tgt, &interrupted).await
                {
                    Ok(true) => break,
                    Ok(false) => true,
                    Err(Error::WorkflowInterrupted(_)) => {
                        // TODO: implement some way to report the worker status
                        // rather than just logging
                        info!("workflow interrupted due to user request");
                        return (planner, system);
                    }
                    Err(Error::PlanSearchFailed(PlanningError::WorkflowNotFound)) => {
                        warn!("no plan found");
                        false
                    }
                    Err(Error::PlanSearchFailed(e)) => {
                        if cfg!(debug_assertions) {
                            // Return the error in debug mode
                            error!("plan search failed: {}", e);
                            return (planner, system);
                        } else {
                            warn!("plan search failed: {}", e);
                        }
                        false
                    }
                    Err(e) => {
                        warn!("failed to execute the workflow: {}", e);
                        false
                    }
                };

                if !found && tries >= opts.max_retries {
                    error!("failed to reach the target state after {} tries", tries);
                    return (planner, system);
                }

                // Exponential backoff
                let wait = std::cmp::min(opts.min_wait_ms * 2u64.pow(tries), opts.max_wait_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(wait)).await;

                // Only backoff if we did not find the target
                tries += if found { 0 } else { 1 };
            }
            (planner, system)
        });

        Worker::from_inner(Running {
            handle,
            cancelled,
            opts,
        })
    }
}

#[derive(Debug)]
pub struct Timeout;
impl std::error::Error for Timeout {}
impl Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "timeout reached while waiting for the worker to finish")
    }
}

impl<T: DeserializeOwned> Worker<T, Running> {
    pub async fn cancel(self) -> Worker<T, Ready> {
        let Running {
            handle: task,
            cancelled,
            opts,
        } = self.inner;

        // Cancel the task
        cancelled.store(true, Ordering::Relaxed);

        info!("worker interrupted, waiting for running tasks to finish");
        let (planner, system) = task.await.unwrap();
        Worker::from_inner(Ready {
            planner,
            system,
            opts,
        })
    }

    // TODO: this is not great because a timeout means that
    // you'll never be able to use the worker again
    pub async fn wait(
        self,
        timeout: Option<std::time::Duration>,
    ) -> Result<Worker<T, Ready>, Timeout> {
        let Running { handle, opts, .. } = self.inner;

        match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, handle).await {
                Ok(Ok((planner, system))) => Ok(Worker::from_inner(Ready {
                    planner,
                    system,
                    opts,
                })),
                Ok(Err(e)) => {
                    // A panic happened while waiting for the handle
                    // this should not happen
                    panic!("unexpected worker failure: {:?}", e);
                }
                Err(_) => Err(Timeout),
            },
            None => {
                // Wait indefinitely
                let (planner, system) = handle.await.unwrap();
                Ok(Worker::from_inner(Ready {
                    planner,
                    system,
                    opts,
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

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_worker() {
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
            ])));

        let worker = worker.wait(None).await.unwrap();
        let state = worker.state();
        assert_eq!(
            state,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    async fn test_worker_cancel() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2);

        // interrupt the workflow after the first step
        sleep(Duration::from_millis(10)).await;
        let worker = worker.cancel().await;
        let state = worker.state();
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn test_worker_timeout() {
        init();
        let worker = Worker::new()
            .job("", update(plus_one))
            .initial_state(0)
            .seek_target(2);

        let worker = worker.wait(Some(Duration::from_millis(1))).await;
        assert!(worker.is_err());
    }
}
