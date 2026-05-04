//! Types and utilities to generate and execute task Workflows

use async_trait::async_trait;
use json_patch::AddOperation;
use jsonptr::resolve::ResolveError;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use tracing::{info, instrument};

use crate::dag::{Dag, ExecutionStatus as DagExecutionStatus, Task};
use crate::error::{AggregateError, Error, ErrorKind};
use crate::json::{Operation, Patch, PatchOperation, RemoveOperation, ReplaceOperation, Value};
use crate::runtime::{Channel, System};
use crate::sync::{channel, Interrupt, Reader, Sender};
use crate::system_ext::SystemExt;
use crate::task::{Action, Id};

/// Unique representation of a task acting on a specific path and system state.
///
/// The hash of this structure is used as the [`WorkUnit`] id
#[derive(Hash)]
struct WorkUnitId<'s> {
    /// The task id
    task_id: Id,
    /// The task path
    path: String,
    /// The state that is used to test the action
    state: &'s Value,
}

#[derive(Clone, PartialEq, Eq)]
/// Utility type to encode a single work unit in a workflow
pub(super) struct WorkUnit {
    /// Unique id for the action. This is calculed by hashing a WorkUnitId
    pub id: u64,

    /// The action to execute
    ///
    /// Only atomic tasks can be added to a worflow item
    action: Action,

    /// The output of the task during planning
    ///
    /// This will be used at runtime to compare to the result of [dry_run](`crate::task::Action::dry_run`)
    /// and abort the execution if that fails
    output: Vec<PatchOperation>,
}

impl WorkUnit {
    /// Create a new WorkUnit
    pub fn new(id: u64, action: Action, output: Vec<PatchOperation>) -> Self {
        Self { id, action, output }
    }

    /// Calculate the id of a given action and state value.
    ///
    /// Use this before calling [`new`]
    pub fn new_id(task: &Action, state: &Value) -> u64 {
        let pointer = task.context().path.as_ref();

        // Resolve the value that will be modified by
        // the task. If the value does not exist yet, we use Null
        let state = pointer.resolve(state).unwrap_or(&Value::Null);

        let action_id = WorkUnitId {
            task_id: task.id(),
            path: task.context().path.to_string(),
            state,
        };

        // Create a DefaultHasher
        let mut hasher = DefaultHasher::new();

        // Hash the data
        action_id.hash(&mut hasher);

        // Retrieve the hash value
        hasher.finish()
    }
}

impl Display for WorkUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.action.fmt(f)
    }
}

#[async_trait]
impl Task for WorkUnit {
    type Input = System;
    type Changes = Patch;
    type Error = Error;

    #[instrument(name = "run_task", skip_all, fields(task=%self.action), err)]
    async fn run(&self, input: &System, sender: &Sender<Patch>) -> Result<Patch, Self::Error> {
        info!("starting");
        // dry-run the task to test that conditions hold
        // before executing the action should not really fail at this point
        let Patch(changes) = self.action.dry_run(input)?;
        if changes != self.output {
            // If the result is different then we assume it's because the
            // conditions changed since planning
            return Err(Error::new(
                ErrorKind::ConditionNotMet,
                format!(
                    "runtime and planning states do not match for task {}",
                    self.action
                ),
            ));
        }

        // Take snapshot of the path before execution
        let ptr = self.action.context().path.as_ref();
        let mut initial_state = match ptr.resolve(input.inner_state()) {
            Ok(value) => Some(value.clone()),
            Err(ResolveError::NotFound { .. } | ResolveError::OutOfBounds { .. }) => None,
            // this should not happen at this point
            Err(e) => return Err(Error::internal(e)),
        };
        // Track state locally to compute rollback. Safe because the DAG
        // guarantees non-overlapping paths between concurrent tasks.
        let mut updated_state = input.clone();

        let (proxy_tx, mut proxy_rx) = channel::<(Patch, Option<Value>)>(1);

        // create the task future
        let channel = Channel::from(proxy_tx);
        let mut task_fut = std::pin::pin!(self.action.run(input, &channel));
        let task_res = loop {
            // Task completion is checked first. This is safe because the ack'd
            // channel guarantees no message can be pending when the task completes
            // (the task blocks on ack before it can return).
            let checkpoint = tokio::select! {
                biased;
                res = &mut task_fut => {
                    break res
                },
                res = proxy_rx.recv() => match res {
                    Some(mut msg) => {
                        let (changes, checkpoint) = std::mem::take(&mut msg.data);
                        updated_state.patch(&changes)?;
                        sender.send(changes).await.map_err(Error::internal)?;
                        msg.ack();

                        checkpoint
                    },
                    None => break Err(Error::internal("task channel closed")),
                },
            };

            // update the initial state if a checkpoint was provided
            if let Some(value) = checkpoint {
                initial_state = Some(value);
            }
        };

        let err = match task_res {
            Ok(patch) => return Ok(patch),
            Err(e) => e,
        };

        let final_state = match ptr.resolve(updated_state.inner_state()) {
            Ok(value) => Some(value),
            Err(ResolveError::NotFound { .. } | ResolveError::OutOfBounds { .. }) => None,
            // this should not happen at this point
            Err(e) => return Err(Error::internal(e)),
        };

        // Prepare rollback: compare final state against the rollback target
        // (checkpoint or initial). Check equality first so we can move
        // initial_state into the patch without cloning.
        let rollback = if final_state == initial_state.as_ref() {
            vec![]
        } else {
            match (final_state, initial_state) {
                (Some(_), None) => vec![PatchOperation::Remove(RemoveOperation {
                    path: ptr.to_buf(),
                })],
                (None, Some(value)) => vec![PatchOperation::Add(AddOperation {
                    path: ptr.to_buf(),
                    value,
                })],
                (Some(_), Some(value)) => vec![PatchOperation::Replace(ReplaceOperation {
                    path: ptr.to_buf(),
                    value,
                })],
                _ => unreachable!(),
            }
        };

        if !rollback.is_empty() {
            sender
                .send(Patch(rollback))
                .await
                .map_err(Error::internal)?;
        }

        Err(err)
    }
}

/// An ignored operation during planning
///
/// Ignored operations are created when exceptions happen. They may provide
/// an optional reason coming from the exception description
#[derive(Clone, Debug)]
pub struct Ignored {
    pub operation: Operation,
    pub reason: Option<String>,
}

impl fmt::Display for Ignored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.operation.fmt(f)?;
        if let Some(reason) = self.reason.as_ref() {
            write!(f, "(reason: {reason})")?;
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
/// Encodes a graph of tasks to performed by the Worker after planning
///
/// Internally a Workflow is represented by a [DAG](`Dag`), where each work unit is an atomic
/// [Task](`crate::task::Task`) selected by the planner, and includes the result of the task
/// evaluation during planning.
///
/// The Workflow is executed by the Worker in the following way:
/// - Workflow DAG branches are executed concurrently
/// - When executing each node in the DAG, the task is first tested on the current state, and its
///   results are compared to the results during planning.
///     - If the result is different, a requirement for executing the task has changed and the full
///       workflow execution is terminated.
///     - Otherwise, the effectful part of the task is executed, communicating changes back to the
///       `Worker`.
/// - If any task returns an error, the workflow execution is interrupted.
///
/// Workflow implements [`Display`], using the [string representation defined for
/// Dag](`crate::dag::Dag#string-representation-of-a-dag`), where each task is rendered from its provided
/// [description](`crate::job::Job::with_description`).
pub struct Workflow {
    /// The internal DAG that the workflow implements
    pub(super) dag: Dag<WorkUnit>,

    /// List of ignored operations during planning due to exceptions
    pub(super) ignored: Vec<Ignored>,

    /// The worker that created this workflow
    pub(super) worker_id: u64,
}

/// Runtime status of a workflow execution
pub(super) enum WorkflowStatus {
    /// The workflow execution terminated successfully
    Completed,

    /// The workflow execution was interrupted by user request
    Interrupted,
}

impl From<DagExecutionStatus> for WorkflowStatus {
    fn from(status: DagExecutionStatus) -> WorkflowStatus {
        match status {
            DagExecutionStatus::Completed => WorkflowStatus::Completed,
            DagExecutionStatus::Interrupted => WorkflowStatus::Interrupted,
        }
    }
}

impl Workflow {
    pub(super) fn new(dag: Dag<WorkUnit>) -> Self {
        Workflow {
            dag,
            ignored: Vec::new(),
            worker_id: 0,
        }
    }

    /// Return the list of skipped operations during planning
    /// due to matching exceptions
    pub fn exceptions(&self) -> Vec<&Ignored> {
        self.ignored.iter().collect()
    }

    /// Return `true` if the Workflow's internal graph has no elements
    pub fn is_empty(&self) -> bool {
        self.dag.is_empty()
    }

    /// Get the worker ID that created this workflow
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    pub(super) async fn execute(
        self,
        sys_reader: &Reader<System>,
        patch_tx: Sender<Patch>,
        interrupt: Interrupt,
    ) -> Result<WorkflowStatus, AggregateError<Error>> {
        self.dag
            .execute(sys_reader, patch_tx, interrupt)
            .await
            .map(|s| s.into())
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the proxy-loop and rollback logic inside `WorkUnit::run`.
    //!
    //! Strategy: drive `WorkUnit::run` through the public `Worker::run_task` entry point.
    //! Each test constructs a handler that performs a specific sequence of flush / commit
    //! calls before either succeeding or failing, then asserts the final observed state
    //! sequence (including any rollback patch) matches expectations.

    use std::pin::pin;

    use crate::error::Error;
    use crate::extract::{Target, View};
    use crate::job::{create, update};
    use crate::state::State;
    use crate::task::{with_io, IO};
    use crate::worker::{Worker, WorkerEvent, WorkflowStatus};

    // Collect every `StateUpdated` value emitted by a `run_workflow` stream and
    // return the final `WorkflowStatus`.
    async fn collect_stream<T: State + Send + Unpin + 'static>(
        worker: Worker<T, crate::worker::Ready>,
        workflow: crate::worker::Workflow,
    ) -> (Vec<T>, WorkflowStatus) {
        let mut states = Vec::new();
        let mut final_status = WorkflowStatus::Success;
        {
            let mut stream = pin!(worker.run_workflow(workflow));
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    WorkerEvent::StateUpdated(s) => states.push(s),
                    WorkerEvent::WorkflowFinished(status) => {
                        final_status = status;
                        break;
                    }
                }
            }
        }
        (states, final_status)
    }

    use tokio_stream::StreamExt;

    // -----------------------------------------------------------------------
    // 1. Task fails without any intermediate sends → no rollback patch emitted,
    //    state stays at its initial value.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_no_send_failure_no_rollback() {
        fn successful_task(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32, Error> {
            if *counter < tgt {
                *counter += 1;
            }
            with_io(
                counter,
                |_| async move { Err(Error::runtime("internal error")) },
            )
        }

        let worker = Worker::new()
            .job("", update(successful_task))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(1).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        // The only state update should be the rollback. Because the task never flushed
        // anything, final_state == initial_state so no rollback patch is emitted at all.
        assert!(
            states.is_empty(),
            "expected no state updates, got: {:?}",
            states
        );
    }

    // -----------------------------------------------------------------------
    // 2. Task calls flush() then fails → rollback restores original state.
    //    Flush sends `None` as checkpoint, so rollback target must remain the
    //    pre-task initial state, not None.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_flush_then_failure_rolls_back_to_initial() {
        fn flush_then_fail(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32, Error> {
            if *counter < tgt {
                *counter = tgt;
            }
            with_io(counter, |mut counter| async move {
                // Flush an intermediate change (sends None checkpoint)
                counter.flush().await?;
                Err(Error::runtime("failure after flush"))
            })
        }

        let worker = Worker::new()
            .job("", update(flush_then_fail))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(5).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        // Sequence: flush emits 5, then rollback emits 0
        assert!(
            states.len() >= 2,
            "expected at least 2 state updates, got {:?}",
            states
        );
        let flushed = states[0];
        let rolled_back = states[1];
        assert_eq!(flushed, 5, "flush should have sent 5");
        assert_eq!(rolled_back, 0, "rollback should restore 0");
    }

    // -----------------------------------------------------------------------
    // 3. Task calls commit() then flush() then fails → rollback to the
    //    committed checkpoint, NOT to initial. Regression: the old code did
    //    `initial_state = checkpoint` so a subsequent flush (sending None)
    //    would clobber the rollback target back to None.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_commit_flush_failure_rolls_back_to_checkpoint() {
        fn commit_flush_fail(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32, Error> {
            if *counter < tgt {
                *counter = tgt;
            }
            with_io(counter, |mut counter| async move {
                counter.commit().await?;
                *counter += 3;
                counter.flush().await?;
                Err(Error::runtime("failure after commit then flush"))
            })
        }

        let worker = Worker::new()
            .job("", update(commit_flush_fail))
            .initial_state(0)
            .unwrap();

        let workflow = worker.find_workflow(5).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        // Sequence: commit 5, flush 8, rollback to 5
        assert_eq!(
            states,
            vec![5, 8, 5],
            "expected [commit 5, flush 8, rollback 5], got {:?}",
            states
        );
    }

    // -----------------------------------------------------------------------
    // 4. Create operation: path didn't exist before, task sends Add then fails
    //    → rollback removes the path.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_create_flush_then_failure_removes_path() {
        use crate::state::Map;

        fn create_entry(
            mut view: View<Option<i32>>,
            Target(tgt): Target<i32>,
        ) -> IO<Option<i32>, Error> {
            *view = Some(tgt);
            with_io(view, |mut view| async move {
                view.flush().await?;
                Err(Error::runtime("create failed after flush"))
            })
        }

        let worker = Worker::new()
            .job("/{key}", create(create_entry))
            .initial_state(Map::<String, i32>::new())
            .unwrap();

        let target = Map::from([("foo".to_string(), 0)]);
        let workflow = worker.find_workflow(target).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        let final_state = states.last().expect("expected at least one state update");
        assert!(
            !final_state.contains_key("foo"),
            "rollback should have removed the created key, but state is {:?}",
            final_state
        );
    }

    // -----------------------------------------------------------------------
    // 5. Delete operation: path existed, task removes it then fails
    //    → rollback re-creates the path with the original value (Add).
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_delete_flush_then_failure_recreates_path() {
        use crate::state::Map;

        fn delete_entry(view: View<i32>) -> IO<Option<i32>, Error> {
            let view = view.delete();
            with_io(view, |mut view| async move {
                view.flush().await?;
                Err(Error::runtime("delete failed after flush"))
            })
        }

        let worker = Worker::new()
            .job("/{key}", crate::job::delete(delete_entry))
            .initial_state(Map::from([("foo".to_string(), 42)]))
            .unwrap();

        let target = Map::<String, i32>::new();
        let workflow = worker.find_workflow(target).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        let final_state = states.last().expect("expected at least one state update");
        assert_eq!(
            final_state.get("foo").copied(),
            Some(42),
            "rollback should have re-created the deleted key with original value, but state is {:?}",
            final_state
        );
    }

    // -----------------------------------------------------------------------
    // 6. List state: task flushes a modification then fails → rollback restores
    //    the original list.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_list_flush_then_failure_restores_original() {
        use crate::state::List;

        fn append_then_fail(
            mut view: View<List<i32>>,
            Target(tgt): Target<List<i32>>,
        ) -> IO<List<i32>, Error> {
            *view = tgt;
            with_io(view, |mut view| async move {
                view.flush().await?;
                Err(Error::runtime("failure after list modification"))
            })
        }

        let initial = List::from(vec![1, 2, 3]);
        let worker = Worker::new()
            .job("", update(append_then_fail))
            .initial_state(initial.clone())
            .unwrap();

        let target = List::from(vec![1, 2, 3, 4, 5]);
        let workflow = worker.find_workflow(target).unwrap().unwrap();
        let (states, status) = collect_stream(worker, workflow).await;

        assert!(matches!(status, WorkflowStatus::Aborted(_)));
        let rolled_back = states.last().expect("expected at least one state update");
        assert_eq!(
            *rolled_back, initial,
            "rollback should restore the original list [1, 2, 3]"
        );
    }
}
