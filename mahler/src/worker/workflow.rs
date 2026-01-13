//! Types and utilities to generate and execute task Workflows

use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::{info, instrument};

use crate::dag::{Dag, ExecutionStatus as DagExecutionStatus, Task};
use crate::error::{AggregateError, Error, ErrorKind};
use crate::json::{Operation, Patch, PatchOperation, Path, Value};
use crate::runtime::{Channel, System};
use crate::sync::{Interrupt, RwLock, Sender};
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
pub(crate) struct WorkUnit {
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
    async fn run(&self, system: &System, sender: &Sender<Patch>) -> Result<Patch, Self::Error> {
        info!("starting");
        // dry-run the task to test that conditions hold
        // before executing the action should not really fail at this point
        let Patch(changes) = self.action.dry_run(system)?;
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

        self.action
            .run(system, &Channel::from(sender.clone()))
            .await
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

    /// List of skipped paths/operations during planning due to a halted state
    pub(super) ignored: Vec<Path>,

    /// The list of operations that triggered this workflow creation
    pub(super) operations: Vec<Operation>,

    /// The worker that created this workflow
    pub(super) worker_id: u64,

    /// The generation when this workflow was created
    pub(super) generation: u64,
}

/// Runtime status of a workflow execution
pub(crate) enum WorkflowStatus {
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
    pub(crate) fn new(dag: Dag<WorkUnit>) -> Self {
        Workflow {
            dag,
            ignored: Vec::new(),
            operations: Vec::new(),
            worker_id: 0,
            generation: 0,
        }
    }

    /// Return the list of ignored paths during planning due to halted states
    pub fn ignored(&self) -> Vec<&Path> {
        self.ignored.iter().collect()
    }

    /// Return the list of operations that triggered this workflow to be built
    pub fn operations(&self) -> Vec<&Operation> {
        self.operations.iter().collect()
    }

    /// Return `true` if the Workflow's internal graph has no elements
    pub fn is_empty(&self) -> bool {
        self.dag.is_empty()
    }

    /// Get the worker ID that created this workflow
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// Get the generation when this workflow was created
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) async fn execute(
        self,
        sys_reader: &Arc<RwLock<System>>,
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
