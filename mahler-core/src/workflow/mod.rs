//! Types and utilities to generate and execute task Workflows

use async_trait::async_trait;
use json_patch::{Patch, PatchOperation};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, instrument};

use crate::system::System;
use crate::task::{Action, Error as TaskError};

mod aggregate_error;
mod channel;
mod dag;
mod interrupt;

pub use dag::*;
pub use interrupt::Interrupt;

pub(crate) use aggregate_error::*;
pub(crate) use channel::*;
pub(crate) use interrupt::*;

#[derive(Hash)]
/// Unique representation of a task acting on a specific path and system state.
///
/// The hash of this structure is used as the [`WorkUnit`] id
struct WorkUnitId<'s> {
    /// The task id
    task_id: String,
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
            task_id: String::from(task.id()),
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
    type Error = TaskError;

    #[instrument(name = "run_task", skip_all, fields(task=%self.action), err)]
    async fn run(&self, system: &System) -> Result<Patch, TaskError> {
        info!("starting");
        // dry-run the task to test that conditions hold
        // before executing the action should not really fail at this point
        let Patch(changes) = self.action.dry_run(system)?;
        if changes != self.output {
            // If the result is different then we assume it's because the
            // conditions changed since planning
            return Err(TaskError::ConditionFailed);
        }

        self.action.run(system).await
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
/// Dag](`Dag#string-representation-of-a-dag`), where each task is rendered from its provided
/// [description](`crate::task::Job::with_description`).
pub struct Workflow(pub(crate) Dag<WorkUnit>);

/// Runtime status of a workflow execution
pub(crate) enum WorkflowStatus {
    /// The workflow execution terminated successfully
    Completed,

    /// The workflow execution was interrupted by user request
    Interrupted,
}

impl From<ExecutionStatus> for WorkflowStatus {
    fn from(status: ExecutionStatus) -> WorkflowStatus {
        match status {
            ExecutionStatus::Completed => WorkflowStatus::Completed,
            ExecutionStatus::Interrupted => WorkflowStatus::Interrupted,
        }
    }
}

impl Workflow {
    /// Return `true` if the Workflow's internal graph has no elements
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) async fn execute(
        self,
        sys_reader: &Arc<RwLock<System>>,
        patch_tx: Sender<Patch>,
        interrupt: Interrupt,
    ) -> Result<WorkflowStatus, AggregateError<TaskError>> {
        self.0
            .execute(sys_reader, patch_tx, interrupt)
            .await
            .map(|s| s.into())
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
