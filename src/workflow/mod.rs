use async_trait::async_trait;
use json_patch::{Patch, PatchOperation};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::instrument;

use crate::system::System;
use crate::task::{Action, Error as TaskError};

mod aggregate_error;
mod channel;
mod dag;
mod interrupt;

pub use aggregate_error::*;
pub(crate) use channel::*;
pub use dag::*;
pub(crate) use interrupt::*;

#[derive(Hash)]
struct WorkUnitId<'s> {
    /// The task id
    task_id: String,
    /// The task path
    path: String,
    /// The state that is used to test the action
    state: &'s Value,
}

#[derive(Clone)]
pub(crate) struct WorkUnit {
    /**
     * Unique id for the action. This is calculated by
     * hashing the .
     */
    pub id: u64,

    /**
     * The task to execute
     *
     * Only atomic tasks should be added to a worflow item
     */
    action: Action,

    /**
     * The output of the task during planning
     */
    output: Vec<PatchOperation>,
}

impl WorkUnit {
    pub fn new(id: u64, action: Action, output: Vec<PatchOperation>) -> Self {
        Self { id, action, output }
    }

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

    #[instrument(name="run_task", skip_all, fields(id=%self.action.id(), task=%self.action, state=%system.root()), err)]
    async fn run(&self, system: &System) -> Result<Patch, TaskError> {
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
pub struct Workflow {
    pub(crate) dag: Dag<WorkUnit>,
    pub(crate) pending: Vec<PatchOperation>,
}

pub enum WorkflowStatus {
    Completed,
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
    pub(crate) fn as_dag(&self) -> &Dag<WorkUnit> {
        &self.dag
    }

    pub(crate) fn reverse(self) -> Self {
        let Self { dag, pending } = self;
        Self {
            dag: dag.reverse(),
            pending,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dag.is_empty()
    }

    #[instrument(name = "run_workflow", skip_all, err)]
    pub(crate) async fn execute(
        self,
        system: &Arc<RwLock<System>>,
        channel: Sender<Patch>,
        interrupt: Interrupt,
    ) -> Result<WorkflowStatus, AggregateError<TaskError>> {
        self.dag
            .execute(system, channel, interrupt)
            .await
            .map(|s| s.into())
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}
