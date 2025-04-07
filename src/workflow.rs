use async_trait::async_trait;
use json_patch::PatchOperation;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use tracing::instrument;

use crate::dag::{Dag, ExecutionStatus, Task};
use crate::system::System;
use crate::task::{Action, Error as TaskError};

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
    // TODO: store the planing output of the task
    // to compare during execution
}

impl WorkUnit {
    pub fn new(id: u64, action: Action) -> Self {
        Self { id, action }
    }

    pub fn new_id(task: &Action, state: &Value) -> Result<u64, jsonptr::resolve::ResolveError> {
        let pointer = task.context().path.as_ref();

        // Resolve the value that will be modified by
        // the task
        let state = pointer.resolve(state)?;

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
        Ok(hasher.finish())
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
    type Output = ();
    type Error = TaskError;

    #[instrument(name="run_task", skip_all, fields(task=%self.action), err)]
    async fn run(&self, system: &mut System) -> Result<(), TaskError> {
        // TODO: dry-run the task to test that conditions hold
        // before executing the action

        self.action.run(system).await
    }
}

#[derive(Default, Clone)]
pub struct Workflow {
    pub(crate) dag: Dag<WorkUnit>,
    pub(crate) pending: Vec<PatchOperation>,
}

pub enum Status {
    Completed,
    Interrupted,
}

impl From<ExecutionStatus> for Status {
    fn from(status: ExecutionStatus) -> Status {
        match status {
            ExecutionStatus::Completed => Status::Completed,
            ExecutionStatus::Interrupted => Status::Interrupted,
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
    pub async fn execute(
        self,
        system: &mut System,
        interrupted: &AtomicBool,
    ) -> Result<Status, TaskError> {
        self.dag
            .execute(system, interrupted)
            .await
            .map(|s| s.into())
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}
