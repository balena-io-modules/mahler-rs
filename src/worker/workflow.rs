use json_patch::PatchOperation;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;

use super::RuntimeError;
use crate::dag::{Dag, Node};
use crate::system::System;
use crate::task::Action;

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
    pub task: Action,
}

impl WorkUnit {
    pub fn new(id: u64, task: Action) -> Self {
        Self { id, task }
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

impl From<WorkUnit> for Action {
    fn from(action: WorkUnit) -> Action {
        action.task
    }
}

impl Display for WorkUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.task.fmt(f)
    }
}

impl<T: Into<Action> + Clone> Dag<T> {
    pub(crate) async fn execute(
        self,
        system: &mut System,
        interrupted: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        // TODO: implement parallel execution of the DAG
        for node in self.iter() {
            let value = {
                if let Node::Item { value, .. } = &*node.read().unwrap() {
                    // This clone is necessary for now because of the iterator, but a future version
                    // of execute will just consume the DAG, avoiding the need
                    // for cloning
                    value.clone()
                } else {
                    continue;
                }
            };

            let task = value.into();
            task.run(system).await?;

            // Check if the task was interrupted before continuing
            if interrupted.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(RuntimeError::Interrupted)?;
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct Workflow {
    pub(crate) dag: Dag<WorkUnit>,
    pub(crate) pending: Vec<PatchOperation>,
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

    pub(crate) async fn execute(
        self,
        system: &mut System,
        interrupted: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        self.dag.execute(system, interrupted).await
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}
