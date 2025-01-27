use json_patch::PatchOperation;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};

use crate::dag::Dag;
use crate::task::Task;

#[derive(Hash)]
struct ActionId<'s> {
    /// The task id
    task: String,
    /// The task path
    path: String,
    /// The state that is used to test the action
    state: &'s Value,
}

#[derive(Clone)]
pub(crate) struct Action {
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
    pub task: Task,
}

impl Action {
    pub fn new(id: u64, task: Task) -> Self {
        Self { id, task }
    }

    pub fn new_id(task: &Task, state: &Value) -> Result<u64, jsonptr::resolve::ResolveError> {
        let pointer = task.context().path.as_ref();

        // Resolve the value that will be modified by
        // the task
        let state = pointer.resolve(state)?;

        let action_id = ActionId {
            task: String::from(task.id()),
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

impl Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.task.fmt(f)
    }
}

#[derive(Default, Clone)]
pub struct Workflow {
    pub(crate) dag: Dag<Action>,
    pub(crate) pending: Vec<PatchOperation>,
}

impl Workflow {
    pub(crate) fn as_dag(&self) -> &Dag<Action> {
        &self.dag
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}
