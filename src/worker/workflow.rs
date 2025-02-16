use json_patch::PatchOperation;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;

use crate::dag::{Dag, Node};
use crate::system::System;
use crate::task::{IntoTask, Task};
use crate::{Error, IntoError};

#[derive(Debug)]
pub struct Interrupted {}
impl std::error::Error for Interrupted {}
impl Display for Interrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "execution of the workflow was interrupted")
    }
}
impl IntoError for Interrupted {
    fn into_error(self) -> Error {
        Error::WorkflowInterrupted(self)
    }
}

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

impl IntoTask for Action {
    fn into_task(self) -> Task {
        self.task
    }
}

impl Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.task.fmt(f)
    }
}

impl<T: IntoTask + Clone> Dag<T> {
    pub async fn execute(self, system: &mut System, interrupted: &AtomicBool) -> Result<(), Error> {
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

            let task = value.into_task();
            task.run(system).await?;

            // Check if the task was interrupted before continuing
            if interrupted.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(Interrupted {})?;
            }
        }
        Ok(())
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
    ) -> Result<(), Error> {
        self.dag.execute(system, interrupted).await
    }
}

impl Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.dag.fmt(f)
    }
}
