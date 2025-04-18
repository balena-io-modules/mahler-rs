use super::context::Context;
use super::description::Description;
use super::handler::Handler;
use super::Task;
use std::cmp::Ordering;

#[derive(PartialEq, PartialOrd, Eq, Ord, Debug, Clone)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Any,
    None,
}

/// Jobs are generic work definitions.
///
/// They are assignable to an operation and can be given a priority
#[derive(Debug, Clone)]
pub struct Job {
    operation: Operation,
    task: Task,
    priority: u8,
}

impl Job {
    pub(crate) fn new(task: Task) -> Self {
        Job {
            operation: Operation::Update,
            task,
            // all tasks have the lowest priority
            priority: 0,
        }
    }

    pub fn id(&self) -> &str {
        self.task.id()
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    /// Set job priority.
    ///
    /// This defines search priority when looking for jobs
    /// the lower the value, the higher the priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    fn with_operation(mut self, operation: Operation) -> Self {
        self.operation = operation;
        self
    }

    pub fn with_description<D, T>(mut self, description: D) -> Self
    where
        D: Description<T>,
    {
        self.task = self.task.with_description(description);
        self
    }

    pub(crate) fn clone_task(&self, context: Context) -> Task {
        self.task.clone().with_context(context)
    }
}

macro_rules! define_job {
    ($func_name:ident, $operation:expr) => {
        pub fn $func_name<H, T, O, I>(handler: H) -> Job
        where
            H: Handler<T, O, I>,
            I: 'static,
        {
            Job::new(handler.into_task()).with_operation($operation)
        }
    };
}

define_job!(create, Operation::Create);
define_job!(update, Operation::Update);
define_job!(delete, Operation::Delete);
define_job!(any, Operation::Any);
define_job!(none, Operation::None);

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.task.id() == other.task.id()
            && self.operation == other.operation
            && self.priority == other.priority
    }
}
impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task
            .degree()
            .cmp(&other.task.degree())
            .then(self.operation.cmp(&other.operation))
            // lower priority is better
            .then(other.priority.cmp(&self.priority))
            .then(self.task.id().cmp(other.task.id()))
    }
}
