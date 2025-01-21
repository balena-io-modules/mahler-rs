use crate::task::{Handler, Job};
use std::cmp::Ordering;

#[derive(PartialEq, PartialOrd, Eq, Ord, Debug, Clone)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Any,
    None,
}

pub struct Intent {
    pub(crate) operation: Operation,
    pub(crate) job: Job,
    priority: u8,
}

impl Intent {
    pub(crate) fn new(job: Job) -> Self {
        Intent {
            operation: Operation::Update,
            job,
            priority: u8::MAX,
        }
    }

    /// Set intent priority.
    ///
    /// This defines search priority when looking for jobs
    /// the lower the value, the higher the priority
    pub fn with_priority(self, priority: u8) -> Self {
        let Intent { operation, job, .. } = self;
        Intent {
            operation,
            job,
            priority,
        }
    }

    fn with_operation(self, operation: Operation) -> Self {
        let Intent { priority, job, .. } = self;
        Intent {
            operation,
            job,
            priority,
        }
    }
}

macro_rules! define_intent {
    ($func_name:ident, $operation:expr) => {
        pub fn $func_name<H, T, O, I>(handler: H) -> Intent
        where
            H: Handler<T, O, I>,
            I: 'static,
        {
            Intent::new(handler.into_job()).with_operation($operation)
        }
    };
}

define_intent!(create, Operation::Create);
define_intent!(update, Operation::Update);
define_intent!(delete, Operation::Delete);
define_intent!(any, Operation::Any);
define_intent!(none, Operation::None);

impl PartialEq for Intent {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Intent {}

impl PartialOrd for Intent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Intent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.job
            .cmp(&other.job)
            .then(self.operation.cmp(&other.operation))
            .then(self.priority.cmp(&other.priority))
    }
}
