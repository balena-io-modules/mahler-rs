use json_patch::Patch;
use std::fmt;

use super::boxed::*;
use super::context::Context;
use super::{Handler, Task};

/// The Job degree denotes its cardinality or its position in a search tree
///
/// - Atom jobs are the leafs in the search tree, they define the work to be
///   executed and cannot be expanded
/// - List jobs define work in terms of other tasks, they are expanded recursively
///   in order to get to a list of atoms
#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub(crate) enum Degree {
    List,
    Atom,
}

/// Jobs are generic work definitions. They can be converted to tasks
/// by calling into_task with a specific context.
///
/// Jobs are re-usable
pub struct Job {
    pub(crate) id: &'static str,
    pub(crate) degree: Degree,
    builder: BoxedIntoTask,
}

impl fmt::Debug for Job {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Job<'a> {
            id: &'a str,
            degree: &'a Degree,
        }

        let Self { id, degree, .. } = self;
        fmt::Debug::fmt(&Job { id, degree }, f)
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Job {}

impl Job {
    pub(crate) fn from_action<A, T, I>(action: A) -> Self
    where
        A: Handler<T, Patch, I>,
        I: Send + 'static,
    {
        let id = std::any::type_name::<A>();

        Self {
            id,
            builder: BoxedIntoTask::from_action(action),
            degree: Degree::Atom,
        }
    }

    pub(crate) fn from_method<M, T>(method: M) -> Self
    where
        M: Handler<T, Vec<Task>>,
    {
        let id = std::any::type_name::<M>();
        Self {
            id,
            degree: Degree::List,
            builder: BoxedIntoTask::from_method(method),
        }
    }

    pub fn id(&self) -> &str {
        self.id
    }

    pub fn into_task(&self, context: Context) -> Task {
        self.builder.clone().into_task(self.id, context)
    }
}
