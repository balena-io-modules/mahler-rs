use json_patch::Patch;

use super::boxed::*;
use super::{Handler, Task};
use crate::system::Context;

/// Jobs are generic work definitions. They can be converted to tasks
/// by calling into_task with a specific context.
///
/// Jobs are re-usable
pub struct Job<S> {
    id: String,
    builder: BoxedIntoTask<S>,
}

impl<S> Job<S> {
    pub(crate) fn from_action<A, T, I>(action: A) -> Self
    where
        A: Handler<S, T, Patch, I>,
        S: 'static,
        I: 'static,
    {
        let id = std::any::type_name::<A>().to_string();

        Self {
            id,
            builder: BoxedIntoTask::from_action(action),
        }
    }

    pub(crate) fn from_method<M, T>(method: M) -> Self
    where
        M: Handler<S, T, Vec<Task<S>>>,
        S: 'static,
    {
        let id = std::any::type_name::<M>().to_string();
        Self {
            id,
            builder: BoxedIntoTask::from_method(method),
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn into_task(&self, context: Context<S>) -> Task<S> {
        self.builder.clone().into_task(self.id.clone(), context)
    }
}
