use super::boxed::*;
use super::{Handler, Method, Task};
use crate::system::Context;

/// Jobs are generic work definitions. They can be converted to tasks
/// by calling into_task with a specific context.
///
/// Jobs are re-usable
pub struct Job<S> {
    builder: BoxedIntoTask<S>,
}

impl<S> Job<S> {
    pub(crate) fn from_handler<H, T, I>(handler: H) -> Self
    where
        H: Handler<S, T, I>,
        S: 'static,
        I: 'static,
    {
        Self {
            builder: BoxedIntoTask::from_handler(handler),
        }
    }

    pub(crate) fn from_method<M, T>(method: M) -> Self
    where
        M: Method<S, T>,
        S: 'static,
    {
        Self {
            builder: BoxedIntoTask::from_method(method),
        }
    }

    pub fn into_task(&self, context: Context<S>) -> Task<S> {
        self.builder.clone().into_task(context)
    }
}
