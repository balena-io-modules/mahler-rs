mod action;
mod handler;

pub use action::*;

use crate::context::Context;
use handler::Handler;

pub struct Task<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    action: H,
    _state: std::marker::PhantomData<&'system S>,
    _args: std::marker::PhantomData<T>,
}

impl<'system, S, T, H> Task<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    pub fn from(handler: H) -> Self {
        Task {
            action: handler,
            _state: std::marker::PhantomData,
            _args: std::marker::PhantomData,
        }
    }

    pub fn bind(self, context: Context<S>) -> Action<'system, S, T, H> {
        Action::from(self.action, context)
    }
}
