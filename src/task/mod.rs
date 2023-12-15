mod action;
mod handler;

pub use action::*;

use crate::context::Context;
use handler::Handler;

pub trait Task<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    fn get_handler(&self) -> H;

    fn bind(&self, context: Context<S>) -> Action<'system, S, T, H> {
        Action::from(self.get_handler(), context)
    }
}

pub struct ActionTask<'system, S, T, H> {
    handler: H,
    _system: std::marker::PhantomData<&'system S>,
    _args: std::marker::PhantomData<T>,
}

impl<'system, S, T, H> ActionTask<'system, S, T, H> {
    pub fn from(handler: H) -> Self {
        ActionTask {
            handler,
            _system: std::marker::PhantomData::<&'system S>,
            _args: std::marker::PhantomData::<T>,
        }
    }
}

impl<'system, S, T, H> Task<'system, S, T, H> for ActionTask<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    fn get_handler(&self) -> H {
        self.handler.clone()
    }
}
