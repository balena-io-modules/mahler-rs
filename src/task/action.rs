use super::Handler;
use crate::context::Context;

pub struct Action<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    handler: H,
    context: Context<S>,
    _marker: std::marker::PhantomData<&'system T>,
}

impl<'system, S, T, H> Action<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    pub fn from(handler: H, context: Context<S>) -> Self {
        Action {
            handler,
            context,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn run(self, state: &'system mut S) {
        self.handler.call(state, self.context);
    }
}
