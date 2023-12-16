use super::handler::Handler;
use crate::context::Context;

pub struct Action<'system, S, T, E, H = E, R = ()>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
    H: Handler<'system, S, T, R>,
{
    effect: E,
    handler: H,
    context: Context<S>,
    _marker: std::marker::PhantomData<&'system T>,
    _res: std::marker::PhantomData<R>,
}

impl<'system, S, T, E, H, R> Action<'system, S, T, E, H, R>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
    H: Handler<'system, S, T, R>,
{
    pub fn from(effect: E, handler: H, context: Context<S>) -> Self {
        Action {
            effect,
            handler,
            context,
            _marker: std::marker::PhantomData,
            _res: std::marker::PhantomData,
        }
    }

    pub fn effect(self, state: &'system mut S) {
        self.effect.call(state, self.context);
    }

    // Run is the same as effect for now, but eventually
    // we'll have a different handler
    pub fn run(self, state: &'system mut S) {
        self.handler.call(state, self.context);
    }
}
