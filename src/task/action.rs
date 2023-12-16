use super::handler::Handler;
use crate::context::Context;

pub struct Action<'system, S, T, E>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
{
    effect: E,
    context: Context<S>,
    _marker: std::marker::PhantomData<&'system T>,
}

impl<'system, S, T, E> Action<'system, S, T, E>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
{
    pub fn from(effect: E, context: Context<S>) -> Self {
        Action {
            effect,
            context,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn effect(self, state: &'system mut S) {
        self.effect.call(state, self.context);
    }

    // Run is the same as effect for now, but eventually
    // we'll have a different handler
    pub fn run(self, state: &'system mut S) {
        self.effect.call(state, self.context);
    }
}
