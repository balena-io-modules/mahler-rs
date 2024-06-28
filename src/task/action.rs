use std::marker::PhantomData;

use crate::state::Context;

use super::effect::Effect;
use super::Handler;

pub struct Action<S, T, E, A> {
    effect: E,
    action: A,
    context: Context<S>,
    _args: PhantomData<T>,
}

impl<'system, S, T, E, A> Action<S, T, E, A>
where
    E: Effect<'system, S, T>,
    A: Handler<'system, S, T>,
{
    pub fn new(effect: E, action: A, context: Context<S>) -> Self {
        Action {
            effect,
            action,
            context,
            _args: PhantomData::<T>,
        }
    }

    pub fn dry_run(self, state: &'system mut S) {
        self.effect.call(state, &self.context);
    }

    pub fn run(self, state: &'system mut S) -> A::Future {
        self.action.call(state, &self.context)
    }
}
