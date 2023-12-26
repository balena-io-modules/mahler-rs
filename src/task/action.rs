use super::handler::Handler;
use crate::{entity::Entity, state::State};

pub struct Action<'system, S, T, E, H = E, R = ()>
where
    S: Entity + Clone,
    E: Handler<'system, S, T, ()>,
    H: Handler<'system, S, T, R>,
{
    effect: E,
    handler: H,
    target: S,
    _marker: std::marker::PhantomData<&'system T>,
    _res: std::marker::PhantomData<R>,
}

impl<'system, S, T, E, H, R> Action<'system, S, T, E, H, R>
where
    S: Entity + Clone,
    E: Handler<'system, S, T, ()>,
    H: Handler<'system, S, T, R>,
{
    pub fn from(effect: E, handler: H, target: S) -> Self {
        Action {
            effect,
            handler,
            target,
            _marker: std::marker::PhantomData,
            _res: std::marker::PhantomData,
        }
    }

    pub fn effect(self, state: &'system mut State) {
        self.effect.call(state, &self.target);
    }

    // Run is the same as effect for now, but eventually
    // we'll have a different handler
    pub fn run(self, state: &'system mut State) {
        self.handler.call(state, &self.target);
    }
}
