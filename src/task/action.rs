use std::marker::PhantomData;

use crate::system::{Context, System};
use json_patch::Patch;

use super::effect::Effect;
use super::Handler;

pub struct Action<S, T, E, A> {
    effect: E,
    action: A,
    context: Context<S>,
    _args: PhantomData<T>,
}

impl<S, T, E, A> Action<S, T, E, A>
where
    E: Effect<S, T>,
    A: Handler<S, T>,
    S: Clone,
{
    pub fn new(effect: E, action: A, context: Context<S>) -> Self {
        Action {
            effect,
            action,
            context,
            _args: PhantomData::<T>,
        }
    }

    /// Dry run produces a list of changes that the action
    /// performs
    pub fn dry_run(self, system: &System) -> Patch {
        self.effect.call(system.clone(), self.context)
    }

    pub async fn run(self, system: &mut System) {
        let sys_writer = self.action.call(system.clone(), self.context.clone()).await;
        sys_writer.write(system);
    }
}
