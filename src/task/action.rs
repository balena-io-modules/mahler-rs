use crate::system::{Context, System};
use json_patch::Patch;
use std::future::Future;
use std::pin::Pin;

use super::effect::Effect;
use super::Handler;

type Output = Pin<Box<dyn Future<Output = Patch>>>;
type DryRun<S> = Box<dyn FnOnce(&System, Context<S>) -> Patch>;
type Run<S> = Box<dyn FnOnce(&System, Context<S>) -> Output>;

pub struct Action<S> {
    context: Context<S>,
    dry_run: DryRun<S>,
    run: Run<S>,
}

impl<S> Action<S> {
    pub(crate) fn new<E, H, T>(effect: E, handler: H, context: Context<S>) -> Self
    where
        E: Effect<S, T>,
        H: Handler<S, T>,
    {
        Self {
            context,
            dry_run: Box::new(|system: &System, context: Context<S>| {
                effect.call(system.clone(), context)
            }),
            run: Box::new(|system: &System, context: Context<S>| {
                Box::pin(handler.call(system.clone(), context))
            }),
        }
    }

    pub fn dry_run(self, system: &System) -> Patch {
        (self.dry_run)(system, self.context)
    }

    pub async fn run(self, system: &mut System) {
        let changes = (self.run)(system, self.context).await;
        // TODO: return result
        system.patch(changes).unwrap();
    }
}

pub(crate) trait ToAction<S> {
    fn to_action(&self, context: Context<S>) -> Action<S>;
}

pub(crate) struct ActionBuilder<E, H, S> {
    pub(crate) effect: E,
    pub(crate) handler: H,
    pub(crate) build: fn(E, H, Context<S>) -> Action<S>,
}

impl<E, H, S> ToAction<S> for ActionBuilder<E, H, S>
where
    E: Clone,
    H: Clone,
{
    fn to_action(&self, context: Context<S>) -> Action<S> {
        (self.build)(self.effect.clone(), self.handler.clone(), context)
    }
}
