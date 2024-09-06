use super::{Action, Effect, Method, Task};
use crate::system::Context;

pub(crate) struct BoxedIntoTask<S>(Box<dyn ErasedIntoTask<S>>);

impl<S> BoxedIntoTask<S> {
    pub fn from_action<E, A, T>(effect: E, action: A) -> Self
    where
        E: Effect<S, T>,
        A: Action<S, T>,
        S: 'static,
    {
        Self(Box::new(MakeUnitTask {
            effect,
            action,
            into_task: |effect: E, action: A, context: Context<S>| {
                Task::unit(effect, action, context)
            },
        }))
    }

    pub fn from_method<M, T>(method: M) -> Self
    where
        M: Method<S, T>,
        S: 'static,
    {
        Self(Box::new(MakeGroupTask {
            method,
            into_task: |method: M, context: Context<S>| Task::group(method, context),
        }))
    }

    pub fn into_task(self, context: Context<S>) -> Task<S> {
        self.0.into_task(context)
    }
}

impl<S> Clone for BoxedIntoTask<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

trait ErasedIntoTask<S> {
    fn clone_box(&self) -> Box<dyn ErasedIntoTask<S>>;

    fn into_task(self: Box<Self>, context: Context<S>) -> Task<S>;
}

struct MakeUnitTask<E, A, S> {
    pub(crate) effect: E,
    pub(crate) action: A,
    pub(crate) into_task: fn(E, A, Context<S>) -> Task<S>,
}

impl<S, E, A> ErasedIntoTask<S> for MakeUnitTask<E, A, S>
where
    S: 'static,
    E: Clone + 'static,
    A: Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoTask<S>> {
        Box::new(Self {
            effect: self.effect.clone(),
            action: self.action.clone(),
            into_task: self.into_task,
        })
    }

    fn into_task(self: Box<Self>, context: Context<S>) -> Task<S> {
        (self.into_task)(self.effect.clone(), self.action.clone(), context)
    }
}

struct MakeGroupTask<M, S> {
    pub(crate) method: M,
    pub(crate) into_task: fn(M, Context<S>) -> Task<S>,
}

impl<M, S> ErasedIntoTask<S> for MakeGroupTask<M, S>
where
    S: 'static,
    M: Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoTask<S>> {
        Box::new(Self {
            method: self.method.clone(),
            into_task: self.into_task,
        })
    }

    fn into_task(self: Box<Self>, context: Context<S>) -> Task<S> {
        (self.into_task)(self.method.clone(), context)
    }
}
