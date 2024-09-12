use super::{Handler, Method, Task};
use crate::system::Context;

pub(crate) struct BoxedIntoTask<S>(Box<dyn ErasedIntoTask<S>>);

impl<S> BoxedIntoTask<S> {
    pub fn from_handler<H, T, I>(handler: H) -> Self
    where
        H: Handler<S, T, I>,
        S: 'static,
        I: 'static,
    {
        Self(Box::new(MakeAtomTask {
            handler,
            into_task: |handler: H, context: Context<S>| Task::atom(handler, context),
        }))
    }

    pub fn from_method<M, T>(method: M) -> Self
    where
        M: Method<S, T>,
        S: 'static,
    {
        Self(Box::new(MakeListTask {
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

struct MakeAtomTask<H, S> {
    pub(crate) handler: H,
    pub(crate) into_task: fn(H, Context<S>) -> Task<S>,
}

impl<S, H> ErasedIntoTask<S> for MakeAtomTask<H, S>
where
    S: 'static,
    H: Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoTask<S>> {
        Box::new(Self {
            handler: self.handler.clone(),
            into_task: self.into_task,
        })
    }

    fn into_task(self: Box<Self>, context: Context<S>) -> Task<S> {
        (self.into_task)(self.handler.clone(), context)
    }
}

struct MakeListTask<M, S> {
    pub(crate) method: M,
    pub(crate) into_task: fn(M, Context<S>) -> Task<S>,
}

impl<M, S> ErasedIntoTask<S> for MakeListTask<M, S>
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
