use json_patch::Patch;

use super::{Context, Handler, Task};

pub(crate) struct BoxedIntoTask(Box<dyn ErasedIntoTask>);

impl BoxedIntoTask {
    pub fn from_action<A, T, I>(action: A) -> Self
    where
        A: Handler<T, Patch, I>,
        I: Send + 'static,
    {
        Self(Box::new(MakeIntoTask {
            handler: action,
            into_task: |handler, context| Task::from_action(handler, context),
        }))
    }

    pub fn from_method<M, T>(method: M) -> Self
    where
        M: Handler<T, Vec<Task>>,
    {
        Self(Box::new(MakeIntoTask {
            handler: method,
            into_task: |handler, context| Task::from_method(handler, context),
        }))
    }

    pub fn into_task(self, context: Context) -> Task {
        self.0.into_task(context)
    }
}

impl Clone for BoxedIntoTask {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

trait ErasedIntoTask: Send + Sync {
    fn clone_box(&self) -> Box<dyn ErasedIntoTask>;

    fn into_task(self: Box<Self>, context: Context) -> Task;
}

struct MakeIntoTask<H> {
    pub(crate) handler: H,
    pub(crate) into_task: fn(H, Context) -> Task,
}

impl<H> ErasedIntoTask for MakeIntoTask<H>
where
    H: Send + Sync + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoTask> {
        Box::new(Self {
            handler: self.handler.clone(),
            into_task: self.into_task,
        })
    }

    fn into_task(self: Box<Self>, context: Context) -> Task {
        (self.into_task)(self.handler, context)
    }
}
