use json_patch::Patch;

use super::{Context, Handler, Task};

pub(crate) struct BoxedIntoTask(Box<dyn ErasedIntoTask>);

impl BoxedIntoTask {
    pub fn from_action<A, T, I>(action: A) -> Self
    where
        A: Handler<T, Patch, I>,
        I: 'static,
    {
        Self(Box::new(MakeIntoTask {
            handler: action,
            into_task: |id, handler, context| Task::atom(id, handler, context),
        }))
    }

    pub fn from_method<M, T>(method: M) -> Self
    where
        M: Handler<T, Vec<Task>>,
    {
        Self(Box::new(MakeIntoTask {
            handler: method,
            into_task: |id, method, context| Task::list(id, method, context),
        }))
    }

    pub fn into_task(self, id: &'static str, context: Context) -> Task {
        self.0.into_task(id, context)
    }
}

impl Clone for BoxedIntoTask {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

trait ErasedIntoTask: Send {
    fn clone_box(&self) -> Box<dyn ErasedIntoTask>;

    fn into_task(self: Box<Self>, id: &'static str, context: Context) -> Task;
}

struct MakeIntoTask<H> {
    pub(crate) handler: H,
    pub(crate) into_task: fn(&'static str, H, Context) -> Task,
}

impl<H> ErasedIntoTask for MakeIntoTask<H>
where
    H: Send + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn ErasedIntoTask> {
        Box::new(Self {
            handler: self.handler.clone(),
            into_task: self.into_task,
        })
    }

    fn into_task(self: Box<Self>, id: &'static str, context: Context) -> Task {
        (self.into_task)(id, self.handler, context)
    }
}
