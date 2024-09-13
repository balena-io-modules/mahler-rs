use json_patch::Patch;

use super::boxed::*;
use super::{Handler, Task};
use crate::system::Context;

fn get_id_and_name<T>() -> (String, String) {
    let id = std::any::type_name::<T>();

    // Find and cut the rest of the path
    let name = match &id[..id.len() - 3].rfind(':') {
        Some(pos) => &id[pos + 1..id.len()],
        None => &id[..id.len()],
    };

    (id.to_string(), name.to_string())
}

/// Jobs are generic work definitions. They can be converted to tasks
/// by calling into_task with a specific context.
///
/// Jobs are re-usable
pub struct Job<S> {
    id: String,
    description: Box<dyn Fn(Context<S>) -> String>,
    builder: BoxedIntoTask<S>,
}

impl<S> Job<S> {
    pub(crate) fn from_action<A, T, I>(action: A) -> Self
    where
        A: Handler<S, T, Patch, I>,
        S: 'static,
        I: 'static,
    {
        let (id, name) = get_id_and_name::<A>();

        Self {
            id,
            description: Box::new(move |context| {
                let path = context.path;
                if path.to_str().is_empty() {
                    return name.clone();
                }
                format!("{}: {}", name, path)
            }),
            builder: BoxedIntoTask::from_action(action),
        }
    }

    pub(crate) fn from_method<M, T>(method: M) -> Self
    where
        M: Handler<S, T, Vec<Task<S>>>,
        S: 'static,
    {
        let (id, name) = get_id_and_name::<M>();
        Self {
            id,
            description: Box::new(move |context| {
                let path = context.path;
                if path.to_str().is_empty() {
                    return name.clone();
                }
                format!("{}: {}", name, path)
            }),
            builder: BoxedIntoTask::from_method(method),
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn description(&self, context: Context<S>) -> String {
        (self.description)(context)
    }

    pub fn into_task(&self, context: Context<S>) -> Task<S> {
        self.builder.clone().into_task(context)
    }
}
