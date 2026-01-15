use crate::error::{Error, ErrorKind};
use crate::json::Patch;
use crate::result::Result;

use super::effect::Effect;
use super::Task;

pub trait IntoResult<O> {
    fn into_result(self) -> Result<O>;
}

/// Allow tasks to return an Option
impl<T, O> IntoResult<O> for Option<T>
where
    T: IntoResult<O>,
{
    fn into_result(self) -> Result<O> {
        self.map(|value| value.into_result())
            .ok_or(ErrorKind::ConditionNotMet)?
    }
}

impl From<()> for Effect<Patch, Error, ()> {
    fn from(value: ()) -> Effect<Patch, Error, ()> {
        Effect::of(value)
            .with_io(|value| async move { Ok(value) })
            .map(|_| Patch(vec![]))
    }
}

/// Allow tasks to return a value that implements
/// `IntoResult<Patch>`, e.g. View
impl<I> From<I> for Effect<Patch, Error, I>
where
    I: IntoResult<Patch> + Send + 'static,
{
    fn from(value: I) -> Effect<Patch, Error, I> {
        Effect::of(value).and_then(move |o| o.into_result())
    }
}

// Allow tasks to return a pure `Vec<Task>`
// and this will convert them into an effect
impl From<Vec<Task>> for Effect<Vec<Task>, Error> {
    fn from(vec: Vec<Task>) -> Effect<Vec<Task>, Error> {
        Effect::of(vec)
    }
}

// Allow methods to return a task slice
// and this will convert them into an effect
impl<const N: usize> From<[Task; N]> for Effect<Vec<Task>, Error> {
    fn from(slice: [Task; N]) -> Effect<Vec<Task>, Error> {
        Effect::of(slice.into())
    }
}

// Allow methods to return a single task
impl From<Task> for Effect<Vec<Task>, Error> {
    fn from(task: Task) -> Effect<Vec<Task>, Error> {
        Effect::of(vec![task])
    }
}

impl<T> From<Option<T>> for Effect<Vec<Task>, Error>
where
    T: Into<Effect<Vec<Task>, Error>>,
{
    fn from(opt: Option<T>) -> Effect<Vec<Task>, Error> {
        opt.map(|t| t.into())
            .unwrap_or_else(|| Effect::from_error(ErrorKind::ConditionNotMet.into()))
    }
}
