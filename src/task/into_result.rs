use json_patch::Patch;

use super::Task;

use super::condition_failed::ConditionFailed;
use super::effect::{Effect, IntoEffect};
use super::errors::TaskError;
use crate::system::System;

/// The task outcome is a type alias
/// of Result
pub type Result<O> = core::result::Result<O, TaskError>;

pub trait IntoResult<O> {
    fn into_result(self, system: &System) -> Result<O>;
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O> {
        self.map(|value| value.into_result(system))
            .ok_or_else(ConditionFailed::default)?
    }
}

/// Implement IntoEffect for any effect that has an IntoResult as the output. This means that, for
/// instance, Effect<View<T>> implements IntoEffect, so effects can use extractors to interact
/// with the state
impl<I, E> IntoEffect<Patch, TaskError, I> for Effect<I, E>
where
    I: IntoResult<Patch> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_effect(self, system: &System) -> Effect<Patch, TaskError, I> {
        let system = system.clone();
        self.map_err(|e| TaskError::Other(Box::new(e)))
            .and_then(move |o| o.into_result(&system))
    }
}

/// Implement IntoEffect for any Result that has an IntoResult as the output. This means that, for
/// instance, Result<View<T>, E> can be directly returned by the handler and will be converted into
/// an effect
impl<I> IntoEffect<Patch, TaskError, I> for core::result::Result<I, ConditionFailed>
where
    I: IntoResult<Patch> + Send + 'static,
{
    fn into_effect(self, system: &System) -> Effect<Patch, TaskError, I> {
        let system = system.clone();
        let value = self.map_err(TaskError::from);
        Effect::from(value)
            .with_io(|o| async move { Ok(o) })
            .and_then(move |o| o.into_result(&system))
    }
}

// Allow tasks to return a pure Vec<Task>
// and this will convert them into an effect
impl IntoEffect<Vec<Task>, TaskError> for Vec<Task> {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, TaskError> {
        Effect::of(self)
    }
}

// Allow tasks to return a  Result<Vec<Task>, E>
// and this will convert them into an effect
impl<E> IntoEffect<Vec<Task>, TaskError> for core::result::Result<Vec<Task>, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, TaskError> {
        Effect::from(self.map_err(|e| TaskError::Other(Box::new(e))))
    }
}

// Allow tasks to return a task slice
// and this will convert them into an effect
impl<const N: usize> IntoEffect<Vec<Task>, TaskError> for [Task; N] {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, TaskError> {
        Effect::of(self.into())
    }
}

// Allow tasks to return a Result<[Task; N], E>
// and this will convert them into an effect
impl<E, const N: usize> IntoEffect<Vec<Task>, TaskError> for core::result::Result<[Task; N], E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, TaskError> {
        Effect::from(
            self.map_err(|e| TaskError::Other(Box::new(e)))
                .map(|a| a.into()),
        )
    }
}
