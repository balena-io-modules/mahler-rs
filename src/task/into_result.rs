use json_patch::Patch;

use super::Task;

use super::effect::{Effect, IntoEffect};
use super::errors::{Error, RuntimeError};
use crate::system::System;

/// The task outcome is a type alias
/// of Result
pub type Result<O> = core::result::Result<O, Error>;

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
            .ok_or_else(|| Error::ConditionFailed)?
    }
}

/// Implement IntoEffect for any effect that has an IntoResult as the output. This means that, for
/// instance, Effect<View<T>> implements IntoEffect, so effects can use extractors to interact
/// with the state
impl<I, E> IntoEffect<Patch, Error, I> for Effect<I, E>
where
    I: IntoResult<Patch> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_effect(self, system: &System) -> Effect<Patch, Error, I> {
        let system = system.clone();
        self.map_err(|e| Error::from(RuntimeError(Box::new(e))))
            .and_then(move |o| o.into_result(&system))
    }
}

// Allow tasks to return a pure Vec<Task>
// and this will convert them into an effect
impl IntoEffect<Vec<Task>, Error> for Vec<Task> {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::of(self)
    }
}

// Allow tasks to return a task slice
// and this will convert them into an effect
impl<const N: usize> IntoEffect<Vec<Task>, Error> for [Task; N] {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::of(self.into())
    }
}
