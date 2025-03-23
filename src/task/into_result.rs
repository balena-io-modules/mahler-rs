use json_patch::Patch;
use std::future::Future;

use super::Task;

use super::effect::{Effect, IntoEffect};
use super::errors::{Error, RuntimeError};
use crate::system::System;

pub trait IntoResult<O> {
    fn into_result(self, system: &System) -> Result<O, Error>;
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O, Error> {
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

pub(crate) trait Immutable {
    fn immutable() -> Self;
}

/// Implement IntoEffect for any function returning a future that can be resolved
/// into a result. This means that the function only has an async part and
/// will never be chosen during planning because it doesn't perform any changes
/// to the system. However it could be used in a method along other operations
impl<I, E, Fut> IntoEffect<Patch, Error, I> for Fut
where
    Fut: Future<Output = Result<I, E>> + Send + 'static,
    I: IntoResult<Patch> + Immutable + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_effect(self, system: &System) -> Effect<Patch, Error, I> {
        let system = system.clone();
        // The only way to make this work is to introduce this
        // immutable trait that tells the type I to ignore the
        // call to into_result and just return an empty patch
        // this is a really dumb hack, but it works
        Effect::of(I::immutable())
            .with_io(move |_| self)
            .map_err(|e| Error::from(RuntimeError(Box::new(e))))
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
