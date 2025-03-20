use json_patch::Patch;
use serde::Serialize;

use super::job::Job;
use super::{Context, Effect, IntoEffect, IntoResult, Task};
use crate::error::Error;
use crate::system::{FromSystem, System};

pub trait Handler<T, O, I = O>: Clone + Sync + Send + 'static {
    fn call(&self, system: &System, context: &Context) -> Effect<O, Error, I>;

    /// Get the unique identifier of the job handler
    fn id(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Convert the handler into a job
    fn into_job(self) -> Job;

    /// Return true if the handler can be parallelized
    ///
    /// This means the handler only scoped extractors and
    /// no global extractors, allowing it to be used in parallel
    /// with other handler with non-conflicting scope.
    fn is_scoped(&self) -> bool;

    /// Create a task from the handler
    ///
    /// This is a convenience function that is equivalent to
    /// calling `into_job().to_task(context)`
    fn into_task(self) -> Task {
        self.into_job().build_task(Context::new())
    }

    /// Create a task from the handler with a specific target
    ///
    /// Important: This function will panic if serialization of the target into JSON fails
    /// Use `into_task().try_target()` if you want to handle the error. This is done for convenience as
    /// serialization errors should be rare and this makes the code more concise.
    fn with_target<S: Serialize>(self, target: S) -> Task {
        self.into_task().with_target(target)
    }

    /// Create a task from the handler with a specific path argument
    fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Task {
        self.into_task().with_arg(key, value)
    }
}

/// Implement IntoEffect for any effect that has an IntoResult as the output. This means that, for
/// instance, Effect<View<T>> implements into effect, so effects can use extractors to interact
/// with the state
impl<I: IntoResult<Patch> + Send + 'static, E: std::error::Error + Send + Sync + 'static>
    IntoEffect<Patch, Error, I> for Effect<I, E>
{
    fn into_effect(self, system: &System) -> Effect<Patch, Error, I> {
        let system = system.clone();
        self.map_err(|e| Error::Other(Box::new(e)))
            .and_then(move |o| o.into_result(&system))
    }
}

impl IntoEffect<Vec<Task>, Error> for Vec<Task> {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::of(self)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> IntoEffect<Vec<Task>, Error>
    for Result<Vec<Task>, E>
{
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::from(self.map_err(|e| Error::Other(Box::new(e))))
    }
}

impl<const N: usize> IntoEffect<Vec<Task>, Error> for [Task; N] {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::of(self.into())
    }
}

impl<E: std::error::Error + Send + Sync + 'static, const N: usize> IntoEffect<Vec<Task>, Error>
    for Result<[Task; N], E>
{
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::from(
            self.map_err(|e| Error::Other(Box::new(e)))
                .map(|a| a.into()),
        )
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, $($ty,)* Res, I> Handler<($($ty,)*), Patch, I> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: IntoEffect<Patch, Error, I> + Send,
            $($ty: FromSystem,)*
            I: Send + 'static
        {

            fn call(&self, system: &System, context: &Context) -> Effect<Patch, Error, I>{
                $(
                    let $ty = match $ty::from_system(system, context) {
                        Ok(value) => value,
                        // TODO: communicate the function name as part of the error
                        Err(failure) => return Effect::from_error(failure.into())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_effect(system)
            }

            fn is_scoped(&self) -> bool {
                // The handler is scoped if all of its arguments are scoped
                true $(&& $ty::is_scoped())*
            }

            fn into_job(self) -> Job {
                Job::from_action(self)
            }
        }
    };
}

impl_action_handler!(T1,);
impl_action_handler!(T1, T2);
impl_action_handler!(T1, T2, T3);
impl_action_handler!(T1, T2, T3, T4);
impl_action_handler!(T1, T2, T3, T4, T5);
impl_action_handler!(T1, T2, T3, T4, T5, T6);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

macro_rules! impl_method_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, $($ty,)* Res> Handler<($($ty,)*), Vec<Task>> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: IntoEffect<Vec<Task>, Error>,
            $($ty: FromSystem,)*
        {

            fn call(&self, system: &System, context: &Context) -> Effect<Vec<Task>, Error> {
                $(
                    let $ty = match $ty::from_system(system, context) {
                        Ok(value) => value,
                        Err(failure) => return Effect::from_error(failure.into())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_effect(system)
            }

            fn is_scoped(&self) -> bool {
                // The handler is scoped if all of its arguments are scoped
                true $(&& $ty::is_scoped())*
            }

            fn into_job(self) -> Job {
                Job::from_method(self)
            }
        }
    };
}

impl_method_handler!(T1,);
impl_method_handler!(T1, T2);
impl_method_handler!(T1, T2, T3);
impl_method_handler!(T1, T2, T3, T4);
impl_method_handler!(T1, T2, T3, T4, T5);
impl_method_handler!(T1, T2, T3, T4, T5, T6);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
