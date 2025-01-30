use json_patch::Patch;
use serde::Serialize;

use super::{Context, Effect, IntoEffect, IntoResult, Job, Task};
use crate::error::{Error, IntoError};
use crate::system::{FromSystem, System};

pub trait Handler<T, O, I = O>: Clone + Send + 'static {
    fn call(&self, system: &System, context: &Context) -> Effect<O, Error, I>;

    fn into_job(self) -> Job;

    fn into_task(self, context: Context) -> Task {
        self.into_job().into_task(context)
    }

    fn try_target<S: Serialize>(self, target: S) -> Result<Task, Error> {
        let context = Context::new().try_target(target)?;
        Ok(self.into_job().into_task(context))
    }

    fn with_target<S: Serialize>(self, target: S) -> Task {
        let context = Context::new().try_target(target).unwrap();
        self.into_job().into_task(context)
    }
}

/// Implement IntoEffect for any effect that has an IntoResult as the output. This means that, for
/// instance, Effect<View<T>> implements into effect, so effects can use extractors to interact
/// with the state
impl<I: IntoResult<Patch> + 'static, E: std::error::Error + 'static> IntoEffect<Patch, Error, I>
    for Effect<I, E>
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

impl<E: std::error::Error + 'static> IntoEffect<Vec<Task>, Error> for Result<Vec<Task>, E> {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::from(self.map_err(|e| Error::Other(Box::new(e))))
    }
}

impl<const N: usize> IntoEffect<Vec<Task>, Error> for [Task; N] {
    fn into_effect(self, _: &System) -> Effect<Vec<Task>, Error> {
        Effect::of(self.into())
    }
}

impl<E: std::error::Error + 'static, const N: usize> IntoEffect<Vec<Task>, Error>
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
            F: Fn($($ty,)*) -> Res + Clone + Send +'static,
            Res: IntoEffect<Patch, Error, I>,
            $($ty: FromSystem,)*
            I: 'static
        {

            fn call(&self, system: &System, context: &Context) -> Effect<Patch, Error, I>{
                $(
                    let $ty = match $ty::from_system(system, context) {
                        Ok(value) => value,
                        Err(failure) => return Effect::from_error(failure.into_error())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_effect(&system)
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
            F: Fn($($ty,)*) -> Res + Clone + Send +'static,
            Res: IntoEffect<Vec<Task>, Error>,
            $($ty: FromSystem,)*
        {

            fn call(&self, system: &System, context: &Context) -> Effect<Vec<Task>, Error> {
                $(
                    let $ty = match $ty::from_system(system, context) {
                        Ok(value) => value,
                        Err(failure) => return Effect::from_error(failure.into_error())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_effect(&system)
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
