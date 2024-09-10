use json_patch::Patch;

use super::effect::Effect;
use super::{IntoEffect, IntoResult, Job, Task};
use crate::error::{Error, IntoError};
use crate::system::{Context, FromSystem, System};

pub trait Handler<S, T, I>: Clone + Send + 'static {
    fn call(self, system: &System, context: Context<S>) -> Effect<Patch, Error, I>;

    fn into_job(self) -> Job<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
        I: 'static,
    {
        Job::from_handler(self)
    }

    fn into_task(self, context: Context<S>) -> Task<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
        I: 'static,
    {
        self.into_job().into_task(context)
    }
}

impl<O, E, I> IntoEffect<O, E, I> for Effect<O, E, I> {
    fn into_effect(self, _: &System) -> Effect<O, E, I> {
        self
    }
}

impl<I: IntoResult<Patch> + 'static, E: std::error::Error + 'static> IntoEffect<Patch, Error, I>
    for Effect<I, E>
{
    fn into_effect(self, system: &System) -> Effect<Patch, Error, I> {
        let system = system.clone();
        self.map_err(|e| Error::Other(Box::new(e)))
            .and_then(move |o| o.into_result(&system))
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Res, I> Handler<S, ($($ty,)*), I> for F
        where
            F: FnOnce($($ty,)*) -> Res + Clone + Send +'static,
            Res: IntoEffect<Patch, Error, I>,
            $($ty: FromSystem<S>,)*
            I: 'static
        {

            fn call(self, system: &System, context: Context<S>) -> Effect<Patch, Error, I>{
                $(
                    let $ty = match $ty::from_system(&system, &context) {
                        Ok(value) => value,
                        Err(failure) => return Effect::error(failure.into_error())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_effect(&system)
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
