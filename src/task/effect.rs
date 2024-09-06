use json_patch::Patch;
use std::{
    future::{ready, Ready},
    marker::PhantomData,
};

use super::{action::Action, Job, Task};
use crate::error::IntoError;
use crate::{
    system::{Context, FromSystem, System},
    task::result::{IntoResult, Result},
};

pub trait Effect<S, T>: Clone + Send + 'static {
    fn call(self, system: System, context: Context<S>) -> Result<Patch>;

    fn into_job(self) -> Job<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
    {
        Job::from_action(self.clone(), IntoAction::new(self))
    }

    fn into_task(self, context: Context<S>) -> Task<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
    {
        self.into_job().into_task(context)
    }
}

macro_rules! impl_effect_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Res> Effect<S, ($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Res + Clone + Send +'static,

            Res: IntoResult<Output = Patch>,
            $($ty: FromSystem<S>,)*
        {

            fn call(self, system: System, context: Context<S>) -> Result<Patch> {
                $(
                    let $ty = match $ty::from_system(&system, &context) {
                        Ok(value) => value,
                        Err(failure) => return Err(failure.into_error())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_result(&system)
            }
        }
    };
}

impl_effect_handler!(T1,);
impl_effect_handler!(T1, T2);
impl_effect_handler!(T1, T2, T3);
impl_effect_handler!(T1, T2, T3, T4);
impl_effect_handler!(T1, T2, T3, T4, T5);
impl_effect_handler!(T1, T2, T3, T4, T5, T6);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_effect_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

pub(crate) struct IntoAction<S, T, E>
where
    E: Effect<S, T>,
{
    effect: E,
    _state: PhantomData<S>,
    _args: PhantomData<T>,
}

impl<S, T, E> Clone for IntoAction<S, T, E>
where
    E: Effect<S, T>,
{
    fn clone(&self) -> Self {
        IntoAction {
            effect: self.effect.clone(),
            _state: PhantomData::<S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<S, T, E> IntoAction<S, T, E>
where
    E: Effect<S, T>,
{
    pub fn new(effect: E) -> Self {
        IntoAction {
            effect,
            _state: PhantomData::<S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<S, T, E> Action<S, T> for IntoAction<S, T, E>
where
    S: Send + Sync + 'static,
    E: Effect<S, T> + Send + 'static,
    T: Send + 'static,
{
    type Future = Ready<Result<Patch>>;

    fn call(self, system: System, context: Context<S>) -> Self::Future {
        ready(self.effect.call(system, context))
    }
}
