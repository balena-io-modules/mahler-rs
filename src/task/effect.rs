use super::handler::Handler;
use crate::system::{Context, FromSystem, IntoPatch, System};
use json_patch::Patch;
use std::{
    future::{ready, Ready},
    marker::PhantomData,
};

pub trait Effect<S, T>: Clone + Send + Sized + 'static {
    fn call(self, system: System, context: Context<S>) -> Patch;
}

macro_rules! impl_effect_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Res> Effect<S, ($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Res + Clone + Send +'static,
            Res: IntoPatch,
            $($ty: FromSystem<S>,)*
        {

            fn call(self, system: System, context: Context<S>) -> Patch {
                $(
                    let $ty = $ty::from_system(&system, &context);
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_patch(&system)
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

pub(crate) struct IntoHandler<S, T, E>
where
    E: Effect<S, T>,
{
    effect: E,
    _state: PhantomData<S>,
    _args: PhantomData<T>,
}

impl<S, T, E> Clone for IntoHandler<S, T, E>
where
    E: Effect<S, T>,
{
    fn clone(&self) -> Self {
        IntoHandler {
            effect: self.effect.clone(),
            _state: PhantomData::<S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<S, T, E> IntoHandler<S, T, E>
where
    E: Effect<S, T>,
{
    pub fn new(effect: E) -> Self {
        IntoHandler {
            effect,
            _state: PhantomData::<S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<S, T, E> Handler<S, T> for IntoHandler<S, T, E>
where
    S: Send + Sync + 'static,
    E: Effect<S, T> + Send + 'static,
    T: Send + 'static,
{
    type Future = Ready<Patch>;

    fn call(self, system: System, context: Context<S>) -> Self::Future {
        ready(self.effect.call(system, context))
    }
}
