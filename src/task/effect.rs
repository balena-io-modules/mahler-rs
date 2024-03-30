use std::{
    future::{ready, Ready},
    marker::PhantomData,
};

use crate::state::{Context, FromContext, FromState};

use super::handler::Handler;
use super::Task;

pub trait Effect<'system, S, T>: Clone + Sized {
    fn call(self, state: &'system mut S, context: &Context<S>);
}

impl<'system, F, S> Effect<'system, S, ()> for F
where
    F: FnOnce() + Clone + 'static,
    S: Send + 'static,
{
    fn call(self, _: &mut S, _: &Context<S>) {
        (self)()
    }
}

macro_rules! impl_effect_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<'system, S, F, $first, $($ty,)*> Effect<'system, S, ($first, $($ty,)*)> for F
        where
            F: FnOnce($first, $($ty,)*) + Clone +'static,
            S: 'static,
            $first: FromState<'system, S>,
            $($ty: FromContext<S>,)*
        {

            fn call(self, state: &'system mut S, context: &Context<S>) {
                $(
                    let $ty = $ty::from_context(state, context);
                )*

                // From system requires a mutable reference so we have to
                // do this last
                let $first = $first::from_state(state, context);


                (self)($first, $($ty,)*)
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

pub struct IntoHandler<'system, S, T, E>
where
    E: Effect<'system, S, T>,
{
    effect: E,
    _state: PhantomData<&'system S>,
    _args: PhantomData<T>,
}

impl<'system, S, T, E> Clone for IntoHandler<'system, S, T, E>
where
    E: Effect<'system, S, T>,
{
    fn clone(&self) -> Self {
        IntoHandler {
            effect: self.effect.clone(),
            _state: PhantomData::<&'system S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<'system, S, T, E> IntoHandler<'system, S, T, E>
where
    E: Effect<'system, S, T>,
{
    fn new(effect: E) -> Self {
        IntoHandler {
            effect,
            _state: PhantomData::<&'system S>,
            _args: PhantomData::<T>,
        }
    }
}

impl<'system, S, T, E> Handler<'system, S, T> for IntoHandler<'system, S, T, E>
where
    S: Send + Sync + 'static,
    E: Effect<'system, S, T> + Send,
    T: Send,
{
    type Future = Ready<()>;

    fn call(self, state: &'system mut S, context: &Context<S>) -> Self::Future {
        self.effect.call(state, context);
        ready(())
    }
}

impl<'system, S, T, E> From<E> for Task<'system, S, T, E, IntoHandler<'system, S, T, E>>
where
    S: Send + Sync + 'static,
    E: Effect<'system, S, T> + Send,
    T: Send,
{
    fn from(effect: E) -> Self {
        Task::new(effect.clone(), IntoHandler::new(effect))
    }
}
