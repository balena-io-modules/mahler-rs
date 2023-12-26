use std::future::Future;

use crate::entity::Entity;
use crate::state::{FromState, FromStateMut, State};

pub trait Handler<'system, E, T, R>: Clone + Send + Sized + 'static
where
    E: Entity,
{
    fn call(self, state: &'system mut State, target: &E) -> R;
}

impl<'system, F, E, R> Handler<'system, E, (), R> for F
where
    F: FnOnce() -> R + Clone + Send + 'static,
    E: Entity + Send + 'static,
    R: Future<Output = ()>,
{
    fn call(self, _: &'system mut State, _: &E) -> R {
        (self)()
    }
}

macro_rules! impl_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<'system, E, F, $first, $($ty,)* R> Handler<'system, E, ($first, $($ty,)*), R> for F
        where
            F: FnOnce($first, $($ty,)*) -> R + Clone + Send + 'static,
            E: Entity + Clone + Send + 'static,
            $first: FromStateMut<'system, E>,
            $($ty: FromState<E>,)*
        {
            fn call(self, state: &'system mut State, target: &E) -> R {
                $(
                    let $ty = $ty::from_state(state, target);
                )*

                // From system requires a mutable reference so we have to
                // do this last
                let $first = $first::from_state_mut(state, target);

                (self)($first, $($ty,)*)
            }
        }
    };
}

impl_handler!(T1,);
impl_handler!(T1, T2);
impl_handler!(T1, T2, T3);
impl_handler!(T1, T2, T3, T4);
impl_handler!(T1, T2, T3, T4, T5);
impl_handler!(T1, T2, T3, T4, T5, T6);
impl_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
