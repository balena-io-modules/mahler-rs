use std::future::Future;

use crate::context::{Context, FromContext};
use crate::system::FromSystem;

pub trait Handler<'system, S, T, R>: Clone + Send + Sized + 'static
where
    S: Clone,
{
    fn call(self, state: &'system mut S, context: Context<S>) -> R;
}

impl<'system, F, S, R> Handler<'system, S, (), R> for F
where
    F: FnOnce() -> R + Clone + Send + 'static,
    S: Clone + Send + 'static,
    R: Future<Output = ()>,
{
    fn call(self, _: &'system mut S, _: Context<S>) -> R {
        (self)()
    }
}

macro_rules! impl_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(unused)]
        impl<'system, S, F, $first, $($ty,)* R> Handler<'system, S, ($first, $($ty,)*), R> for F
        where
            F: FnOnce($first, $($ty,)*) -> R + Clone + Send + 'static,
            S: Clone + Send + 'static,
            $first: FromSystem<'system, S>,
            $($ty: FromContext<S>,)*
        {
            fn call(self, state: &'system mut S, context: Context<S>) -> R {
                (self)($first::from_state(state), $($ty::from_context(&context),)*)
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
