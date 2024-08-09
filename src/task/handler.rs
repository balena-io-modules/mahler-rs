use json_patch::Patch;
use std::future::Future;
use std::pin::Pin;

use super::effect::Effect;
use super::Task;

use crate::system::{Context, IntoPatch, System, SystemReader};

pub trait Handler<S: Clone, T>: Clone + Send + Sized + 'static {
    type Future: Future<Output = Patch> + Send + 'static;

    fn call(self, state: System, context: Context<S>) -> Self::Future;

    fn with_effect<E: Effect<S, T>>(self, effect: E) -> Task<S, T, E, Self> {
        Task::new(effect, self)
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Fut, Res> Handler<S, ($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Fut + Clone + Send + 'static,
            S: Clone + Send + Sync + 'static,
            Fut: Future<Output = Res> + Send,
            Res: IntoPatch,
            $($ty: SystemReader<S> + Send,)*
        {

            // TODO: this should return a result
            type Future = Pin<Box<dyn Future<Output = Patch> + Send>>;

            fn call(self, system: System, context: Context<S>) -> Self::Future {
                Box::pin(async move {
                    $(
                        let $ty = $ty::from_system(&system, &context);
                    )*

                    // Execute the handler
                    let res = (self)($($ty,)*).await;

                    // Update the system using the response
                    res.into_patch(&system)
                })
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
