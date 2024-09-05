use super::effect::Effect;
use crate::task::Job;
use std::future::Future;
use std::pin::Pin;

use crate::system::{Context, FromSystem, System};
use crate::task::result::{IntoResult, Result};

pub(crate) type ActionResult = Pin<Box<dyn Future<Output = Result>>>;

pub trait Action<S, T>: Clone + Send + Sized + 'static {
    type Future: Future<Output = Result> + 'static;

    fn call(self, state: System, context: Context<S>) -> Self::Future;

    fn with_effect<E>(self, effect: E) -> Job<S>
    where
        S: 'static,
        E: Effect<S, T>,
    {
        Job::new(effect, self)
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Fut, Res> Action<S, ($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Fut + Clone + Send + 'static,
            S: Send + Sync + 'static,
            Fut: Future<Output = Res> + Send,
            Res: IntoResult,
            $($ty: FromSystem<S> + Send,)*
        {

            type Future = ActionResult;

            fn call(self, system: System, context: Context<S>) -> Self::Future {
                Box::pin(async move {
                    $(
                        let $ty = match $ty::from_system(&system, &context) {
                            Ok(value) => value,
                            Err(failure) => return failure.into_result(&system)
                        };
                    )*

                    // Execute the handler
                    let res = (self)($($ty,)*).await;

                    // Update the system using the response
                   res.into_result(&system)
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
