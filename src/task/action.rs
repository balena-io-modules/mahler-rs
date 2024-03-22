use std::future::Future;
use std::marker::PhantomData;

use crate::state::{Context, FromContext, FromState, State};

use super::effect::Effect;
use super::Task;

pub trait Handler<'system, S, T>: Clone + Send + Sized
where
    S: State,
{
    type Future: Future<Output = ()> + Send;

    fn call(self, state: &'system mut S, context: Context<S>) -> Self::Future;

    fn with_effect<E: Effect<'system, S, T>>(self, effect: E) -> Task<'system, S, T, E, Self> {
        Task::new(effect, self)
    }
}

impl<'system, F, S, R> Handler<'system, S, ()> for F
where
    F: FnOnce() -> R + Clone + Send + 'static,
    S: State + Send + 'static,
    R: Future<Output = ()> + Send,
{
    type Future = R;

    fn call(self, _: &'system mut S, _: Context<S>) -> Self::Future {
        (self)()
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<'system, S, F, $first, $($ty,)* R> Handler<'system, S, ($first, $($ty,)*)> for F
        where
            F: FnOnce($first, $($ty,)*) -> R + Clone + Send + 'static,
            S: State + Send + 'static,
            R: Future<Output = ()> + Send,
            $first: FromState<'system, S> + Send,
            $($ty: FromContext<S> + Send,)*
        {
            type Future = R;

            fn call(self, state: &'system mut S, context: Context<S>) -> Self::Future {
                $(
                    // TODO: it would be nice to use references here to avoid clonning
                    let $ty = $ty::from_context(state, context.clone());
                )*

                // From system requires a mutable reference so we have to
                // do this last
                let $first = $first::from_state(state, context);

                (self)($first, $($ty,)*)
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

pub struct Action<'system, S, T, E, A>
where
    S: State,
    E: Effect<'system, S, T>,
    A: Handler<'system, S, T>,
{
    effect: E,
    action: A,
    context: Context<S>,
    _args: PhantomData<&'system T>,
}

impl<'system, S, T, E, A> Action<'system, S, T, E, A>
where
    S: State,
    E: Effect<'system, S, T>,
    A: Handler<'system, S, T>,
{
    pub fn new(effect: E, action: A, context: Context<S>) -> Self {
        Action {
            effect,
            action,
            context,
            _args: PhantomData::<&'system T>,
        }
    }

    pub fn dry_run(self, state: &'system mut S) {
        self.effect.call(state, self.context.clone());
    }

    pub fn run(self, state: &'system mut S) -> A::Future {
        self.action.call(state, self.context.clone())
    }
}
