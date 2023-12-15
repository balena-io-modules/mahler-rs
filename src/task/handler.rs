use crate::context::{Context, FromContext};
use crate::system::FromSystem;

pub trait Handler<'system, T, S>: Sized
where
    S: Clone,
{
    fn call(&self, state: &'system mut S, context: Context<S>);
}

impl<'system, F, S> Handler<'system, (), S> for F
where
    F: Fn(),
    S: Clone,
{
    fn call(&self, _: &'system mut S, _: Context<S>) {
        (self)();
    }
}

impl<'system, F, S, T1> Handler<'system, (T1,), S> for F
where
    F: Fn(T1),
    S: Clone,
    T1: FromSystem<'system, S>,
{
    fn call(&self, state: &'system mut S, _: Context<S>) {
        (self)(T1::from_state(state));
    }
}

impl<'system, F, S, T1, T2> Handler<'system, (T1, T2), S> for F
where
    F: Fn(T1, T2),
    S: Clone,
    T1: FromSystem<'system, S>,
    T2: FromContext<S>,
{
    fn call(&self, state: &'system mut S, context: Context<S>) {
        (self)(T1::from_state(state), T2::from_context(&context));
    }
}
