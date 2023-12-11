use std::{
    fmt::{Display, Formatter},
    ops::Deref,
    ops::DerefMut,
};

pub struct Context<S>
where
    S: Clone,
{
    target: S,
}

pub trait FromContext<S>
where
    S: Clone,
{
    fn from_context(context: &Context<S>) -> Self;
}

pub trait FromState<'system, S>
where
    S: Clone,
{
    fn from_state(state: &'system mut S) -> Self;
}

pub struct State<'system, S>(&'system mut S);

impl<'system, S> DerefMut for State<'system, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<'system, S> Deref for State<'system, S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<S> Display for State<'_, S>
where
    S: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'system, S> FromState<'system, S> for State<'system, S>
where
    S: Clone,
{
    fn from_state(state: &'system mut S) -> Self {
        State(state)
    }
}

pub struct Target<S>(pub S);
impl<'system, S> FromContext<S> for Target<S>
where
    S: Clone,
{
    fn from_context(context: &Context<S>) -> Self {
        Target(context.target.clone())
    }
}

pub trait Handler<'system, T, S>
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
    T1: FromState<'system, S>,
{
    fn call(&self, state: &'system mut S, _: Context<S>) {
        (self)(T1::from_state(state));
    }
}

impl<'system, F, S, T1, T2> Handler<'system, (T1, T2), S> for F
where
    F: Fn(T1, T2),
    S: Clone,
    T1: FromState<'system, S>,
    T2: FromContext<S>,
{
    fn call(&self, state: &'system mut S, context: Context<S>) {
        (self)(T1::from_state(state), T2::from_context(&context));
    }
}

pub struct Action<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    handler: H,
    context: Context<S>,
    _marker: std::marker::PhantomData<&'system T>,
}

impl<'system, S, T, H> Action<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    pub fn from(handler: H, context: Context<S>) -> Self {
        Action {
            handler,
            context,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn run(self, state: &'system mut S) {
        self.handler.call(state, self.context);
    }
}

pub struct Task<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    action: H,
    _state: std::marker::PhantomData<&'system S>,
    _args: std::marker::PhantomData<T>,
}

impl<'system, S, T, H> Task<'system, S, T, H>
where
    S: Clone,
    H: Handler<'system, T, S>,
{
    pub fn from(handler: H) -> Self {
        Task {
            action: handler,
            _state: std::marker::PhantomData,
            _args: std::marker::PhantomData,
        }
    }

    pub fn bind(self, context: Context<S>) -> Action<'system, S, T, H> {
        Action::from(self.action, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let task = Task::from(|mut counter: State<i32>, Target(tgt): Target<i32>| {
            if *counter < tgt {
                *counter = *counter + 1;
            }
        });
        let action = task.bind(Context { target: 1 });
        let mut counter = 0;
        action.run(&mut counter);

        // The referenced value was modified
        assert_eq!(counter, 1);
    }
}
