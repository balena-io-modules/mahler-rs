use serde::Serialize;

pub trait State: Serialize + Clone {}
impl<S: Serialize + Clone> State for S {}

#[derive(Clone)]
pub struct Context<S: State> {
    pub target: S,
}

pub(crate) trait FromState<'system, S>
where
    S: State,
{
    fn from_state(state: &'system mut S, context: Context<S>) -> Self;
}

pub(crate) trait FromContext<S>
where
    S: State,
{
    fn from_context(state: &S, context: Context<S>) -> Self;
}
