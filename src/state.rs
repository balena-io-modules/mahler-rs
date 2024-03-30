#[derive(Clone)]
pub struct Context<S: Clone> {
    pub target: S,
}

pub(crate) trait FromState<'system, S: Clone> {
    fn from_state(state: &'system mut S, context: Context<S>) -> Self;
}

pub(crate) trait FromContext<S: Clone> {
    fn from_context(state: &S, context: Context<S>) -> Self;
}
