pub struct Context<S> {
    pub target: S,
}

pub(crate) trait FromState<'system, S> {
    fn from_state(state: &'system mut S, context: &Context<S>) -> Self;
}

pub(crate) trait FromContext<S> {
    fn from_context(state: &S, context: &Context<S>) -> Self;
}
