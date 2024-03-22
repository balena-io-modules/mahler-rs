use std::ops::Deref;

use crate::state::{Context, FromContext, State};

pub struct Target<S: State>(pub S);

impl<S: State> FromContext<S> for Target<S> {
    fn from_context(_: &S, context: Context<S>) -> Self {
        Target(context.target)
    }
}

impl<S: State> Deref for Target<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
