use std::ops::Deref;

use crate::state::{Context, FromContext};

pub struct Target<S>(pub S);

impl<S: Clone> FromContext<S> for Target<S> {
    fn from_context(_: &S, context: Context<S>) -> Self {
        Target(context.target)
    }
}

impl<S: Clone> Deref for Target<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
