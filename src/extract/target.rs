use crate::system::{Context, System, SystemReader};
use std::ops::Deref;

pub struct Target<S>(pub S);

impl<S: Clone> SystemReader<S> for Target<S> {
    fn from_system(_: &System, context: &Context<S>) -> Self {
        Target(context.target.clone())
    }
}

impl<S> Deref for Target<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
