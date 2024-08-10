use super::Context;
use super::System;

pub(crate) trait FromSystem<S> {
    fn from_system(state: &System, context: &Context<S>) -> Self;
}
