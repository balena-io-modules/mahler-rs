use super::Context;
use super::System;

pub(crate) trait FromSystem<S>: Sized {
    type Error: std::error::Error;

    fn from_system(state: &System, context: &Context<S>) -> Result<Self, Self::Error>;
}
