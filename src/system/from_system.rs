use crate::task::IntoResult;

use super::Context;
use super::System;

pub(crate) trait FromSystem<S>: Sized {
    type Error: IntoResult;

    fn from_system(state: &System, context: &Context<S>) -> Result<Self, Self::Error>;
}
