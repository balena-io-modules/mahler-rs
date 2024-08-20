use crate::task::IntoOutcome;

use super::Context;
use super::System;

pub(crate) trait FromSystem<S>: Sized {
    type Error: IntoOutcome;

    fn from_system(state: &System, context: &Context<S>) -> Result<Self, Self::Error>;
}
