use crate::error::IntoError;

use super::System;
use crate::task::Context;

pub trait FromSystem: Sized {
    type Error: IntoError + 'static;

    fn from_system(state: &System, context: &Context) -> Result<Self, Self::Error>;
}
