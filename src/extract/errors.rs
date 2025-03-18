use std::fmt::{self, Display};

use crate::error::{self, IntoError};

#[derive(Debug)]
pub struct InputError(anyhow::Error);

impl std::error::Error for InputError {}

impl Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for InputError {
    fn into_error(self) -> error::Error {
        error::Error::InputError(self)
    }
}

impl From<anyhow::Error> for InputError {
    fn from(value: anyhow::Error) -> Self {
        InputError(value)
    }
}

#[derive(Debug)]
pub struct OutputError(pub(super) anyhow::Error);

impl std::error::Error for OutputError {}

impl Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for OutputError {
    fn into_error(self) -> error::Error {
        error::Error::OutputError(self)
    }
}

impl From<anyhow::Error> for OutputError {
    fn from(value: anyhow::Error) -> Self {
        OutputError(value)
    }
}
