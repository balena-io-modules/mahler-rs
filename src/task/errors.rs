use std::fmt::{self, Display};

use super::result::*;
use crate::error::{Error, IntoError};
use crate::system::System;

#[derive(Debug)]
pub struct ConditionFailed(String);

impl Default for ConditionFailed {
    fn default() -> Self {
        ConditionFailed::new("unknown")
    }
}

impl ConditionFailed {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::error::Error for ConditionFailed {}

impl Display for ConditionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for ConditionFailed {
    fn into_error(self) -> Error {
        Error::TaskConditionFailed(self)
    }
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O> {
        match self {
            None => Err(ConditionFailed::default())?,
            Some(value) => value.into_result(system),
        }
    }
}

#[derive(Debug)]
pub struct InvalidTarget(pub(super) anyhow::Error);

impl std::error::Error for InvalidTarget {}

impl Display for InvalidTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for InvalidTarget {
    fn into_error(self) -> Error {
        Error::TargetError(self)
    }
}

impl From<anyhow::Error> for InvalidTarget {
    fn from(value: anyhow::Error) -> Self {
        InvalidTarget(value)
    }
}
