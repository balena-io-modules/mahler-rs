use std::fmt::{self, Display};
use thiserror::Error;

use super::result::*;
use crate::system::System;

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("failed to extract task input: ${0}")]
    InputError(#[from] crate::extract::InputError),

    #[error("failed to calculate task result: ${0}")]
    OutputError(#[from] crate::extract::OutputError),

    #[error("failed to read system state: ${0}")]
    SystemReadError(#[from] crate::system::SystemReadError),

    #[error("failed to update system state: ${0}")]
    SystemWriteError(#[from] crate::system::SystemWriteError),

    #[error("condition failed: ${0}")]
    TaskConditionFailed(#[from] crate::task::ConditionFailed),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

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

#[derive(Debug, Error)]
#[error("failed to configure target for task: {0}")]
pub struct InvalidTarget(#[from] anyhow::Error);
