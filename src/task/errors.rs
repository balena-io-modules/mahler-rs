use thiserror::Error;

use super::result::*;
use crate::system::System;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct TaskInputError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct TaskOutputError(#[from] anyhow::Error);

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("failed to extract task input: ${0}")]
    WrongInput(#[from] TaskInputError),

    #[error("failed to calculate task result: ${0}")]
    OutputError(#[from] TaskOutputError),

    #[error("failed to read system state: ${0}")]
    SystemReadError(#[from] crate::system::SystemReadError),

    #[error("failed to update system state: ${0}")]
    SystemWriteError(#[from] crate::system::SystemWriteError),

    #[error("condition failed: ${0}")]
    ConditionFailed(#[from] crate::task::ConditionFailed),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Error)]
#[error("condition failed: {0}")]
pub struct ConditionFailed(String);

impl Default for ConditionFailed {
    fn default() -> Self {
        ConditionFailed::new("unknown")
    }
}

impl ConditionFailed {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O> {
        self.map(|value| value.into_result(system))
            .ok_or_else(ConditionFailed::default)?
    }
}

#[derive(Debug, Error)]
#[error("failed to configure target for task: {0}")]
pub struct InvalidTarget(#[from] anyhow::Error);
