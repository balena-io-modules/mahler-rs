use thiserror::Error;

use super::condition_failed::ConditionFailed;

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

    #[error(transparent)]
    ConditionFailed(#[from] ConditionFailed),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Error)]
#[error("failed to configure target for task: {0}")]
pub struct InvalidTarget(#[from] anyhow::Error);
