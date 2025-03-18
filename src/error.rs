use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to extract task input: ${0}")]
    InputError(#[from] super::extract::InputError),

    #[error("failed to calculate task result: ${0}")]
    OutputError(#[from] super::extract::OutputError),

    #[error("failed to configure target for task: ${0}")]
    TargetError(#[from] super::task::InvalidTarget),

    // TODO: remove
    #[error("cannot serialize value: ${0}")]
    SerializationError(#[from] serde_json::error::Error),

    #[error("failed to read system state: ${0}")]
    SystemReadError(#[from] super::system::SystemReadError),

    #[error("failed to update system state: ${0}")]
    SystemWriteError(#[from] super::system::SystemWriteError),

    #[error("condition failed: ${0}")]
    TaskConditionFailed(#[from] super::task::ConditionFailed),

    // TODO: remove
    #[error("planning error: ${0}")]
    PlanSearchFailed(#[from] super::worker::PlanningError),

    // TODO: remove
    #[error("workflow interrupted")]
    WorkflowInterrupted(#[from] super::worker::Interrupted),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send>),
}

pub trait IntoError {
    fn into_error(self) -> Error;
}

impl IntoError for Error {
    fn into_error(self) -> Error {
        self
    }
}
