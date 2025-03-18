use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to extract task input: ${0}")]
    InputError(#[from] super::extract::InputError),

    #[error("failed to calculate task result: ${0}")]
    OutputError(#[from] super::extract::OutputError),

    #[error("cannot serialize value: ${0}")]
    SerializationError(#[from] serde_json::error::Error),

    #[error("cannot read system state: ${0}")]
    StateReadFailed(#[from] super::system::SystemReadError),

    #[error("cannot write system state: ${0}")]
    StateWriteFailed(#[from] super::system::SystemWriteError),

    #[error("condition failed: ${0}")]
    TaskConditionFailed(#[from] super::task::ConditionFailed),

    #[error("planning error: ${0}")]
    PlanSearchFailed(#[from] super::worker::PlanningError),

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
