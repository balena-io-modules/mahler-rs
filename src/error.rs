use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("cannot serialize value: ${0}")]
    SerializationError(#[from] serde_json::error::Error),

    #[error("cannot read system state: ${0}")]
    StateReadFailed(#[from] super::system::SystemReadError),

    #[error("cannot write system state: ${0}")]
    StateWriteFailed(#[from] super::system::SystemWriteError),

    #[error("cannot extract args: ${0}")]
    ArgsExtractFailed(#[from] super::extract::ArgsDeserializationError),

    #[error("cannot extract target: ${0}")]
    TargetExtractFailed(#[from] super::extract::TargetExtractError),

    #[error("cannot extract view: ${0}")]
    ViewExtractFailed(#[from] super::extract::ViewExtractError),

    #[error("cannot extract system")]
    SystemExtractFailed(#[from] super::extract::SystemExtractError),

    #[error("cannot calculate view result: ${0}")]
    ViewResultFailed(#[from] super::extract::ViewResultError),

    #[error("condition failed: ${0}")]
    TaskConditionFailed(#[from] super::task::ConditionFailed),

    #[error("planning error: ${0}")]
    PlanSearchFailed(#[from] super::worker::PlanningError),

    #[error("workflow interrupted")]
    WorkflowInterrupted(#[from] super::worker::Interrupted),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error>),
}

pub trait IntoError {
    fn into_error(self) -> Error;
}

impl IntoError for Error {
    fn into_error(self) -> Error {
        self
    }
}
