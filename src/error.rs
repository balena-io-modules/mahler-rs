use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerializationError(#[from] serde_json::error::Error),

    #[error(transparent)]
    FailedToDeserializePathParams(#[from] super::extract::PathDeserializationError),

    #[error(transparent)]
    PatchFailed(#[from] json_patch::PatchError),

    #[error("the string `{0}` is not a valid path")]
    InvalidPath(String),

    #[error("no target available on the context")]
    TargetIsNone,

    #[error("cannot expand an atom task")]
    CannotExpandTask,

    #[error("condition failed: ${0}")]
    ConditionFailed(String),

    #[error("cannot resolve state path `{path}`: ${reason}")]
    TargetResolveFailed {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },

    #[error("cannot resolve path `{path}` on system state: ${reason}")]
    PointerResolveFailed {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },

    #[error("cannot assign path `{path}` on system state: ${reason}")]
    PointerAssignFailed {
        path: String,
        reason: jsonptr::assign::AssignError,
    },

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
