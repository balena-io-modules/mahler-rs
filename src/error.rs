use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerializationError(#[from] serde_json::error::Error),

    #[error(transparent)]
    PatchFailed(#[from] json_patch::PatchError),

    #[error("the string `{0}` is not a valid path")]
    InvalidPath(String),

    #[error("no target available for the task")]
    TargetNotAvailable,

    #[error("path `{0}` does not exist on the given target")]
    TargetNotFound(String),

    #[error("path `{0}` does not exist on the system state")]
    PathNotFound(String),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error>),
}
