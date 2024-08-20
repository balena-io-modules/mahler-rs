use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerializationError(#[from] serde_json::error::Error),

    #[error(transparent)]
    PatchFailed(#[from] json_patch::PatchError),

    #[error("the string `{0}` is not a valid path")]
    InvalidPath(String),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error>),
}
