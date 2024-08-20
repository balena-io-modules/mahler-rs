use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    SerdeError(#[from] serde_json::error::Error),
    #[error(transparent)]
    PatchError(#[from] json_patch::PatchError),
    #[error(transparent)]
    RuntimeError(#[from] anyhow::Error),
}
