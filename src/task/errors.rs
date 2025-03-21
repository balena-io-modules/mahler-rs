use thiserror::Error;

#[derive(Debug, Error)]
#[error("input error: {0}")]
pub struct InputError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error("unexpected error, this might be a bug: {0}")]
pub struct UnexpectedError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error("task runtime error: {0}")]
pub struct RuntimeError(pub(super) Box<dyn std::error::Error + Send + Sync>);

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    BadInput(#[from] InputError),

    #[error(transparent)]
    Unexpected(#[from] UnexpectedError),

    #[error("condition failed")]
    ConditionFailed,

    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}
