use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct InputError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct UnexpectedError(#[from] anyhow::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct RuntimeError(pub(super) Box<dyn std::error::Error + Send + Sync>);

#[derive(Error, Debug)]
pub enum Error {
    #[error("input error: {0}")]
    BadInput(#[from] InputError),

    #[error("unexpected error, this might be a bug: {0}")]
    Unexpected(#[from] UnexpectedError),

    #[error("condition failed")]
    ConditionFailed,

    #[error("task runtime error: {0}")]
    Runtime(#[from] RuntimeError),
}
