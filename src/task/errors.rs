use thiserror::Error;

use crate::errors::{ExtractionError, IOError, MethodError};

#[derive(Error, Debug)]
/// Type for errors that can happen when operating with a [`super::Task`]
pub enum Error {
    #[error(transparent)]
    /// Arguments to the task could not be extracted.
    /// This is likely to be an error with the task definition.
    CannotExtractArgs(#[from] ExtractionError),

    #[error(transparent)]
    /// An error happened while trying to expand the method
    /// task into its sub-tasks. This is likely an issue with the
    /// method definition
    CannotExpandMethod(#[from] MethodError),

    #[error("condition failed")]
    /// The task condition was not met
    ConditionFailed,

    #[error(transparent)]
    /// An error happened while executing the task within the workflow.
    /// These errors only happen at runtime, never at the planning stage
    /// of the worker.
    IO(#[from] IOError),
}
