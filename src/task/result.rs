use crate::{system::System, task::TaskError};

/// The task outcome is a type alias
/// of Result
pub type Result<O> = core::result::Result<O, TaskError>;

pub trait IntoResult<O> {
    fn into_result(self, system: &System) -> Result<O>;
}

impl<T, E, O> IntoResult<O> for core::result::Result<T, E>
where
    T: IntoResult<O>,
    E: std::error::Error + Sync + Send + 'static,
{
    fn into_result(self, system: &System) -> Result<O> {
        match self {
            Ok(value) => value.into_result(system),
            Err(e) => Err(TaskError::Other(Box::new(e))),
        }
    }
}
