use crate::{error::Error, system::System};

/// The task outcome is a type alias
/// of Result
pub(crate) type Result<O> = core::result::Result<O, Error>;

pub trait IntoResult<O> {
    fn into_result(self, system: &System) -> Result<O>;
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O> {
        match self {
            None => Err(Error::ConditionFailed("unknown".to_string())),
            Some(value) => value.into_result(system),
        }
    }
}

impl<T, E, O> IntoResult<O> for core::result::Result<T, E>
where
    T: IntoResult<O>,
    E: std::error::Error + Sync + Send + 'static,
{
    fn into_result(self, system: &System) -> Result<O> {
        match self {
            Ok(value) => value.into_result(system),
            Err(e) => Err(Error::Other(Box::new(e))),
        }
    }
}
