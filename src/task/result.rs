use crate::{error::Error, system::System};

/// The task outcome is a type alias
/// of Result
pub(crate) type Result<O> = core::result::Result<O, Error>;

pub trait IntoResult {
    type Output;

    fn into_result(self, system: &System) -> Result<Self::Output>;
}

impl<T, O> IntoResult for Option<T>
where
    O: Default,
    T: IntoResult<Output = O>,
{
    type Output = O;

    fn into_result(self, system: &System) -> Result<Self::Output> {
        match self {
            None => Err(Error::ConditionFailed("unknown".to_string())),
            Some(value) => value.into_result(system),
        }
    }
}

impl<T, E, O> IntoResult for core::result::Result<T, E>
where
    T: IntoResult<Output = O>,
    E: std::error::Error + Sync + Send + 'static,
{
    type Output = O;

    fn into_result(self, system: &System) -> Result<Self::Output> {
        match self {
            Ok(value) => value.into_result(system),
            Err(e) => Err(Error::Other(Box::new(e))),
        }
    }
}
