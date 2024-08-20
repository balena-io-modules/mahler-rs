use crate::{error::Error, system::System};
use json_patch::Patch;

/// The task outcome is a type alias
/// of Result
pub type Result = core::result::Result<Patch, Error>;

pub trait IntoResult {
    fn into_result(self, system: &System) -> Result;
}

impl<T> IntoResult for Option<T>
where
    T: IntoResult,
{
    fn into_result(self, system: &System) -> Result {
        match self {
            // TODO: should this be an error
            None => Ok(Patch(vec![])),
            Some(value) => value.into_result(system),
        }
    }
}

impl<T, E> IntoResult for core::result::Result<T, E>
where
    T: IntoResult,
    E: std::error::Error + Sync + Send + 'static,
{
    fn into_result(self, system: &System) -> Result {
        match self {
            Ok(value) => value.into_result(system),
            Err(e) => Err(Error::Other(Box::new(e))),
        }
    }
}

impl IntoResult for Error {
    fn into_result(self, _: &System) -> Result {
        Err(self)
    }
}

impl IntoResult for Patch {
    fn into_result(self, _: &System) -> Result {
        Ok(self)
    }
}
