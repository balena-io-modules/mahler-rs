use crate::{error::Error, system::System};
use json_patch::Patch;

/// The task outcome is a type alias
/// of Result
pub type Outcome = Result<Patch, Error>;

pub trait IntoOutcome {
    fn into_outcome(self, system: &System) -> Outcome;
}

impl<T> IntoOutcome for Option<T>
where
    T: IntoOutcome,
{
    fn into_outcome(self, system: &System) -> Outcome {
        match self {
            // TODO: should this be an error
            None => Ok(Patch(vec![])),
            Some(value) => value.into_outcome(system),
        }
    }
}

impl<T, E> IntoOutcome for core::result::Result<T, E>
where
    T: IntoOutcome,
    E: std::error::Error + Sync + Send + 'static,
{
    fn into_outcome(self, system: &System) -> Outcome {
        match self {
            Ok(value) => value.into_outcome(system),
            Err(e) => Err(Error::RuntimeError(anyhow::Error::new(e))),
        }
    }
}

impl IntoOutcome for Error {
    fn into_outcome(self, _: &System) -> Outcome {
        Err(self)
    }
}

impl IntoOutcome for Patch {
    fn into_outcome(self, _: &System) -> Outcome {
        Ok(self)
    }
}
