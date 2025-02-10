use serde::de::DeserializeOwned;
use std::fmt::{self, Display};

use crate::error::{Error, IntoError};

use crate::system::{FromSystem, System as SystemState};
use crate::task::Context;

#[derive(Debug)]
pub struct SystemExtractError(serde_json::error::Error);

impl std::error::Error for SystemExtractError {}

impl Display for SystemExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for SystemExtractError {
    fn into_error(self) -> Error {
        Error::SystemExtractFailed(self)
    }
}

/// Extracts the root of the system state
#[derive(Debug, Clone)]
pub struct System<S>(pub S);

impl<S: DeserializeOwned> FromSystem for System<S> {
    type Error = SystemExtractError;

    fn from_system(system: &SystemState, _: &Context) -> Result<Self, Self::Error> {
        // This will fail if the value cannot be deserialized into the target type
        let state =
            serde_json::from_value::<S>(system.root().clone()).map_err(SystemExtractError)?;

        Ok(Self(state))
    }

    // The System extractor allows a handler to read from anywhere
    // in the state, breaking the scope of the handler and preventing
    // parallelization
    fn is_scoped() -> bool {
        false
    }
}
