use anyhow::Context as AnyhowCtx;
use serde::de::DeserializeOwned;

use crate::errors::ExtractionError;
use crate::system::{FromSystem, System as SystemState};
use crate::task::Context;

/// Extracts the root of the system state
#[derive(Debug, Clone)]
pub struct System<S>(pub S);

impl<S: DeserializeOwned> FromSystem for System<S> {
    type Error = ExtractionError;

    fn from_system(system: &SystemState, _: &Context) -> Result<Self, Self::Error> {
        // This will fail if the value cannot be deserialized into the target type
        let state = serde_json::from_value::<S>(system.root().clone()).with_context(|| {
            format!(
                "Failed to deserialize system state into {}",
                std::any::type_name::<S>()
            )
        })?;

        Ok(Self(state))
    }

    // The System extractor allows a handler to read from anywhere
    // in the state, breaking the scope of the handler and preventing
    // parallelization
    fn is_scoped() -> bool {
        false
    }
}
