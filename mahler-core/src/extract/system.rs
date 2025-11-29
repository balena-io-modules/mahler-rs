use anyhow::Context as AnyhowCtx;
use serde::de::DeserializeOwned;

use crate::errors::ExtractionError;
use crate::system::System as SystemState;
use crate::task::{Context, FromSystem};

/// Extracts the global system state managed by the [Worker](`crate::worker::Worker`)
///
/// This extractor is useful for Jobs that need to *peek* into another part of the system state
/// outside the scope given by the assigned path. Note that using this extractor makes the Job not
/// able to run concurrently.
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::System,
///     task::{Handler, update},
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// fn accessing_global_state(System(state): System<SystemState>) {
///     // ...
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(accessing_global_state))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
///
/// # Errors
///
/// Initializing the extractor will fail if the worker state cannot be deserialized into type
/// `<S>`.
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
    // in the state, breaking the scoping of the handler and preventing
    // concurrency
    fn is_scoped() -> bool {
        false
    }
}
