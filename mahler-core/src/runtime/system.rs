use std::{fmt::Display, sync::Arc};

use crate::serde::{de::DeserializeOwned, Serialize};

use crate::json::Value;
use crate::result::Result;

use super::resources::Resources;

/// A snapshot of the system state and associated resources
///
/// This is used by the Worker to keep track of the system state while
/// executing tasks. It is also used by the planner to keep track of the accumulated results of
/// different operations in the tentative plan.
///
/// Cloning a `System` is cheap: the state is shared between clones and only copied
/// when one of them is modified while others still hold it.
#[derive(Clone)]
pub struct System {
    state: Arc<Value>,
    resources: Resources,
}

impl std::fmt::Debug for System {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("System")
            .field("state", &self.state)
            .field("resources", &"Resources { ... }")
            .finish()
    }
}

impl Display for System {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.state.fmt(f)
    }
}

impl System {
    /// Try to create a System instance from a serializable state
    ///
    /// It will return an [Error](`crate::error::Error`) of type [ErrorKind::Serialization](`crate::error::ErrorKind::Serialization`) if the
    /// state cannot be serialized into JSON
    pub fn try_from<S>(state: S) -> Result<Self>
    where
        S: Serialize,
    {
        let state = serde_json::to_value(state)?;
        Ok(Self {
            state: Arc::new(state),
            resources: Resources::new(),
        })
    }

    /// Get a reference to the inner system state as a [json::Value](`Value`)
    pub fn inner_state(&self) -> &Value {
        &self.state
    }

    /// Get a mutable reference to the inner system state as a [json::Value](`Value`)
    ///
    /// This copies the state first if it is shared with other clones of the `System`
    pub fn inner_state_mut(&mut self) -> &mut Value {
        Arc::make_mut(&mut self.state)
    }

    /// Try to deserialize the inner system state into the desired type
    pub fn state<S>(&self) -> Result<S>
    where
        S: DeserializeOwned,
    {
        let s = S::deserialize(self.state.as_ref())?;
        Ok(s)
    }

    /// Update the system resources
    pub fn set_resources(&mut self, resources: Resources) {
        self.resources = resources;
    }

    /// Get a reference resource of type `<R>` from the system
    pub fn resource<R>(&self) -> Option<Arc<R>>
    where
        R: Send + Sync + 'static,
    {
        self.resources.get::<R>()
    }
}
