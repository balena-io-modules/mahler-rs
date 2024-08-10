use json_patch::{patch, Patch};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::path::Path;

mod into_patch;
pub(crate) use into_patch::*;
mod from_system;
pub(crate) use from_system::*;
mod system_writer;
pub(crate) use system_writer::*;
mod context;
pub use context::*;

#[derive(Clone)]
pub struct System {
    state: Value,
}

#[derive(Debug)]
pub enum Error {
    DeserializationError(String),
    PatchError(String),
}

impl<S> From<S> for System
where
    S: Serialize,
{
    fn from(state: S) -> Self {
        serde_json::to_value(state)
            .map(|state| Self { state })
            .expect("Input value to be serializable")
    }
}

impl System {
    pub(crate) fn pointer(&self, path: Path) -> &Value {
        // TODO: this function should return Option<&Value> as the
        // path may not exist on the state
        self.state.pointer(path.as_ref()).unwrap()
    }

    pub(crate) fn patch(&mut self, changes: Patch) -> Result<(), Error> {
        patch(&mut self.state, &changes).map_err(|e| Error::PatchError(e.to_string()))
    }

    pub(crate) fn pointer_mut(&mut self, path: Path) -> &mut Value {
        // TODO: this function should return Option<&mut Value> as the
        // path may not exist on the state
        self.state.pointer_mut(path.as_ref()).unwrap()
    }

    pub fn state<S: DeserializeOwned>(&self) -> Result<S, Error> {
        serde_json::from_value(self.state.clone())
            .map_err(|e| Error::DeserializationError(e.to_string()))
    }
}
