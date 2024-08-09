use json_patch::{patch, Patch};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::path::Path;

mod into_patch;
pub(crate) use into_patch::*;
mod system_reader;
pub(crate) use system_reader::*;
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
    pub fn pointer(&self, path: Path) -> &Value {
        // Path already validates the path
        self.state.pointer(path.as_ref()).unwrap()
    }

    pub fn patch(&mut self, changes: Patch) -> Result<(), Error> {
        patch(&mut self.state, &changes).map_err(|e| Error::PatchError(e.to_string()))
    }

    pub fn pointer_mut(&mut self, path: Path) -> &mut Value {
        self.state.pointer_mut(path.as_ref()).unwrap()
    }

    pub fn state<S: DeserializeOwned>(&self) -> Result<S, Error> {
        serde_json::from_value(self.state.clone())
            .map_err(|e| Error::DeserializationError(e.to_string()))
    }
}
