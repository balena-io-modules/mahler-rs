use json_patch::{patch, Patch};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::error::Error;
use crate::path::Path;

mod from_system;
pub(crate) use from_system::*;
mod context;
pub use context::*;

#[derive(Clone)]
pub struct System {
    state: Value,
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
    pub(crate) fn root(&self) -> &Value {
        &self.state
    }

    pub(crate) fn pointer(&self, path: Path) -> &Value {
        // TODO: this function should return Option<&Value> as the
        // path may not exist on the state
        self.state.pointer(path.as_ref()).unwrap()
    }

    pub(crate) fn patch(&mut self, changes: Patch) -> Result<(), Error> {
        patch(&mut self.state, &changes)?;
        Ok(())
    }

    pub(crate) fn pointer_mut(&mut self, path: Path) -> &mut Value {
        // TODO: this function should return Option<&mut Value> as the
        // path may not exist on the state
        self.state.pointer_mut(path.as_ref()).unwrap()
    }

    pub fn state<S: DeserializeOwned>(&self) -> Result<S, Error> {
        let s = serde_json::from_value(self.state.clone())?;
        Ok(s)
    }
}
