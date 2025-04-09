use json_patch::{patch, Patch};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::ops::Deref;
use thiserror::Error;

mod from_system;
pub(crate) use from_system::*;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct SerializationError(#[from] serde_json::error::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct PatchError(#[from] json_patch::PatchError);

#[derive(Clone, Debug)]
pub struct System {
    state: Value,
}

impl System {
    pub fn try_from<S: Serialize>(state: S) -> Result<Self, SerializationError> {
        let state = serde_json::to_value(state)?;
        Ok(Self { state })
    }

    pub(crate) fn new(state: Value) -> Self {
        Self { state }
    }

    pub(crate) fn root(&self) -> &Value {
        &self.state
    }

    pub(crate) fn patch(&mut self, changes: Patch) -> Result<(), PatchError> {
        patch(&mut self.state, &changes)?;
        Ok(())
    }

    pub fn state<S: DeserializeOwned>(&self) -> Result<S, SerializationError> {
        let s = serde_json::from_value(self.state.clone())?;
        Ok(s)
    }
}

impl Deref for System {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
