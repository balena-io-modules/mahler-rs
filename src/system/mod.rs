use json_patch::{patch, Patch};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    ops::Deref,
    sync::Arc,
};
use thiserror::Error;

mod from_system;
pub(crate) use from_system::*;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct SerializationError(#[from] serde_json::error::Error);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct PatchError(#[from] json_patch::PatchError);

#[derive(Clone)]
pub struct Resources(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl Default for Resources {
    fn default() -> Self {
        Self::new()
    }
}

impl Resources {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn with_res<R>(mut self, res: R) -> Self
    where
        R: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<R>();
        self.0.insert(type_id, Arc::new(res));
        self
    }

    pub fn get<R>(&self) -> Option<Arc<R>>
    where
        R: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<R>();
        self.0.get(&type_id).and_then(|res| {
            // Clone the Arc<dyn Any + Send + Sync> first
            let arc = Arc::clone(res);

            // Then downcast it into Arc<E>
            arc.downcast::<R>().ok()
        })
    }
}

#[derive(Clone)]
pub struct System {
    state: Value,
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

impl System {
    pub fn try_from<S: Serialize>(state: S) -> Result<Self, SerializationError> {
        let state = serde_json::to_value(state)?;
        Ok(Self {
            state,
            resources: Resources::new(),
        })
    }

    pub(crate) fn new(state: Value) -> Self {
        Self {
            state,
            resources: Resources::new(),
        }
    }

    pub(crate) fn resources(&self) -> &Resources {
        &self.resources
    }

    pub fn root(&self) -> &Value {
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

    pub(crate) fn with_resources(mut self, env: Resources) -> Self {
        self.resources = env;
        self
    }

    pub fn with_res<R>(mut self, res: R) -> Self
    where
        R: Send + Sync + 'static,
    {
        self.resources = self.resources.with_res(res);
        self
    }

    pub(crate) fn get_res<R>(&self) -> Option<Arc<R>>
    where
        R: Send + Sync + 'static,
    {
        self.resources.get::<R>()
    }
}

impl Deref for System {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
