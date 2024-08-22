use crate::error::Error;
use crate::path::Path;
use crate::system::{Context, FromSystem, System};
use crate::task::{IntoResult, Result};
use json_patch::diff;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct View<S, T = S> {
    state: T,
    path: Path,
    _system: PhantomData<S>,
}

impl<S, T: DeserializeOwned> FromSystem<S> for View<S, T> {
    type Error = Error;

    fn from_system(
        system: &System,
        context: &Context<S>,
    ) -> core::result::Result<Self, Self::Error> {
        // TODO: if the parent of the target does not exist
        // this function should error, if the parent exists the value
        // should be None
        // TODO: we also need a way to create the value if it doesn't exist
        // this will depend on the type of the parent, the parent may be an array
        // or a map and we need to create the relevant key in the parent
        let value = system.pointer(context.path.clone()).unwrap();
        let state = serde_json::from_value::<T>(value.clone())?;

        Ok(View {
            state,
            path: context.path.clone(),
            _system: PhantomData::<S>,
        })
    }
}

impl<S, T> Deref for View<S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S, T> DerefMut for View<S, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S, T: Serialize> IntoResult for View<S, T> {
    fn into_result(self, system: &System) -> Result {
        // Get the root value
        let mut system_after = system.clone();

        // Write the changes to the system copy
        let pointer = system_after.pointer_mut(self.path).unwrap();

        // Should we use error handling here? A serialization error
        // at this point would be strange
        *pointer = serde_json::to_value(self.state)?;

        // Return the difference between the roots
        Ok(diff(system.root(), system_after.root()))
    }
}
