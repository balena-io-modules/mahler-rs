use crate::path::Path;
use crate::system::{Context, FromSystem, IntoPatch, System};
use json_patch::{diff, Patch};
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
    fn from_system(system: &System, context: &Context<S>) -> Self {
        // TODO: if the parent of the target does not exist
        // this function should error, if the parent exists but the
        // value does not, then the state should be None
        let value = system.pointer(context.path.clone());

        // TODO: return an error here
        let state = serde_json::from_value::<T>(value.clone()).unwrap();

        View {
            state,
            path: context.path.clone(),
            _system: PhantomData::<S>,
        }
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

impl<S, T: Serialize> IntoPatch for View<S, T> {
    fn into_patch(self, system: &System) -> Patch {
        // Get the root value
        let mut system_after = system.clone();

        // Write the changes to the system copy
        let pointer = system_after.pointer_mut(self.path);

        // Should we use error handling here? A serialization error
        // at this point would be strange
        *pointer = serde_json::to_value(self.state).unwrap();

        // Return the difference between the roots
        diff(system.root(), system_after.root())
    }
}
