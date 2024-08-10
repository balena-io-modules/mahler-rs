use crate::path::Path;
use crate::system::{Context, FromSystem, IntoPatch, IntoSystemWriter, System, SystemWriter};
use json_patch::{diff, Patch};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub struct View<S, O = S> {
    state: O,
    path: Path,
    _marker: PhantomData<S>,
}

impl<S, O: DeserializeOwned> FromSystem<S> for View<S, O> {
    fn from_system(system: &System, context: &Context<S>) -> Self {
        let value = system.pointer(context.path.clone());
        let state = serde_json::from_value::<O>(value.clone()).unwrap();

        View {
            state,
            path: context.path.clone(),
            _marker: PhantomData::<S>,
        }
    }
}

impl<S, O> Deref for View<S, O> {
    type Target = O;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S, O> DerefMut for View<S, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S, O: Clone + Serialize> IntoPatch for View<S, O> {
    fn into_patch(self, system: &System) -> Patch {
        // Get the root value
        let before = system.pointer(self.path);
        let after = serde_json::to_value(self.state).unwrap();

        diff(before, &after)
    }
}

impl<S, O: Serialize + 'static> IntoSystemWriter for View<S, O> {
    fn into_system_writer(self) -> SystemWriter {
        SystemWriter::new(|system: &mut System| {
            let root = system.pointer_mut(self.path);
            *root = serde_json::to_value(self.state).unwrap();
        })
    }
}
