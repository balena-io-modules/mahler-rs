use crate::path::Path;
use crate::system::{Context, FromSystem, IntoPatch, IntoSystemWriter, System, SystemWriter};
use json_patch::{diff, Patch};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{self};
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct State<S>(S);

impl<S: DeserializeOwned> FromSystem<S> for State<S> {
    fn from_system(system: &System, _: &Context<S>) -> Self {
        State(system.state().unwrap())
    }
}

impl<S: Clone + Serialize> IntoPatch for State<S> {
    fn into_patch(self, system: &System) -> Patch {
        // Get the root value
        let before = system.pointer(Path::default());
        let after = serde_json::to_value(self.0).unwrap();

        diff(before, &after)
    }
}

impl<S: Serialize + 'static> IntoSystemWriter for State<S> {
    fn into_system_writer(self) -> SystemWriter {
        SystemWriter::new(move |system: &mut System| {
            let root = system.pointer_mut(Path::default());
            *root = serde_json::to_value(self.0).unwrap();
        })
    }
}

impl<S> Deref for State<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for State<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
