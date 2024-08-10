use crate::system::{Context, FromSystem, System};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::ops::Deref;

pub struct Target<S, T = S> {
    target: T,
    _system: PhantomData<S>,
}

impl<S: Serialize + Clone, T: DeserializeOwned> FromSystem<S> for Target<S, T> {
    fn from_system(_: &System, context: &Context<S>) -> Self {
        // TODO: catch error
        let tgt = serde_json::to_value(context.target.clone()).unwrap();
        // TODO: Pointer may return None so the target field should be an option
        let value = tgt.pointer(context.path.as_ref()).unwrap();

        // TODO: catch value
        let target = serde_json::from_value::<T>(value.clone()).unwrap();
        Target {
            target,
            _system: PhantomData::<S>,
        }
    }
}

impl<S, T> Deref for Target<S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.target
    }
}
