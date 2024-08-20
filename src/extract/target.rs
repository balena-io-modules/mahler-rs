use crate::error::Error;
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
    type Error = Error;

    fn from_system(_: &System, context: &Context<S>) -> Result<Self, Self::Error> {
        let tgt = serde_json::to_value(context.target.clone())?;

        // TODO: allow target to be None
        let value = tgt.pointer(context.path.as_ref()).unwrap();
        let target = serde_json::from_value::<T>(value.clone())?;

        Ok(Target {
            target,
            _system: PhantomData::<S>,
        })
    }
}

impl<S, T> Deref for Target<S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.target
    }
}
