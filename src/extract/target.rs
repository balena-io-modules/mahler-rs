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
        if context.target.is_none() {
            return Err(Error::TargetIsNone);
        }

        let tgt = serde_json::to_value(context.target.clone())?;

        // Return an error if the context path does not exist in the target object
        let pointer = context.path.as_ref();
        let value = pointer
            .resolve(&tgt)
            .map_err(|e| Error::TargetResolveFailed {
                path: context.path.to_string(),
                reason: e,
            })?;

        // This will fail if the value cannot be deserialized into the target type
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
