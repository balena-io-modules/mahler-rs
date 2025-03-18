use anyhow::Context as AnyhowCxt;
use serde::de::DeserializeOwned;
use std::ops::Deref;

use super::errors::InputError;
use crate::system::{FromSystem, System};
use crate::task::Context;

pub struct Target<T>(pub T);

impl<T: DeserializeOwned> FromSystem for Target<T> {
    type Error = InputError;

    fn from_system(_: &System, context: &Context) -> Result<Self, Self::Error> {
        let value = &context.target;

        // This will fail if the value cannot be deserialized into the target type
        let target = serde_json::from_value::<T>(value.clone()).with_context(|| {
            format!(
                "Failed to deserialize {value} into {}",
                std::any::type_name::<T>()
            )
        })?;

        Ok(Target(target))
    }
}

impl<T> Deref for Target<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
