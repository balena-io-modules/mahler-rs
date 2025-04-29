use anyhow::Context as AnyhowCxt;
use serde::de::DeserializeOwned;
use std::ops::Deref;

use crate::errors::ExtractionError;
use crate::system::{FromSystem, System};
use crate::task::{Context, FromContext};

pub struct Target<T>(pub T);

impl<T: DeserializeOwned> FromContext for Target<T> {
    type Error = ExtractionError;

    fn from_context(context: &Context) -> Result<Self, Self::Error> {
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

impl<T: DeserializeOwned> FromSystem for Target<T> {
    type Error = ExtractionError;

    fn from_system(_: &System, context: &Context) -> Result<Self, Self::Error> {
        Self::from_context(context)
    }
}

impl<T> Deref for Target<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
