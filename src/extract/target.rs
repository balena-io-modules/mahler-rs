use serde::de::DeserializeOwned;
use std::fmt::{self, Display};
use std::ops::Deref;

use crate::error::{self, IntoError};
use crate::system::{FromSystem, System};
use crate::task::Context;

#[derive(Debug)]
pub enum TargetExtractError {
    DeserializationFailed(serde_json::error::Error),
}

impl std::error::Error for TargetExtractError {}

impl Display for TargetExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetExtractError::DeserializationFailed(err) => err.fmt(f)?,
        };
        Ok(())
    }
}

impl IntoError for TargetExtractError {
    fn into_error(self) -> error::Error {
        error::Error::TargetExtractFailed(self)
    }
}

pub struct Target<T>(pub T);

impl<T: DeserializeOwned> FromSystem for Target<T> {
    type Error = TargetExtractError;

    fn from_system(_: &System, context: &Context) -> Result<Self, Self::Error> {
        let value = &context.target;

        // This will fail if the value cannot be deserialized into the target type
        let target = serde_json::from_value::<T>(value.clone())
            .map_err(TargetExtractError::DeserializationFailed)?;

        Ok(Target(target))
    }
}

impl<T> Deref for Target<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
