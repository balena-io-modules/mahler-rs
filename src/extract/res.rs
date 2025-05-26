use anyhow::Context as AnyhowCxt;
use std::ops::Deref;
use std::sync::Arc;

use crate::errors::ExtractionError;
use crate::system::System;
use crate::task::{Context, FromSystem};

pub struct Res<R>(Arc<R>);

impl<R: Send + Sync + 'static> FromSystem for Res<R> {
    type Error = ExtractionError;

    fn from_system(system: &System, _: &Context) -> Result<Self, Self::Error> {
        let arc = system
            .get_res::<R>()
            .with_context(|| {
                format!(
                    "failed to find resource of type {}",
                    std::any::type_name::<R>()
                )
            })
            .map_err(ExtractionError::from)?;
        Ok(Res(arc))
    }
}

impl<R> Deref for Res<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
