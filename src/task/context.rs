use jsonptr::PointerBuf;
use serde_json::Value;

use super::errors::Error;
use crate::path::{Path, PathArgs};

#[derive(Clone, Default, Debug)]
/// Describes the Task applicability context
pub struct Context {
    pub(crate) target: Value,
    pub(crate) path: Path,
    pub(crate) args: PathArgs,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target(self, target: Value) -> Self {
        Self { target, ..self }
    }

    pub fn with_path(self, path: impl AsRef<str>) -> Self {
        let path = Path::new(
            PointerBuf::parse(path.as_ref())
                // this is a bug if it happens
                .expect("invalid JSON Pointer path")
                .as_ptr(),
        );
        Self { path, ..self }
    }

    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        let Self { mut args, .. } = self;
        args.insert(key, value);
        Self { args, ..self }
    }
}

/// Trait for types that can be initialized from a context
///
/// See [`crate::extract`] for more info.
///
/// Types created from the context do not need to know the runtime state of the system and only use
/// the task configuration for initialization. They can be used on this crate to create a task [description](`super::Description`).
pub trait FromContext: Sized {
    type Error: Into<Error> + 'static;

    fn from_context(context: &Context) -> Result<Self, Self::Error>;
}
