use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::path::{Path, PathArgs};

#[derive(Clone, Default)]
pub struct Context {
    pub(crate) target: Value,
    pub(crate) path: Path,
    pub(crate) args: PathArgs,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target<S: Serialize>(self, target: S) -> Self {
        // TODO: propagate the error. Serialization errors
        // are probably rare, but it still might be needed
        let target = serde_json::to_value(target).unwrap();
        Self {
            target,
            path: self.path,
            args: self.args,
        }
    }

    /// This is only used for tests, end users should not
    /// set the context path as that is set when creating
    /// the job domain
    pub(crate) fn with_path(self, path: &'static str) -> Self {
        Self {
            target: self.target,
            path: Path::from_static(path),
            args: self.args,
        }
    }

    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        let Self {
            target,
            path,
            mut args,
        } = self;

        args.0.push((Arc::from(key.as_ref()), value.into()));

        Self { target, path, args }
    }
}
