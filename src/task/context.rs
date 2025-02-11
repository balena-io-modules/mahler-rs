use jsonptr::PointerBuf;
use serde::Serialize;
use serde_json::Value;

use super::result::Result;
use crate::path::{Path, PathArgs};

#[derive(Clone, Default, Debug)]
pub struct Context {
    pub(crate) target: Value,
    pub(crate) path: Path,
    pub(crate) args: PathArgs,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn try_target<S: Serialize>(self, target: S) -> Result<Self> {
        let target = serde_json::to_value(target)?;
        Ok(Self {
            target,
            path: self.path,
            args: self.args,
        })
    }

    pub(crate) fn with_target(self, target: Value) -> Self {
        Self {
            target,
            path: self.path,
            args: self.args,
        }
    }

    /// This is only used for tests, end users should not
    /// set the context path as that is set when creating
    /// the job domain
    pub(crate) fn with_path(self, path: impl AsRef<str>) -> Self {
        Self {
            target: self.target,
            path: Path::new(PointerBuf::parse(&path).unwrap().as_ptr()),
            args: self.args,
        }
    }

    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        let Self {
            target,
            path,
            mut args,
        } = self;

        args.insert(key, value);

        Self { target, path, args }
    }
}
