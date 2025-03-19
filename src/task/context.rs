use jsonptr::PointerBuf;
use serde_json::Value;

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

    pub fn with_target(self, target: Value) -> Self {
        Self { target, ..self }
    }

    pub fn with_path(self, path: impl AsRef<str>) -> Self {
        let path = Path::new(
            PointerBuf::parse(&path)
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
