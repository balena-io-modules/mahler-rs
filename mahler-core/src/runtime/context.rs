use jsonptr::{PointerBuf, Token};

use crate::json::{Path, PathArgs, Value};

#[derive(Clone, Default, Debug, PartialEq, Eq)]
/// Describes the Task applicability context
pub struct Context {
    /// The global target passed to the planner
    pub target: Value,

    /// A specific target passed to the task as an
    /// override via with_target
    pub target_override: Option<Value>,

    /// The task path in the system state
    pub path: Path,

    /// The task path arguments if any
    pub args: PathArgs,
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a new context with the provided target
    pub fn with_target(self, target: Value) -> Self {
        Self {
            target_override: Some(target),
            ..self
        }
    }

    /// Return a new context with the provided path
    pub fn with_path(self, path: impl AsRef<str>) -> Self {
        let path = Path::new(
            PointerBuf::parse(path.as_ref())
                // this is a bug if it happens
                .expect("invalid JSON Pointer path")
                .as_ptr(),
        );
        Self { path, ..self }
    }

    /// Return a new context including the key, value pair as an argument
    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        let Self { mut args, .. } = self;

        // escape the argument value
        let value: String = value.into();
        let encoded = Token::from(value).encoded().to_owned();
        args.insert(key, encoded);
        Self { args, ..self }
    }

    /// Return a new context with the provided path arguments
    pub fn with_args(self, args: PathArgs) -> Self {
        Self { args, ..self }
    }
}
