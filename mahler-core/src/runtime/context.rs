use std::sync::{Arc, OnceLock};

use jsonptr::{PointerBuf, Token};

use crate::json::{Path, PathArgs, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
/// Describes the Task applicability context
pub struct Context {
    /// The global target passed to the planner
    ///
    /// Shared between every task created during a planning run
    pub target: Arc<Value>,

    /// A specific target passed to the task as an
    /// override via with_target
    pub target_override: Option<Arc<Value>>,

    /// The task path in the system state
    pub path: Path,

    /// The task path arguments if any
    pub args: PathArgs,
}

impl Default for Context {
    fn default() -> Self {
        // every task is created with a default context, sharing the null target avoids
        // allocating one for each of them
        static NULL_TARGET: OnceLock<Arc<Value>> = OnceLock::new();
        Self {
            target: NULL_TARGET.get_or_init(|| Arc::new(Value::Null)).clone(),
            target_override: None,
            path: Path::default(),
            args: PathArgs::default(),
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a new context with the provided task-specific target override
    pub fn with_target_override(self, target: Value) -> Self {
        Self {
            target_override: Some(Arc::new(target)),
            ..self
        }
    }

    /// Return a new context with the provided path
    pub fn with_path(self, path: impl AsRef<str>) -> Self {
        let path = Path::from(
            PointerBuf::parse(path.as_ref())
                // this is a bug if it happens
                .expect("invalid JSON Pointer path"),
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
