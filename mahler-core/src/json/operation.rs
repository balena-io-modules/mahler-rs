use json_patch::{AddOperation, PatchOperation, RemoveOperation, ReplaceOperation};
use std::fmt;

use super::path::Path;
use super::value::Value;

/// An operation on the system state
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Operation {
    Create { path: Path, value: Value },
    Update { path: Path, value: Value },
    Delete { path: Path },
}

impl fmt::Display for Operation {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Operation::*;

        write!(fmt, "{{")?;
        match *self {
            Create {
                ref path,
                ref value,
            } => {
                write!(fmt, "\"op\": \"create\",")?;
                write!(fmt, "\"path\": {path},")?;
                write!(fmt, "\"value\": {value}")?;
            }
            Update {
                ref path,
                ref value,
            } => {
                write!(fmt, "\"op\": \"update\",")?;
                write!(fmt, "\"path\": {path},")?;
                write!(fmt, "\"value\": {value}")?;
            }
            Delete { ref path } => {
                write!(fmt, "\"op\": \"delete\",")?;
                write!(fmt, "\"path\": {path}")?;
            }
        }
        write!(fmt, "}}")
    }
}

impl From<PatchOperation> for Operation {
    fn from(op: PatchOperation) -> Self {
        match op {
            PatchOperation::Add(AddOperation { path, value }) => Operation::Create {
                path: Path::new(&path),
                value,
            },
            PatchOperation::Replace(ReplaceOperation { path, value }) => Operation::Update {
                path: Path::new(&path),
                value,
            },
            PatchOperation::Remove(RemoveOperation { path }) => Operation::Delete {
                path: Path::new(&path),
            },
            // move/copy/test are not supported, but it is not necessary as they are not
            // never returned by json_patch::diff
            _ => unreachable!("unsuppported operation {op}"),
        }
    }
}
