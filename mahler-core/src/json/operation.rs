use std::cmp::Ordering;
use std::fmt;

use super::patch::{AddOperation, PatchOperation, RemoveOperation, ReplaceOperation};
use super::path::Path;
use super::value::Value;

use crate::serde::{ser::SerializeMap, Serialize, Serializer};

/// An operation on the system state
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Operation {
    Create { path: Path, value: Value },
    Update { path: Path, value: Value },
    Delete { path: Path },
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use Operation::*;
        match self {
            Create { path, value } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("op", "create")?;
                map.serialize_entry("path", path)?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            Update { path, value } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("op", "update")?;
                map.serialize_entry("path", path)?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            Delete { path } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("op", "delete")?;
                map.serialize_entry("path", path)?;
                map.end()
            }
        }
    }
}

impl Operation {
    /// Return the operation path
    pub fn path(&self) -> &Path {
        use Operation::*;
        match *self {
            Create { ref path, .. } => path,
            Update { ref path, .. } => path,
            Delete { ref path, .. } => path,
        }
    }
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
                path: Path::from(path),
                value,
            },
            PatchOperation::Replace(ReplaceOperation { path, value }) => Operation::Update {
                path: Path::from(path),
                value,
            },
            PatchOperation::Remove(RemoveOperation { path }) => Operation::Delete {
                path: Path::from(path),
            },
            // move/copy/test are not supported, but it is not necessary as they are not
            // never returned by json_patch::diff
            _ => unreachable!("unsuppported operation {op}"),
        }
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Operation {
    fn cmp(&self, other: &Self) -> Ordering {
        let thispath = self.path().as_ref();
        let otherpath = other.path().as_ref();

        // Order operations by path length
        thispath
            .count()
            .cmp(&otherpath.count())
            .then(thispath.cmp(otherpath))
    }
}

/// Allows to match a specific operation, any operation or none
#[derive(PartialEq, PartialOrd, Eq, Ord, Debug, Clone)]
pub enum OperationMatcher {
    /// Do not match any operation
    None,
    /// Match on any operation create/update/delete
    Any,
    /// Match a [remove](https://datatracker.ietf.org/doc/html/rfc6902#section-4.2) operation
    /// (equivalent to [`Operation::Delete`])
    Delete,
    /// Match an [add](https://datatracker.ietf.org/doc/html/rfc6902#section-4.1) operation
    /// (equivalent to [`Operation::Create`])
    Create,
    /// Match a [replace](https://datatracker.ietf.org/doc/html/rfc6902#section-4.3) operation
    /// (equivalent to [`Operation::Update`])
    Update,
}

impl OperationMatcher {
    /// Returns true if the matcher allows the given operation
    pub fn matches(&self, operation: &Operation) -> bool {
        self.allows(&operation.into())
    }

    /// Returns true if the matcher allows operations of the given kind
    ///
    /// The kind is the matcher of the operation (see `From<&Operation>`), i.e. one of
    /// `Create`, `Update` or `Delete`
    pub fn allows(&self, kind: &OperationMatcher) -> bool {
        use OperationMatcher::*;
        match *self {
            None => false,
            Any => true,
            Create | Update | Delete => self == kind,
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        use OperationMatcher::*;
        match *self {
            None => "none",
            Any => "any",
            Update => "update",
            Create => "create",
            Delete => "delete",
        }
    }
}

impl From<&Operation> for OperationMatcher {
    fn from(operation: &Operation) -> Self {
        match operation {
            Operation::Create { .. } => OperationMatcher::Create,
            Operation::Update { .. } => OperationMatcher::Update,
            Operation::Delete { .. } => OperationMatcher::Delete,
        }
    }
}

impl fmt::Display for OperationMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}
