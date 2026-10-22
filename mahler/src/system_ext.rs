use jsonptr::Pointer;

use crate::error::Error;
use crate::json::{AddOperation, PatchOperation, RemoveOperation, ReplaceOperation, Value};
use crate::result::Result;
use crate::runtime::System;

pub trait SystemExt {
    /// Update the internal copy of the system state
    ///
    /// If applying the changes fails, the state is left as it was before the call.
    fn patch(&mut self, changes: &[PatchOperation]) -> Result<()>;

    /// Update the internal copy of the system state, appending to `undo` the operations
    /// that revert the changes
    ///
    /// If applying the changes fails, the state is left as it was before the call.
    fn patch_with_undo(
        &mut self,
        changes: impl IntoIterator<Item = PatchOperation>,
        undo: &mut Vec<PatchOperation>,
    ) -> Result<()>;

    /// Revert the changes recorded by [`SystemExt::patch_with_undo`]
    fn revert(&mut self, undo: Vec<PatchOperation>) -> Result<()>;
}

impl SystemExt for System {
    fn patch(&mut self, changes: &[PatchOperation]) -> Result<()> {
        json_patch::patch(self.inner_state_mut(), changes).map_err(Error::internal)?;
        Ok(())
    }

    fn patch_with_undo(
        &mut self,
        changes: impl IntoIterator<Item = PatchOperation>,
        undo: &mut Vec<PatchOperation>,
    ) -> Result<()> {
        let mut changes = changes.into_iter().peekable();
        if changes.peek().is_none() {
            // avoid copying a shared state when there is nothing to apply
            return Ok(());
        }

        let doc = self.inner_state_mut();
        let start = undo.len();
        for op in changes {
            match apply(doc, op) {
                Ok(inverse) => undo.push(inverse),
                Err(e) => {
                    // the operations applied so far are known to be revertible
                    for inverse in undo.drain(start..).rev() {
                        apply(doc, inverse)?;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn revert(&mut self, undo: Vec<PatchOperation>) -> Result<()> {
        if undo.is_empty() {
            return Ok(());
        }
        let doc = self.inner_state_mut();
        for inverse in undo.into_iter().rev() {
            apply(doc, inverse)?;
        }
        Ok(())
    }
}

fn invalid_op(op: &str, path: &Pointer) -> Error {
    Error::internal(format!("cannot apply {op} operation at {path}"))
}

/// Apply a single operation to the document, returning the operation that reverts it
///
/// The replaced or removed value is moved into the returned operation rather than
/// copied. Only add, remove and replace are supported, as these are the only operations
/// produced by mahler. On error the document is not modified.
///
/// [`json_patch::patch`] keeps an equivalent undo log internally but does not expose it,
/// which the planner needs to revert successfully applied changes.
fn apply(doc: &mut Value, op: PatchOperation) -> Result<PatchOperation> {
    match op {
        PatchOperation::Replace(ReplaceOperation { path, value }) => {
            let slot = path
                .resolve_mut(doc)
                .map_err(|_| invalid_op("replace", &path))?;
            let old = std::mem::replace(slot, value);
            Ok(PatchOperation::Replace(ReplaceOperation {
                path,
                value: old,
            }))
        }
        PatchOperation::Remove(RemoveOperation { path }) => {
            let removed = path.split_back().and_then(|(parent, last)| {
                match parent.resolve_mut(doc).ok()? {
                    Value::Object(obj) => obj.remove(last.decoded().as_ref()),
                    Value::Array(arr) => {
                        let index = last.to_index().ok()?.for_len(arr.len()).ok()?;
                        Some(arr.remove(index))
                    }
                    _ => None,
                }
            });
            let value = removed.ok_or_else(|| invalid_op("remove", &path))?;
            Ok(PatchOperation::Add(AddOperation { path, value }))
        }
        PatchOperation::Add(AddOperation { path, value }) => {
            let Some((parent, last)) = path.split_back() else {
                // adding at the root replaces the whole document
                let old = std::mem::replace(doc, value);
                return Ok(PatchOperation::Replace(ReplaceOperation {
                    path,
                    value: old,
                }));
            };
            let inverse = match parent.resolve_mut(doc) {
                Ok(Value::Object(obj)) => match obj.insert(last.decoded().into_owned(), value) {
                    Some(old) => PatchOperation::Replace(ReplaceOperation { path, value: old }),
                    None => PatchOperation::Remove(RemoveOperation { path }),
                },
                Ok(Value::Array(arr)) => {
                    let index = last
                        .to_index()
                        .ok()
                        .and_then(|index| index.for_len_incl(arr.len()).ok())
                        .ok_or_else(|| invalid_op("add", &path))?;
                    arr.insert(index, value);
                    PatchOperation::Remove(RemoveOperation {
                        path: parent.with_trailing_token(index),
                    })
                }
                _ => return Err(invalid_op("add", &path)),
            };
            Ok(inverse)
        }
        op => Err(Error::internal(format!("unsupported operation {op}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::Patch;
    use serde_json::{from_value, json};

    fn ops(value: Value) -> Vec<PatchOperation> {
        from_value::<Patch>(value).unwrap().0
    }

    #[test]
    fn it_reverts_changes() {
        let initial = json!({"a": {"b": 1, "c": [1, 2, 3]}, "d": "x"});
        let mut system = System::try_from(initial.clone()).unwrap();

        let changes = ops(json!([
            {"op": "replace", "path": "/a/b", "value": 2},
            {"op": "add", "path": "/a/c/1", "value": 9},
            {"op": "add", "path": "/a/c/-", "value": 10},
            {"op": "remove", "path": "/a/c/0"},
            {"op": "add", "path": "/e", "value": {"f": true}},
            {"op": "add", "path": "/d", "value": "y"},
            {"op": "remove", "path": "/a/b"},
        ]));

        let mut expected = initial.clone();
        json_patch::patch(&mut expected, &changes).unwrap();

        let mut undo = Vec::new();
        system.patch_with_undo(changes.clone(), &mut undo).unwrap();
        assert_eq!(system.inner_state(), &expected);

        system.revert(undo).unwrap();
        assert_eq!(system.inner_state(), &initial);
    }

    #[test]
    fn it_leaves_the_state_unchanged_on_failure() {
        let initial = json!({"a": {"b": 1}});
        let mut system = System::try_from(initial.clone()).unwrap();

        let changes = ops(json!([
            {"op": "replace", "path": "/a/b", "value": 2},
            {"op": "remove", "path": "/a/missing"},
        ]));

        let mut undo = Vec::new();
        assert!(system.patch_with_undo(changes.clone(), &mut undo).is_err());
        assert!(undo.is_empty());
        assert_eq!(system.inner_state(), &initial);
    }

    #[test]
    fn it_replaces_the_root() {
        let initial = json!({"a": 1});
        let mut system = System::try_from(initial.clone()).unwrap();

        let changes = ops(json!([{"op": "add", "path": "", "value": [1]}]));
        let mut undo = Vec::new();
        system.patch_with_undo(changes.clone(), &mut undo).unwrap();
        assert_eq!(system.inner_state(), &json!([1]));

        system.revert(undo).unwrap();
        assert_eq!(system.inner_state(), &initial);
    }
}
