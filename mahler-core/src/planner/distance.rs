use json_patch::{diff, Patch, PatchOperation, RemoveOperation, ReplaceOperation};
use jsonptr::Pointer;
use serde_json::Value;
use std::collections::btree_set::Iter;
use std::fmt::{self, Display};
use std::{cmp::Ordering, collections::BTreeSet};

use crate::json::Path;
use crate::task::Operation as JobOperation;

#[derive(Debug)]
pub struct Distance {
    changes: Patch,
    operations: BTreeSet<Operation>,
}

fn insert_remove_ops(ops: &mut BTreeSet<Operation>, path: &Pointer, value: &Value) {
    let mut queue = Vec::new();
    queue.push((path.to_buf(), value));

    while let Some((path, value)) = queue.pop() {
        if value.is_object() {
            let obj = value.as_object().unwrap();
            for (k, v) in obj.iter() {
                let path = path.concat(Pointer::parse(&format!("/{k}")).unwrap());
                // Insert a remove operation for each child
                ops.insert(Operation::from(PatchOperation::Remove(RemoveOperation {
                    path: path.clone(),
                })));

                // Append the value to the queue
                queue.push((path, v));
            }
        }

        if value.is_array() {
            let obj = value.as_array().unwrap();
            for (k, v) in obj.iter().enumerate() {
                let path = path.concat(Pointer::parse(&format!("/{k}")).unwrap());

                // Insert a remove operation for each child
                ops.insert(Operation::from(PatchOperation::Remove(RemoveOperation {
                    path: path.clone(),
                })));

                // Append the value to the queue
                queue.push((path, v));
            }
        }
    }
}

impl Display for Distance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.changes.fmt(f)
    }
}

impl Distance {
    /// Calculate the distance between some state and target
    ///
    /// The distance is the list of operations required in order to convert the
    /// the state into the target. The distance also includes alternate paths
    ///
    /// For instance if the target creates a new value at the
    /// pointer `/a/b/c`, that new value could come from a create operation on the
    /// given pointer, but also from an update operation on `/a/b` or `/a` or `/`.
    ///
    /// Similarly, in order to delete a pointer `/a/b`, just removing the path could be enough, but
    /// it might be necessary to remove every child of `/a/b` before being able to remove the
    /// path.
    ///
    /// The distance encodes all the possible operations that can be used to move
    /// between two states.
    ///
    /// When creating a new distance, an ignore list can be added. Changes for path prefixed by any of the
    /// paths in the ignore list are not added to the operations list.
    pub fn new(src: &Value, tgt: &Value, ignore: &[Path]) -> Distance {
        let mut operations = BTreeSet::new();

        // calculate differences between the system root and
        // the target
        let changes = diff(src, tgt);
        for op in &changes.0 {
            // For every operation on the list of changes
            let path = op.path();

            if ignore.iter().any(|p| p.is_prefix_of(&Path::new(path))) {
                continue;
            }

            let mut parent = path;

            // get all paths up to the root
            while let Some(newparent) = parent.parent() {
                // get the target at the parent to use as value
                // no matter the operation, the parent of the target should
                // always exist. If it doesn't there is a bug (probbly in jsonptr)
                let value = newparent
                    .resolve(tgt)
                    .expect(&format!("path `{path}` should be resolvable on state"));

                // Insert a replace operation for each one
                operations.insert(Operation::from(PatchOperation::Replace(ReplaceOperation {
                    path: newparent.to_buf(),
                    value: value.clone(),
                })));

                parent = newparent;
            }

            // for every delete operation '/a/b/c', add child
            // nodes of the deleted path with 'remove' operation
            if let PatchOperation::Remove(_) = op {
                // By definition of the remove operation the path should be
                // resolvable on the left side of the diff
                let value = path
                    .resolve(src)
                    .expect(&format!("path `{path}` should be resolvable on state"));

                // add remove operations for inner values
                insert_remove_ops(&mut operations, path, value);
            }

            // Finally insert the actual operation
            operations.insert(Operation::from(op.clone()));
        }

        Distance {
            changes,
            operations,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn operations(&self) -> Iter<'_, Operation> {
        self.operations.iter()
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) struct Operation(PatchOperation);

impl Operation {
    pub fn path(&self) -> &Pointer {
        self.0.path()
    }

    pub fn matches(&self, op: &JobOperation) -> bool {
        match self.0 {
            PatchOperation::Add(..) => op == &JobOperation::Create,
            PatchOperation::Replace(..) => op == &JobOperation::Update,
            PatchOperation::Remove(..) => op == &JobOperation::Delete,
            _ => false,
        }
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Operation {
    fn cmp(&self, other: &Self) -> Ordering {
        let thispath = self.0.path();
        let otherpath = other.0.path();

        // Order operations by path length
        thispath
            .count()
            .cmp(&otherpath.count())
            .then(thispath.cmp(otherpath))
    }
}

impl From<PatchOperation> for Operation {
    fn from(op: PatchOperation) -> Operation {
        Operation(op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn distance_eq(src: Value, tgt: Value, result: Vec<Value>) {
        let distance = Distance::new(&src, &tgt, &[]);

        // Serialize results to make comparison easier
        let ops: Vec<Value> = distance
            .operations()
            .map(|Operation(o)| serde_json::to_value(o).unwrap())
            .collect();

        assert_eq!(ops, result)
    }

    #[test]
    fn it_calculates_possible_changes_to_target() {
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2, "b": "one", "c": {}}),
            vec![
                json!({"op": "replace", "path": "", "value": {"a": 2, "b": "one", "c": {}}}),
                json!({"op": "replace", "path": "/a", "value": 2}),
                json!({"op": "replace", "path": "/c", "value": {}}),
                json!({"op": "remove", "path": "/c/k"}),
            ],
        );
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2}),
            vec![
                json!({"op": "replace", "path": "", "value": {"a": 2}}),
                json!({"op": "replace", "path": "/a", "value": 2}),
                json!({"op": "remove", "path": "/b"}),
                json!({"op": "remove", "path": "/c"}),
                json!({"op": "remove", "path": "/c/k"}),
            ],
        );
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2, "b": "two", "c": {"k": "v"}}),
            vec![
                json!({"op": "replace", "path": "", "value": {"a": 2, "b": "two", "c": {"k": "v"}}}),
                json!({"op": "replace", "path": "/a", "value": 2}),
                json!({"op": "replace", "path": "/b", "value": "two"}),
            ],
        );
        distance_eq(
            json!({"a": {"b": {"c": {"d": "e"}}}}),
            json!({"a": {"b": {}}}),
            vec![
                json!({"op": "replace", "path": "", "value": {"a": {"b": {}}}}),
                json!({"op": "replace", "path": "/a", "value": {"b": {}}}),
                json!({"op": "replace", "path": "/a/b", "value": {}}),
                json!({"op": "remove", "path": "/a/b/c"}),
                json!({"op": "remove", "path": "/a/b/c/d"}),
            ],
        );
    }
}
