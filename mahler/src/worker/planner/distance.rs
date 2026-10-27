use jsonptr::{Pointer, PointerBuf};
use serde_json::Error;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;

use crate::json::{diff_serialized, to_value, Operation, OperationMatcher, Path, Value};
use crate::serde::Serialize;

#[derive(Debug)]
pub struct Distance {
    pub required: Vec<Operation>,
    pub ignored: Vec<Operation>,
    /// Every path that may be operated on to reach the target, with the kind of operation.
    ///
    /// The kind is always one of `Create`, `Update` or `Delete`. Values are not kept as
    /// only the path and the kind are used to look for matching jobs.
    pub operations: Vec<(Path, OperationMatcher)>,
}

fn insert_remove_ops<'a>(
    ops: &mut BTreeMap<Path, OperationMatcher>,
    path: &Pointer,
    value: &'a Value,
) {
    let mut queue = vec![(path.to_buf(), value)];

    while let Some((path, value)) = queue.pop() {
        let mut push = |child: PointerBuf, v: &'a Value| {
            // only containers have children left to visit
            if v.is_object() || v.is_array() {
                queue.push((child.clone(), v));
            }
            // `or_insert` keeps the first operation recorded for a path
            ops.entry(Path::from(child))
                .or_insert(OperationMatcher::Delete);
        };

        match value {
            Value::Object(obj) => obj
                .iter()
                .for_each(|(k, v)| push(path.with_trailing_token(k.as_str()), v)),
            Value::Array(arr) => arr
                .iter()
                .enumerate()
                .for_each(|(i, v)| push(path.with_trailing_token(i), v)),
            _ => {}
        }
    }
}

impl fmt::Display for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{\"required\": [")?;
        for (i, op) in self.required.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            op.fmt(f)?;
        }
        write!(f, "], \"ignored\": [")?;
        for (i, op) in self.ignored.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            op.fmt(f)?;
        }
        write!(f, "]}}")
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
    ///
    /// # Errors
    ///
    /// Fails if the state cannot be converted into JSON
    pub fn new<S: Serialize>(src: &S, tgt: &Value, ignore: &[Path]) -> Result<Distance, Error> {
        // Ordered by path (length, then lexicographically), the first operation recorded
        // for a path wins
        let mut operations = BTreeMap::new();

        // calculate differences between the system root and
        // the target
        let changes = diff_serialized(src, tgt)?;

        let (ignored, required): (Vec<_>, Vec<_>) = changes
            .into_iter()
            .map(Operation::from)
            .partition(|op| ignore.iter().any(|p| p.is_prefix_of(op.path())));

        let mut src_value = None;
        for op in required.iter() {
            // For every operation on the list of changes
            let path = op.path().as_ref();

            // every parent up to the root may be updated to reach the target
            let mut parent = path;
            while let Some(newparent) = parent.parent() {
                // every path in the map already has its ancestors in the map
                if let Entry::Vacant(entry) = operations.entry(Path::new(newparent)) {
                    entry.insert(OperationMatcher::Update);
                } else {
                    break;
                }
                parent = newparent;
            }

            // for every delete operation '/a/b/c', add child
            // nodes of the deleted path with 'remove' operation
            if let Operation::Delete { .. } = op {
                // The state is only converted to JSON if something is removed from it
                if src_value.is_none() {
                    src_value = Some(to_value(src)?);
                }
                let src_value = src_value.as_ref().expect("value was converted above");

                // By definition of the remove operation the path should be
                // resolvable on the left side of the diff
                let value = path
                    .resolve(src_value)
                    .unwrap_or_else(|e| panic!("path `{path}` should be resolvable on state: {e}"));

                // add remove operations for inner values
                insert_remove_ops(&mut operations, path, value);
            }

            // Finally insert the actual operation
            operations.entry(op.path().clone()).or_insert(op.into());
        }

        Ok(Distance {
            ignored,
            required,
            operations: operations.into_iter().collect(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.required.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn distance_eq(src: Value, tgt: Value, result: Vec<Value>) {
        let distance = Distance::new(&src, &tgt, &[]).unwrap();

        // Serialize results to make comparison easier
        let ops: Vec<Value> = distance
            .operations
            .iter()
            .map(|(path, kind)| json!({"op": kind.to_string(), "path": path}))
            .collect();

        assert_eq!(ops, result)
    }

    #[test]
    fn it_calculates_possible_changes_to_target() {
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2, "b": "one", "c": {}}),
            vec![
                json!({"op": "update", "path": ""}),
                json!({"op": "update", "path": "/a"}),
                json!({"op": "update", "path": "/c"}),
                json!({"op": "delete", "path": "/c/k"}),
            ],
        );
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2}),
            vec![
                json!({"op": "update", "path": ""}),
                json!({"op": "update", "path": "/a"}),
                json!({"op": "delete", "path": "/b"}),
                json!({"op": "delete", "path": "/c"}),
                json!({"op": "delete", "path": "/c/k"}),
            ],
        );
        distance_eq(
            json!({"a": 1, "b": "one", "c": {"k": "v"}}),
            json!({"a": 2, "b": "two", "c": {"k": "v"}}),
            vec![
                json!({"op": "update", "path": ""}),
                json!({"op": "update", "path": "/a"}),
                json!({"op": "update", "path": "/b"}),
            ],
        );
        distance_eq(
            json!({"a": {"b": {"c": {"d": "e"}}}}),
            json!({"a": {"b": {}}}),
            vec![
                json!({"op": "update", "path": ""}),
                json!({"op": "update", "path": "/a"}),
                json!({"op": "update", "path": "/a/b"}),
                json!({"op": "delete", "path": "/a/b/c"}),
                json!({"op": "delete", "path": "/a/b/c/d"}),
            ],
        );
    }
}
