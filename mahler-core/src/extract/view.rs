use anyhow::Context as AnyhowCtx;
use json_patch::{
    diff, AddOperation, CopyOperation, MoveOperation, Patch, PatchOperation, RemoveOperation,
    ReplaceOperation, TestOperation,
};
use jsonptr::resolve::ResolveError;
use jsonptr::PointerBuf;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::ops::{Deref, DerefMut};

use crate::errors::ExtractionError;
use crate::path::Path;
use crate::system::System;
use crate::task::{Context, Error, FromSystem, IntoResult};

/// Extracts a view to a sub-element of the global state indicated
/// by the path.
///
/// The type of the sub-element is given by the type parameter T.
///
/// The `View` extractor expects that the location pointed by the Job path
/// exists and is deserializable into T. If the value may not exist (or is null),
/// then make sure to use `View<Option<T>>`.
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::View,
///     task::{Handler, create, update, with_io, IO},
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// fn foo_bar(mut view: View<i32>) -> IO<i32> {
///     // view can be dereferenced into the given type
///     // and is guaranteed to exist at this point
///     if *view < 5 {
///         *view += 1;
///     }
///
///     with_io(view, |view| async {
///         // do something with view at runtime
///         Ok(view)
///     })
/// }
///
/// fn create_counter(mut view: View<Option<i32>>) -> IO<Option<i32>> {
///     if view.is_none() {
///         // Initialize with default value if it doesn't exist
///         *view = Some(0);
///     }
///
///     with_io(view, |view| async {
///         // do something with view at runtime
///         Ok(view)
///     })
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", create(create_counter))
///     .job("/{foo}/{bar}", update(foo_bar))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
///
/// # Errors
///
/// Initializing the extractor will fail if the path assigned to the job cannot be resolved or the
/// value pointed by the path cannot be deserialized into type `<T>`
#[derive(Debug)]
pub struct View<T> {
    initial: Value,
    state: T,
    path: Path,
}

impl<T> View<T> {
    // The only way to create a pointer is via the
    // from_system method
    fn new(initial: Value, state: T, path: Path) -> Self {
        Self {
            initial,
            state,
            path,
        }
    }
}

impl<T: DeserializeOwned> FromSystem for View<T> {
    type Error = ExtractionError;

    fn from_system(system: &System, context: &Context) -> Result<Self, Self::Error> {
        let json_ptr = context.path.as_ref();
        let root = system.root();

        // Use the parent of the pointer unless we are at the root
        let parent = json_ptr.parent().unwrap_or(json_ptr);

        // Try to resolve the parent or fail
        // XXX: how can this happen?
        parent
            .resolve(root)
            .with_context(|| format!("failed to resolve path {}", context.path))?;

        // At this point we assume that if the pointer cannot be
        // resolved is because the value does not exist yet unless
        // the parent is a scalar
        let (state, initial): (T, Value) = match json_ptr.resolve(root) {
            Ok(value) => (
                serde_json::from_value::<T>(value.clone()).with_context(|| {
                    format!(
                        "failed to deserialize {} from: {value}",
                        std::any::type_name::<T>()
                    )
                })?,
                value.clone(),
            ),
            Err(e) => match e {
                ResolveError::NotFound { .. } => (
                    // if the value does not exist, see if we can deserialize null into the type
                    serde_json::from_value::<T>(Value::Null).with_context(|| {
                        format!(
                            "failed to deserialize {} from: null",
                            std::any::type_name::<T>()
                        )
                    })?,
                    Value::Null,
                ),
                ResolveError::OutOfBounds { .. } => (
                    serde_json::from_value::<T>(Value::Null).with_context(|| {
                        format!(
                            "failed to deserialize {} from: null",
                            std::any::type_name::<T>()
                        )
                    })?,
                    Value::Null,
                ),
                _ => {
                    // XXX: how can this happen?
                    return Err(e)
                        .with_context(|| format!("failed to resolve path {}", context.path))?;
                }
            },
        };

        Ok(View::new(initial, state, context.path.clone()))
    }
}

impl<T> Deref for View<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T> DerefMut for View<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

fn prepend_path(pointer: PointerBuf, patch: Patch) -> Patch {
    let Patch(changes) = patch;
    let changes = changes
        .into_iter()
        .map(|op| match op {
            PatchOperation::Replace(ReplaceOperation { path, value }) => {
                PatchOperation::Replace(ReplaceOperation {
                    path: pointer.concat(&path),
                    value,
                })
            }
            PatchOperation::Remove(RemoveOperation { path }) => {
                PatchOperation::Remove(RemoveOperation {
                    path: pointer.concat(&path),
                })
            }
            PatchOperation::Add(AddOperation { path, value }) => {
                PatchOperation::Add(AddOperation {
                    path: pointer.concat(&path),
                    value,
                })
            }
            PatchOperation::Move(MoveOperation { from, path }) => {
                PatchOperation::Move(MoveOperation {
                    from,
                    path: pointer.concat(&path),
                })
            }
            PatchOperation::Copy(CopyOperation { from, path }) => {
                PatchOperation::Copy(CopyOperation {
                    from,
                    path: pointer.concat(&path),
                })
            }
            PatchOperation::Test(TestOperation { path, value }) => {
                PatchOperation::Test(TestOperation {
                    path: pointer.concat(&path),
                    value,
                })
            }
        })
        .collect::<Vec<PatchOperation>>();
    Patch(changes)
}

impl<T: Serialize> IntoResult<Patch> for View<T> {
    fn into_result(self) -> Result<Patch, Error> {
        let before = self.initial;

        // This should not happen unless there is a bug (hopefully).
        // if this happens during worker operation, it will be caught
        // as a panic in the task
        let after = serde_json::to_value(self.state).expect("failed to serialize view value");

        let patch = match (before, after) {
            (Value::Null, Value::Null) => Patch(vec![]),
            (Value::Null, after) => Patch(vec![PatchOperation::Add(AddOperation {
                path: self.path.into(),
                value: after,
            })]),
            (_, Value::Null) => Patch(vec![PatchOperation::Remove(RemoveOperation {
                path: self.path.into(),
            })]),
            (before, after) => prepend_path(self.path.into(), diff(&before, &after)),
        };

        Ok(patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::State;
    use crate::system::System;
    use json_patch::Patch;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, Debug)]
    struct MyState {
        numbers: HashMap<String, i32>,
    }

    impl State for MyState {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct StateVec {
        numbers: Vec<String>,
    }

    impl State for StateVec {
        type Target = Self;
    }

    #[test]
    fn it_extracts_an_existing_value_using_optional_view() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/one")).unwrap();

        assert_eq!(view.as_ref(), Some(&1));

        let value = view.as_mut().unwrap();
        *value = 2;

        // Get the list changes to the view

        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/one", "value": 2 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_fails_if_optional_view_path_is_invalid() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        assert!(View::<Option<i32>>::from_system(
            &system,
            &Context::new().with_path("/numbers/one/two"),
        )
        .is_err());
        assert!(
            View::<Option<i32>>::from_system(&system, &Context::new().with_path("/none/two"),)
                .is_err()
        );
    }

    #[test]
    fn it_assigns_a_value_to_optional_view_path() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        view.replace(3);

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/three", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_allows_changing_a_value_with_a_view() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/two")).unwrap();
        *view = 3;

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/two", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_fails_to_initialize_view_if_path_does_not_exist() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        assert!(
            View::<i32>::from_system(&system, &Context::new().with_path("/numbers/three")).is_err()
        );
        assert!(
            View::<i32>::from_system(&system, &Context::new().with_path("/none/three")).is_err()
        );
    }

    #[test]
    fn it_initializes_optional_view_with_default() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        let value = view.get_or_insert(0);
        *value = 3;

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/three", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_deletes_an_existing_value_with_optional_view() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/one")).unwrap();

        // Delete the value
        view.take();

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/one" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_extracts_an_existing_value_on_a_vec_with_optional_view() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers/1")).unwrap();

        assert_eq!(view.as_ref(), Some(&"two".to_string()));

        let value = view.as_mut().unwrap();
        *value = "TWO".to_string();

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/1", "value": "TWO" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_creates_a_value_on_a_vec_with_optional_view() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string()],
        };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers/2")).unwrap();

        assert_eq!(view.as_ref(), None);
        view.replace("three".into());

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/2", "value": "three" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_deletes_a_value_on_a_vec_with_optional_view() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let mut system = System::try_from(state).unwrap();

        let mut view: View<Option<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers/1")).unwrap();

        // Remove the second element
        view.take();

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            // Removing a value from the middle of the array requires shifting the indexes
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/1" },
            ]))
            .unwrap()
        );

        system.patch(changes).unwrap();
        assert_eq!(
            system.root(),
            &serde_json::from_value::<Value>(json!({"numbers": ["one", "three"]})).unwrap()
        );
    }

    #[test]
    fn it_deletes_a_value_from_the_end_of_a_vec_with_optional_view() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers/2")).unwrap();

        // Remove the third element
        view.take();

        // Get the list changes to the view
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/2" },
            ]))
            .unwrap()
        );
    }
}
