use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation, ReplaceOperation};
use jsonptr::resolve::ResolveError;
use jsonptr::PointerBuf;
use serde_json::Value;
use std::ops::{Deref, DerefMut};

use crate::error::{Error, ErrorKind};
use crate::json::{diff, diff_to_serialized, Path};
use crate::result::Result;
use crate::runtime::{Channel, Context, FromSystem, System};
use crate::serde::{de::DeserializeOwned, Serialize};
use crate::task::IntoResult;

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
///     task::{with_io, IO},
///     job::{create, update},
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
    initial: Initial,
    state: T,
    path: Path,
    channel: Channel,
}

/// The value at the view path to compare the view against when creating a patch
#[derive(Debug)]
enum Initial {
    /// The value is read from the system the view was created from. Sharing the system
    /// avoids copying the value for views that are never turned into a patch
    System(System),
    /// The value was set after the view was created
    Value(Value),
}

impl Initial {
    fn resolve(&self, path: &Path) -> &Value {
        match self {
            Initial::System(system) => path
                .as_ref()
                .resolve(system.inner_state())
                .unwrap_or(&Value::Null),
            Initial::Value(value) => value,
        }
    }
}

impl<T> View<T> {
    // The only way to create a pointer is via the
    // from_system method
    fn new(initial: Initial, state: T, path: Path, channel: Channel) -> Self {
        Self {
            initial,
            state,
            path,
            channel,
        }
    }

    fn create_patch(&self, after: Value) -> Patch
    where
        T: Serialize,
    {
        let path = || PointerBuf::from(self.path.clone());
        let before = self.initial.resolve(&self.path);

        match (before, after) {
            (&Value::Null, Value::Null) => Patch(vec![]),
            (&Value::Null, after) => Patch(vec![PatchOperation::Add(AddOperation {
                path: path(),
                value: after,
            })]),
            (_, Value::Null) => Patch(vec![PatchOperation::Remove(RemoveOperation {
                path: path(),
            })]),
            (before, after) => Patch(diff(before, &after, self.path.as_ref())),
        }
    }

    /// Flush the changes to the view back to the worker
    ///
    /// This will communicate any changes to the view back to the worker to update the runtime
    /// state immediately, allowing followers to observe intermediate progress during long-running tasks.
    ///
    /// Note that flushed changes can still be rolled back if the task fails after flushing.
    ///
    /// # Errors
    ///
    /// Flush will fail if the type `<T>` [cannot be serialized into JSON](https://docs.rs/serde_json/latest/serde_json/fn.to_value.html#errors).
    /// It will also fail if the channel to the worker has closed, which should not happen unless
    /// there is a bug with Mahler (please report).
    ///
    /// Either error will also occur when processing the result of the handler, so it should be
    /// safe to ignore if you want the operation to finish.
    ///
    /// # Example
    ///
    /// ```rust
    /// use mahler::{
    ///     error::Error,
    ///     extract::View,
    ///     task::{with_io, IO},
    ///     job::update,
    ///     worker::Worker,
    /// };
    ///
    /// fn process_items(mut view: View<Vec<String>>) -> IO<Vec<String>> {
    ///     with_io(view, |mut view| async move {
    ///         // Process items one at a time, flushing after each
    ///         for i in 0..10 {
    ///             view.push(format!("item_{}", i));
    ///
    ///             // Flush intermediate progress so followers can see it
    ///             let _ = view.flush().await;
    ///
    ///             // Do some work
    ///             tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    ///         }
    ///         Ok(view)
    ///     })
    /// }
    /// ```
    pub async fn flush(&mut self) -> Result<()>
    where
        T: Serialize,
    {
        // Update the view only if the channel is connected
        if !self.channel.is_detached() {
            // An error should be rare
            // https://docs.rs/serde_json/latest/serde_json/fn.to_value.html#errors
            let after = crate::json::to_value(&self.state)?;

            // Create a patch from the initial state
            let changes = self.create_patch(after.clone());

            // Send the changes and wait for acknowledgement
            self.channel.send(changes).await?;

            // Update initial to match the current state
            self.initial = Initial::Value(after);
        }

        Ok(())
    }

    /// Commit the current state changes, updating the rollback point.
    ///
    /// Like [`flush`](View::flush), this sends changes to the worker immediately.
    /// Additionally, it marks the current state as the new rollback point — if
    /// the task fails after this call, only changes made *after* the commit
    /// are reverted.
    ///
    /// # Warning
    ///
    /// Committed changes persist in the global state even if the task fails
    /// later. For example, a `create` task that calls `commit()` will leave
    /// the entity in the state even if subsequent IO steps fail. Use only
    /// when partial progress must survive task failure.
    pub async fn commit(&mut self) -> Result<()>
    where
        T: Serialize,
    {
        if !self.channel.is_detached() {
            let after = crate::json::to_value(&self.state)?;
            let changes = self.create_patch(after.clone());
            self.initial = Initial::Value(after.clone());
            self.channel.send_and_commit(changes, after).await?;
        }
        Ok(())
    }

    /// Delete the value at the path pointed by the view.
    ///
    /// Takes ownership of the view and returns a `View<Option<T>>`
    /// as result
    pub fn delete(self) -> View<Option<T>> {
        let Self {
            initial,
            path,
            channel,
            ..
        } = self;
        View {
            initial,
            state: None,
            path,
            channel,
        }
    }
}

impl<T> View<Option<T>> {
    /// Initialize the value a the path pointed by the View
    ///
    /// Takes ownership of the View and returns a `View<T>`. If the existing view already
    /// contained a value, that value is dropped.
    pub fn create(self, value: T) -> View<T> {
        let Self {
            initial,
            path,
            channel,
            ..
        } = self;

        View {
            initial,
            state: value,
            path,
            channel,
        }
    }
}

impl<T: DeserializeOwned> FromSystem for View<T> {
    fn from_system(system: &System, context: &Context, channel: &Channel) -> Result<Self> {
        let pointer = context.path.as_ref();
        let root = system.inner_state();

        // Use the parent of the pointer unless we are at the root
        let parent = pointer.parent().unwrap_or(pointer);

        // Try to resolve the parent or fail
        parent.resolve(root).map_err(|e| match e {
            ResolveError::NotFound { .. } | ResolveError::OutOfBounds { .. } => {
                // we return an unexpected error here because the parent should exist at this point
                Error::internal(e)
            }
            ResolveError::Unreachable { .. }
            | jsonptr::resolve::Error::FailedToParseIndex { .. } => {
                // these two mean there is probably an error with the defined route
                Error::new(ErrorKind::InvalidRoute, e)
            }
        })?;

        // At this point we assume that if the pointer cannot be
        // resolved is because the value does not exist yet unless
        // the parent is a scalar
        let (state, initial): (T, Initial) = match pointer.resolve(root) {
            Ok(value) => (
                T::deserialize(value)
                    .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?,
                Initial::System(system.clone()),
            ),
            Err(e) => match e {
                // the value does not exist or may be a new index in an array, see if we can
                // deserialize null into the type. The initial value resolves to null
                ResolveError::NotFound { .. } | ResolveError::OutOfBounds { .. } => (
                    serde_json::from_value::<T>(Value::Null)
                        .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?,
                    Initial::System(system.clone()),
                ),
                _ => {
                    // The remaining errors are Unreachable and FailedToParse index
                    // both of which mean the user given route is invalid
                    return Err(Error::internal(e));
                }
            },
        };

        Ok(View::new(
            initial,
            state,
            context.path.clone(),
            channel.clone(),
        ))
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

impl<T: Serialize> IntoResult<Patch> for View<T> {
    fn into_result(self) -> Result<Patch> {
        let before = self.initial.resolve(&self.path);
        if before.is_null() {
            // the whole value is added, so it has to be converted anyway
            let after = crate::json::to_value(&self.state).map_err(Error::internal)?;
            return Ok(self.create_patch(after));
        }

        // Diff the view against its initial value without converting all of it to JSON.
        // An error here should be rare
        let mut changes =
            diff_to_serialized(before, &self.state, self.path.as_ref()).map_err(Error::internal)?;

        // a view set to null removes the value
        if let [PatchOperation::Replace(ReplaceOperation { path, value })] = changes.as_slice() {
            if value.is_null() && path.as_str() == self.path.as_str() {
                changes = vec![PatchOperation::Remove(RemoveOperation {
                    path: PointerBuf::from(self.path),
                })];
            }
        }

        Ok(Patch(changes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::System;
    use crate::state::State;
    use crate::system_ext::SystemExt;

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

        let mut view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/one"),
            &Channel::detached(),
        )
        .unwrap();

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
            &Channel::detached()
        )
        .is_err());
        assert!(View::<Option<i32>>::from_system(
            &system,
            &Context::new().with_path("/none/two"),
            &Channel::detached(),
        )
        .is_err());
    }

    #[test]
    fn it_assigns_a_value_to_optional_view_path() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/three"),
            &Channel::detached(),
        )
        .unwrap();

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

        let mut view: View<i32> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/two"),
            &Channel::detached(),
        )
        .unwrap();
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

        assert!(View::<i32>::from_system(
            &system,
            &Context::new().with_path("/numbers/three"),
            &Channel::detached(),
        )
        .is_err());
        assert!(View::<i32>::from_system(
            &system,
            &Context::new().with_path("/none/three"),
            &Channel::detached(),
        )
        .is_err());
    }

    #[test]
    fn it_initializes_optional_view_with_default() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/three"),
            &Channel::detached(),
        )
        .unwrap();

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
    fn it_initializes_optional_view_with_create() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };

        let system = System::try_from(state).unwrap();

        let view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/three"),
            &Channel::detached(),
        )
        .unwrap();

        assert_eq!(view.as_ref(), None);

        // Initialize the view via a `create` call
        let mut view = view.create(0);
        *view = 3;

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

        let mut view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/one"),
            &Channel::detached(),
        )
        .unwrap();

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

        let mut view: View<Option<String>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/1"),
            &Channel::detached(),
        )
        .unwrap();

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

        let mut view: View<Option<String>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/2"),
            &Channel::detached(),
        )
        .unwrap();

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

        let mut view: View<Option<String>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/1"),
            &Channel::detached(),
        )
        .unwrap();

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

        system.patch(&changes).unwrap();
        assert_eq!(
            system.inner_state(),
            &serde_json::from_value::<Value>(json!({"numbers": ["one", "three"]})).unwrap()
        );
    }

    #[test]
    fn it_deletes_a_value_from_the_end_of_a_vec_with_optional_view() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::try_from(state).unwrap();

        let mut view: View<Option<String>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/2"),
            &Channel::detached(),
        )
        .unwrap();

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

    #[tokio::test]
    async fn it_flush_is_noop_with_detached_channel() {
        let system = System::try_from(StateVec {
            numbers: Vec::new(),
        })
        .unwrap();

        let mut view: View<Vec<String>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers"),
            &Channel::detached(),
        )
        .unwrap();

        view.push("one".into());

        // Flush should return Ok with detached channel
        view.flush().await.unwrap();

        // Initial state should not be updated (detached channel skips the body)
        view.push("two".into());

        // The final patch should contain all changes from the original state
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/0", "value": "one" },
              { "op": "add", "path": "/numbers/1", "value": "two" },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_flushes_view_changes_with_attached_channel() {
        use crate::sync;
        use std::sync::Arc;

        let system = System::try_from(StateVec {
            numbers: Vec::new(),
        })
        .unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(1);
        let channel = Channel::from(tx);

        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_clone.lock().await.push(msg.data.0.clone());
                msg.ack();
            }
        });

        let mut view: View<Vec<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers"), &channel).unwrap();

        view.push("one".into());
        view.flush().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check that the flush patch was received
        let patches = received.lock().await;
        assert_eq!(patches.len(), 1);
        assert_eq!(
            patches[0],
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/0", "value": "one" },
            ]))
            .unwrap()
        );
        drop(patches);

        // Modify again after flush
        view.push("two".into());

        // The final patch should only contain changes after the last flush
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/1", "value": "two" },
            ]))
            .unwrap()
        );

        drop(channel);
        ack_task.abort();
    }

    #[tokio::test]
    async fn it_flushes_multiple_times() {
        use crate::sync;
        use std::sync::Arc;

        let system = System::try_from(StateVec {
            numbers: Vec::new(),
        })
        .unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(10);
        let channel = Channel::from(tx);

        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_clone.lock().await.push(msg.data.0.clone());
                msg.ack();
            }
        });

        let mut view: View<Vec<String>> =
            View::from_system(&system, &Context::new().with_path("/numbers"), &channel).unwrap();

        // Push items one at a time with flushes
        for i in 0..3 {
            view.push(format!("item_{i}"));
            view.flush().await.unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Each flush should only contain the new element
        let patches = received.lock().await;
        assert_eq!(patches.len(), 3);
        for (idx, patch) in patches.iter().enumerate() {
            assert_eq!(
                *patch,
                serde_json::from_value::<Patch>(json!([
                  { "op": "add", "path": format!("/numbers/{idx}"), "value": format!("item_{idx}") },
                ]))
                .unwrap()
            );
        }
        drop(patches);

        // After all flushes, no more changes
        let changes = view.into_result().unwrap();
        assert_eq!(changes, Patch(vec![]));

        drop(channel);
        ack_task.abort();
    }

    #[tokio::test]
    async fn it_flushes_optional_view_creation() {
        use crate::sync;
        use std::sync::Arc;

        let state = MyState {
            numbers: HashMap::new(),
        };
        let system = System::try_from(state).unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(1);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.0.clone());
                msg.ack();
            }
        });

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/new"), &channel)
                .unwrap();

        // Create a new value
        view.replace(42);

        // Commit the creation
        view.flush().await.unwrap();

        // Wait for the message to be received
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check that the add patch was sent
        let patches = received_patches.lock().await;
        assert_eq!(patches.len(), 1);
        assert_eq!(
            patches[0],
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/new", "value": 42 },
            ]))
            .unwrap()
        );
        drop(patches);

        // No more changes after flush
        let changes = view.into_result().unwrap();
        assert_eq!(changes, Patch(vec![]));

        drop(channel);
        ack_task.abort();
    }

    #[tokio::test]
    async fn it_flushes_optional_view_deletion() {
        use crate::sync;
        use std::sync::Arc;

        let mut numbers = HashMap::new();
        numbers.insert("to_delete".to_string(), 99);

        let state = MyState { numbers };
        let system = System::try_from(state).unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(1);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.0.clone());
                msg.ack();
            }
        });

        let mut view: View<Option<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/to_delete"),
            &channel,
        )
        .unwrap();

        // Delete the value
        view.take();

        // Commit the deletion
        view.flush().await.unwrap();

        // Wait for the message to be received
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check that the remove patch was sent
        let patches = received_patches.lock().await;
        assert_eq!(patches.len(), 1);
        assert_eq!(
            patches[0],
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/to_delete" },
            ]))
            .unwrap()
        );
        drop(patches);

        // No more changes after flush
        let changes = view.into_result().unwrap();
        assert_eq!(changes, Patch(vec![]));

        drop(channel);
        ack_task.abort();
    }

    #[tokio::test]
    async fn it_commit_is_noop_with_detached_channel() {
        let system = System::try_from(StateVec {
            numbers: Vec::new(),
        })
        .unwrap();

        let mut view: View<Vec<i32>> = View::from_system(
            &system,
            &Context::new().with_path("/numbers"),
            &Channel::detached(),
        )
        .unwrap();

        view.push(10);

        // Commit should return Ok with detached channel
        view.commit().await.unwrap();

        // Initial state should not be updated (detached channel skips the body)
        view.push(20);

        // The final patch should contain all changes from the original state
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/0", "value": 10 },
              { "op": "add", "path": "/numbers/1", "value": 20 },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_commits_view_changes_with_attached_channel() {
        use crate::sync;
        use std::sync::Arc;

        let system = System::try_from(StateVec {
            numbers: Vec::new(),
        })
        .unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(1);
        let channel = Channel::from(tx);

        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_clone.lock().await.push(msg.data.clone());
                msg.ack();
            }
        });

        let mut view: View<Vec<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers"), &channel).unwrap();

        view.push(10);
        view.commit().await.unwrap();
        view.push(20);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let messages = received.lock().await;
        assert_eq!(messages.len(), 1);
        let (patch, checkpoint) = &messages[0];
        assert_eq!(
            *patch,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/0", "value": 10 },
            ]))
            .unwrap()
        );
        // commit() sends the new state value as the checkpoint
        assert_eq!(*checkpoint, Some(json!([10])));
        drop(messages);

        drop(channel);
        ack_task.abort();

        // The final patch should contain only changes after the commit
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/1", "value": 20 },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_commits_optional_view_creation() {
        use crate::sync;
        use std::sync::Arc;

        let state = MyState {
            numbers: HashMap::new(),
        };
        let system = System::try_from(state).unwrap();

        let (tx, mut rx) = sync::channel::<(Patch, Option<Value>)>(1);
        let channel = Channel::from(tx);

        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_clone.lock().await.push(msg.data.clone());
                msg.ack();
            }
        });

        let mut view: View<Option<i32>> =
            View::from_system(&system, &Context::new().with_path("/numbers/new"), &channel)
                .unwrap();

        view.replace(42);
        view.commit().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let messages = received.lock().await;
        assert_eq!(messages.len(), 1);
        let (patch, checkpoint) = &messages[0];
        assert_eq!(
            *patch,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/new", "value": 42 },
            ]))
            .unwrap()
        );
        // commit() sends the committed value as the checkpoint
        assert_eq!(*checkpoint, Some(json!(42)));
        drop(messages);

        drop(channel);
        ack_task.abort();
    }
}
