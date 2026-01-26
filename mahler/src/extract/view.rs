use json_patch::{
    diff, AddOperation, CopyOperation, MoveOperation, Patch, PatchOperation, RemoveOperation,
    ReplaceOperation, TestOperation,
};
use jsonptr::resolve::ResolveError;
use jsonptr::PointerBuf;
use serde_json::Value;
use std::ops::{Deref, DerefMut};

use crate::error::{Error, ErrorKind};
use crate::json::Path;
use crate::result::Result;
use crate::runtime::{Channel, Context, FromSystem, System};
use crate::serde::{de::DeserializeOwned, Serialize};
use crate::task::IntoResult;

/// Prepend the given pointer to every operation in the Patch
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
    initial: Value,
    state: T,
    path: Path,
    channel: Channel,
}

impl<T> View<T> {
    // The only way to create a pointer is via the
    // from_system method
    fn new(initial: Value, state: T, path: Path, channel: Channel) -> Self {
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
        let path: PointerBuf = self.path.clone().into();
        let before = &self.initial;

        match (before, after) {
            (&Value::Null, Value::Null) => Patch(vec![]),
            (&Value::Null, after) => Patch(vec![PatchOperation::Add(AddOperation {
                path,
                value: after,
            })]),
            (_, Value::Null) => Patch(vec![PatchOperation::Remove(RemoveOperation { path })]),
            (before, after) => prepend_path(path, diff(before, &after)),
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
            let after = serde_json::to_value(&self.state)?;

            // Create a patch from the initial state
            let changes = self.create_patch(after.clone());

            // Send the changes and wait for acknowledgement
            self.channel.send(changes).await?;

            // Update initial to match the current state
            self.initial = after;
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
        let (state, initial): (T, Value) = match pointer.resolve(root) {
            Ok(value) => (
                serde_json::from_value::<T>(value.clone())
                    .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?,
                value.clone(),
            ),
            Err(e) => match e {
                ResolveError::NotFound { .. } => (
                    // if the value does not exist, see if we can deserialize null into the type
                    serde_json::from_value::<T>(Value::Null)
                        .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?,
                    Value::Null,
                ),
                ResolveError::OutOfBounds { .. } => (
                    // the value may be a new index in an array, try to deserialize null
                    serde_json::from_value::<T>(Value::Null)
                        .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?,
                    Value::Null,
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
        // An error here should be rare
        let after = serde_json::to_value(&self.state).map_err(Error::internal)?;
        let patch = self.create_patch(after);

        Ok(patch)
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
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);

        let state = MyState { numbers };
        let system = System::try_from(state).unwrap();

        let mut view: View<i32> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/one"),
            &Channel::detached(),
        )
        .unwrap();

        *view = 10;

        // Commit should return Ok with detached channel
        view.flush().await.unwrap();

        // Initial state should not be updated
        *view = 20;

        // The final patch should contain all changes from the original state
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/one", "value": 20 },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_flushes_view_changes_with_attached_channel() {
        use crate::sync;
        use std::sync::Arc;

        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = MyState { numbers };
        let system = System::try_from(state).unwrap();

        let (tx, mut rx) = sync::channel::<Patch>(1);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.clone());
                msg.ack();
            }
        });

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/one"), &channel)
                .unwrap();

        // Modify the value
        *view = 10;

        // Commit the changes
        view.flush().await.unwrap();

        // Wait for the message to be received
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Check that the patch was received
        let patches = received_patches.lock().await;
        assert_eq!(patches.len(), 1);
        assert_eq!(
            patches[0],
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/one", "value": 10 },
            ]))
            .unwrap()
        );
        drop(patches);

        // Modify again
        *view = 20;

        // The final patch should only contain changes after the last flush
        let changes = view.into_result().unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/one", "value": 20 },
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

        let mut numbers = HashMap::new();
        numbers.insert("counter".to_string(), 0);

        let state = MyState { numbers };
        let system = System::try_from(state).unwrap();

        let (tx, mut rx) = sync::channel::<Patch>(10);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.clone());
                msg.ack();
            }
        });

        let mut view: View<i32> = View::from_system(
            &system,
            &Context::new().with_path("/numbers/counter"),
            &channel,
        )
        .unwrap();

        // Simulate multiple increments with flushes
        for i in 1..=5 {
            *view = i;
            view.flush().await.unwrap();
        }

        // Wait for all messages to be received
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify all patches were received
        let patches = received_patches.lock().await;
        assert_eq!(patches.len(), 5);
        for (idx, patch) in patches.iter().enumerate() {
            let i = idx + 1;
            assert_eq!(
                *patch,
                serde_json::from_value::<Patch>(json!([
                  { "op": "replace", "path": "/numbers/counter", "value": i },
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

        let (tx, mut rx) = sync::channel::<Patch>(1);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.clone());
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

        let (tx, mut rx) = sync::channel::<Patch>(1);
        let channel = Channel::from(tx);

        // Spawn a task to receive and acknowledge messages
        let received_patches = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_patches_clone = received_patches.clone();
        let ack_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                received_patches_clone.lock().await.push(msg.data.clone());
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
}
