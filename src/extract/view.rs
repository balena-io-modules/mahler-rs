use json_patch::{diff, Patch};
use jsonptr::resolve::ResolveError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};

use crate::error::{Error, IntoError};
use crate::path::Path;
use crate::system::{FromSystem, System};
use crate::task::{Context, Effect, IntoEffect, IntoResult, Result};

/// Extracts a sub-element of a state S as indicated by
/// a path.
///
/// The state is None if the path does not exist under the parent
/// path and it can be created using the `create` function
#[derive(Debug, Clone)]
pub struct View<T> {
    state: Option<T>,
    path: Path,
}

#[derive(Debug)]
pub enum ViewExtractError {
    CannotResolvePath {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },
    DeserializationFailed(serde_json::error::Error),
}

impl std::error::Error for ViewExtractError {}

impl Display for ViewExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewExtractError::CannotResolvePath { path, reason } => {
                write!(
                    f,
                    "cannot resolve path `{}` on system state: {}",
                    path, reason
                )?;
            }
            ViewExtractError::DeserializationFailed(err) => err.fmt(f)?,
        }

        Ok(())
    }
}

impl IntoError for ViewExtractError {
    fn into_error(self) -> Error {
        Error::ViewExtractFailed(self)
    }
}

#[derive(Debug)]
pub enum ViewResultError {
    PathAssignFailed {
        path: String,
        reason: jsonptr::assign::AssignError,
    },
    DeserializationFailed(serde_json::error::Error),
}

impl std::error::Error for ViewResultError {}

impl Display for ViewResultError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewResultError::PathAssignFailed { path, reason } => {
                write!(
                    f,
                    "cannot assign path `{}` on system state: {}",
                    path, reason
                )
            }
            ViewResultError::DeserializationFailed(err) => err.fmt(f),
        }
    }
}

impl IntoError for ViewResultError {
    fn into_error(self) -> Error {
        Error::ViewResultFailed(self)
    }
}

impl<T> View<T> {
    pub(super) fn new(state: Option<T>, path: Path) -> Self {
        View { state, path }
    }

    /// Modify the internal view value to the value
    /// passed as input
    pub fn replace(&mut self, value: impl Into<T>) -> &mut T {
        self.state = Some(value.into());
        self.state.as_mut().unwrap()
    }

    /// Clear the internal view value
    pub fn remove(&mut self) {
        self.state = None;
    }
}

impl<T: Default> View<T> {
    /// Assign the default value for the type
    /// to the view
    pub fn zero(&mut self) -> &mut T {
        self.replace(T::default())
    }
}

impl<T: DeserializeOwned> FromSystem for View<T> {
    type Error = ViewExtractError;

    fn from_system(system: &System, context: &Context) -> core::result::Result<Self, Self::Error> {
        let pointer = context.path.as_ref();
        let root = system.root();

        // Use the parent of the pointer unless we are at the root
        let parent = pointer.parent().unwrap_or(pointer);

        // Try to resolve the parent or fail
        parent
            .resolve(root)
            .map_err(|e| ViewExtractError::CannotResolvePath {
                path: context.path.to_string(),
                reason: e,
            })?;

        // At this point we assume that if the pointer cannot be
        // resolved is because the value does not exist yet unless
        // the parent is a scalar
        let state: Option<T> = match pointer.resolve(root) {
            Ok(value) => Some(
                serde_json::from_value::<T>(value.clone())
                    .map_err(ViewExtractError::DeserializationFailed)?,
            ),
            Err(e) => match e {
                ResolveError::NotFound { .. } => None,
                ResolveError::OutOfBounds { .. } => None,
                _ => {
                    return Err(ViewExtractError::CannotResolvePath {
                        path: context.path.to_string(),
                        reason: e,
                    })
                }
            },
        };

        Ok(View::new(state, context.path.clone()))
    }
}

impl<T> Deref for View<T> {
    type Target = Option<T>;

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
    fn into_result(self, system: &System) -> Result<Patch> {
        // Get the root value
        let mut after = system.clone();
        let root = after.root_mut();

        let pointer = self.path.as_ref();

        if let Some(state) = self.state {
            let value =
                serde_json::to_value(state).map_err(ViewResultError::DeserializationFailed)?;

            // Assign the state to the copy
            pointer
                .assign(root, value)
                .map_err(|e| ViewResultError::PathAssignFailed {
                    path: self.path.to_string(),
                    reason: e,
                })?;
        } else {
            // Otherwise delete the path at the pointer
            pointer.delete(root);
        }
        Ok(diff(system.root(), root))
    }
}

impl<T: Serialize> IntoEffect<Patch, Error> for View<T> {
    fn into_effect(self, system: &System) -> Effect<Patch, Error> {
        Effect::from(self.into_result(system))
    }
}

/// Extracts a sub-element of a state S as indicated by
/// a path.
///
/// The state is always initialized to the type default,
/// no matter if it already exists.
#[derive(Debug, Clone)]
pub struct Create<T> {
    state: T,
    path: Path,
}

impl<T: DeserializeOwned + Default> FromSystem for Create<T> {
    type Error = ViewExtractError;

    fn from_system(system: &System, context: &Context) -> core::result::Result<Self, Self::Error> {
        // We unwrap the call to parse because the path should
        // be validated at this point
        let pointer = context.path.as_ref();
        let root = system.root();

        // Use the parent of the pointer unless we are at the root
        let parent = pointer.parent().unwrap_or(pointer);

        // Try to resolve the parent or fail
        parent
            .resolve(root)
            .map_err(|e| ViewExtractError::CannotResolvePath {
                path: context.path.to_string(),
                reason: e,
            })?;

        // At this point we assume that if the pointer cannot be
        // resolved is because the value does not exist yet unless
        // the parent is a scalar
        if let Err(e) = pointer.resolve(root) {
            match e {
                ResolveError::NotFound { .. } => (),
                ResolveError::OutOfBounds { .. } => (),
                _ => {
                    return Err(ViewExtractError::CannotResolvePath {
                        path: context.path.to_string(),
                        reason: e,
                    })
                }
            };
        }

        Ok(Create {
            state: T::default(),
            path: context.path.clone(),
        })
    }
}

impl<T: Serialize> IntoResult<Patch> for Create<T> {
    fn into_result(self, system: &System) -> Result<Patch> {
        View::<T>::new(Some(self.state), self.path).into_result(system)
    }
}

impl<T: Serialize> IntoEffect<Patch, Error> for Create<T> {
    fn into_effect(self, system: &System) -> Effect<Patch, Error> {
        Effect::from(self.into_result(system))
    }
}

impl<T> Deref for Create<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T> DerefMut for Create<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

/// Extracts a sub-element of a state S as indicated by
/// a path.
///
/// Initializing the extractor will fail if there is no value at the
/// given path
#[derive(Debug, Clone)]
pub struct Update<T> {
    state: T,
    path: Path,
}

impl<T: DeserializeOwned> FromSystem for Update<T> {
    type Error = ViewExtractError;

    fn from_system(system: &System, context: &Context) -> core::result::Result<Self, Self::Error> {
        let pointer = context.path.as_ref();
        let root = system.root();

        // If the pointer cannot be resolved for any
        // reason, return an error
        let value = pointer
            .resolve(root)
            .map_err(|e| ViewExtractError::CannotResolvePath {
                path: context.path.to_string(),
                reason: e,
            })?;
        let state = serde_json::from_value::<T>(value.clone())
            .map_err(ViewExtractError::DeserializationFailed)?;

        Ok(Update {
            state,
            path: context.path.clone(),
        })
    }
}

impl<T> Deref for Update<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T> DerefMut for Update<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<T: Serialize> IntoResult<Patch> for Update<T> {
    fn into_result(self, system: &System) -> Result<Patch> {
        View::<T>::new(Some(self.state), self.path).into_result(system)
    }
}
impl<T: Serialize> IntoEffect<Patch, Error> for Update<T> {
    fn into_effect(self, system: &System) -> Effect<Patch, Error> {
        Effect::from(self.into_result(system))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::System;
    use json_patch::Patch;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, Debug)]
    struct State {
        numbers: HashMap<String, i32>,
    }

    #[derive(Serialize, Deserialize)]
    struct StateVec {
        numbers: Vec<String>,
    }

    #[test]
    fn it_extracts_an_existing_value() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/one")).unwrap();

        assert_eq!(view.as_ref(), Some(&1));

        let value = view.as_mut().unwrap();
        *value = 2;

        // Get the list changes to the view

        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/one", "value": 2 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_fails_if_path_is_invalid() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        assert!(
            View::<i32>::from_system(&system, &Context::new().with_path("/numbers/one/two"),)
                .is_err()
        );
        assert!(
            View::<i32>::from_system(&system, &Context::new().with_path("/none/two"),).is_err()
        );
    }

    #[test]
    fn it_creates_a_value() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        view.replace(3);

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/three", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_creates_a_value_with_a_create_view() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: Create<i32> =
            Create::from_system(&system, &Context::new().with_path("/numbers/three")).unwrap();
        *view = 3;

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/three", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_fails_to_initialize_create_view_if_path_is_invalid() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        assert!(
            Create::<i32>::from_system(&system, &Context::new().with_path("/none/three")).is_err()
        );
    }

    #[test]
    fn it_allows_changing_a_value_with_an_update_view() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: Update<i32> =
            Update::from_system(&system, &Context::new().with_path("/numbers/two")).unwrap();
        *view = 3;

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/two", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_fails_to_initialize_update_view_if_path_does_not_exit() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        assert!(
            Update::<i32>::from_system(&system, &Context::new().with_path("/numbers/three"))
                .is_err()
        );
        assert!(
            Update::<i32>::from_system(&system, &Context::new().with_path("/none/three")).is_err()
        );
    }

    #[test]
    fn it_inits_a_value_with_default() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        let value = view.zero();
        *value = 3;

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/three", "value": 3 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_deletes_an_existing_value() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<i32> =
            View::from_system(&system, &Context::new().with_path("/numbers/one")).unwrap();

        // Delete the value
        view.remove();

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/one" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_extracts_an_existing_value_on_a_vec() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::from(state);

        let mut view: View<String> =
            View::from_system(&system, &Context::new().with_path("/numbers/1")).unwrap();

        assert_eq!(view.as_ref(), Some(&"two".to_string()));

        let value = view.as_mut().unwrap();
        *value = "TWO".to_string();

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/1", "value": "TWO" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_creates_a_value_on_a_vec() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string()],
        };

        let system = System::from(state);

        let mut view: View<String> =
            View::from_system(&system, &Context::new().with_path("/numbers/2")).unwrap();

        assert_eq!(view.as_ref(), None);
        view.replace("three");

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "add", "path": "/numbers/2", "value": "three" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_deletes_a_value_on_a_vec() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::from(state);

        let mut view: View<String> =
            View::from_system(&system, &Context::new().with_path("/numbers/1")).unwrap();

        // Remove the second element
        view.remove();

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            // Removing a value from the middle of the array requires shifting the indexes
            serde_json::from_value::<Patch>(json!([
              { "op": "replace", "path": "/numbers/1", "value": "three" },
              { "op": "remove", "path": "/numbers/2" },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_deletes_a_value_from_the_end_of_a_vec() {
        let state = StateVec {
            numbers: vec!["one".to_string(), "two".to_string(), "three".to_string()],
        };

        let system = System::from(state);

        let mut view: View<String> =
            View::from_system(&system, &Context::new().with_path("/numbers/2")).unwrap();

        // Remove the third element
        view.remove();

        // Get the list changes to the view
        let changes = view.into_result(&system).unwrap();
        assert_eq!(
            changes,
            serde_json::from_value::<Patch>(json!([
              { "op": "remove", "path": "/numbers/2" },
            ]))
            .unwrap()
        );
    }
}
