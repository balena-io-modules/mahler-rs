use crate::error::Error;
use crate::path::Path;
use crate::system::{Context, FromSystem, System};
use crate::task::{IntoResult, Result};
use json_patch::diff;
use jsonptr::resolve::ResolveError;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct View<S, T = S> {
    state: Option<T>,
    path: Path,
    _system: PhantomData<S>,
}

impl<S, T> View<S, T> {
    pub fn create(&mut self, value: T) -> &mut T {
        self.state = Some(value);
        self.state.as_mut().unwrap()
    }

    pub fn delete(&mut self) {
        self.state = None;
    }
}

impl<S, T: Default> View<S, T> {
    pub fn init(&mut self) -> &mut T {
        self.state = Some(T::default());
        self.state.as_mut().unwrap()
    }
}

impl<S, T: DeserializeOwned> FromSystem<S> for View<S, T> {
    type Error = Error;

    fn from_system(
        system: &System,
        context: &Context<S>,
    ) -> core::result::Result<Self, Self::Error> {
        let pointer = context.path.as_ref();
        let root = system.root();

        // Use the parent of the pointer unless we are at the root
        let parent = pointer.parent().unwrap_or(pointer);

        // Try to resolve the parent or fail
        parent
            .resolve(root)
            .map_err(|e| Error::PointerResolveFailed {
                path: context.path.to_string(),
                reason: e,
            })?;

        // At this point we assume that if the pointer cannot be
        // resolved is because the value does not exist yet
        let state: Option<T> = match pointer.resolve(root) {
            Ok(value) => Some(serde_json::from_value::<T>(value.clone())?),
            Err(e) => match e {
                ResolveError::NotFound { .. } => None,
                ResolveError::OutOfBounds { .. } => None,
                _ => {
                    return Err(Error::PointerResolveFailed {
                        path: context.path.to_string(),
                        reason: e,
                    })
                }
            },
        };

        Ok(View {
            state,
            path: context.path.clone(),
            _system: PhantomData::<S>,
        })
    }
}

impl<S, T> Deref for View<S, T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S, T> DerefMut for View<S, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S, T: Serialize> IntoResult for View<S, T> {
    fn into_result(self, system: &System) -> Result {
        // Get the root value
        let mut after = system.clone();
        let root = after.root_mut();

        let pointer = self.path.as_ref();

        if let Some(state) = self.state {
            let value = serde_json::to_value(state)?;

            // Assign the state to the copy
            pointer
                .assign(root, value)
                .map_err(|e| Error::PointerAssignFailed {
                    path: self.path.to_string(),
                    reason: e,
                })?;
        } else {
            // Otherwise delete the path at the pointer
            pointer.delete(root);
        }

        // Return the difference between the roots
        Ok(diff(system.root(), root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::{Context, System};
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

        let mut view: View<State, i32> =
            View::from_system(&system, &Context::default().with_path("/numbers/one")).unwrap();

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

        assert!(View::<State, i32>::from_system(
            &system,
            &Context::default().with_path("/numbers/one/two"),
        )
        .is_err());
        assert!(View::<State, i32>::from_system(
            &system,
            &Context::default().with_path("/none/two"),
        )
        .is_err());
    }

    #[test]
    fn it_creates_a_value() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<State, i32> =
            View::from_system(&system, &Context::default().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        view.create(3);

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
    fn it_inits_a_value_with_default() {
        let mut numbers = HashMap::new();
        numbers.insert("one".to_string(), 1);
        numbers.insert("two".to_string(), 2);

        let state = State { numbers };

        let system = System::from(state);

        let mut view: View<State, i32> =
            View::from_system(&system, &Context::default().with_path("/numbers/three")).unwrap();

        assert_eq!(view.as_ref(), None);

        let value = view.init();
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

        let mut view: View<State, i32> =
            View::from_system(&system, &Context::default().with_path("/numbers/one")).unwrap();

        // Delete the value
        view.delete();

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

        let mut view: View<State, String> =
            View::from_system(&system, &Context::default().with_path("/numbers/1")).unwrap();

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

        let mut view: View<State, String> =
            View::from_system(&system, &Context::default().with_path("/numbers/2")).unwrap();

        assert_eq!(view.as_ref(), None);
        view.create("three".to_string());

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

        let mut view: View<State, String> =
            View::from_system(&system, &Context::default().with_path("/numbers/1")).unwrap();

        // Remove the second element
        view.delete();

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

        let mut view: View<State, String> =
            View::from_system(&system, &Context::default().with_path("/numbers/2")).unwrap();

        // Remove the third element
        view.delete();

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
