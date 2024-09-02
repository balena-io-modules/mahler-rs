mod action;
mod result;
use crate::system::Context;
use action::effect::{Effect, IntoHandler};
pub use action::{Action, ActionHandler};
use action::{ActionBuilder, ToAction};
pub(crate) use result::*;

pub struct Task<S> {
    builder: Box<dyn ToAction<S>>,
}

impl<S> Task<S> {
    fn new<E, H, T>(effect: E, handler: H) -> Self
    where
        E: Effect<S, T>,
        H: ActionHandler<S, T>,
        S: 'static,
    {
        Self {
            builder: Box::new(ActionBuilder {
                effect,
                handler,
                build: |effect: E, handler: H, context: Context<S>| {
                    Action::new(effect, handler, context)
                },
            }),
        }
    }

    pub fn from<E, T>(effect: E) -> Self
    where
        E: Effect<S, T> + 'static,
        T: Send + 'static,
        S: Send + Sync + 'static,
    {
        Self::new(effect.clone(), IntoHandler::new(effect))
    }

    pub fn bind(&self, context: Context<S>) -> Action<S> {
        self.builder.to_action(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{Target, Update};
    use crate::system::{Context, System};
    use json_patch::Patch;
    use serde::{Deserialize, Serialize};
    use serde_json::{from_value, json};
    use std::collections::HashMap;
    use thiserror::Error;

    #[derive(Error, Debug)]
    enum MyError {
        #[error("counter already reached")]
        CounterReached,
    }

    fn my_task_effect(mut counter: Update<i32>, tgt: Target<i32>) -> impl IntoResult {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    async fn my_task_action(mut counter: Update<i32>, tgt: Target<i32>) -> impl IntoResult {
        if *counter < *tgt {
            *counter += 1;
        } else {
            return Err(MyError::CounterReached);
        }

        Ok(counter)
    }

    #[test]
    fn it_allows_to_dry_run_tasks() {
        let system = System::from(0);
        let task = Task::from(my_task_effect);
        let action = task.bind(Context::from(1));

        // Get the list of changes that the action performs
        let changes = action.dry_run(&system).unwrap();
        assert_eq!(
            changes,
            from_value::<Patch>(json!([
              { "op": "replace", "path": "", "value": 1 },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let mut system = System::from(1);
        let task = Task::from(my_task_effect);
        let action = task.bind(Context::from(1));

        // Run the action
        action.run(&mut system).await.unwrap();

        let state = system.state::<i32>().unwrap();

        // The referenced value was modified
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let mut system = System::from(0);
        let task = my_task_action.with_effect(my_task_effect);
        let action = task.bind(Context::from(1));

        // Run the action
        action.run(&mut system).await.unwrap();

        // Check that the system state was modified
        let state = system.state::<i32>().unwrap();
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_actions_returning_errors() {
        let mut system = System::from(1);
        let task = my_task_action.with_effect(my_task_effect);
        let action = task.bind(Context::from(1));

        let res = action.run(&mut system).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "counter already reached");
    }

    // State needs to be clone in order for Target to implement IntoSystem
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct State {
        counters: HashMap<String, i32>,
    }

    fn update_counter(
        mut counter: Update<State, i32>,
        tgt: Target<State, i32>,
    ) -> Update<State, i32> {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    #[tokio::test]
    async fn it_modifies_system_sub_elements() {
        let state = State {
            counters: [("a".to_string(), 0), ("b".to_string(), 0)].into(),
        };

        let mut system = System::from(state);
        let task = Task::from(update_counter);
        let action = task.bind(
            Context::from(State {
                counters: [("a".to_string(), 2), ("b".to_string(), 1)].into(),
            })
            .with_path("/counters/a"),
        );

        // Run the action
        action.run(&mut system).await.unwrap();

        let state = system.state::<State>().unwrap();

        // Only the referenced value was modified
        assert_eq!(
            state,
            State {
                counters: [("a".to_string(), 1), ("b".to_string(), 0)].into()
            }
        );
    }
}
