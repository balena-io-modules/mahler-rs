mod action;
mod result;
use crate::system::Context;
use action::effect::{Effect, IntoHandler};
pub use action::{Action, Handler};
use action::{ActionBuilder, ToAction};
pub use result::{IntoResult, Result};

pub struct Task<S> {
    builder: Box<dyn ToAction<S>>,
}

impl<S> Task<S> {
    fn new<E, H, T>(effect: E, handler: H) -> Self
    where
        E: Effect<S, T>,
        H: Handler<S, T>,
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
    use crate::extract::{Target, View};
    use crate::system::{Context, System};
    use json_patch::Patch;
    use serde_json::{from_value, json};
    use thiserror::Error;

    #[derive(Error, Debug)]
    enum MyError {
        #[error("counter already reached")]
        CounterReached,
    }

    fn my_task_effect(mut counter: View<i32>, tgt: Target<i32>) -> View<i32> {
        let value = counter.as_mut().unwrap();
        if *value < *tgt {
            *value += 1;
        }

        // View implements IntoPatch
        counter
    }

    async fn my_task_action(
        mut counter: View<i32>,
        tgt: Target<i32>,
    ) -> core::result::Result<View<i32>, MyError> {
        let value = counter.as_mut().unwrap();
        if *value < *tgt {
            *value += 1;
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
}
