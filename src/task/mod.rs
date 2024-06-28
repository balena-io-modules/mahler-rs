mod action;
mod effect;
mod handler;

use std::marker::PhantomData;

use crate::state::Context;

pub use action::Action;
use effect::Effect;
pub use handler::Handler;

pub struct Task<S, T, E, A> {
    effect: E,
    action: A,
    _state: PhantomData<S>,
    _args: PhantomData<T>,
}

impl<'system, S, T, E, A> Task<S, T, E, A>
where
    E: Effect<'system, S, T>,
    A: Handler<'system, S, T>,
{
    pub fn new(effect: E, action: A) -> Self {
        Task {
            effect,
            action,
            _state: PhantomData::<S>,
            _args: PhantomData::<T>,
        }
    }

    pub fn bind(self, context: Context<S>) -> Action<S, T, E, A> {
        Action::new(self.effect, self.action, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{State, Target};
    use crate::state::Context;

    fn my_task_effect(mut counter: State<i32>, Target(tgt): Target<i32>) {
        if *counter < tgt {
            *counter += 1;
        }
    }

    async fn my_task_action(mut counter: State<'_, i32>, tgt: Target<i32>) {
        if *counter < *tgt {
            *counter += 1;
        }
    }

    #[test]
    fn it_allows_to_dry_run_tasks() {
        let task = Task::from(my_task_effect);
        let action = task.bind(Context { target: 1 });

        let mut counter = 0;
        action.dry_run(&mut counter);

        // The referenced value was modified
        assert_eq!(counter, 1);
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let task = Task::from(my_task_effect);
        let action = task.bind(Context { target: 1 });

        let mut counter = 0;
        // Run the action
        action.run(&mut counter).await;

        // The referenced value was modified
        assert_eq!(counter, 1);
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let task = my_task_action.with_effect(my_task_effect);
        let action = task.bind(Context { target: 1 });

        let mut counter = 0;
        // Run the action
        action.run(&mut counter).await;

        // The referenced value was modified
        assert_eq!(counter, 1);
    }
}
