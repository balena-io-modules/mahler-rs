mod action;
mod boxed;
mod effect;
mod method;
mod result;

use json_patch::{Patch, PatchOperation};

use crate::error::Error;
use crate::system::{Context, System};
use action::ActionOutput;
use boxed::*;

pub use action::Action;
pub use effect::Effect;
pub use method::Method;
pub(crate) use result::*;

/// Jobs are generic work definitions. They can be converted to tasks
/// by calling into_task with a specific context.
///
/// Jobs are re-usable
pub struct Job<S> {
    builder: BoxedIntoTask<S>,
}

impl<S> Job<S> {
    pub(crate) fn from_action<E, A, T>(effect: E, action: A) -> Self
    where
        E: Effect<S, T>,
        A: Action<S, T>,
        S: 'static,
    {
        Self {
            builder: BoxedIntoTask::from_action(effect, action),
        }
    }

    pub(crate) fn from_method<M, T>(method: M) -> Self
    where
        M: Method<S, T>,
        S: 'static,
    {
        Self {
            builder: BoxedIntoTask::from_method(method),
        }
    }

    pub fn into_task(&self, context: Context<S>) -> Task<S> {
        self.builder.clone().into_task(context)
    }
}

type DryRun<S> = Box<dyn FnOnce(&System, Context<S>) -> Result<Patch>>;
type Run<S> = Box<dyn FnOnce(&System, Context<S>) -> ActionOutput>;
type Expand<S> = Box<dyn FnOnce(&System, Context<S>) -> core::result::Result<Vec<Task<S>>, Error>>;

/// A task is either a concrete unit of work or a grouping of work that can be either be run in
/// sequence or in parallel
pub enum Task<S> {
    Unit {
        context: Context<S>,
        dry_run: DryRun<S>,
        run: Run<S>,
    },
    Group {
        context: Context<S>,
        expand: Expand<S>,
    },
}

impl<S> Task<S> {
    pub(crate) fn unit<E, H, T>(effect: E, handler: H, context: Context<S>) -> Self
    where
        E: Effect<S, T>,
        H: Action<S, T>,
    {
        Self::Unit {
            context,
            dry_run: Box::new(|system: &System, context: Context<S>| {
                effect.call(system.clone(), context)
            }),
            run: Box::new(|system: &System, context: Context<S>| {
                Box::pin(handler.call(system.clone(), context))
            }),
        }
    }

    pub(crate) fn group<M, T>(method: M, context: Context<S>) -> Self
    where
        M: Method<S, T>,
    {
        Self::Group {
            context,
            expand: Box::new(|system: &System, context: Context<S>| {
                method.call(system.clone(), context)
            }),
        }
    }

    /// Run every action in the job sequentially and return the
    /// aggregate changes.
    pub fn dry_run(self, system: &System) -> Result<Patch> {
        match self {
            Self::Unit {
                context, dry_run, ..
            } => (dry_run)(system, context),
            Self::Group { context, expand } => {
                // NOTE: This is only implemented for testing purposes, the planner will need
                // to dry run tasks as units to check for conflicts
                let mut changes: Vec<PatchOperation> = vec![];
                let jobs = (expand)(system, context)?;
                let mut system = system.clone();
                for job in jobs {
                    job.dry_run(&system).and_then(|Patch(patch)| {
                        // Save a copy of the changes
                        changes.append(&mut patch.clone());
                        // And apply the changes to the system copy
                        system.patch(Patch(patch))
                    })?;
                }
                Ok(Patch(changes))
            }
        }
    }

    /// Run the job sequentially
    pub async fn run(self, system: &mut System) -> core::result::Result<(), Error> {
        match self {
            Self::Unit { context, run, .. } => {
                let changes = (run)(system, context).await?;
                system.patch(changes)
            }
            Self::Group {
                // NOTE: This is only implemented for testing purposes, workflows returned
                // by the planner will only include unit tasks as the workflow defines whether
                // the tasks can be executed concurrently or in parallel
                context,
                expand,
                ..
            } => {
                let jobs = (expand)(system, context)?;
                for job in jobs {
                    Box::pin(job.run(system)).await?;
                }
                Ok(())
            }
        }
    }

    /// Expand the job into its composing sub-jobs.
    ///
    /// If the job is a Unit, it will return an empty vector
    pub fn expand(self, system: &System) -> core::result::Result<Vec<Task<S>>, Error> {
        match self {
            Self::Unit { .. } => Err(Error::CannotExpandTask),
            Self::Group {
                context, expand, ..
            } => (expand)(system, context),
        }
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

    fn plus_one(mut counter: Update<i32>, tgt: Target<i32>) -> Update<i32> {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_two(counter: Update<i32>, tgt: Target<i32>) -> Vec<Task<i32>> {
        if *tgt - *counter < 2 {
            return vec![];
        }

        vec![
            plus_one.into_task(Context::from_target(*tgt)),
            plus_one.into_task(Context::from_target(*tgt)),
        ]
    }

    async fn plus_one_action(
        mut counter: Update<i32>,
        tgt: Target<i32>,
    ) -> core::result::Result<Update<i32>, MyError> {
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
        let job = plus_one.into_job();
        let task = job.into_task(Context::from_target(1));

        // Get the list of changes that the action performs
        let changes = task.dry_run(&system).unwrap();
        assert_eq!(
            changes,
            from_value::<Patch>(json!([
              { "op": "replace", "path": "", "value": 1 },
            ]))
            .unwrap()
        );
    }

    #[test]
    fn it_allows_to_dry_run_composite_tasks() {
        let system = System::from(0);
        let job = plus_two.into_job();
        let task = job.into_task(Context::from_target(2));

        // Get the list of changes that the method performs
        let changes = task.dry_run(&system).unwrap();
        assert_eq!(
            changes,
            from_value::<Patch>(json!([
              { "op": "replace", "path": "", "value": 1 },
              { "op": "replace", "path": "", "value": 2 },
            ]))
            .unwrap()
        );
    }

    #[tokio::test]
    async fn it_allows_to_run_composite_tasks() {
        let mut system = System::from(0);
        let task = plus_two.into_task(Context::from_target(2));

        // Run the action
        task.run(&mut system).await.unwrap();

        let state = system.state::<i32>().unwrap();

        // The referenced value was modified
        assert_eq!(state, 2);
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let mut system = System::from(1);
        let task = plus_one.into_task(Context::from_target(1));

        // Run the action
        task.run(&mut system).await.unwrap();

        let state = system.state::<i32>().unwrap();

        // The referenced value was modified
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let mut system = System::from(0);
        let job = plus_one_action.with_effect(plus_one);
        let task = job.into_task(Context::from_target(1));

        // Run the action
        task.run(&mut system).await.unwrap();

        // Check that the system state was modified
        let state = system.state::<i32>().unwrap();
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_actions_returning_errors() {
        let mut system = System::from(1);
        let job = plus_one_action.with_effect(plus_one);
        let task = job.into_task(Context::from_target(1));

        let res = task.run(&mut system).await;
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
        let task = update_counter.into_job();
        let action = task.into_task(
            Context::from_target(State {
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
