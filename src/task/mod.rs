mod boxed;
mod context;
mod effect;
mod errors;
mod handler;
mod intent;
mod into_result;
mod job;

use anyhow::anyhow;
use anyhow::Context as AnyhowCtx;
use json_patch::Patch;
use serde::Serialize;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::system::System;

pub(crate) use context::*;
pub(crate) use errors::*;
pub(crate) use into_result::*;

pub use effect::*;
pub use errors::InputError;
pub use handler::*;
pub use intent::*;

type ActionOutput = Pin<Box<dyn Future<Output = Result<Patch, Error>> + Send>>;
type DryRun = Arc<dyn Fn(&System, &Context) -> Result<Patch, Error> + Send + Sync>;
type Run = Arc<dyn Fn(&System, &Context) -> ActionOutput + Send + Sync>;
type Expand = Arc<dyn Fn(&System, &Context) -> Result<Vec<Task>, Error> + Send + Sync>;

#[derive(Clone)]
pub struct Action {
    id: &'static str,
    scoped: bool,
    context: Context,
    dry_run: DryRun,
    run: Run,
}

impl Action {
    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    pub fn id(&self) -> &str {
        self.id
    }

    fn try_target<S: Serialize>(self, target: S) -> Result<Self, InputError> {
        let target = serde_json::to_value(target).context("Serialization failed")?;
        Ok(Self {
            context: self.context.with_target(target),
            ..self
        })
    }

    fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        Self {
            context: self.context.with_arg(key, value),
            ..self
        }
    }

    fn with_path(self, path: impl AsRef<str>) -> Self {
        Self {
            context: self.context.with_path(path),
            ..self
        }
    }

    /// Run the task sequentially
    pub(crate) async fn run(&self, system: &mut System) -> Result<(), Error> {
        let Action { context, run, .. } = self;
        let changes = (run)(system, context).await?;
        system
            .patch(changes)
            .map_err(|e| UnexpectedError::from(anyhow!(e)))?;
        Ok(())
    }

    pub(crate) fn dry_run(&self, system: &System) -> Result<Patch, Error> {
        let Action {
            context, dry_run, ..
        } = self;
        (dry_run)(system, context)
    }
}

impl Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self
            .id
            .split("::")
            .skip(1)
            .collect::<Vec<&str>>()
            .join("::");
        write!(f, "{}({})", id, self.context.path)
    }
}

#[derive(Clone)]
pub struct Method {
    id: &'static str,
    scoped: bool,
    context: Context,
    expand: Expand,
}

impl Method {
    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    pub fn id(&self) -> &str {
        self.id
    }

    fn try_target<S: Serialize>(self, target: S) -> Result<Self, InputError> {
        let target = serde_json::to_value(target).context("Serialization failed")?;
        Ok(Self {
            context: self.context.with_target(target),
            ..self
        })
    }

    fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        Self {
            context: self.context.with_arg(key, value),
            ..self
        }
    }

    fn with_path(self, path: impl AsRef<str>) -> Self {
        Self {
            context: self.context.with_path(path),
            ..self
        }
    }

    pub(crate) fn expand(&self, system: &System) -> Result<Vec<Task>, Error> {
        let Method {
            context, expand, ..
        } = self;
        (expand)(system, context)
    }
}

/// A task is either a concrete unit of work (action) or rule to
/// derive a list of tasks (method)
#[derive(Clone)]
pub enum Task {
    Action(Action),
    Method(Method),
}

impl Task {
    pub(crate) fn from_action<H, T, I>(action: H, context: Context) -> Self
    where
        H: Handler<T, Patch, I>,
        I: Send + 'static,
    {
        let handler_clone = action.clone();
        Self::Action(Action {
            id: action.id(),
            scoped: action.is_scoped(),
            context,
            dry_run: Arc::new(move |system: &System, context: &Context| {
                let effect = handler_clone.call(system, context);
                effect.pure()
            }),
            run: Arc::new(move |system: &System, context: &Context| {
                let effect = action.call(system, context);

                Box::pin(async { effect.run().await })
            }),
        })
    }

    pub(crate) fn from_method<H, T>(method: H, context: Context) -> Self
    where
        H: Handler<T, Vec<Task>>,
    {
        Self::Method(Method {
            id: method.id(),
            scoped: method.is_scoped(),
            context,
            expand: Arc::new(move |system: &System, context: &Context| {
                // List tasks cannot perform changes to the system
                // so the Effect returned by this handler is assumed to
                // be pure
                method.call(system, context).pure()
            }),
        })
    }

    /// Get the unique identifier for the task
    pub fn id(&self) -> &str {
        match self {
            Self::Action(Action { id, .. }) => id,
            Self::Method(Method { id, .. }) => id,
        }
    }

    pub(crate) fn context(&self) -> &Context {
        match self {
            Self::Action(Action { context, .. }) => context,
            Self::Method(Method { context, .. }) => context,
        }
    }

    /// Return true if the task can be parallelized
    pub fn is_scoped(&self) -> bool {
        match self {
            Self::Action(Action { scoped, .. }) => *scoped,
            Self::Method(Method { scoped, .. }) => *scoped,
        }
    }

    /// Set a target for the task
    ///
    /// This returns a result with an error if the serialization of the target fails
    pub fn try_target<S: Serialize>(self, target: S) -> Result<Self, InputError> {
        match self {
            Self::Action(task) => task.try_target(target).map(Self::Action),
            Self::Method(task) => task.try_target(target).map(Self::Method),
        }
    }

    /// Set a target for the task
    ///
    /// This function will panic if the serialization of the target fails
    pub fn with_target<S: Serialize>(self, target: S) -> Self {
        self.try_target(target).unwrap()
    }

    /// Set an argument for the task
    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        match self {
            Self::Action(task) => Self::Action(task.with_arg(key, value)),
            Self::Method(task) => Self::Method(task.with_arg(key, value)),
        }
    }

    pub(crate) fn with_path(self, path: impl AsRef<str>) -> Self {
        match self {
            Self::Action(task) => Self::Action(task.with_path(path)),
            Self::Method(task) => Self::Method(task.with_path(path)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{System as Sys, Target, View};
    use crate::system::System;
    use serde::{Deserialize, Serialize};
    use serde_json::{from_value, json};
    use std::collections::HashMap;
    use thiserror::Error;
    use tokio::time::{sleep, Duration};

    #[derive(Error, Debug)]
    #[error("and error happened")]
    struct SomeError;

    fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
        if *counter < tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_one_async_with_effect(
        mut counter: View<i32>,
        Target(tgt): Target<i32>,
    ) -> Effect<View<i32>> {
        if *counter < tgt {
            *counter += 1;
        }

        Effect::of(counter).with_io(|counter| async {
            sleep(Duration::from_millis(10)).await;
            Ok(counter)
        })
    }

    fn plus_one_async_with_error(
        mut counter: View<i32>,
        Target(tgt): Target<i32>,
    ) -> Effect<View<i32>, SomeError> {
        if *counter < tgt {
            *counter += 1;
        }

        Effect::of(counter).with_io(|_| async {
            sleep(Duration::from_millis(10)).await;
            Err(SomeError)
        })
    }

    #[test]
    fn it_gets_metadata_from_function() {
        assert_eq!(plus_one.id(), "gustav::task::tests::plus_one");
    }

    #[test]
    fn it_identifies_task_scoping_based_on_args() {
        let task = plus_one.with_target(1);
        assert!(task.is_scoped());

        #[derive(Serialize, Deserialize, Debug)]
        struct State {
            numbers: HashMap<String, i32>,
        }

        fn plus_one_sys(
            mut counter: View<i32>,
            Target(tgt): Target<i32>,
            Sys(_): Sys<State>,
        ) -> View<i32> {
            if *counter < tgt {
                *counter += 1;
            }

            // Update implements IntoResult
            counter
        }

        // The plus_one_sys uses the System extractor so
        // it is not scoped
        let task = plus_one_sys.with_target(1);
        assert!(!task.is_scoped());
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let mut system = System::try_from(0).unwrap();
        let task = plus_one.with_target(1);

        if let Task::Action(action) = task {
            // Run the action
            action.run(&mut system).await.unwrap();

            let state = system.state::<i32>().unwrap();

            // The referenced value was modified
            assert_eq!(state, 1);
        } else {
            panic!("Expected an Action task");
        }
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let mut system = System::try_from(0).unwrap();
        let task = plus_one_async_with_effect.with_target(1);

        if let Task::Action(action) = task {
            // Run the action
            action.run(&mut system).await.unwrap();

            // Check that the system state was modified
            let state = system.state::<i32>().unwrap();
            assert_eq!(state, 1);
        } else {
            panic!("Expected an Action task");
        }
    }

    #[tokio::test]
    async fn it_allows_actions_returning_runtime_errors() {
        let mut system = System::try_from(0).unwrap();
        let task = plus_one_async_with_error.with_target(1);

        if let Task::Action(action) = task {
            let res = action.run(&mut system).await;
            assert!(res.is_err());
            assert_eq!(
                res.unwrap_err().to_string(),
                "task runtime error: and error happened"
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    #[test]
    fn it_allows_to_dry_run_actions_returning_error() {
        let system = System::try_from(1).unwrap();
        let task = plus_one_async_with_error.with_target(2);

        if let Task::Action(action) = task {
            let changes = action.dry_run(&system).unwrap();
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "", "value": 2 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    #[test]
    fn it_allows_to_dry_run_pure_actions() {
        let system = System::try_from(1).unwrap();
        let task = plus_one.with_target(2);

        if let Task::Action(action) = task {
            let changes = action.dry_run(&system).unwrap();
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "", "value": 2 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    #[test]
    fn it_allows_to_dry_run_async_actions() {
        let system = System::try_from(1).unwrap();
        let task = plus_one_async_with_effect.with_target(2);

        if let Task::Action(action) = task {
            let changes = action.dry_run(&system).unwrap();
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "", "value": 2 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    // State needs to be clone in order for Target to implement IntoSystem
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct State {
        counters: HashMap<String, i32>,
    }

    fn update_counter(mut counter: View<i32>, tgt: Target<i32>) -> View<i32> {
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

        let mut system = System::try_from(state).unwrap();
        let task = update_counter.with_target(2).with_path("/counters/a");

        if let Task::Action(action) = task {
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
        } else {
            panic!("Expected an Action Task");
        }
    }
}
