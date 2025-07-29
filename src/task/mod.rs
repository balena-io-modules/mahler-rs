//! Types and traits for declaring and operating with Jobs and Tasks
//!
//! ## Method Expansion Control
//!
//! Methods can control how their tasks are expanded for execution using wrapper types:
//!
//! - [`Sequence`]: Forces sequential execution regardless of task scoping
//! - [`Set`]: Allows concurrent execution regardless of task scoping
//! - [`Vec<Task>`]: Uses automatic detection based on task scoping (default)
mod context;
mod description;
mod effect;
mod errors;
mod from_system;
mod handler;
mod into_result;
mod job;
mod with_io;

use json_patch::Patch;
use serde::Serialize;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;

use crate::errors::SerializationError;
use crate::path::Path;
use crate::system::System;

pub(crate) use context::*;
pub(crate) use into_result::*;

pub use context::FromContext;
pub use description::*;
pub use effect::*;
pub use errors::*;
pub use from_system::*;
pub use handler::*;
pub use job::*;
pub use with_io::*;

pub mod prelude {
    //! Core types and traits for setting up tasks
    pub use super::handler::*;
    pub use super::job::{any, create, delete, none, update};
    pub use super::with_io::*;
    pub use super::{Sequence, Set, Task};
}

type ActionOutput = Pin<Box<dyn Future<Output = Result<Patch, Error>> + Send>>;
type DryRunFn = Arc<dyn Fn(&System, &Context) -> Result<Patch, Error> + Send + Sync>;
type RunFn = Arc<dyn Fn(&System, &Context) -> ActionOutput + Send + Sync>;
type ExpandFn = Arc<dyn Fn(&System, &Context) -> Result<Vec<Task>, Error> + Send + Sync>;
type DescribeFn = Arc<dyn Fn(&Context) -> Result<String, Error> + Send + Sync>;

#[derive(Clone)]
/// An atomic task
pub struct Action {
    id: &'static str,
    scoped: bool,
    context: Context,
    dry_run: DryRunFn,
    run: RunFn,
    describe: DescribeFn,
}

impl PartialEq for Action {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.scoped == other.scoped && self.context == other.context
    }
}

impl Eq for Action {}

impl fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Action")
            .field("id", &self.id)
            .field("context", &self.context)
            .field("scoped", &self.scoped)
            .finish()
    }
}

fn default_description(id: &'static str, ctx: &Context) -> String {
    format!("{}({})", id, ctx.path)
}

impl Action {
    pub(crate) fn new<H, T, I>(action: H, context: Context) -> Self
    where
        H: Handler<T, Patch, I>,
        I: Send + 'static,
    {
        let handler_clone = action.clone();
        let id = action.id();
        Self {
            id,
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
            describe: Arc::new(move |context: &Context| Ok(default_description(id, context))),
        }
    }

    /// Get the internal task context
    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    /// Get the unique identifier for the task
    ///
    /// The task id is the [`Handler`] type name
    pub fn id(&self) -> &str {
        self.id
    }

    /// Run the task on the system and return a list of changes
    pub(crate) async fn run(&self, system: &System) -> Result<Patch, Error> {
        let Action { context, run, .. } = self;
        (run)(system, context).await
    }

    /// Simulate the effect of the task on the system
    pub(crate) fn dry_run(&self, system: &System) -> Result<Patch, Error> {
        let Action {
            context, dry_run, ..
        } = self;
        (dry_run)(system, context)
    }
}

impl Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = (self.describe)(self.context()).unwrap_or_else(|e| {
            warn!("failed to expand description for task {}: {}", self.id, e);
            default_description(self.id, self.context())
        });
        write!(f, "{description}")
    }
}

#[derive(Clone, Debug, Default)]
/// Controls how method tasks are expanded for execution
pub enum Expansion {
    #[default]
    /// Detect concurrency from task scoping automatically
    Detect,
    /// Force sequential execution regardless of task scoping
    Sequential,
    /// Allow concurrent execution regardless of task scoping
    Independent,
}

/// Wrapper type that forces sequential execution of tasks
///
/// Use this when you need to guarantee that tasks execute in order,
/// even if they would normally support concurrency based on their scoping.
pub struct Sequence(Vec<Task>);

impl From<Vec<Task>> for Sequence {
    fn from(vec: Vec<Task>) -> Self {
        Self(vec)
    }
}

impl<const N: usize> From<[Task; N]> for Sequence {
    fn from(arr: [Task; N]) -> Self {
        Self(Vec::from(arr))
    }
}

impl From<Sequence> for Effect<Vec<Task>, Error> {
    fn from(seq: Sequence) -> Effect<Vec<Task>, Error> {
        Effect::of(seq.0)
    }
}

impl WithExpansion for Sequence {
    fn expansion() -> Expansion {
        Expansion::Sequential
    }
}

/// Wrapper type that allows concurrent execution of tasks
///
/// Use this when you want to enable concurrent execution of tasks
/// regardless of their individual scoping requirements.
pub struct Set(Vec<Task>);

impl From<Vec<Task>> for Set {
    fn from(vec: Vec<Task>) -> Self {
        Self(vec)
    }
}

impl<const N: usize> From<[Task; N]> for Set {
    fn from(arr: [Task; N]) -> Self {
        Self(Vec::from(arr))
    }
}

impl From<Set> for Effect<Vec<Task>, Error> {
    fn from(par: Set) -> Effect<Vec<Task>, Error> {
        Effect::of(par.0)
    }
}

impl WithExpansion for Set {
    fn expansion() -> Expansion {
        Expansion::Independent
    }
}

#[derive(Clone)]
/// A compound task, i.e. a task that can be expanded into child tasks
pub struct Method {
    id: &'static str,
    context: Context,
    expand: ExpandFn,
    describe: DescribeFn,
    expansion: Expansion,
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Method")
            .field("id", &self.id)
            .field("context", &self.context)
            .field("expansion", &self.expansion)
            .finish()
    }
}

impl Method {
    pub(crate) fn new<H, T>(method: H, context: Context, expansion: Expansion) -> Self
    where
        H: Handler<T, Vec<Task>>,
    {
        let id = method.id();
        Method {
            id,
            context,
            expand: Arc::new(move |system: &System, context: &Context| {
                method.call(system, context).pure()
            }),
            describe: Arc::new(move |context: &Context| Ok(default_description(id, context))),
            expansion,
        }
    }

    /// Get the internal context for the task
    pub(crate) fn context(&self) -> &Context {
        &self.context
    }

    /// Get the unique identifier for the task
    ///
    /// The task id is the [`Handler`] type name
    pub fn id(&self) -> &str {
        self.id
    }

    /// Expand the method into its component tasks
    pub(crate) fn expand(&self, system: &System) -> Result<Vec<Task>, Error> {
        let Method {
            context, expand, ..
        } = self;
        (expand)(system, context)
    }

    pub fn expansion(&self) -> &Expansion {
        &self.expansion
    }
}

impl Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let description = (self.describe)(self.context()).unwrap_or_else(|e| {
            warn!("failed to expand description for task {}: {}", self.id, e);
            default_description(self.id, self.context())
        });
        write!(f, "{description}")
    }
}

#[derive(Debug, Clone)]
/// Utility type to operate either with atomic or compound tasks
///
/// A task describes the application of a [`Handler`] to a context.
pub enum Task {
    /// An atomic task
    Action(Action),
    /// A compound task, i.e. a task that can be expanded into child tasks
    Method(Method),
}

impl From<Action> for Task {
    fn from(action: Action) -> Self {
        Self::Action(action)
    }
}

impl From<Method> for Task {
    fn from(method: Method) -> Self {
        Self::Method(method)
    }
}

impl Task {
    /// Get the unique identifier for the task
    ///
    /// The task id is the [`Handler`] type name
    pub fn id(&self) -> &str {
        match self {
            Self::Action(Action { id, .. }) => id,
            Self::Method(Method { id, .. }) => id,
        }
    }

    /// Return true if the task is a method
    pub fn is_method(&self) -> bool {
        matches!(self, Task::Method(_))
    }

    /// Get the internal path that the task applies to
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Action(Action { context, .. }) => &context.path,
            Self::Method(Method { context, .. }) => &context.path,
        }
    }

    pub(crate) fn context_mut(&mut self) -> &mut Context {
        match self {
            Self::Action(Action { context, .. }) => context,
            Self::Method(Method { context, .. }) => context,
        }
    }

    pub(crate) fn context(&mut self) -> &Context {
        match self {
            Self::Action(Action { context, .. }) => context,
            Self::Method(Method { context, .. }) => context,
        }
    }

    /// Return true if the task only operates within its assigned path
    ///
    /// A scoped task can be executed concurrently with other tasks that have non conflicting paths
    pub fn is_scoped(&self, system: &System) -> bool {
        match self {
            Self::Action(Action { scoped, .. }) => *scoped,
            Self::Method(method) => method
                .expand(system)
                // Iterate over the result. Assume the method is not
                // scoped if an error happens
                .iter()
                .all(|tasks| tasks.iter().all(|task| task.is_scoped(system))),
        }
    }

    /// Set a target for the task
    ///
    /// This returns a result with an error if the serialization of the target fails
    pub fn try_target<S: Serialize>(self, target: S) -> Result<Self, SerializationError> {
        let target = serde_json::to_value(target)?;
        Ok(match self {
            Self::Action(mut action) => {
                action.context = action.context.with_target(target);
                Self::Action(action)
            }
            Self::Method(mut method) => {
                method.context = method.context.with_target(target);
                Self::Method(method)
            }
        })
    }

    /// Set a target for the task
    ///
    /// This function will panic if the serialization of the target fails
    ///
    /// ```rust
    /// use mahler::task::prelude::*;
    ///
    /// fn foo() {}
    ///
    /// // Assign the value of the `foo` path argument to the task.
    /// let task = foo.into_task().with_target(10);
    /// ```
    pub fn with_target<S: Serialize>(self, target: S) -> Self {
        self.try_target(target).unwrap()
    }

    /// Set an argument for the task
    ///
    /// ```rust
    /// use mahler::task::prelude::*;
    ///
    /// fn foo() {}
    ///
    /// // Assign the value of the `foo` path argument to the task.
    /// let task = foo.into_task().with_arg("foo", "123");
    /// ```
    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        match self {
            Self::Action(mut action) => {
                action.context = action.context.with_arg(key, value);
                Self::Action(action)
            }
            Self::Method(mut method) => {
                method.context = method.context.with_arg(key, value);
                Self::Method(method)
            }
        }
    }

    /// Set a path for the task
    ///
    /// This is called by the planner, the path is obtained by finding the task by id on the
    /// planner domain and replacing the arguments set by the user
    pub(crate) fn with_path(self, path: impl AsRef<str>) -> Self {
        match self {
            Self::Action(mut action) => {
                action.context = action.context.with_path(path);
                Self::Action(action)
            }
            Self::Method(mut method) => {
                method.context = method.context.with_path(path);
                Self::Method(method)
            }
        }
    }

    /// Set a context for the task
    pub(crate) fn with_context(self, context: Context) -> Self {
        match self {
            Self::Action(task) => Self::Action(Action { context, ..task }),
            Self::Method(task) => Self::Method(Method { context, ..task }),
        }
    }

    /// Set a description for the task
    ///
    /// This is for internal use only, task descriptions must be defined using
    /// [`Job::with_description`]
    pub(crate) fn with_description<D, T>(self, description: D) -> Self
    where
        D: Description<T>,
    {
        let describe: DescribeFn = Arc::new(move |ctx| description.call(ctx));
        match self {
            Self::Action(task) => Self::Action(Action { describe, ..task }),
            Self::Method(task) => Self::Method(Method { describe, ..task }),
        }
    }
}

impl Display for Task {
    /// Human readable description for the Task
    ///
    /// The description will be obtained from the [`Description`] handler set by calling
    /// [`Job::with_description`]. If no description is set it defaults to `<task.id>(<task.path>)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Action(action) => action.fmt(f),
            Self::Method(method) => method.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{System as Sys, Target, View};
    use crate::system::System;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_json::{from_value, json};
    use std::collections::HashMap;
    use thiserror::Error;
    use tokio::time::{sleep, Duration};

    #[derive(Error, Debug)]
    #[error("some error happened")]
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
        assert_eq!(plus_one.id(), "mahler::task::tests::plus_one");
    }

    #[test]
    fn it_allows_to_describe_a_task() {
        let task = plus_one.into_task().with_description(|| "+1");
        assert_eq!(task.to_string(), "+1");

        let task = plus_one
            .into_task()
            .with_description(|Target(tgt): Target<i32>| format!("+1 until {tgt}"));

        // The target has not been assigned so the default description is returned
        assert_eq!(task.to_string(), "mahler::task::tests::plus_one()");

        let task = task.with_target(2);
        // Now the description can be used
        assert_eq!(task.to_string(), "+1 until 2");
    }

    #[test]
    fn it_identifies_task_scoping_based_on_args() {
        let task = plus_one.with_target(1);
        assert!(matches!(task, Task::Action(Action { scoped: true, .. })));

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
        assert!(matches!(task, Task::Action(Action { scoped: false, .. })));
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let system = System::try_from(0).unwrap();
        let task = plus_one.with_target(1);

        if let Task::Action(action) = task {
            // Run the action
            let changes = action.run(&system).await.unwrap();

            // The referenced value was modified
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "", "value": 1 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let system = System::try_from(0).unwrap();
        let task = plus_one_async_with_effect.with_target(1);

        if let Task::Action(action) = task {
            // Run the action
            let changes = action.run(&system).await.unwrap();

            // The referenced value was modified
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "", "value": 1 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action task");
        }
    }

    #[tokio::test]
    async fn it_allows_actions_returning_runtime_errors() {
        let system = System::try_from(0).unwrap();
        let task = plus_one_async_with_error.with_target(1);

        if let Task::Action(action) = task {
            let res = action.run(&system).await;
            assert!(res.is_err());
            assert_eq!(res.unwrap_err().to_string(), "some error happened");
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

        let system = System::try_from(state).unwrap();
        let task = update_counter.with_target(2).with_path("/counters/a");

        if let Task::Action(action) = task {
            // Run the action
            let changes = action.run(&system).await.unwrap();

            // Only the referenced value was modified
            assert_eq!(
                changes,
                from_value::<Patch>(json!([
                  { "op": "replace", "path": "/counters/a", "value": 1 },
                ]))
                .unwrap()
            );
        } else {
            panic!("Expected an Action Task");
        }
    }

    fn plus_two_with_error(
        counter: View<i32>,
        Target(tgt): Target<i32>,
    ) -> Result<Vec<Task>, SerializationError> {
        if tgt - *counter > 1 {
            return Ok(vec![
                plus_one.into_task().try_target(tgt)?,
                plus_one.into_task().try_target(tgt)?,
            ]);
        }

        Ok(vec![])
    }

    #[test]
    fn it_allows_expanding_methods_with_result() {
        let task = plus_two_with_error.with_target(3);
        let system = System::try_from(0).unwrap();

        if let Task::Method(method) = task {
            let tasks = method.expand(&system).unwrap();
            assert_eq!(
                tasks.iter().map(|t| t.id()).collect::<Vec<&str>>(),
                vec![plus_one.id(), plus_one.id()]
            );
        } else {
            panic!("Expected a method task");
        }
    }

    #[test]
    fn it_catches_input_errors_in_method_expansions() {
        let task = plus_two_with_error.with_target("a");
        let system = System::try_from(0).unwrap();

        if let Task::Method(method) = task {
            assert!(matches!(
                method.expand(&system),
                Err(Error::CannotExtractArgs(_))
            ));
        } else {
            panic!("Expected a method task");
        }
    }

    fn plus_two_with_option(counter: View<i32>, Target(tgt): Target<i32>) -> Option<[Task; 2]> {
        if tgt - *counter > 1 {
            return Some([
                plus_one.into_task().with_target(tgt),
                plus_one.into_task().with_target(tgt),
            ]);
        }

        None
    }

    #[test]
    fn it_catches_condition_failure_in_methods_returning_option() {
        let task = plus_two_with_option.with_target(1);
        let system = System::try_from(0).unwrap();

        if let Task::Method(method) = task {
            assert!(matches!(
                method.expand(&system),
                Err(Error::ConditionFailed)
            ));
        } else {
            panic!("Expected a method task");
        }
    }
}
