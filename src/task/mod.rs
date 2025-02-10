mod boxed;
mod context;
mod effect;
mod handler;
mod job;
mod result;

use json_patch::{Patch, PatchOperation};
use serde::Serialize;
use std::fmt::{self, Display};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use crate::error::{Error, IntoError};
use crate::system::System;

pub use context::*;
pub use effect::*;
pub use handler::*;
pub use job::*;
pub(crate) use result::*;

#[derive(Debug)]
pub struct ConditionFailed(String);

impl Default for ConditionFailed {
    fn default() -> Self {
        ConditionFailed::new("unknown")
    }
}

impl ConditionFailed {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::error::Error for ConditionFailed {}

impl Display for ConditionFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IntoError for ConditionFailed {
    fn into_error(self) -> Error {
        Error::TaskConditionFailed(self)
    }
}

impl<T, O> IntoResult<O> for Option<T>
where
    O: Default,
    T: IntoResult<O>,
{
    fn into_result(self, system: &System) -> Result<O> {
        match self {
            None => Err(ConditionFailed::default())?,
            Some(value) => value.into_result(system),
        }
    }
}

type ActionOutput = Pin<Box<dyn Future<Output = Result<Patch>>>>;
type DryRun = Rc<dyn Fn(&System, &Context) -> Result<Patch>>;
type Run = Rc<dyn Fn(&System, &Context) -> ActionOutput>;
type Expand = Rc<dyn Fn(&System, &Context) -> core::result::Result<Vec<Task>, Error>>;

/// A task is either a concrete unit (atom) of work or a list of tasks
/// that can be run in sequence or in parallel
#[derive(Clone)]
pub enum Task {
    Atom {
        id: &'static str,
        scoped: bool,
        context: Context,
        dry_run: DryRun,
        run: Run,
    },
    List {
        id: &'static str,
        scoped: bool,
        context: Context,
        expand: Expand,
    },
}

impl Task {
    pub(crate) fn atom<H, T, I>(id: &'static str, handler: H, context: Context) -> Self
    where
        H: Handler<T, Patch, I>,
        I: 'static,
    {
        let handler_clone = handler.clone();
        Self::Atom {
            id,
            scoped: handler.is_scoped(),
            context,
            dry_run: Rc::new(move |system: &System, context: &Context| {
                let effect = handler_clone.call(system, context);
                effect.pure()
            }),
            run: Rc::new(move |system: &System, context: &Context| {
                let effect = handler.call(system, context);

                Box::pin(async {
                    match effect.run().await {
                        Ok(changes) => Ok(changes),
                        Err(err) => Err(Error::Other(Box::new(err))),
                    }
                })
            }),
        }
    }

    pub(crate) fn list<H, T>(id: &'static str, handler: H, context: Context) -> Self
    where
        H: Handler<T, Vec<Task>>,
    {
        Self::List {
            id,
            scoped: handler.is_scoped(),
            context,
            expand: Rc::new(move |system: &System, context: &Context| {
                // List tasks cannot perform changes to the system
                // so the Effect returned by this handler is assumed to
                // be pure
                handler.call(system, context).pure()
            }),
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Atom { id, .. } => id,
            Self::List { id, .. } => id,
        }
    }

    pub fn context(&self) -> &Context {
        match self {
            Self::Atom { context, .. } => context,
            Self::List { context, .. } => context,
        }
    }

    pub fn is_scoped(&self) -> bool {
        match self {
            Self::Atom { scoped, .. } => *scoped,
            Self::List { scoped, .. } => *scoped,
        }
    }

    pub fn try_target<S: Serialize>(self, target: S) -> Result<Self> {
        let target = serde_json::to_value(target)?;
        let res = match self {
            Self::Atom {
                id,
                scoped,
                context,
                dry_run,
                run,
            } => Self::Atom {
                id,
                scoped,
                context: context.with_target(target),
                dry_run,
                run,
            },
            Self::List {
                id,
                scoped,
                context,
                expand,
                ..
            } => {
                let Context { args, path, .. } = context;
                Self::List {
                    id,
                    scoped,
                    context: Context { target, args, path },
                    expand,
                }
            }
        };

        Ok(res)
    }

    pub fn with_target<S: Serialize>(self, target: S) -> Self {
        self.try_target(target).unwrap()
    }

    pub fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        match self {
            Self::Atom {
                id,
                scoped,
                context,
                dry_run,
                run,
            } => Self::Atom {
                id,
                scoped,
                context: context.with_arg(key, value),
                dry_run,
                run,
            },
            Self::List {
                id,
                scoped,
                context,
                expand,
                ..
            } => Self::List {
                id,
                scoped,
                context: context.with_arg(key, value),
                expand,
            },
        }
    }

    pub(crate) fn with_path(self, path: impl AsRef<str>) -> Self {
        match self {
            Self::Atom {
                id,
                scoped,
                context,
                dry_run,
                run,
            } => Self::Atom {
                id,
                scoped,
                context: context.with_path(path),
                dry_run,
                run,
            },
            Self::List {
                id,
                scoped: linear,
                context,
                expand,
                ..
            } => Self::List {
                id,
                scoped: linear,
                context: context.with_path(path),
                expand,
            },
        }
    }

    /// Run every action in the task sequentially and return the
    /// aggregate changes.
    /// TODO: this should probably only have crate visibility
    pub fn dry_run(&self, system: &System) -> Result<Patch> {
        match self {
            Self::Atom {
                context, dry_run, ..
            } => (dry_run)(system, context),
            Self::List {
                context, expand, ..
            } => {
                let mut changes: Vec<PatchOperation> = vec![];
                let jobs = (expand)(system, context)?;
                let mut system = system.clone();
                for job in jobs {
                    let Patch(patch) = job.dry_run(&system)?;

                    // Append a copy of the patch to the total changes
                    changes.append(&mut patch.clone());

                    // And apply the changes to the system copy
                    system.patch(Patch(patch))?;
                }
                Ok(Patch(changes))
            }
        }
    }

    /// Run the task sequentially
    pub async fn run(&self, system: &mut System) -> Result<()> {
        match self {
            Self::Atom { context, run, .. } => {
                let changes = (run)(system, context).await?;
                system.patch(changes)?;
                Ok(())
            }
            Self::List {
                context, expand, ..
            } => {
                let jobs = (expand)(system, context)?;
                for job in jobs {
                    Box::pin(job.run(system)).await?;
                }
                Ok(())
            }
        }
    }

    /// Expand the task into its composing sub-jobs.
    ///
    /// If the task is an atom the expansion will fail
    pub fn expand(&self, system: &System) -> Result<Vec<Task>> {
        match self {
            Self::Atom { .. } => Ok(vec![]),
            Self::List {
                context, expand, ..
            } => (expand)(system, context),
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self
            .id()
            .split("::")
            .skip(1)
            .collect::<Vec<&str>>()
            .join("::");
        write!(f, "{}({})", id, self.context().path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{Target, Update};
    use crate::system::System;
    use json_patch::Patch;
    use serde::{Deserialize, Serialize};
    use serde_json::{from_value, json};
    use std::collections::HashMap;
    use thiserror::Error;
    use tokio::time::{sleep, Duration};

    #[derive(Error, Debug)]
    enum MyError {
        #[error("counter already reached")]
        CounterReached,
    }

    fn plus_one(mut counter: Update<i32>, Target(tgt): Target<i32>) -> Update<i32> {
        if *counter < tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_two(counter: Update<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
        if tgt - *counter < 2 {
            // Returning an empty result tells the planner
            // the task is not applicable to reach the target
            return vec![];
        }

        vec![plus_one.with_target(tgt), plus_one.with_target(tgt)]
    }

    fn plus_one_async(
        counter: Update<i32>,
        Target(tgt): Target<i32>,
    ) -> Effect<Update<i32>, MyError> {
        if *counter >= tgt {
            return Effect::from_error(MyError::CounterReached);
        }

        Effect::of(counter).with_io(|mut counter| async {
            sleep(Duration::from_millis(10)).await;
            *counter += 1;
            Ok(counter)
        })
    }

    #[test]
    fn it_gets_metadata_from_function() {
        let job = plus_one.into_job();

        assert_eq!(job.id(), "gustav::task::tests::plus_one");
    }

    #[test]
    fn it_allows_to_dry_run_tasks() {
        let system = System::from(0);
        let task = plus_one.with_target(1);

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
        let task = plus_two.with_target(2);

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
        let task = plus_two.with_target(2);

        // Run the action
        task.run(&mut system).await.unwrap();

        let state = system.state::<i32>().unwrap();

        // The referenced value was modified
        assert_eq!(state, 2);
    }

    #[tokio::test]
    async fn it_runs_async_actions() {
        let mut system = System::from(0);
        let task = plus_one.with_target(1);

        // Run the action
        task.run(&mut system).await.unwrap();

        let state = system.state::<i32>().unwrap();

        // The referenced value was modified
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_extending_actions_with_effect() {
        let mut system = System::from(0);
        let task = plus_one_async.with_target(1);

        // Run the action
        task.run(&mut system).await.unwrap();

        // Check that the system state was modified
        let state = system.state::<i32>().unwrap();
        assert_eq!(state, 1);
    }

    #[tokio::test]
    async fn it_allows_actions_returning_errors() {
        let mut system = System::from(1);
        let task = plus_one_async.with_target(1);

        let res = task.run(&mut system).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "counter already reached");
    }

    // State needs to be clone in order for Target to implement IntoSystem
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct State {
        counters: HashMap<String, i32>,
    }

    fn update_counter(mut counter: Update<i32>, tgt: Target<i32>) -> Update<i32> {
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
        let task = update_counter.with_target(2).with_path("/counters/a");

        // Run the action
        task.run(&mut system).await.unwrap();

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
