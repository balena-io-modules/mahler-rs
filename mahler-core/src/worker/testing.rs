use serde::{de::DeserializeOwned, Serialize};

use super::{Ready, Uninitialized, Worker, WorkerState};

use crate::error::Error;
use crate::planner::{Domain, Planner, SearchError};
use crate::runtime::{Context, Resources, System};
use crate::state::State;
use crate::task::Task;
use crate::workflow::Workflow;

impl AsRef<Resources> for Uninitialized {
    fn as_ref(&self) -> &Resources {
        &self.resources
    }
}

impl AsRef<Domain> for Uninitialized {
    fn as_ref(&self) -> &Domain {
        &self.domain
    }
}

impl AsRef<Resources> for Ready {
    fn as_ref(&self) -> &Resources {
        &self.resources
    }
}

impl AsRef<Domain> for Ready {
    fn as_ref(&self) -> &Domain {
        &self.domain
    }
}

impl<O: State, S: WorkerState + AsRef<Resources> + AsRef<Domain>> Worker<O, S> {
    #[cfg_attr(docsrs, doc(cfg(debug_assertions)))]
    /// Find a workflow for testing purposes within the context of the worker
    ///
    /// # Example
    /// ```rust
    /// use mahler::task::{self, prelude::*};
    /// use mahler::extract::{View, Target};
    /// use mahler::worker::Worker;
    /// use mahler::workflow::{Dag, seq};
    ///
    /// fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
    ///    if *counter < tgt {
    ///        // Modify the counter if we are below target
    ///        *counter += 1;
    ///    }
    ///
    ///    // Return the updated counter
    ///    with_io(counter, |counter| async {
    ///        Ok(counter)
    ///    })
    /// }
    ///
    /// // Setup the worker domain and resources
    /// let worker = Worker::new()
    ///                 .job("", update(plus_one).with_description(|| "+1"));
    /// let workflow = worker.find_workflow(0, 2).unwrap();
    ///
    /// // We expect a linear DAG with two tasks
    /// let expected: Dag<&str> = seq!("+1", "+1");
    /// assert_eq!(workflow.to_string(), expected.to_string());
    /// ```
    ///
    /// # Panics
    ///
    /// This function will panic if any error happens during planning
    pub fn find_workflow(&self, cur: O, tgt: O::Target) -> Result<Workflow, SearchError> {
        let mut ini = System::try_from(cur).expect("failed to serialize initial state");
        let resources: &Resources = self.inner.as_ref();
        ini.set_resources(resources.clone());
        let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

        let domain: &Domain = self.inner.as_ref();
        let planner = Planner::new(domain.clone());

        planner.find_workflow::<O>(&ini, &tgt)
    }

    async fn run_task_with_system(&self, mut task: Task, system: &mut System) -> Result<(), Error> {
        let task_id = task.id().to_string();
        let Context { args, .. } = task.context_mut();

        let domain: &Domain = self.inner.as_ref();
        let path = domain
            .find_path_for_job(task_id.as_str(), args)
            .expect("could not find path for task");

        let task = task.with_path(path);
        match &task {
            Task::Action(action) => {
                let changes = action.run(system).await?;
                system
                    .patch(changes)
                    .expect("failed to patch the system state");
            }
            Task::Method(method) => {
                let tasks = method.expand(system)?;
                for mut task in tasks {
                    // Propagate the parent args to the child task
                    for (k, v) in method.context().args.iter() {
                        task = task.with_arg(k, v)
                    }
                    Box::pin(self.run_task_with_system(task, system)).await?;
                }
            }
        }

        Ok(())
    }

    #[cfg_attr(docsrs, doc(cfg(debug_assertions)))]
    /// Test a task within the context of the worker domain
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// use mahler::task::{self, prelude::*};
    /// use mahler::extract::{View, Target};
    /// use mahler::worker::{Worker, Ready};
    ///
    /// fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
    ///    if *counter < tgt {
    ///        // Modify the counter if we are below target
    ///        *counter += 1;
    ///    }
    ///
    ///    // Return the updated counter
    ///    with_io(counter, |counter| async {
    ///        sleep(Duration::from_millis(10)).await;
    ///        Ok(counter)
    ///    })
    /// }
    ///
    /// # tokio_test::block_on(async {
    /// // Setup the worker domain and resources
    /// let worker: Worker<i32, _> = Worker::new().job("", update(plus_one));
    ///
    /// // Run task emulating a target of 2 and initial state of 0
    /// assert_eq!(worker.run_task(0, plus_one.with_target(2)).await.unwrap(), 1);
    ///
    /// // Run task emulating a target of 2 and initial state of 2 (no changes)
    /// let worker: Worker<i32, _> = Worker::new().job("", update(plus_one));
    /// assert_eq!(worker.run_task(2, plus_one.with_target(2)).await.unwrap(), 2);
    /// # })
    /// ```
    ///
    /// # Panics
    /// This function will panic if a sewrialization or internal error happens during execution
    pub async fn run_task(&self, initial_state: O, mut task: Task) -> Result<O, Error>
    where
        O: Serialize + DeserializeOwned,
    {
        let mut system =
            System::try_from(initial_state).expect("failed to serialize initial state");

        let resources: &Resources = self.inner.as_ref();
        system.set_resources(resources.clone());

        let task_id = task.id().to_string();
        let Context { args, .. } = task.context_mut();
        let domain: &Domain = self.inner.as_ref();
        let path = domain
            .find_path_for_job(task_id.as_str(), args)
            .expect("could not find path for task");

        let task = task.with_path(path);
        self.run_task_with_system(task, &mut system).await?;

        let new_state = system.state().expect("failed to serialize output state");

        Ok(new_state)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;

    use super::*;
    use crate::extract::{Target, View};
    use crate::{par, seq, task::*, workflow::Dag};

    fn plus_one(mut counter: View<i32>, tgt: Target<i32>) -> View<i32> {
        if *counter < *tgt {
            *counter += 1;
        }

        // Update implements IntoResult
        counter
    }

    fn plus_two(counter: View<i32>, tgt: Target<i32>) -> Vec<Task> {
        if *tgt - *counter < 2 {
            // Returning an empty result tells the planner
            // the task is not applicable to reach the target
            return vec![];
        }

        vec![plus_one.with_target(*tgt), plus_one.with_target(*tgt)]
    }

    #[tokio::test]
    async fn it_allows_testing_atomic_tasks() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        impl State for Counters {
            type Target = Self;
        }

        let worker: Worker<Counters, _> = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two));

        let task = plus_one.with_target(3).with_arg("counter", "one");
        let res = worker
            .run_task(
                Counters(HashMap::from([
                    ("one".to_string(), 1),
                    ("two".to_string(), 0),
                ])),
                task,
            )
            .await
            .unwrap();

        assert_eq!(
            res,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    async fn it_allows_testing_compound_tasks() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        impl State for Counters {
            type Target = Self;
        }

        let worker: Worker<Counters, _> = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let task = plus_two.with_target(3).with_arg("counter", "one");

        let res = worker
            .run_task(
                Counters(HashMap::from([
                    ("one".to_string(), 0),
                    ("two".to_string(), 0),
                ])),
                task,
            )
            .await
            .unwrap();

        assert_eq!(
            res,
            Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 0),
            ]))
        );
    }

    #[tokio::test]
    async fn it_allows_searching_for_workflow() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        impl State for Counters {
            type Target = Self;
        }

        let worker: Worker<Counters, _> = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two));

        let workflow = worker
            .find_workflow(
                serde_json::from_value(json!({"one": 0, "two": 0})).unwrap(),
                serde_json::from_value(json!({"one": 2, "two": 1})).unwrap(),
            )
            .unwrap();

        // We expect a linear DAG with three tasks
        let expected: Dag<&str> = par!(
            "mahler_core::worker::testing::tests::plus_one(/one)",
            "mahler_core::worker::testing::tests::plus_one(/two)"
        ) + seq!("mahler_core::worker::testing::tests::plus_one(/one)",);

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
