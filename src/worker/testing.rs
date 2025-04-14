use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use super::{Ready, Uninitialized, Worker};
use crate::planner::Error as PlannerError;
use crate::system::System;
use crate::task;
use crate::{task::Task, workflow::Workflow};

impl<T> Worker<T, Uninitialized> {
    async fn run_task_with_system(
        &self,
        task: Task,
        system: &mut System,
    ) -> Result<(), task::Error> {
        let path = self
            .inner
            .domain
            .find_path_for_job(task.id(), &task.context().args)
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

    /// Test a task within the context of the worker domain
    ///
    /// This function is only meant for testing and is not available
    /// if debug_assertions is disabled.
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// use gustav::task::{self, prelude::*};
    /// use gustav::extract::{View, Target};
    /// use gustav::worker::Worker;
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
    /// // Setup the worker domain and resources
    /// let worker = Worker::new().job("", update(plus_one));
    ///
    /// # tokio_test::block_on(async {
    /// // Run task emulating a target of 2 and initial state of 0
    /// assert_eq!(worker.run_task(plus_one.with_target(2), 0).await, Ok(1));
    ///
    /// // Run task emulating a target of 2 and initial state of 2 (no changes)
    /// assert_eq!(worker.run_task(plus_one.with_target(2), 2).await, Ok(2));
    /// # })
    /// ```
    pub async fn run_task(&self, task: Task, state: T) -> Result<T, task::Error>
    where
        T: Serialize + DeserializeOwned,
    {
        // Create a system from the internal
        let mut system = System::try_from(state)
            .expect("failed to serialize input state")
            .with_resources(self.inner.resources.clone());

        let path = self
            .inner
            .domain
            .find_path_for_job(task.id(), &task.context().args)
            .expect("could not find path for task");

        let task = task.with_path(path);
        self.run_task_with_system(task, &mut system).await?;

        let new_state = system.state().expect("failed to serialize output state");

        Ok(new_state)
    }
}

#[derive(Debug, Error)]
#[error("workflow not found")]
pub struct NotFound;

impl<T> Worker<T, Ready> {
    /// Find a workflow within the context of the worker
    ///
    /// This function is only meant for testing and is not available
    /// if debug_assertions is disabled.
    ///
    /// # Example
    /// ```rust
    /// use gustav::task::{self, prelude::*};
    /// use gustav::extract::{View, Target};
    /// use gustav::worker::Worker;
    /// use gustav::{Dag, seq};
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
    /// let worker = Worker::new().job("", update(plus_one).with_description(|| "+1")).initial_state(0).unwrap();
    /// let workflow = worker.find_workflow(2).unwrap();
    ///
    /// // We expect a linear DAG with two tasks
    /// let expected: Dag<&str> = seq!("+1", "+1");
    /// assert_eq!(workflow.to_string(), expected.to_string());
    /// ```
    pub fn find_workflow<S>(&self, tgt: S) -> Result<Workflow, NotFound>
    where
        S: Serialize,
    {
        let cur = &self.inner.system;
        let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

        match self.inner.planner.find_workflow(cur, &tgt) {
            Ok(workflow) => Ok(workflow),
            Err(PlannerError::NotFound) => Err(NotFound),
            Err(e) => panic!("unexpected planning error: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::collections::HashMap;

    use super::*;
    use crate::extract::{Target, View};
    use crate::{seq, task::*, Dag};

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

        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two));

        let task = plus_one.with_target(3).with_arg("counter", "one");

        let res = worker
            .run_task(
                task,
                Counters(HashMap::from([
                    ("one".to_string(), 1),
                    ("two".to_string(), 0),
                ])),
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

        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two));

        let task = plus_two.with_target(3).with_arg("counter", "one");

        let res = worker
            .run_task(
                task,
                Counters(HashMap::from([
                    ("one".to_string(), 0),
                    ("two".to_string(), 0),
                ])),
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

    #[test]
    fn it_allows_searching_for_workflow() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Counters(HashMap<String, i32>);

        let worker = Worker::new()
            .job("/{counter}", update(plus_one))
            .job("/{counter}", update(plus_two))
            .initial_state(Counters(HashMap::from([
                ("one".to_string(), 0),
                ("two".to_string(), 0),
            ])))
            .unwrap();

        let workflow = worker
            .find_workflow(Counters(HashMap::from([
                ("one".to_string(), 2),
                ("two".to_string(), 1),
            ])))
            .unwrap();

        // We expect a linear DAG with three tasks
        let expected: Dag<&str> = seq!(
            "gustav::worker::testing::tests::plus_one(/two)",
            "gustav::worker::testing::tests::plus_one(/one)",
            "gustav::worker::testing::tests::plus_one(/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
