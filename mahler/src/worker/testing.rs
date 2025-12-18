use std::sync::Arc;

use super::domain::Domain;
use super::planner::{self, PlanningError};
use super::workflow::Workflow;
use super::{Ready, Uninitialized, Worker, WorkerState};

use crate::error::{Error, ErrorKind};
use crate::runtime::{Resources, System};
use crate::serde::{de::DeserializeOwned, Serialize};
use crate::state::State;
use crate::sync;
use crate::system_ext::SystemExt;
use crate::task::Task;

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
    /// use mahler::task::{IO, with_io};
    /// use mahler::extract::{View, Target};
    /// use mahler::worker::Worker;
    /// use mahler::dag::{Dag, seq};
    /// use mahler::job::update;
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
    pub fn find_workflow(&self, cur: O, tgt: O::Target) -> Result<Workflow, PlanningError> {
        let mut ini = System::try_from(cur).map_err(Error::from)?;
        let resources: &Resources = self.inner.as_ref();
        ini.set_resources(resources.clone());
        let tgt = serde_json::to_value(tgt).map_err(Error::from)?;

        let domain: &Domain = self.inner.as_ref();

        planner::find_workflow_for_target::<O>(domain, &ini, &tgt)
    }

    #[cfg_attr(docsrs, doc(cfg(debug_assertions)))]
    /// Test a task within the context of the worker domain
    ///
    /// # Example
    /// ```rust
    /// use std::time::Duration;
    /// use tokio::time::sleep;
    ///
    /// use mahler::error::ErrorKind;
    /// use mahler::task::{Handler, IO, with_io};
    /// use mahler::extract::{View, Target};
    /// use mahler::worker::{Worker, Ready};
    /// use mahler::job::update;
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
    /// // Run task emulating a target of 2 and initial state of 2 (condition not met)
    /// let worker: Worker<i32, _> = Worker::new().job("", update(plus_one));
    /// let err = worker.run_task(2, plus_one.with_target(2)).await.unwrap_err();
    /// assert_eq!(err.kind(), ErrorKind::ConditionNotMet);
    /// # })
    /// ```
    pub async fn run_task(&self, initial_state: O, task: Task) -> Result<O, Error>
    where
        O: Serialize + DeserializeOwned,
    {
        let mut system = System::try_from(initial_state)?;

        // look for a workflow for the task
        let workflow = planner::find_workflow_for_task(task, self.inner.as_ref(), &system)?;

        let resources: &Resources = self.inner.as_ref();
        system.set_resources(resources.clone());

        // FIXME: this implementation is too messy, a future change will allow
        // worker to execute workflows generated from a search and we should be
        // able to clean this up
        let sys_reader = Arc::new(sync::RwLock::new(system));
        let (tx, mut rx) = sync::channel(10);
        let interrupt = sync::Interrupt::new();

        // Await for changes
        {
            let sys_writer = sys_reader.clone();
            tokio::spawn(async move {
                while let Some(mut msg) = rx.recv().await {
                    let changes = std::mem::take(&mut msg.data);
                    let mut system = sys_writer.write().await;
                    system.patch(changes).unwrap();
                    msg.ack();
                }
            });
        }

        // Run the workflow
        workflow
            .execute(&sys_reader, tx, interrupt)
            .await
            .map_err(|errors| {
                // this should not really happen since we have the only copy
                // of the system
                if errors
                    .iter()
                    .any(|e| e.kind() == ErrorKind::ConditionNotMet)
                {
                    return Error::from(ErrorKind::ConditionNotMet);
                }
                Error::runtime(errors)
            })?;

        let system = sys_reader.read().await;
        let new_state = system.state()?;

        Ok(new_state)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;

    use super::*;
    use crate::dag::{par, seq, Dag};
    use crate::extract::{Target, View};
    use crate::job::*;
    use crate::task::*;

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
            "mahler::worker::testing::tests::plus_one(/one)",
            "mahler::worker::testing::tests::plus_one(/two)"
        ) + seq!("mahler::worker::testing::tests::plus_one(/one)",);

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
