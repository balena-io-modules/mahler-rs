use json_patch::Patch;
use log::warn;
use serde::Serialize;
use std::fmt::{self, Display};

use crate::error::{Error, IntoError};
use crate::path::Path;
use crate::system::System;
use crate::task::{ConditionFailed, Context, Task};

use super::distance::Distance;
use super::domain::Domain;
use super::workflow::{Action, Workflow};
use super::Operation;

pub struct Planner {
    domain: Domain,
}

#[derive(Debug)]
pub enum PlanSearchError {
    SerializationError(serde_json::error::Error),
    CannotResolveTarget {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },
    LoopDetected,
    ConditionFailed(crate::task::ConditionFailed),
    PlanNotFound,
}

impl std::error::Error for PlanSearchError {}

impl Display for PlanSearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanSearchError::SerializationError(err) => err.fmt(f),
            PlanSearchError::LoopDetected => {
                write!(f, "loop detected")
            }
            PlanSearchError::ConditionFailed(err) => write!(f, "condition failed: {}", err),
            PlanSearchError::CannotResolveTarget { path, reason } => {
                write!(f, "cannot resolve target path `{}`: {}", path, reason)
            }
            PlanSearchError::PlanNotFound => {
                write!(f, "plan not found")
            }
        }
    }
}

impl IntoError for PlanSearchError {
    fn into_error(self) -> Error {
        Error::PlanSearchFailed(self)
    }
}

impl Planner {
    pub fn new(domain: Domain) -> Self {
        Self { domain }
    }

    fn try_task(
        &self,
        task: Task,
        cur_state: &System,
        cur_plan: Workflow,
    ) -> Result<Workflow, PlanSearchError> {
        match task {
            Task::Atom { .. } => {
                let action_id = Action::new_id(&task, cur_state.root()).map_err(|e| {
                    PlanSearchError::CannotResolveTarget {
                        path: task.context().path.to_string(),
                        reason: e,
                    }
                })?;

                // Detect loops in the plan
                if cur_plan.as_dag().some(|a| a.id == action_id) {
                    return Err(PlanSearchError::LoopDetected);
                }

                // Test the task
                let Patch(changes) = task.dry_run(cur_state).map_err(|err| match err {
                    Error::TaskConditionFailed(e) => PlanSearchError::ConditionFailed(e),
                    _ => {
                        warn!(
                            "Ignoring task {} due to unexpected error during planning: {}",
                            task, err
                        );
                        PlanSearchError::PlanNotFound
                    }
                })?;

                // If no changes are returned, then assume the condition has failed
                // TODO: make an exception if we are inside a compound task
                if changes.is_empty() {
                    return Err(PlanSearchError::ConditionFailed(ConditionFailed::default()));
                }

                // Otherwise add the task to the plan
                let Workflow { dag, pending } = cur_plan;
                let dag = dag.with_head(Action::new(action_id, task));
                let pending = [pending, changes].concat();

                Ok(Workflow { dag, pending })
            }
            Task::List { .. } => unimplemented!(),
        }
    }

    pub fn find_plan<S>(&self, cur: S, tgt: S) -> Result<Workflow, Error>
    where
        S: Serialize,
    {
        let cur = serde_json::to_value(cur).map_err(PlanSearchError::SerializationError)?;
        let tgt = serde_json::to_value(tgt).map_err(PlanSearchError::SerializationError)?;

        let system = System::new(cur);

        // Store the initial state and an empty plan on the stack
        let mut stack = vec![(system, Workflow::default())];

        while let Some((cur_state, cur_plan)) = stack.pop() {
            let distance = Distance::new(&cur_state, &tgt);

            // we reached the target
            if distance.is_empty() {
                // return the existing plan
                return Ok(cur_plan);
            }

            for op in distance.iter() {
                // Find applicable tasks
                let path = Path::new(op.path());
                let matching = self.domain.at(path.to_str());
                if let Some((args, intents)) = matching {
                    // Calculate the target for the job path
                    let pointer = path.as_ref();
                    let target = pointer.resolve(&tgt).map_err(|e| {
                        PlanSearchError::CannotResolveTarget {
                            path: path.to_string(),
                            reason: e,
                        }
                    })?;

                    // Create the calling context for the job
                    let context = Context {
                        path,
                        args,
                        target: target.clone(),
                    };
                    for intent in intents {
                        // If the intent is applicable to the operation
                        if op.matches(&intent.operation) || intent.operation != Operation::Any {
                            let task = intent.job.into_task(context.clone());
                            let task_id = task.to_string();

                            // apply the task to the state, if it progresses the plan, then select
                            // it and put the new state with the new plan on the stack
                            if let Ok(Workflow { dag, pending }) =
                                self.try_task(task, &cur_state, cur_plan.clone())
                            {
                                // If there are no changes introduced by the task, then it doesn't
                                // contribute to the plan
                                if pending.is_empty() {
                                    warn!(
                                        "Ignoring task {} that does not contribute towards the plan",
                                        task_id
                                    );
                                    continue;
                                }

                                // If we got here, the task is part of a new potential workflow
                                // so we to make a copy of the system
                                let mut new_sys = cur_state.clone();

                                // Update the state and the workflow
                                new_sys.patch(Patch(pending))?;
                                let new_plan = Workflow {
                                    dag,
                                    pending: vec![],
                                };

                                // add the new initial state and plan to the stack
                                // TODO: limit the size of the stack to avoid following unhealthy
                                // workflows
                                stack.push((new_sys, new_plan));
                            }
                        }
                    }
                }
            }
        }

        Err(PlanSearchError::PlanNotFound)?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::extract::{Target, Update};
    use crate::worker::{update, Domain};
    use crate::{seq, Dag};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn plus_one(mut counter: Update<i32>, Target(tgt): Target<i32>) -> Update<i32> {
        if *counter < tgt {
            *counter += 1;
        }

        counter
    }

    fn minus_one(mut counter: Update<i32>, Target(tgt): Target<i32>) -> Update<i32> {
        if *counter > tgt {
            *counter -= 1;
        }

        counter
    }

    #[test]
    fn it_calculates_a_simple_linear_workflow() {
        init();

        let domain = Domain::new()
            .job("", update(plus_one))
            .job("", update(minus_one));

        let planner = Planner::new(domain);
        let workflow = planner.find_plan(0, 2).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "worker::planner::tests::plus_one()",
            "worker::planner::tests::plus_one()"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_simple_workflow_on_a_complex_state() {
        init();

        #[derive(Serialize)]
        struct MyState {
            counters: HashMap<String, i32>,
        }

        let initial = MyState {
            counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
        };

        let target = MyState {
            counters: HashMap::from([("one".to_string(), 2), ("two".to_string(), 2)]),
        };

        let domain = Domain::new()
            .job("/counters/{counter}", update(minus_one))
            .job("/counters/{counter}", update(plus_one));

        let planner = Planner::new(domain);
        let workflow = planner.find_plan(initial, target).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/two)",
            "worker::planner::tests::plus_one(/counters/two)"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
