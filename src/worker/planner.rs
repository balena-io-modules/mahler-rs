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
use super::{DomainSearchError, Operation};

pub struct Planner {
    domain: Domain,
}

#[derive(Debug, PartialEq)]
pub enum PlanningError {
    CannotResolvePath {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },
    MissingArgs(Vec<String>),
    TaskNotFound,
    LoopDetected,
    WorkflowNotFound,
    MaxDepthReached,
}

impl std::error::Error for PlanningError {}

impl Display for PlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanningError::LoopDetected => {
                write!(f, "loop detected")
            }
            PlanningError::CannotResolvePath { path, reason } => {
                write!(f, "cannot resolve path `{}`: {}", path, reason)
            }
            PlanningError::WorkflowNotFound => {
                write!(f, "plan not found")
            }
            PlanningError::MissingArgs(args) => {
                write!(f, "missing args: {:?}", args)
            }
            PlanningError::TaskNotFound => {
                write!(f, "task not found on the planning domain")
            }
            PlanningError::MaxDepthReached => {
                write!(f, "max search depth reached while looking for a plan")
            }
        }
    }
}

impl IntoError for PlanningError {
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
        stack_len: u32,
    ) -> Result<Workflow, Error> {
        match &task {
            Task::Atom {
                context, dry_run, ..
            } => {
                let action_id = Action::new_id(&task, cur_state.root()).map_err(|e| {
                    PlanningError::CannotResolvePath {
                        path: task.context().path.to_string(),
                        reason: e,
                    }
                })?;

                // Detect loops in the plan
                if cur_plan.as_dag().some(|a| a.id == action_id) {
                    return Err(PlanningError::LoopDetected)?;
                }

                // Test the task
                let Patch(changes) = dry_run(cur_state, context)?;

                // If no changes are returned, then assume the condition has failed
                // unless we are inside a compound task
                if stack_len == 0 && changes.is_empty() {
                    return Err(ConditionFailed::default())?;
                }

                // Otherwise add the task to the plan
                let Workflow { dag, pending } = cur_plan;
                let dag = dag.with_head(Action::new(action_id, task));
                let pending = [pending, changes].concat();

                Ok(Workflow { dag, pending })
            }
            Task::List {
                context, expand, ..
            } => {
                let tasks = expand(cur_state, context)?;

                let mut cur_state = cur_state.clone();
                let mut cur_plan = cur_plan;
                for mut t in tasks.into_iter() {
                    // Merge the parent args into the child task args
                    for (k, v) in context.args.iter() {
                        t = t.with_arg(k, v)
                    }
                    let path = self
                        .domain
                        .get_path(t.id(), &t.context().args)
                        // The user may have not have put the child task in the
                        // domain, in which case we need to return an error
                        .map_err(|e| match e {
                            DomainSearchError::JobNotFound => PlanningError::TaskNotFound,
                            DomainSearchError::MissingArgs(args) => {
                                PlanningError::MissingArgs(args)
                            }
                        })?;

                    let task = t.with_path(path);
                    let Workflow { dag, pending } =
                        self.try_task(task, &cur_state, cur_plan, stack_len + 1)?;

                    // patch the state before passing it to the next task
                    cur_state.patch(Patch(pending.clone()))?;
                    cur_plan = Workflow { dag, pending };
                }
                Ok(cur_plan)
            }
        }
    }

    pub fn find_plan<S>(&self, cur: S, tgt: S) -> Result<Workflow, Error>
    where
        S: Serialize,
    {
        let cur = serde_json::to_value(cur)?;
        let tgt = serde_json::to_value(tgt)?;

        let system = System::new(cur);

        // Store the initial state and an empty plan on the stack
        let mut stack = vec![(system, Workflow::default(), 0)];

        // TODO: we should merge non conflicting workflows
        // for parallelism
        while let Some((cur_state, cur_plan, depth)) = stack.pop() {
            // we need to limit the search depth to avoid following
            // a buggy task forever
            // TODO: make this configurable
            if depth >= 256 {
                return Err(PlanningError::MaxDepthReached)?;
            }

            let distance = Distance::new(&cur_state, &tgt);

            // we reached the target
            if distance.is_empty() {
                // return the existing plan reversing it first
                return Ok(cur_plan.reverse());
            }

            for op in distance.iter() {
                // Find applicable tasks
                let path = Path::new(op.path());
                let matching = self.domain.at(path.to_str());
                if let Some((args, intents)) = matching {
                    // Calculate the target for the job path
                    let pointer = path.as_ref();

                    // This should technically never happen
                    let target =
                        pointer
                            .resolve(&tgt)
                            .map_err(|e| PlanningError::CannotResolvePath {
                                path: path.to_string(),
                                reason: e,
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
                            match self.try_task(task, &cur_state, cur_plan.clone(), 0) {
                                Ok(Workflow { dag, pending }) => {
                                    // If there are no changes introduced by the task, then it doesn't
                                    // contribute to the plan
                                    if pending.is_empty() {
                                        warn!(
                                        "ignoring task {} that does not make changes to the state",
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
                                    stack.push((new_sys, new_plan, depth + 1));
                                }
                                // Ignore harmless errors
                                Err(Error::TaskConditionFailed(_)) => {}
                                Err(Error::PlanSearchFailed(PlanningError::LoopDetected)) => {}
                                Err(Error::PlanSearchFailed(PlanningError::WorkflowNotFound)) => {}
                                // Otherwise return the error if debugging or raise a
                                // warning if on a final release
                                Err(err) => {
                                    if cfg!(debug_assertions) {
                                        return Err(err);
                                    } else {
                                        warn!(
                                    "ignoring task {} due to unexpected error during planning: {}",
                                    task_id, err
                                );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(PlanningError::WorkflowNotFound)?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::extract::{Target, Update};
    use crate::task::*;
    use crate::worker::{none, update, Domain};
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

    fn buggy_plus_one(mut counter: Update<i32>, Target(tgt): Target<i32>) -> Update<i32> {
        if *counter < tgt {
            // This is the wrong operation
            *counter -= 1;
        }

        counter
    }

    fn plus_two(counter: Update<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
        if tgt - *counter > 1 {
            return vec![plus_one.with_target(tgt), plus_one.with_target(tgt)];
        }

        vec![]
    }

    fn plus_three(counter: Update<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
        if tgt - *counter > 2 {
            return vec![plus_two.with_target(tgt), plus_one.with_target(tgt)];
        }

        vec![]
    }

    fn minus_one(mut counter: Update<i32>, Target(tgt): Target<i32>) -> Update<i32> {
        if *counter > tgt {
            *counter -= 1;
        }

        counter
    }

    #[test]
    fn it_calculates_a_linear_workflow() {
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
    fn it_aborts_search_if_plan_length_grows_too_much() {
        init();

        let domain = Domain::new()
            .job("", update(buggy_plus_one))
            .job("", update(minus_one));

        let planner = Planner::new(domain);
        let workflow = planner.find_plan(0, 2);
        assert!(workflow.is_err());
        if let Err(Error::PlanSearchFailed(err)) = workflow {
            assert_eq!(err, PlanningError::MaxDepthReached);
        }
    }

    #[test]
    fn it_calculates_a_linear_workflow_with_compound_tasks() {
        init();

        let domain = Domain::new()
            .job("", update(plus_two))
            .job("", none(plus_one));

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
    fn it_calculates_a_linear_workflow_on_a_complex_state() {
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
            "worker::planner::tests::plus_one(/counters/two)",
            "worker::planner::tests::plus_one(/counters/two)",
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state_with_compound_tasks() {
        init();

        #[derive(Serialize)]
        struct MyState {
            counters: HashMap<String, i32>,
        }

        let initial = MyState {
            counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
        };

        let target = MyState {
            counters: HashMap::from([("one".to_string(), 2), ("two".to_string(), 0)]),
        };

        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let planner = Planner::new(domain);
        let workflow = planner.find_plan(initial, target).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state_with_deep_compound_tasks() {
        init();

        #[derive(Serialize)]
        struct MyState {
            counters: HashMap<String, i32>,
        }

        let initial = MyState {
            counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
        };

        let target = MyState {
            counters: HashMap::from([("one".to_string(), 3), ("two".to_string(), 0)]),
        };

        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", none(plus_two))
            .job("/counters/{counter}", update(plus_three));

        let planner = Planner::new(domain);
        let workflow = planner.find_plan(initial, target).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/one)",
            "worker::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
