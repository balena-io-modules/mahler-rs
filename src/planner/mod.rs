use std::collections::BTreeMap;
use std::fmt::Debug;

use anyhow::{anyhow, Context as AnyhowCtx};
use json_patch::{Patch, PatchOperation};
use jsonptr::PointerBuf;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::{error, field, instrument, trace, trace_span, warn, Level, Span};

use crate::errors::{InternalError, MethodError, SerializationError};
use crate::path::Path;
use crate::system::System;
use crate::task::{self, Context, Operation, Task};
use crate::workflow::{WorkUnit, Workflow};
use crate::Dag;

mod distance;
mod domain;

use distance::*;
pub use domain::*;

#[derive(Debug)]
pub struct Planner(Domain);

#[derive(Debug, Error)]
enum SearchFailed {
    #[error("method error: {0}")]
    BadMethod(#[from] PathSearchError),

    #[error("task error: {0:?}")]
    BadTask(#[from] task::Error),

    #[error("task empty")]
    EmptyTask,

    #[error("loop detected")]
    LoopDetected,

    // this is probably a bug if this error
    // happens
    #[error("internal error: {0:?}")]
    Internal(#[from] anyhow::Error),
}

/// Returns the longest (maximum cardinality) subset of non‐conflicting paths
/// from the input. Two paths are considered to conflict if one is a prefix
/// of the other. In such a case, we choose to keep the path that is not a prefix
/// of any other—that is, the more specific path.
///
/// # Arguments
///
/// * `paths` - A vector of `Path` instances.
///
/// # Returns
///
/// A vector of `Path` which represents one maximal set (greedy solution)
/// in which no path is a prefix of another.
fn longest_non_conflicting(paths: Vec<Path>) -> Vec<Path> {
    // We use a simple O(n^2) approach:
    // For each path, if any other path starts with it, then skip it.
    // TODO: maybe implement a more efficient algorithm with a trie
    let mut result = Vec::new();
    for (i, p) in paths.iter().enumerate() {
        // Check p against every other path in the input.
        let mut p_is_prefix = false;
        for (j, q) in paths.iter().enumerate() {
            if i == j {
                continue;
            }
            // If p is a proper prefix of q, then skip p.
            if q.starts_with(p.as_str()) {
                p_is_prefix = true;
                break;
            }
        }
        if !p_is_prefix {
            result.push(p.clone());
        }
    }
    result
}

/// Computes the longest common prefix over a list of `Path`
fn longest_common_prefix<'a, I>(paths: I) -> Path
where
    I: IntoIterator<Item = &'a Path>,
{
    let mut iter = paths.into_iter();

    // Get the first path to use as the base for comparison
    let first = match iter.next() {
        Some(path) => path.as_ref().tokens().collect::<Vec<_>>(),
        None => return Path::default(),
    };

    let mut prefix = first;

    for path in iter {
        let tokens = path.as_ref().tokens().collect::<Vec<_>>();
        let mut new_prefix = vec![];

        for (a, b) in prefix.iter().zip(tokens.iter()) {
            if a == b {
                new_prefix.push(a.clone());
            } else {
                break;
            }
        }

        prefix = new_prefix;
        if prefix.is_empty() {
            break;
        }
    }

    let buf = PointerBuf::from_tokens(&prefix);

    Path::new(&buf)
}

#[derive(Clone, PartialEq, Eq)]
struct Candidate {
    workflow: Dag<WorkUnit>,
    changes: Vec<PatchOperation>,
    path: Path,
    operation: Operation,
    priority: u8,
    parallelizable: bool,
    is_method: bool,
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by path ordering first
        self.path
            .cmp(&other.path)
            // User defined methods vs actions and automatically generated
            // workflows
            .then(self.is_method.cmp(&other.is_method))
            // Sort by operation (`Any` is after all other)
            .then(self.operation.cmp(&other.operation))
            // Finally sort by job priority
            .then(self.priority.cmp(&other.priority))
    }
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Serialization(#[from] SerializationError),

    #[error(transparent)]
    Task(#[from] task::Error),

    #[error("workflow not found")]
    NotFound,

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl Planner {
    pub fn new(domain: Domain) -> Self {
        Self(domain)
    }

    pub fn domain(&self) -> &Domain {
        &self.0
    }

    #[instrument(level="trace", skip_all, fields(id=%task.id(), path=%task.path()), err(level=Level::TRACE))]
    fn try_task(
        &self,
        task: &Task,
        cur_state: &System,
        cur_plan: Workflow,
        pending_changes: &mut Vec<PatchOperation>,
    ) -> Result<Workflow, SearchFailed> {
        match task {
            Task::Action(action) => {
                let work_id = WorkUnit::new_id(action, cur_state.root());

                // Detect loops first, if the same action is being applied to the same
                // state then abort this search branch
                if cur_plan.as_dag().any(|a| a.id == work_id) {
                    return Err(SearchFailed::LoopDetected)?;
                }

                // Simulate the task and get the list of changes
                let patch = action.dry_run(cur_state).map_err(SearchFailed::BadTask)?;
                if patch.is_empty() {
                    return Err(SearchFailed::EmptyTask);
                }

                // The task has been selected
                trace!(selected=true, changes=%patch);

                let Patch(changes) = patch;
                let Workflow(dag) = cur_plan;

                // Append a new node to the workflow, include a copy
                // of the changes for validation during runtime
                let dag = dag + WorkUnit::new(work_id, action.clone(), changes.clone());

                pending_changes.extend(changes);

                Ok(Workflow(dag))
            }
            Task::Method(method) => {
                // Get the list of referenced tasks
                let tasks = method.expand(cur_state).map_err(SearchFailed::BadTask)?;

                // Extended tasks will store the correct references from the domain with the
                // right path and description
                let mut extended_tasks = Vec::new();

                for mut t in tasks.into_iter() {
                    // The subtask is not allowed to override arguments
                    // in the parent task, so we first make sure to propagate
                    // arguments from the parent
                    for (k, v) in method.context().args.iter() {
                        t = t.with_arg(k, v);
                    }

                    let task_id = t.id().to_string();
                    let Context { args, .. } = t.context_mut();

                    // Find the job path on the domain list
                    let path = self.0.find_path_for_job(&task_id, args)?;

                    // Using the path, now find the actual job on the domain.
                    // The domain job includes metadata like the description that
                    // we want to use in the workflow
                    let job = self
                        .0
                        .find_job(&path, &task_id)
                        // this should never happen
                        .ok_or(anyhow!("failed to find job for path {path}"))?;

                    // Get a copy of the task for the final list
                    let task = job.new_task(t.context().to_owned()).with_path(path.clone());

                    extended_tasks.push(task);
                }

                // Compute a maximal set of non-overlapping (non-prefix) paths for parallelism
                let non_conflicting_paths = longest_non_conflicting(
                    extended_tasks.iter().map(|t| t.path().clone()).collect(),
                );

                // The method is parallelizable if all task paths are in the non-conflicting list
                // and are all scoped (i.e. none of them requires access to System)
                let mut parallelizable = true;
                for task in &extended_tasks {
                    if !task.is_scoped() || !non_conflicting_paths.contains(task.path()) {
                        parallelizable = false;
                        break;
                    }
                }

                let mut cur_plan = cur_plan;

                if parallelizable {
                    let mut branches = vec![];

                    // Create a branch for each task
                    for task in extended_tasks {
                        let Workflow(dag) =
                            self.try_task(&task, cur_state, Workflow::default(), pending_changes)?;

                        branches.push(dag);
                    }

                    // Extend the current plan with the forking dag
                    cur_plan = Workflow(cur_plan.0 + Dag::new(branches));
                } else {
                    // Clone the state in order to apply sequential changes
                    let mut cur_state = cur_state.clone();

                    // If the task is not parallelizable, run tasks in sequence making
                    // sure to apply changes before calling the next task
                    for task in extended_tasks {
                        let mut changes = vec![];
                        let Workflow(dag) =
                            self.try_task(&task, &cur_state, cur_plan, &mut changes)?;

                        // Apply changes before the next task
                        cur_state.patch(Patch(changes.clone())).with_context(|| {
                            format!("failed to apply patch {pending_changes:?}")
                        })?;

                        // Add the changes to the pending list
                        pending_changes.extend(changes);
                        cur_plan = Workflow(dag);
                    }
                }

                // Include changes in the returned plan
                Ok(cur_plan)
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    pub(crate) fn find_workflow<T>(&self, system: &System, tgt: &Value) -> Result<Workflow, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        // The search stack stores (current_state, current_plan, depth)
        let mut stack = vec![(system.clone(), Workflow::default(), 0)];
        let find_workflow_span = Span::current();

        while let Some((cur_state, cur_plan, depth)) = stack.pop() {
            // Prevent infinite recursion (e.g., from buggy tasks or recursive methods)
            if depth >= 256 {
                warn!(parent: &find_workflow_span, "reached max search depth (256)");
                return Err(Error::NotFound)?;
            }

            // Normalize state: deserialize into T and re-serialize to remove internal fields
            let cur = cur_state
                .state::<T>()
                .and_then(System::try_from)
                .map_err(SerializationError::from)?;

            // Compute the difference between current and target state
            let distance = Distance::new(&cur, tgt);

            // If no difference, we’ve reached the goal
            if distance.is_empty() {
                return Ok(cur_plan);
            }

            let next_span = trace_span!("find_next", cur = %&cur_state.root(), tgt=%tgt, candidates=field::Empty);
            let _enter = next_span.enter();

            // List of candidate plans at this level in the stack
            let mut candidates: Vec<Candidate> = Vec::new();

            // Iterate over distance operations and jobs to find possible candidates
            for op in distance.iter() {
                let path = Path::new(op.path());

                // Retrieve matching jobs at this path
                if let Some((args, jobs)) = self.0.find_matching_jobs(path.as_str()) {
                    let pointer = path.as_ref();
                    let target = pointer.resolve(tgt).unwrap_or(&Value::Null);

                    let context = Context {
                        path: path.clone(),
                        args,
                        target: target.clone(),
                    };

                    // Filter `None` jobs from the list
                    for job in jobs.filter(|j| j.operation() != &Operation::None) {
                        if op.matches(job.operation()) || job.operation() == &Operation::Any {
                            let task = job.new_task(context.clone());
                            let mut changes = Vec::new();

                            // Try applying this task to the current state
                            match self.try_task(
                                &task,
                                &cur_state,
                                Workflow::default(),
                                &mut changes,
                            ) {
                                Ok(Workflow(workflow)) if !changes.is_empty() => {
                                    candidates.push(Candidate {
                                        workflow,
                                        changes,
                                        path: path.clone(),
                                        parallelizable: task.is_scoped(),
                                        is_method: task.is_method(),
                                        operation: job.operation().clone(),
                                        priority: job.priority(),
                                    });
                                }

                                // Non-critical errors are ignored (loop, empty, condition failure)
                                Err(SearchFailed::LoopDetected)
                                | Err(SearchFailed::EmptyTask)
                                | Err(SearchFailed::BadTask(task::Error::ConditionFailed)) => {}

                                // Critical internal errors terminate the search
                                Err(SearchFailed::Internal(err)) => {
                                    return Err(InternalError::from(err))?;
                                }

                                // Method expansion failure
                                Err(SearchFailed::BadMethod(err)) => {
                                    let err = MethodError::new(err);
                                    if cfg!(debug_assertions) {
                                        return Err(task::Error::from(err))?;
                                    }
                                    warn!(
                                        parent: &find_workflow_span,
                                        "task {} failed: {} ... ignoring",
                                        task.id(),
                                        err
                                    );
                                }

                                // Other task failure (non-debug: warn and skip)
                                Err(SearchFailed::BadTask(err)) => {
                                    if cfg!(debug_assertions) {
                                        return Err(err)?;
                                    }
                                    warn!(
                                        parent: &find_workflow_span,
                                        "task {} failed: {} ... ignoring",
                                        task.id(),
                                        err
                                    );
                                }

                                _ => {}
                            }
                        }
                    }
                }
            }

            // Compute a maximal set of non-overlapping (non-prefix) paths for parallelism
            let non_conflicting_paths =
                longest_non_conflicting(distance.iter().map(|op| Path::new(op.path())).collect());

            // Find candidates that can be parallelized
            let mut parallelizable: BTreeMap<Path, Candidate> = BTreeMap::new();
            for candidate in candidates.iter() {
                // If the candidate is scoped and the path belongs to the non conflicting path list
                // then add the candidate to the parallelizable list if there isn't a path already
                if candidate.parallelizable
                    && !parallelizable.contains_key(&candidate.path)
                    && non_conflicting_paths.iter().any(|p| p == &candidate.path)
                {
                    parallelizable.insert(candidate.path.clone(), candidate.clone());
                }
            }

            if parallelizable.len() > 1 {
                let mut branches = Vec::new();
                let mut changes = Vec::new();
                let mut total_priority = 0;
                // The path for the candidate is the longest common prefix between child paths
                // XXX: maybe we need to skip the candidate if there is a method for the
                // same path?
                let path = longest_common_prefix(parallelizable.keys());
                for Candidate {
                    workflow,
                    changes: pending,
                    priority,
                    ..
                } in parallelizable.into_values()
                {
                    branches.push(workflow);
                    changes.extend(pending);
                    // Aggregate each branch priority
                    total_priority += priority;
                }

                // Construct a new candidate using the parallel branches
                // NOTE: we could keep adding branches to the DAG as long as there are non conflicting
                // paths with the candidate path. For now we just do this operation once
                candidates.push(Candidate {
                    workflow: Dag::new(branches),
                    changes,
                    parallelizable: true,
                    path,
                    // If there is a method for the same path, give more priority to the method
                    is_method: false,
                    operation: Operation::Update,
                    priority: total_priority,
                })
            }

            // sort candidates
            candidates.sort();

            // Record candidates found for this planning step
            next_span.record("candidates", candidates.len());

            // For each candidate add a new plan to the stack
            for Candidate {
                workflow, changes, ..
            } in candidates.into_iter()
            {
                let mut new_sys = cur_state.clone();
                new_sys
                    .patch(Patch(changes))
                    .with_context(|| "failed to apply patch")
                    .map_err(InternalError::from)?;

                // Extend current plan
                let Workflow(cur_plan) = cur_plan.clone();
                let new_plan = Workflow(cur_plan + workflow);

                // Add updated plan/state to the search stack
                stack.push((new_sys, new_plan, depth + 1));
            }
        }

        // No candidate plan reached the goal state
        Err(Error::NotFound)?
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeMap, HashMap};
    use std::fmt::Display;

    use super::*;
    use crate::extract::{Args, System, Target, View};
    use crate::{dag, par, task::*};
    use crate::{seq, Dag};
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Counters(HashMap<String, i32>);

    fn init() {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_target(false)
                    .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
            )
            .with(EnvFilter::from_default_env())
            .try_init()
            .unwrap_or(());
    }

    fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
        if *counter < tgt {
            *counter += 1;
        }

        counter
    }

    fn buggy_plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
        if *counter < tgt {
            // This is the wrong operation
            *counter -= 1;
        }

        counter
    }

    fn plus_two(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
        if tgt - *counter > 1 {
            return vec![plus_one.with_target(tgt), plus_one.with_target(tgt)];
        }

        vec![]
    }

    fn plus_three(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
        if tgt - *counter > 2 {
            return vec![plus_two.with_target(tgt), plus_one.with_target(tgt)];
        }

        vec![]
    }

    fn minus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
        if *counter > tgt {
            *counter -= 1;
        }

        counter
    }

    pub fn find_plan<S>(planner: Planner, cur: S, tgt: S) -> Result<Workflow, super::Error>
    where
        S: Serialize + DeserializeOwned,
    {
        let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

        let system =
            crate::system::System::try_from(cur).expect("failed to serialize current state");

        let res = planner.find_workflow::<S>(&system, &tgt)?;
        Ok(res)
    }

    #[test]
    fn it_calculates_a_linear_workflow() {
        let domain = Domain::new()
            .job("", update(plus_one))
            .job("", update(minus_one));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, 0, 2).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::planner::tests::plus_one()",
            "mahler::planner::tests::plus_one()"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_ignores_none_jobs() {
        let domain = Domain::new().job("", none(plus_one));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, 0, 2);

        assert!(matches!(workflow, Err(super::Error::NotFound)));
    }

    #[test]
    fn it_aborts_search_if_plan_length_grows_too_much() {
        let domain = Domain::new()
            .job("", update(buggy_plus_one))
            .job("", update(minus_one));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, 0, 2);
        assert!(workflow.is_err());
    }

    #[test]
    fn it_calculates_a_linear_workflow_with_compound_tasks() {
        let domain = Domain::new()
            .job("", update(plus_two))
            .job("", none(plus_one));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, 0, 2).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::planner::tests::plus_one()",
            "mahler::planner::tests::plus_one()"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state() {
        #[derive(Serialize, Deserialize)]
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
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect counters to be updated in parallel
        let expected: Dag<&str> = par!(
            "mahler::planner::tests::plus_one(/counters/one)",
            "mahler::planner::tests::plus_one(/counters/two)",
        ) + par!(
            "mahler::planner::tests::plus_one(/counters/one)",
            "mahler::planner::tests::plus_one(/counters/two)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state_with_compound_tasks() {
        #[derive(Serialize, Deserialize)]
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
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect a parallel dag with two tasks on each branch
        let expected: Dag<&str> = dag!(
            seq!(
                "mahler::planner::tests::plus_one(/counters/one)",
                "mahler::planner::tests::plus_one(/counters/one)",
            ),
            seq!(
                "mahler::planner::tests::plus_one(/counters/two)",
                "mahler::planner::tests::plus_one(/counters/two)",
            )
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state_with_deep_compound_tasks() {
        #[derive(Serialize, Deserialize)]
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
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::planner::tests::plus_one(/counters/one)",
            "mahler::planner::tests::plus_one(/counters/one)",
            "mahler::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_finds_parallel_plans_with_nested_forks() {
        init();
        type Counters = BTreeMap<String, i32>;

        #[derive(Serialize, Deserialize, Debug)]
        struct MyState {
            counters: Counters,
        }

        // This is a very dumb example to test how the planner
        // choses parallelizable methods over automated parallelization
        fn multi_increment(counters: View<Counters>, target: Target<Counters>) -> Vec<Task> {
            counters
                .keys()
                .filter(|k| {
                    target.get(k.as_str()).unwrap_or(&0) - counters.get(k.as_str()).unwrap_or(&0)
                        > 1
                })
                .map(|k| {
                    plus_two
                        .with_arg("counter", k)
                        .with_target(target.get(k.as_str()))
                })
                .collect::<Vec<Task>>()
        }

        fn chunker(counters: View<Counters>, target: Target<Counters>) -> Vec<Task> {
            let to_update = counters
                .keys()
                .filter(|k| {
                    target.get(k.as_str()).unwrap_or(&0) - counters.get(k.as_str()).unwrap_or(&0)
                        > 1
                })
                .collect::<Vec<&String>>();

            let mut tasks: Vec<Task> = Vec::new();
            for chunk in to_update.chunks(2) {
                let mut tgt = (*counters).clone();
                for k in chunk {
                    if target.contains_key(k.as_str()) {
                        tgt.insert(k.to_string(), *target.get(k.as_str()).unwrap_or(&0));
                    }
                }
                tasks.push(multi_increment.with_target(tgt));
            }

            tasks
        }

        let domain = Domain::new()
            .job(
                "/counters/{counter}",
                update(plus_one)
                    .with_description(|Args(counter): Args<String>| format!("{counter}++")),
            )
            .job("/counters/{counter}", update(plus_two))
            .job("/counters", update(chunker))
            .job("/counters", none(multi_increment));

        let initial = MyState {
            counters: BTreeMap::from([
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("c".to_string(), 0),
                ("d".to_string(), 0),
            ]),
        };

        let target = MyState {
            counters: BTreeMap::from([
                ("a".to_string(), 3),
                ("b".to_string(), 2),
                ("c".to_string(), 2),
                ("d".to_string(), 2),
            ]),
        };

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect a parallel dag with two tasks on each branch
        let expected: Dag<&str> = dag!(seq!("a++", "a++"), seq!("b++", "b++"))
            + dag!(seq!("c++", "c++"), seq!("d++", "d++"))
            + seq!("a++");

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn test_stacking_problem() {
        init();

        #[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
        enum Block {
            A,
            B,
            C,
        }

        impl Display for Block {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }

        #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
        enum Location {
            Blk(Block),
            Table,
            Hand,
        }

        impl Location {
            fn is_block(&self) -> bool {
                matches!(self, Location::Blk(_))
            }
        }

        type Blocks = HashMap<Block, Location>;

        #[derive(Serialize, Deserialize, Debug)]
        struct State {
            blocks: Blocks,
        }

        fn is_clear(blocks: &Blocks, loc: &Location) -> bool {
            if loc.is_block() || loc == &Location::Hand {
                // No block is on top of the location
                return blocks.iter().all(|(_, l)| l != loc);
            }
            // the table is always clear
            true
        }

        fn is_holding(blocks: &Blocks) -> bool {
            !is_clear(blocks, &Location::Hand)
        }

        fn all_clear(blocks: &Blocks) -> Vec<&Block> {
            blocks
                .iter()
                .filter(|(b, _)| is_clear(blocks, &Location::Blk((*b).clone())))
                .map(|(b, _)| b)
                .collect()
        }

        // Get a block from the table
        fn pickup(
            mut loc: View<Location>,
            System(sys): System<State>,
            Args(block): Args<Block>,
        ) -> View<Location> {
            // if the block is clear and we are not holding any other blocks
            // we can grab the block
            if *loc == Location::Table
                && is_clear(&sys.blocks, &Location::Blk(block))
                && !is_holding(&sys.blocks)
            {
                *loc = Location::Hand;
            }

            loc
        }

        // Unstack a block from other block
        fn unstack(
            mut loc: View<Location>,
            System(sys): System<State>,
            Args(block): Args<Block>,
        ) -> Option<View<Location>> {
            // if the block is clear and we are not holding any other blocks
            // we can grab the block
            if loc.is_block()
                && is_clear(&sys.blocks, &Location::Blk(block))
                && !is_holding(&sys.blocks)
            {
                *loc = Location::Hand;
                return Some(loc);
            }

            None
        }

        // There is really not that much of a difference between putdown and stack
        // this is just to test that the planner can work with nested methods
        fn putdown(mut loc: View<Location>) -> View<Location> {
            // If we are holding the block and the target is clear
            // then we can modify the block location
            if *loc == Location::Hand {
                *loc = Location::Table
            }

            loc
        }

        fn stack(
            mut loc: View<Location>,
            Target(tgt): Target<Location>,
            System(sys): System<State>,
        ) -> View<Location> {
            // If we are holding the block and the target is clear
            // then we can modify the block location
            if *loc == Location::Hand && is_clear(&sys.blocks, &tgt) {
                *loc = tgt
            }

            loc
        }

        fn take(
            loc: View<Location>,
            System(sys): System<State>,
            Args(block): Args<Block>,
        ) -> Option<Task> {
            if is_clear(&sys.blocks, &Location::Blk(block)) {
                if *loc == Location::Table {
                    return Some(pickup.into_task());
                } else {
                    return Some(unstack.into_task());
                }
            }
            None
        }

        fn put(loc: View<Location>, Target(tgt): Target<Location>) -> Option<Task> {
            if *loc == Location::Hand {
                if tgt == Location::Table {
                    return Some(putdown.into_task());
                } else {
                    return Some(stack.with_target(tgt));
                }
            }
            None
        }

        //
        //  This method implements the following block-stacking algorithm [1]:
        //
        //  - If there's a clear block x that can be moved to a place where it won't
        //    need to be moved again, then return a todo list that includes goals to
        //    move it there, followed by mgoal (to achieve the remaining goals).
        //    Otherwise, if there's a clear block x that needs to be moved out of the
        //    way to make another block movable, then return a todo list that includes
        //    goals to move x to the table, followed by mgoal.
        //  - Otherwise, no blocks need to be moved.
        //    [1] N. Gupta and D. S. Nau. On the complexity of blocks-world
        //    planning. Artificial Intelligence 56(2-3):223–254, 1992.
        //
        //  Source: https://github.com/dananau/GTPyhop/blob/main/Examples/blocks_hgn/methods.py
        //
        fn move_blks(blocks: View<Blocks>, Target(target): Target<Blocks>) -> Vec<Task> {
            for blk in all_clear(&blocks) {
                // we assume that the target is well formed
                let tgt_loc = target.get(blk).unwrap();
                let cur_loc = blocks.get(blk).unwrap();

                // The block is free and it can be moved to the final location (another block or the table)
                if cur_loc != tgt_loc && is_clear(&blocks, tgt_loc) {
                    return vec![
                        take.with_arg("block", blk.to_string()),
                        put.with_arg("block", blk.to_string()).with_target(tgt_loc),
                    ];
                }
            }

            // If we get here, no blocks can be moved to the final location so
            // we move them to the table
            let mut to_table: Vec<Task> = vec![];
            for b in all_clear(&blocks) {
                to_table.push(take.with_arg("block", b.to_string()));
                to_table.push(
                    put.with_target(Location::Table)
                        .with_arg("block", b.to_string()),
                );
            }

            to_table
        }
        let domain = Domain::new()
            .jobs(
                "/blocks/{block}",
                [
                    update(pickup).with_description(|Args(block): Args<String>| {
                        format!("pick up block {block}")
                    }),
                    update(unstack).with_description(|Args(block): Args<String>| {
                        format!("unstack block {block}")
                    }),
                    update(putdown).with_description(|Args(block): Args<String>| {
                        format!("put down block {block}")
                    }),
                    update(stack).with_description(
                        |Args(block): Args<String>, Target(tgt): Target<Location>| {
                            let tgt_block = match tgt {
                                Location::Blk(block) => format!("{block:?}"),
                                _ => format!("{tgt:?}"),
                            };

                            format!("stack block {block} on top of block {tgt_block}")
                        },
                    ),
                    update(take),
                    update(put),
                ],
            )
            .job("/blocks", update(move_blks));

        let planner = Planner::new(domain);

        let initial = State {
            blocks: HashMap::from([
                (Block::A, Location::Table),
                (Block::B, Location::Blk(Block::A)),
                (Block::C, Location::Blk(Block::B)),
            ]),
        };
        let target = State {
            blocks: HashMap::from([
                (Block::A, Location::Blk(Block::B)),
                (Block::B, Location::Blk(Block::C)),
                (Block::C, Location::Table),
            ]),
        };

        let workflow = find_plan(planner, initial, target).unwrap();
        let expected: Dag<&str> = seq!(
            "unstack block C",
            "put down block C",
            "unstack block B",
            "stack block B on top of block C",
            "pick up block A",
            "stack block A on top of block B",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }
}
