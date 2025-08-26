use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use anyhow::{anyhow, Context as AnyhowCtx};
use json_patch::{Patch, PatchOperation};
use jsonptr::PointerBuf;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::field::display;
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

#[derive(Debug, Clone)]
pub struct Planner(Domain);

#[derive(Debug, Error)]
enum SearchFailed {
    #[error("method error: {0}")]
    BadMethod(#[from] PathSearchError),

    #[error("task error: {0:?}")]
    BadTask(#[from] task::Error),

    #[error("task not applicable")]
    EmptyTask,

    // this is probably a bug if this error
    // happens
    #[error("internal error: {0:?}")]
    Internal(#[from] anyhow::Error),
}

/// Returns the longest subset of non-conflicting paths, preferring prefixes over specific paths.
/// When conflicts occur, prioritizes prefixes over more specific paths.
/// For example, if /config appears after /config/some_var, we prefer /config and remove /config/some_var.
fn select_non_conflicting_prefer_prefixes<'a, I>(paths: I) -> Vec<Path>
where
    I: IntoIterator<Item = &'a Path>,
{
    let mut result: Vec<Path> = Vec::new();

    for p in paths.into_iter() {
        // If no existing paths are prefixes of the current path
        if !result.iter().any(|selected| selected.is_prefix_of(p)) {
            // Remove all the paths the current path is a prefix of
            result.retain(|selected| !p.is_prefix_of(selected));

            // And add the new path
            result.push(p.clone());
        }
    }
    result
}

/// Returns true if a new task domain conflicts with existing cumulative changes.
fn domains_are_conflicting<'a, I>(cumulative_domain: &BTreeSet<Path>, domain: I) -> bool
where
    I: Iterator<Item = &'a Path>,
{
    // Check if any path in the domain sets conflict with each other
    for path1 in domain {
        for path2 in cumulative_domain.iter() {
            if path2.is_prefix_of(path1) || path1.is_prefix_of(path2) {
                return true;
            }
        }
    }
    false
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
    domain: BTreeSet<Path>,
    operation: Operation,
    priority: u8,
    is_method: bool,
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by reverse path ordering first, giving shorter paths
        // higher priority
        other
            .path
            .cmp(&self.path)
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

    #[instrument(level="trace", skip_all, fields(id=%task.id(), path=%task.path(), selected=field::Empty, changes=field::Empty), err(level=Level::TRACE))]
    fn try_task(
        &self,
        task: &Task,
        cur_state: &System,
        cur_plan: Workflow,
        domain: &mut BTreeSet<Path>,
        changes: &mut Vec<PatchOperation>,
    ) -> Result<Workflow, SearchFailed> {
        let span = Span::current();
        match task {
            Task::Action(action) => {
                let work_id = WorkUnit::new_id(action, cur_state.root());

                // Simulate the task and get the list of changes
                let patch = action.dry_run(cur_state).map_err(SearchFailed::BadTask)?;
                if patch.is_empty() {
                    return Err(SearchFailed::EmptyTask);
                }

                // The task has been selected
                span.record("selected", display(true));
                span.record("changes", display(&patch));

                let Patch(ops) = patch;
                let Workflow(dag) = cur_plan;

                // Append a new node to the workflow, include a copy
                // of the changes for validation during runtime
                let dag = dag + WorkUnit::new(work_id, action.clone(), ops.clone());

                domain.insert(action.domain());
                changes.extend(ops);

                Ok(Workflow(dag))
            }
            Task::Method(method) => {
                // Get the list of referenced tasks
                let tasks = method.expand(cur_state).map_err(SearchFailed::BadTask)?;

                // Extended tasks will store the correct references from the domain with the
                // right path and description
                let mut extended_tasks = Vec::new();

                for mut t in tasks.into_iter() {
                    let task_id = t.id().to_string();

                    let Context {
                        args: method_args, ..
                    } = method.context();
                    let Context { args, .. } = t.context_mut();

                    // Propagate arguments from the method into the child tasks.
                    // This is just for better user experience as it avoids having to defint
                    // arguments for each sub-task in the methos
                    for (k, v) in method_args.iter() {
                        if !args.contains_key(k) {
                            args.insert(k, v);
                        }
                    }

                    // Find the job path on the domain list, pass the argument for path matching
                    // this will remove any unused arguments in the path
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

                let mut branches = Vec::new();
                let mut cumulative_domain = BTreeSet::new();
                let mut cur_state = cur_state.clone();

                // Iterate over the list of sub-tasks
                for task in extended_tasks {
                    // Run the task as if it was a sequential plan
                    let mut task_domain = BTreeSet::new();
                    let mut task_changes = Vec::new();
                    let Workflow(dag) = self.try_task(
                        &task,
                        &cur_state,
                        Workflow::default(),
                        &mut task_domain,
                        &mut task_changes,
                    )?;

                    // Check if the task domain conflicts with the cumulative domain from branches
                    let dag = if domains_are_conflicting(&cumulative_domain, task_domain.iter()) {
                        // If so, join the existing branches and concatenate the returned workflow
                        let new_dag = Dag::new(branches) + dag;
                        branches = Vec::new();

                        new_dag
                    } else {
                        dag
                    };

                    // Apply the task changes
                    cur_state
                        .patch(Patch(task_changes.to_vec()))
                        .with_context(|| format!("failed to apply patch {task_changes:?}"))?;

                    // Add the new dag to the list of branches and update the cummulative domain
                    branches.push(dag);
                    cumulative_domain.extend(task_domain);

                    // Append the changes to the parent list
                    changes.extend(task_changes);
                }

                // After all tasks are evaluated, join remaining branches
                let cur_plan = Workflow(cur_plan.0 + Dag::new(branches));
                domain.extend(cumulative_domain);

                let patch = Patch(changes.to_vec());
                span.record("selected", display(true));
                span.record("changes", display(patch));

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
                            trace!("found job for operation: {op}");
                            let task = job.new_task(context.clone());
                            let mut changes = Vec::new();
                            let mut domain = BTreeSet::new();

                            // Try applying this task to the current state
                            match self.try_task(
                                &task,
                                &cur_state,
                                Workflow::default(),
                                &mut domain,
                                &mut changes,
                            ) {
                                Ok(Workflow(workflow)) if !changes.is_empty() => {
                                    candidates.push(Candidate {
                                        workflow,
                                        changes,
                                        path: task.path().clone(),
                                        domain,
                                        is_method: task.is_method(),
                                        operation: job.operation().clone(),
                                        priority: job.priority(),
                                    });
                                }

                                // Non-critical errors are ignored (loop, empty, condition failure)
                                Err(SearchFailed::EmptyTask)
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

            // Find the longest list of non-conflicting tasks based on paths (for prioritization)
            let non_conflicting_paths = select_non_conflicting_prefer_prefixes(
                candidates.iter().map(|Candidate { path, .. }| path),
            );

            // Find candidates that can run concurrently using both path and domain-based conflict detection
            let mut concurrent_candidates: BTreeMap<Path, Candidate> = BTreeMap::new();
            let mut cumulative_domain = BTreeSet::new();

            for candidate in candidates.iter() {
                if let Some(prev_candidate) = concurrent_candidates.get(&candidate.path) {
                    if prev_candidate >= candidate {
                        // skip the candidate is there is a previous candidate for the path
                        // higher priority
                        continue;
                    }
                }

                // If the domain of the candidate doesn't conflict with the cumulative domain
                if non_conflicting_paths.iter().any(|p| p == &candidate.path)
                    && !domains_are_conflicting(&cumulative_domain, candidate.domain.iter())
                {
                    cumulative_domain.extend(candidate.domain.clone());
                    concurrent_candidates.insert(candidate.path.clone(), candidate.clone());
                }
            }

            if concurrent_candidates.len() > 1 {
                let mut branches = Vec::new();
                let mut changes = Vec::new();
                let mut domain = BTreeSet::new();
                let mut total_priority = 0;
                // The path for the candidate is the longest common prefix between child paths
                let path = longest_common_prefix(concurrent_candidates.keys());
                for Candidate {
                    workflow,
                    changes: candidate_changes,
                    domain: candidate_domain,
                    priority,
                    ..
                } in concurrent_candidates.into_values()
                {
                    branches.push(workflow);
                    changes.extend(candidate_changes);
                    domain.extend(candidate_domain);
                    // Aggregate each branch priority
                    total_priority += priority;
                }

                // Construct a new candidate using the concurrent branches
                candidates.push(Candidate {
                    workflow: Dag::new(branches),
                    changes,
                    domain,
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

                // Check if the new workflow contains any visited tasks
                if cur_plan.0.any(|unit| workflow.any(|u| u.id == unit.id)) {
                    // skip this candidate because it is creating a loop
                    continue;
                }

                // Extend current plan
                let Workflow(cur_plan) = cur_plan.clone();
                let new_plan = Workflow(cur_plan + workflow);

                // Add the new plan to the search stack
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
    use serde_json::json;
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

    pub fn find_plan<I, O>(planner: Planner, cur: O, tgt: I) -> Result<Workflow, super::Error>
    where
        O: Serialize,
        I: Serialize + DeserializeOwned,
    {
        let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

        let system =
            crate::system::System::try_from(cur).expect("failed to serialize current state");

        let res = planner.find_workflow::<I>(&system, &tgt)?;
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
        init();
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

        // We expect counters to be updated concurrently
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

        // We expect a concurrent dag with two tasks on each branch
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
    fn it_avoids_conflicts_from_methods() {
        init();
        let initial = HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]);
        let target = HashMap::from([("one".to_string(), 1), ("two".to_string(), 1)]);

        fn plus_other(Target(tgt): Target<i32>) -> Vec<Task> {
            vec![
                plus_one.with_arg("counter", "one").with_target(tgt),
                plus_one.with_arg("counter", "two").with_target(tgt),
            ]
        }

        let domain = Domain::new()
            .job("/{counter}", none(plus_one))
            .job("/{counter}", update(plus_other));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect a parallel dag for this specific target
        let expected: Dag<&str> = par!(
            "mahler::planner::tests::plus_one(/one)",
            "mahler::planner::tests::plus_one(/two)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    // This test forces the planner to backtrack and try multiple plans.
    // Because of an issue with the implementation in DAG, this would terminate
    // with a wrong plan with duplicate tasks
    //
    // ```
    // - store device configuration
    // - ensure cleanup
    // + ~ - update device name
    //   - initialize app with uuid 'my-app-uuid'
    // ~ - initialize app with uuid 'my-app-uuid'
    //   - update device name
    // ```
    #[test]
    fn it_finds_a_correct_path_through_backtracking() {
        init();

        #[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
        struct App {
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<String>,
        }

        type Config = HashMap<String, String>;

        #[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
        struct Device {
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<String>,
            #[serde(default)]
            apps: HashMap<String, App>,
            #[serde(default)]
            config: Config,
            #[serde(default)]
            needs_cleanup: bool,
        }

        /// Store configuration in memory
        fn store_config(
            mut config: View<Config>,
            Target(tgt_config): Target<Config>,
        ) -> View<Config> {
            // If a new config received, just update the in-memory state, the config will be handled
            // by the legacy supervisor
            *config = tgt_config;
            config
        }

        fn set_device_name(
            mut name: View<Option<String>>,
            Target(tgt): Target<Option<String>>,
        ) -> View<Option<String>> {
            *name = tgt;
            name
        }

        fn ensure_cleanup(mut device: View<Device>) -> View<Device> {
            device.needs_cleanup = true;
            device
        }

        fn complete_cleanup(mut device: View<Device>) -> View<Device> {
            device.needs_cleanup = false;
            device
        }

        fn dummy_task() {}

        fn do_cleanup(
            System(device): System<Device>,
            Target(tgt_device): Target<Device>,
        ) -> Vec<Task> {
            let into_tgt = Device {
                needs_cleanup: false,
                ..device.clone()
            };
            if into_tgt != tgt_device || !device.needs_cleanup {
                return vec![];
            }

            vec![dummy_task.into_task(), complete_cleanup.into_task()]
        }

        fn prepare_app(
            mut app: View<Option<App>>,
            Target(tgt_app): Target<App>,
        ) -> View<Option<App>> {
            app.replace(tgt_app);
            app
        }

        let domain = Domain::new()
            .job(
                "/name",
                any(set_device_name).with_description(|| "set device name"),
            )
            .job(
                "/config",
                task::update(store_config).with_description(|| "store configuration"),
            )
            .jobs(
                "",
                [
                    update(ensure_cleanup).with_description(|| "ensure cleanup"),
                    update(do_cleanup),
                    none(complete_cleanup).with_description(|| "complete cleanup"),
                ],
            )
            .job("", none(dummy_task).with_description(|| "dummy task"))
            .job(
                "/apps/{app_uuid}",
                create(prepare_app).with_description(|Args(app_uuid): Args<String>| {
                    format!("prepare app {app_uuid}")
                }),
            );

        let initial = serde_json::from_value::<Device>(json!({})).unwrap();
        let target = serde_json::from_value::<Device>(json!({
            "name": "my-device",
            "apps": {
                "my-app": {"name": "my-app-name"}
            },
            "config": {
                "some-var": "some-value"
            }
        }))
        .unwrap();

        let planner = Planner::new(domain);
        // We expect a parallel dag for this specific target
        let expected: Dag<&str> = seq!(
            "store configuration",
            "set device name",
            "prepare app my-app",
        );
        let workflow = find_plan(planner, initial, target).unwrap();

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_avoids_conflict_in_tasks_returned_from_methods() {
        init();

        #[derive(Serialize, Deserialize)]
        struct Service {
            image: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Image {}

        #[derive(Serialize, Deserialize)]
        struct State {
            services: HashMap<String, Service>,
            images: HashMap<String, Image>,
        }

        #[derive(Serialize, Deserialize)]
        struct TargetState {
            services: HashMap<String, Service>,
        }

        fn create_image(mut view: View<Option<Image>>) -> View<Option<Image>> {
            *view = Some(Image {});
            view
        }

        fn create_service_image(
            Target(tgt): Target<Service>,
            System(state): System<State>,
        ) -> Option<Task> {
            if !state.images.contains_key(&tgt.image) {
                return Some(create_image.with_arg("image_name", tgt.image));
            }
            None
        }

        fn create_service(
            mut view: View<Option<Service>>,
            Target(tgt): Target<Service>,
            System(state): System<State>,
        ) -> View<Option<Service>> {
            if state.images.contains_key(&tgt.image) {
                *view = Some(tgt);
            }
            view
        }

        let domain = Domain::new()
            .job(
                "/images/{image_name}",
                none(create_image).with_description(|Args(image_name): Args<String>| {
                    format!("create image '{image_name}'")
                }),
            )
            .jobs(
                "/services/{service_name}",
                [
                    create(create_service).with_description(|Args(service_name): Args<String>| {
                        format!("create service '{service_name}'")
                    }),
                    create(create_service_image),
                ],
            );

        let initial =
            serde_json::from_value::<State>(json!({ "images": {}, "services": {} })).unwrap();
        let target = serde_json::from_value::<TargetState>(
            json!({ "services": {"one":{"image": "ubuntu"}, "two": {"image": "ubuntu"}} }),
        )
        .unwrap();

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        let expected: Dag<&str> = seq!(
            "create image 'ubuntu'",
            "create service 'one'",
            "create service 'two'",
        );
        assert_eq!(expected.to_string(), workflow.to_string());
    }

    #[test]
    fn it_calculates_concurrent_workflows_from_non_conflicting_paths() {
        init();
        type Config = HashMap<String, String>;

        #[derive(Serialize, Deserialize)]
        struct MyState {
            config: Config,
            counters: HashMap<String, i32>,
        }

        fn new_counter(
            mut counter: View<Option<i32>>,
            Target(tgt): Target<i32>,
        ) -> View<Option<i32>> {
            counter.replace(tgt);
            counter
        }

        fn update_config(mut config: View<Config>, Target(tgt): Target<Config>) -> View<Config> {
            *config = tgt;
            config
        }

        fn new_config(
            mut config: View<Option<String>>,
            Target(tgt): Target<String>,
        ) -> View<Option<String>> {
            config.replace(tgt);
            config
        }

        let domain = Domain::new()
            .job(
                "/counters/{counter}",
                create(new_counter).with_description(|Args(counter): Args<String>| {
                    format!("create counter '{counter}'")
                }),
            )
            .job(
                "/config/{config}",
                create(new_config).with_description(|Args(config): Args<String>| {
                    format!("create config '{config}'")
                }),
            )
            .job(
                "/config",
                update(update_config).with_description(|| "update configurations"),
            );

        let initial =
            serde_json::from_value::<MyState>(json!({ "config": {}, "counters": {} })).unwrap();
        let target = serde_json::from_value::<MyState>(
            json!({ "config": {"some_var":"one", "other_var": "two"}, "counters": {"one": 0} }),
        )
        .unwrap();

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        let expected: Dag<&str> = par!("update configurations", "create counter 'one'");
        assert_eq!(expected.to_string(), workflow.to_string());
    }

    #[test]
    fn it_finds_concurrent_plans_with_nested_forks() {
        init();
        type Counters = BTreeMap<String, i32>;

        #[derive(Serialize, Deserialize, Debug)]
        struct MyState {
            counters: Counters,
        }

        // This is a very dumb example to test how the planner
        // choses concurrent methods over automated concurrency
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
            let mut tasks = Vec::new();
            for k in counters
                .keys()
                .filter(|k| {
                    target.get(k.as_str()).unwrap_or(&0) - counters.get(k.as_str()).unwrap_or(&0)
                        > 1
                })
                .take(2)
            // take at most 2 changes and create a multi_increment_step
            {
                let mut tgt = (*counters).clone();
                if target.contains_key(k.as_str()) {
                    tgt.insert(k.to_string(), *target.get(k.as_str()).unwrap_or(&0));
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

        // We expect a concurrent dag with two tasks on each branch
        let expected: Dag<&str> = dag!(seq!("a++", "a++"), seq!("b++", "b++"))
            + dag!(seq!("c++", "c++"), seq!("d++", "d++"))
            + seq!("a++");

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn test_array_element_conflicts() {
        init();

        #[derive(Serialize, Deserialize)]
        struct State {
            items: Vec<String>,
            configs: HashMap<String, String>,
        }

        fn update_item(mut item: View<String>, Target(tgt): Target<String>) -> View<String> {
            *item = tgt;
            item
        }

        fn update_config(mut config: View<String>, Target(tgt): Target<String>) -> View<String> {
            *config = tgt;
            config
        }

        fn create_item(
            mut item: View<Option<String>>,
            Target(tgt): Target<String>,
        ) -> View<Option<String>> {
            *item = Some(tgt);
            item
        }

        fn create_config(
            mut config: View<Option<String>>,
            Target(tgt): Target<String>,
        ) -> View<Option<String>> {
            *config = Some(tgt);
            config
        }

        fn non_conflicting_updates(Target(tgt): Target<State>) -> Vec<Task> {
            vec![
                update_item
                    .with_arg("index", "0")
                    .with_target(tgt.items[0].clone()),
                update_item
                    .with_arg("index", "1")
                    .with_target(tgt.items[1].clone()),
                update_config
                    .with_arg("key", "server")
                    .with_target(tgt.configs.get("server").unwrap().clone()),
                update_config
                    .with_arg("key", "database")
                    .with_target(tgt.configs.get("database").unwrap().clone()),
            ]
        }

        let domain = Domain::new()
            .job("/items/{index}", update(update_item))
            .job("/configs/{key}", update(update_config))
            .job("/items/{index}", create(create_item))
            .job("/configs/{key}", create(create_config))
            .job("/", update(non_conflicting_updates));

        let initial = State {
            items: vec!["old1".to_string(), "old2".to_string()],
            configs: HashMap::from([
                ("server".to_string(), "oldserver".to_string()),
                ("database".to_string(), "olddatabase".to_string()),
            ]),
        };

        let target = State {
            items: vec!["new1".to_string(), "new2".to_string()],
            configs: HashMap::from([
                ("server".to_string(), "newserver".to_string()),
                ("database".to_string(), "newdatabase".to_string()),
            ]),
        };

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        // Should run concurrently because different array elements and map keys don't conflict
        let expected: Dag<&str> = par!("mahler::planner::tests::test_array_element_conflicts::update_config(/configs/database)",
                "mahler::planner::tests::test_array_element_conflicts::update_config(/configs/server)",
                "mahler::planner::tests::test_array_element_conflicts::update_item(/items/0)",
                "mahler::planner::tests::test_array_element_conflicts::update_item(/items/1)",
            );

        assert_eq!(workflow.to_string(), expected.to_string());
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
                write!(f, "{self:?}")
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

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_basic() {
        let paths = vec![Path::from_static("/a"), Path::from_static("/b")];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        assert_eq!(result, paths);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_with_conflicts() {
        let paths = vec![
            Path::from_static("/config/other_var"),
            Path::from_static("/config/some_var"),
            Path::from_static("/counters/one"),
            Path::from_static("/config"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![
            Path::from_static("/counters/one"),
            Path::from_static("/config"),
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_your_example() {
        let paths = vec![
            Path::from_static("/a"),
            Path::from_static("/b"),
            Path::from_static("/b/c"),
            Path::from_static("/b/d"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("/a"), Path::from_static("/b")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_no_later_prefix() {
        let paths = vec![
            Path::from_static("/config/server/host"),
            Path::from_static("/config/server/port"),
            Path::from_static("/database/host"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        assert_eq!(result, paths);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_prefix_first() {
        // Counter example: when prefix comes first, it should be kept
        // and more specific paths should be ignored
        let paths = vec![
            Path::from_static("/config"),
            Path::from_static("/config/server"),
            Path::from_static("/config/client"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("/config")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_prefer_prefixes_root_path() {
        // Edge case: root path should dominate all other paths
        let paths = vec![
            Path::from_static(""),
            Path::from_static("/config"),
            Path::from_static("/counters"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        let expected = vec![Path::from_static("")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_select_non_conflicting_proper_path_prefix_vs_string_prefix() {
        // This test demonstrates the fix: /a should NOT conflict with /aa
        // because /a is not a path prefix of /aa (only a string prefix)
        let paths = vec![
            Path::from_static("/a"),
            Path::from_static("/aa"),
            Path::from_static("/a/b"),
        ];
        let result = select_non_conflicting_prefer_prefixes(&paths);
        // /a should conflict with /a/b but NOT with /aa
        let expected = vec![Path::from_static("/a"), Path::from_static("/aa")];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_longest_common_prefix_empty() {
        let paths: Vec<Path> = vec![];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "");
    }

    #[test]
    fn test_longest_common_prefix_single_path() {
        let paths = vec![Path::from_static("/config/server")];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/config/server");
    }

    #[test]
    fn test_longest_common_prefix_common_prefix() {
        let paths = vec![
            Path::from_static("/config/server/host"),
            Path::from_static("/config/server/port"),
            Path::from_static("/config/server/ssl"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/config/server");
    }

    #[test]
    fn test_longest_common_prefix_no_common_prefix() {
        let paths = vec![
            Path::from_static("/config"),
            Path::from_static("/counters"),
            Path::from_static("/settings"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "");
    }

    #[test]
    fn test_longest_common_prefix_root_paths() {
        let paths = vec![
            Path::from_static("/a/b"),
            Path::from_static("/a/c"),
            Path::from_static("/a/d"),
        ];
        let result = longest_common_prefix(&paths);
        assert_eq!(result.as_str(), "/a");
    }
}
