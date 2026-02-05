use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use jsonptr::PointerBuf;
use tracing::{field, instrument, trace, trace_span, warn, Span};

use crate::dag::Dag;
use crate::error::{Error, ErrorKind};
use crate::json::{OperationMatcher, Patch, PatchOperation, Path, Value};
use crate::result::Result;
use crate::runtime::{Context, System};
use crate::state::State;
use crate::system_ext::SystemExt;
use crate::task::Task;

use super::domain::Domain;
use super::workflow::{WorkUnit, Workflow};

mod distance;
use distance::Distance;

/// Returns the longest subset of non-conflicting paths, preferring prefixes over specific paths.
///
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

/// Computes the longest common prefix over a list of `Path` objects
///
/// # Example
///
/// ```rust,ignore
/// let paths = vec![
///     Path::from_static("/config/server/host"),
///     Path::from_static("/config/server/port"),
///     Path::from_static("/config/server/ssl"),
/// ];
/// let result = longest_common_prefix(&paths);
/// assert_eq!(result.as_str(), "/config/server");
/// ```
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

/// Get a hash for the system state
fn hash_state(state: &System) -> u64 {
    let value = state.inner_state();

    // Create a DefaultHasher
    let mut hasher = DefaultHasher::new();

    // Hash the data
    value.hash(&mut hasher);

    // Retrieve the hash value
    hasher.finish()
}

#[derive(Clone, PartialEq, Eq)]
struct Candidate {
    partial_plan: Dag<WorkUnit>,
    changes: Vec<PatchOperation>,
    path: Path,
    domain: BTreeSet<Path>,
    operation: OperationMatcher,
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
    }
}

/// Convert the task into a workflow if possible
///
/// For primitive tasks, the workflow will just contain a single node, however
/// for composite tasks, the workflow will create a Dag with concurrent branches
/// depending on the operational domain of each sub-task.
///
/// # Arguments
/// * `db` - a reference to the worker [`Domain`]
/// * `task` - the task to convert to a workflow
/// * `cur_state` - the state of the system before running the task
pub fn find_workflow_for_task(mut task: Task, db: &Domain, cur_state: &System) -> Result<Workflow> {
    // Look-up the task on the domain to find its path
    let task_id = task.id();
    let Context { args, .. } = task.context_mut();
    let path = db.find_path(task_id, args)?;

    // Set the correct path for the task
    let task = task.with_path(path);

    let mut task_domain = BTreeSet::new();
    let mut task_changes = Vec::new();
    let dag = try_task_into_workflow(&task, db, cur_state, &mut task_domain, &mut task_changes)?;

    Ok(Workflow::new(dag.reverse()))
}

/// Convert the task into a workflow if possible
///
/// For primitive tasks, the workflow will just contain a single node, however
/// for composite tasks, the workflow will create a Dag with concurrent branches
/// depending on the operational domain of each sub-task.
///
/// # Arguments
/// * `task` - the task to convert to a workflow
/// * `db` - a reference to the worker [`Domain`]
/// * `cur_state` - the state of the system before running the task
/// * `domain` - the task operational domain, used for conflict detection
/// * `change` - the cumulative changes performed by the task and its sub-tasks (if any)
fn try_task_into_workflow(
    task: &Task,
    db: &Domain,
    cur_state: &System,
    domain: &mut BTreeSet<Path>,
    changes: &mut Vec<PatchOperation>,
) -> Result<Dag<WorkUnit>> {
    match task {
        Task::Action(action) => {
            let work_id = WorkUnit::new_id(action, cur_state.inner_state());

            // Simulate the task and get the list of changes
            let patch = action.dry_run(cur_state)?;
            if patch.is_empty() {
                // An empty result from the task is equivalent to
                // the task condition not being met
                return Err(ErrorKind::ConditionNotMet)?;
            }

            let Patch(ops) = patch;

            // Prepend a new node to the workflow, include a copy
            // of the changes for validation during runtime
            let new_plan = Dag::from(WorkUnit::new(work_id, action.clone(), ops.clone()));

            domain.insert(action.domain());
            changes.extend(ops);

            Ok(new_plan)
        }
        Task::Method(method) => {
            // Get the list of referenced tasks
            let tasks = method.expand(cur_state)?;

            // Extended tasks will store the correct references from the domain with the
            // right path and description
            let mut extended_tasks = Vec::new();

            for mut t in tasks.into_iter() {
                let task_id = t.id();

                let Context {
                    args: method_args, ..
                } = method.context();
                let Context { args, .. } = t.context_mut();

                // Propagate arguments from the method into the child tasks.
                // This is just for better user experience as it avoids having to define
                // arguments for each sub-task in the method
                for (k, v) in method_args.iter() {
                    if !args.contains_key(k) {
                        args.insert(k, v);
                    }
                }

                // Find the job path on the domain list, pass the argument for path matching
                // this will remove any unused arguments in the path
                let path = db.find_path(task_id, args)?;

                // Using the path, now find the actual job on the domain.
                // The domain job includes additional configuration like the description that
                // we want to use in the workflow
                let job = db
                    .find_job(&path, task_id)
                    // this should never happen since the path was returned by the domain
                    .ok_or(Error::internal(format!("should find a job for {path}")))?;

                // Get a copy of the task for the final list
                let task = job.new_task(t.context().to_owned()).with_path(path.clone());

                extended_tasks.push(task);
            }

            let mut plan_branches = Vec::new();
            let mut cumulative_domain = BTreeSet::new();
            let mut cur_state = cur_state.clone();

            // Iterate over the list of sub-tasks
            for task in extended_tasks {
                // Run the task as if it was a sequential plan
                let mut task_domain = BTreeSet::new();
                let mut task_changes = Vec::new();
                let partial_plan = try_task_into_workflow(
                    &task,
                    db,
                    &cur_state,
                    &mut task_domain,
                    &mut task_changes,
                )?;

                // Check if the task domain conflicts with the cumulative domain from branches
                let partial_plan =
                    if domains_are_conflicting(&cumulative_domain, task_domain.iter()) {
                        // If so, join the existing branches and concatenate the returned workflow
                        // we reverse the branches to preserve the expected order of tasks in the plan
                        let dag = Dag::new(plan_branches).prepend(partial_plan);
                        plan_branches = Vec::new();

                        dag
                    } else {
                        partial_plan
                    };

                // Apply the task changes
                cur_state.patch(&Patch(task_changes.to_vec()))?;

                // Add the new dag to the list of branches and update the cummulative domain
                plan_branches.push(partial_plan);
                cumulative_domain.extend(task_domain);

                // Append the changes to the parent list
                changes.extend(task_changes);
            }

            // After all tasks are evaluated, join remaining branches
            let new_plan = Dag::new(plan_branches);
            domain.extend(cumulative_domain);

            // Include changes in the returned plan
            Ok(new_plan)
        }
    }
}

/// Find a workflow that takes the system from its current state
/// to the target state using the tasks in the provided Domain
#[instrument(level = "trace", skip_all, err(level = "trace"))]
pub fn find_workflow_to_target<T>(
    db: &Domain,
    system: &System,
    tgt: &Value,
) -> Result<Option<Workflow>>
where
    T: State,
{
    trace!(initial=%system, target=%tgt, "searching for workflow");

    // The search stack stores (current_state, current_plan, depth)
    let mut stack = vec![(system.clone(), Dag::default(), 0)];

    let find_workflow_span = Span::current();
    let mut skipped_state_paths: Vec<Path> = Vec::new();

    // Keep track of visited states
    let mut visited_states = HashSet::new();

    while let Some((cur_state, cur_plan, depth)) = stack.pop() {
        // Prevent infinite recursion (e.g., from buggy tasks or recursive methods)
        if depth >= 256 {
            warn!(parent: &find_workflow_span, "reached max search depth (256)");
            return Err(ErrorKind::PlanningOverflow.into());
        }

        // Normalize state: deserialize into the target type and re-serialize to remove internal fields
        let cur = cur_state
            .state::<T::Target>()
            .and_then(|s| serde_json::to_value(s).map_err(Error::from))?;

        // add the current state to the visited list
        visited_states.insert(hash_state(&cur_state));

        // Compute the difference between current and target state
        let distance = Distance::new(&cur, tgt, &skipped_state_paths);

        // If there are no more operations, we’ve reached the goal
        if distance.is_empty() {
            // we need to reverse the plan before returning
            let mut workflow = Workflow::new(cur_plan.reverse());
            workflow.ignored = distance.ignored;
            return Ok(Some(workflow));
        }

        let next_span = trace_span!("find_next", distance = %distance, cur_plan=field::Empty);
        next_span.in_scope(|| {
            // make a copy of the plan for the logs if in the tracing scope
            next_span.record("cur_plan", field::display(cur_plan.clone().reverse()));
        });
        let _enter = next_span.enter();

        // List of candidate plans at this level in the stack
        let mut candidates: Vec<Candidate> = Vec::new();

        // Iterate over distance operations and jobs to find possible candidates
        for op in distance.operations {
            let path = op.path();
            let pointer = path.as_ref();

            // skip the path if any parent path has been halted
            if skipped_state_paths.iter().any(|p| p.is_prefix_of(path)) {
                continue;
            }

            // resolve the operation pointer on the target state
            let target = pointer.resolve(tgt).unwrap_or(&Value::Null);

            // Look for matching exceptions for the path and operation
            if let Some((args, exceptions)) = db.find_matching_exceptions(path.as_str()) {
                let context = Context {
                    path: path.clone(),
                    args,
                    target: target.clone(),
                };

                // if there are any exceptions that apply to the context, add the path to
                // the list and skip the path
                if exceptions.filter(|j| j.operation().matches(&op)).any(|exception| {
                    exception.test(&cur_state, &context)
                        .inspect_err(
                            |e| warn!(parent: &find_workflow_span, "cannot evaluate {exception}: {e}"),
                        )
                        .unwrap_or(false)
                }) {
                    skipped_state_paths.push(path.clone());
                    continue;
                }
            }

            // Retrieve matching jobs at this path
            if let Some((args, jobs)) = db.find_matching_jobs(path.as_str()) {
                let context = Context {
                    path: path.clone(),
                    args,
                    target: target.clone(),
                };

                // Look for jobs matching the operation
                for job in jobs.filter(|j| j.operation().matches(&op)) {
                    let task = job.new_task(context.clone());
                    let mut changes = Vec::new();
                    let mut domain = BTreeSet::new();

                    // Try applying this task to the current state
                    match try_task_into_workflow(&task, db, &cur_state, &mut domain, &mut changes) {
                        Ok(partial_plan) if !changes.is_empty() => {
                            candidates.push(Candidate {
                                partial_plan,
                                changes,
                                path: task.path().clone(),
                                domain,
                                is_method: task.is_method(),
                                operation: job.operation().clone(),
                            });
                        }

                        Err(e) => match e.kind() {
                            // Skip the task if condition is not met
                            ErrorKind::ConditionNotMet => {}
                            // Critical internal errors terminate the search
                            ErrorKind::Internal => return Err(e),
                            // Other job errors are ignored unless debugging
                            _ => return Err(e),
                        },

                        // ignore if changes is empty
                        _ => {}
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
                if *prev_candidate >= *candidate {
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
            let mut plan_branches = Vec::new();
            let mut changes = Vec::new();
            let mut domain = BTreeSet::new();
            let mut is_method = true;
            // The path for the candidate is the longest common prefix between child paths
            let path = longest_common_prefix(concurrent_candidates.keys());
            for candidate in concurrent_candidates.into_values() {
                // Do not use the candidate individually if already selected as part
                // of a concurrent candidate
                candidates.retain(|c| *c != candidate);

                let Candidate {
                    partial_plan,
                    changes: candidate_changes,
                    domain: candidate_domain,
                    is_method: candidate_is_method,
                    ..
                } = candidate;
                plan_branches.push(partial_plan);
                changes.extend(candidate_changes);
                domain.extend(candidate_domain);
                // Treat the candidate as a method when sorting if all children
                // are methods
                is_method = is_method && candidate_is_method;
            }

            // Construct a new candidate using the concurrent branches
            candidates.push(Candidate {
                partial_plan: Dag::new(plan_branches),
                changes,
                domain,
                path,
                is_method,
                operation: OperationMatcher::Update,
            })
        }

        // sort candidates
        candidates.sort();

        // Record candidates found for this planning step
        trace!(candidates=%candidates.len());

        // Insert the best candidate that doesn't introduce cycles into the stack
        for Candidate {
            partial_plan,
            changes,
            ..
        } in candidates.into_iter().rev()
        {
            let mut new_state = cur_state.clone();
            new_state.patch(&Patch(changes))?;

            // Ignore the candidate if it takes us to a state the planner has visited before,
            // this avoids the planner just trying tasks in a different order or potentially
            // looping forever jumping between a few visited states
            let state_hash = hash_state(&new_state);
            if visited_states.contains(&state_hash) {
                continue;
            }

            // Check if the new workflow contains any visited tasks
            if cur_plan.any(|unit| partial_plan.any(|u| u.id == unit.id)) {
                // skip this candidate because it is creating a loop
                continue;
            }

            // Extend current plan
            let new_plan = cur_plan.shallow_clone().prepend(partial_plan);

            // Add the new plan to the search stack
            stack.push((new_state, new_plan, depth + 1));

            // Only add the most qualified candidate (greedy search)
            break;
        }

        if stack.is_empty() {
            trace!(last_evaluated_state=%cur_state, "no plan was found");
        }
    }

    // No candidate plan reached the goal state
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;
    use std::fmt::Display;

    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};

    use crate::dag::{dag, par, seq, Dag};
    use crate::extract::{Args, System, Target, View};
    use crate::job::*;
    use crate::json::Operation;
    use crate::state::{Map, State};
    use crate::task::prelude::*;

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

    pub fn find_plan<T>(db: &Domain, cur: T, tgt: T::Target) -> Result<Option<Workflow>>
    where
        T: State,
    {
        let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

        let system =
            crate::runtime::System::try_from(cur).expect("failed to serialize current state");

        let res = find_workflow_to_target::<T>(db, &system, &tgt)?;
        Ok(res)
    }

    #[test]
    fn it_calculates_a_linear_workflow() {
        let domain = Domain::new()
            .job("", update(plus_one))
            .job("", update(minus_one));

        let workflow = find_plan(&domain, 0, 2).unwrap().unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::worker::planner::tests::plus_one()",
            "mahler::worker::planner::tests::plus_one()"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_ignores_none_jobs() {
        let domain = Domain::new().job("", none(plus_one));

        let workflow = find_plan(&domain, 0, 2);

        assert!(matches!(workflow, Ok(None)));
    }

    #[test]
    fn it_aborts_search_if_plan_length_grows_too_much() {
        let domain = Domain::new()
            .job("", update(buggy_plus_one))
            .job("", update(minus_one));

        let workflow = find_plan(&domain, 0, 2);
        assert!(workflow.is_err());
    }

    #[test]
    fn it_calculates_a_linear_workflow_with_compound_tasks() {
        init();
        let domain = Domain::new()
            .job("", update(plus_two))
            .job("", none(plus_one));

        let workflow = find_plan(&domain, 0, 2).unwrap().unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::worker::planner::tests::plus_one()",
            "mahler::worker::planner::tests::plus_one()"
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state() {
        #[derive(Serialize, Deserialize)]
        struct MyState {
            counters: HashMap<String, i32>,
        }

        impl State for MyState {
            type Target = Self;
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

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // We expect counters to be updated concurrently
        let expected: Dag<&str> = par!(
            "mahler::worker::planner::tests::plus_one(/counters/one)",
            "mahler::worker::planner::tests::plus_one(/counters/two)",
        ) + par!(
            "mahler::worker::planner::tests::plus_one(/counters/one)",
            "mahler::worker::planner::tests::plus_one(/counters/two)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_calculates_a_linear_workflow_on_a_complex_state_with_compound_tasks() {
        #[derive(Serialize, Deserialize)]
        struct MyState {
            counters: HashMap<String, i32>,
        }

        impl State for MyState {
            type Target = Self;
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

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // We expect a concurrent dag with two tasks on each branch
        let expected: Dag<&str> = dag!(
            seq!(
                "mahler::worker::planner::tests::plus_one(/counters/one)",
                "mahler::worker::planner::tests::plus_one(/counters/one)",
            ),
            seq!(
                "mahler::worker::planner::tests::plus_one(/counters/two)",
                "mahler::worker::planner::tests::plus_one(/counters/two)",
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

        impl State for MyState {
            type Target = Self;
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

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "mahler::worker::planner::tests::plus_one(/counters/one)",
            "mahler::worker::planner::tests::plus_one(/counters/one)",
            "mahler::worker::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    #[test]
    fn it_avoids_conflicts_from_methods() {
        init();
        let initial = Map::from([("one".to_string(), 0), ("two".to_string(), 0)]);
        let target = Map::from([("one".to_string(), 1), ("two".to_string(), 1)]);

        fn plus_other(Target(tgt): Target<i32>) -> Vec<Task> {
            vec![
                plus_one.with_arg("counter", "one").with_target(tgt),
                plus_one.with_arg("counter", "two").with_target(tgt),
            ]
        }

        let domain = Domain::new()
            .job("/{counter}", none(plus_one))
            .job("/{counter}", update(plus_other));

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // We expect a parallel dag for this specific target
        let expected: Dag<&str> = par!(
            "mahler::worker::planner::tests::plus_one(/one)",
            "mahler::worker::planner::tests::plus_one(/two)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    // This test will fail to find a plan due to a bug in the task definitions,
    // with backtracking, the planner might find a correct candidated but the planner avoids
    // backtracking to prevent combinatorial explosion.
    // ```
    #[test]
    fn it_fails_to_find_a_plan_for_a_buggy_task() {
        init();

        #[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
        struct App {
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<String>,
        }

        impl State for App {
            type Target = Self;
        }

        type Config = Map<String, String>;

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

        impl State for Device {
            type Target = Self;
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

            // dummy task is always empty so do_cleanup will never be picked
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
                update(store_config).with_description(|| "store configuration"),
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

        let workflow = find_plan(&domain, initial, target).unwrap();
        assert!(
            workflow.is_none(),
            "unexpected plan:\n{}",
            workflow.unwrap()
        );
    }

    #[test]
    fn it_avoids_conflict_in_tasks_returned_from_methods() {
        init();

        #[derive(Serialize, Deserialize)]
        struct Service {
            image: String,
        }

        impl State for Service {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Image {}

        #[derive(Serialize, Deserialize)]
        struct MySys {
            services: Map<String, Service>,
            images: Map<String, Image>,
        }

        impl State for MySys {
            type Target = MySysTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct MySysTarget {
            services: Map<String, Service>,
        }

        fn create_image(mut view: View<Option<Image>>) -> View<Option<Image>> {
            *view = Some(Image {});
            view
        }

        fn create_service_image(
            Target(tgt): Target<Service>,
            System(state): System<MySys>,
        ) -> Option<Task> {
            if !state.images.contains_key(&tgt.image) {
                return Some(create_image.with_arg("image_name", tgt.image));
            }
            None
        }

        fn create_service(
            mut view: View<Option<Service>>,
            Target(tgt): Target<Service>,
            System(state): System<MySys>,
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
            serde_json::from_value::<MySys>(json!({ "images": {}, "services": {} })).unwrap();
        let target = serde_json::from_value::<MySysTarget>(
            json!({ "services": {"one":{"image": "ubuntu"}, "two": {"image": "ubuntu"}} }),
        )
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

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
        type Config = Map<String, String>;

        #[derive(Serialize, Deserialize)]
        struct MyState {
            config: Config,
            counters: Map<String, i32>,
        }

        impl State for MyState {
            type Target = Self;
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

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        let expected: Dag<&str> = par!("update configurations", "create counter 'one'");
        assert_eq!(expected.to_string(), workflow.to_string());
    }

    #[test]
    fn it_finds_concurrent_plans_with_nested_forks() {
        init();
        type Counters = Map<String, i32>;

        #[derive(Serialize, Deserialize, Debug)]
        struct MyState {
            counters: Counters,
        }

        impl State for MyState {
            type Target = Self;
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
            counters: Map::from([
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("c".to_string(), 0),
                ("d".to_string(), 0),
            ]),
        };

        let target = MyState {
            counters: Map::from([
                ("a".to_string(), 3),
                ("b".to_string(), 2),
                ("c".to_string(), 2),
                ("d".to_string(), 2),
            ]),
        };

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

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
        struct MySys {
            items: Vec<String>,
            configs: HashMap<String, String>,
        }

        impl State for MySys {
            type Target = Self;
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

        fn non_conflicting_updates(Target(tgt): Target<MySys>) -> Vec<Task> {
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

        let initial = MySys {
            items: vec!["old1".to_string(), "old2".to_string()],
            configs: HashMap::from([
                ("server".to_string(), "oldserver".to_string()),
                ("database".to_string(), "olddatabase".to_string()),
            ]),
        };

        let target = MySys {
            items: vec!["new1".to_string(), "new2".to_string()],
            configs: HashMap::from([
                ("server".to_string(), "newserver".to_string()),
                ("database".to_string(), "newdatabase".to_string()),
            ]),
        };

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Should run concurrently because different array elements and map keys don't conflict
        let expected: Dag<&str> = par!("mahler::worker::planner::tests::test_array_element_conflicts::update_config(/configs/database)",
                "mahler::worker::planner::tests::test_array_element_conflicts::update_config(/configs/server)",
                "mahler::worker::planner::tests::test_array_element_conflicts::update_item(/items/0)",
                "mahler::worker::planner::tests::test_array_element_conflicts::update_item(/items/1)",
            );

        assert_eq!(workflow.to_string(), expected.to_string());
    }

    #[test]
    fn test_stacking_problem() {
        init();

        #[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
        enum Block {
            A,
            B,
            C,
        }

        impl State for Block {
            type Target = Self;
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

        impl State for Location {
            type Target = Self;
        }

        impl Location {
            fn is_block(&self) -> bool {
                matches!(self, Location::Blk(_))
            }
        }

        type Blocks = Map<Block, Location>;

        #[derive(Serialize, Deserialize, Debug)]
        struct World {
            blocks: Blocks,
        }

        impl State for World {
            type Target = Self;
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
            System(sys): System<World>,
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
            System(sys): System<World>,
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
            System(sys): System<World>,
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
            System(sys): System<World>,
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

        let initial = World {
            blocks: Map::from([
                (Block::A, Location::Table),
                (Block::B, Location::Blk(Block::A)),
                (Block::C, Location::Blk(Block::B)),
            ]),
        };
        let target = World {
            blocks: Map::from([
                (Block::A, Location::Blk(Block::B)),
                (Block::B, Location::Blk(Block::C)),
                (Block::C, Location::Table),
            ]),
        };

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();
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

    #[test]
    fn test_exception_skips_path_with_simple_condition() {
        init();

        #[derive(Serialize, Deserialize)]
        struct Service {
            running: bool,
            #[serde(default)]
            failed: bool,
        }

        impl State for Service {
            type Target = ServiceTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct ServiceTarget {
            running: bool,
        }

        #[derive(Serialize, Deserialize)]
        struct Device {
            services: Map<String, Service>,
        }

        impl State for Device {
            type Target = DeviceTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct DeviceTarget {
            services: Map<String, ServiceTarget>,
        }

        fn start_service(mut service: View<Service>) -> View<Service> {
            service.running = true;
            service
        }

        // Exception: skip failed services
        fn skip_failed(view: View<Service>) -> bool {
            view.failed
        }

        let domain = Domain::new()
            .job(
                "/services/{service}",
                update(start_service).with_description(|Args(service): Args<String>| {
                    format!("start service {service}")
                }),
            )
            .exception("/services/{service}", crate::exception::update(skip_failed));

        let initial = serde_json::from_value::<Device>(json!({
            "services": {
                "one": { "running": false, "failed": false },
                "two": { "running": false, "failed": true },
                "three": { "running": false, "failed": false }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<DeviceTarget>(json!({
            "services": {
                "one": { "running": true },
                "two": { "running": true },
                "three": { "running": true }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Service "two" should be skipped because it failed
        let expected: Dag<&str> = par!("start service one", "start service three");
        assert_eq!(expected.to_string(), workflow.to_string());

        // Verify that the ignored paths include service two
        assert_eq!(workflow.ignored.len(), 1);
        assert_eq!(
            workflow.ignored[0],
            Operation::Update {
                path: Path::from_static("/services/two/running"),
                value: json!(true)
            }
        );
    }

    #[test]
    fn test_exception_with_target_extractor() {
        init();

        #[derive(Serialize, Deserialize)]
        struct App {
            version: String,
            #[serde(default)]
            pinned_version: Option<String>,
        }

        impl State for App {
            type Target = AppTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct AppTarget {
            version: String,
        }

        #[derive(Serialize, Deserialize)]
        struct System {
            apps: Map<String, App>,
        }

        impl State for System {
            type Target = SystemTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct SystemTarget {
            apps: Map<String, AppTarget>,
        }

        fn update_app(mut app: View<App>, Target(tgt): Target<App>) -> View<App> {
            app.version = tgt.version;
            app
        }

        // Exception: skip if target version doesn't match pinned version
        fn skip_if_pinned(view: View<App>, Target(tgt): Target<App>) -> bool {
            if let Some(ref pinned) = view.pinned_version {
                pinned != &tgt.version
            } else {
                false
            }
        }

        let domain = Domain::new()
            .job(
                "/apps/{app}",
                update(update_app)
                    .with_description(|Args(app): Args<String>| format!("update app {app}")),
            )
            .exception("/apps/{app}", crate::exception::update(skip_if_pinned));

        let initial = serde_json::from_value::<System>(json!({
            "apps": {
                "foo": { "version": "1.0", "pinned_version": "1.0" },
                "bar": { "version": "1.0", "pinned_version": null },
                "baz": { "version": "1.0" }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<SystemTarget>(json!({
            "apps": {
                "foo": { "version": "2.0" },
                "bar": { "version": "2.0" },
                "baz": { "version": "2.0" }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // App "foo" should be skipped because it's pinned to 1.0
        let expected: Dag<&str> = par!("update app bar", "update app baz");
        assert_eq!(expected.to_string(), workflow.to_string());
        assert_eq!(workflow.ignored.len(), 1);
        assert_eq!(
            workflow.ignored[0],
            Operation::Update {
                path: Path::from_static("/apps/foo/version"),
                value: json!("2.0")
            }
        );
    }

    #[test]
    fn test_exception_only_applies_to_matching_operation() {
        init();

        #[derive(Serialize, Deserialize)]
        struct Resource {
            value: i32,
        }

        impl State for Resource {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Container {
            resources: Map<String, Resource>,
        }

        impl State for Container {
            type Target = Self;
        }

        fn create_resource(
            mut res: View<Option<Resource>>,
            Target(tgt): Target<Resource>,
        ) -> View<Option<Resource>> {
            *res = Some(tgt);
            res
        }

        fn update_resource(
            mut res: View<Resource>,
            Target(tgt): Target<Resource>,
        ) -> View<Resource> {
            res.value = tgt.value;
            res
        }

        // Exception: skip updates when value is negative
        fn skip_negative_updates(Target(tgt): Target<Resource>) -> bool {
            tgt.value < 0
        }

        let domain = Domain::new()
            .job(
                "/resources/{name}",
                create(create_resource)
                    .with_description(|Args(name): Args<String>| format!("create resource {name}")),
            )
            .job(
                "/resources/{name}",
                update(update_resource)
                    .with_description(|Args(name): Args<String>| format!("update resource {name}")),
            )
            .exception(
                "/resources/{name}",
                crate::exception::update(skip_negative_updates),
            );

        // Test 1: Update operation should be skipped for negative values
        let initial = serde_json::from_value::<Container>(json!({
            "resources": {
                "a": { "value": 10 },
                "b": { "value": 20 }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<Container>(json!({
            "resources": {
                "a": { "value": -5 },
                "b": { "value": 30 }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Resource "a" update should be skipped, but "b" should be updated
        let expected: Dag<&str> = seq!("update resource b");
        assert_eq!(expected.to_string(), workflow.to_string());
        assert_eq!(workflow.ignored.len(), 1);

        // Test 2: Create operation should NOT be skipped even for negative values
        // because the exception only applies to update operations
        let initial = serde_json::from_value::<Container>(json!({
            "resources": {}
        }))
        .unwrap();

        let target = serde_json::from_value::<Container>(json!({
            "resources": {
                "c": { "value": -10 }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Resource "c" should be created even though value is negative
        let expected: Dag<&str> = seq!("create resource c");
        assert_eq!(expected.to_string(), workflow.to_string());
        assert!(workflow.ignored.is_empty());
    }

    #[test]
    fn test_multiple_exceptions_on_different_paths() {
        init();

        #[derive(Serialize, Deserialize)]
        struct Service {
            running: bool,
            #[serde(default)]
            install_failed: bool,
        }

        impl State for Service {
            type Target = ServiceTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct ServiceTarget {
            running: bool,
        }

        #[derive(Serialize, Deserialize)]
        struct Config {
            value: String,
            #[serde(default)]
            readonly: bool,
        }

        impl State for Config {
            type Target = ConfigTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct ConfigTarget {
            value: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Device {
            services: Map<String, Service>,
            config: Map<String, Config>,
        }

        impl State for Device {
            type Target = DeviceTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct DeviceTarget {
            services: Map<String, ServiceTarget>,
            config: Map<String, ConfigTarget>,
        }

        fn start_service(mut service: View<Service>) -> View<Service> {
            service.running = true;
            service
        }

        fn update_config(mut config: View<Config>, Target(tgt): Target<Config>) -> View<Config> {
            config.value = tgt.value;
            config
        }

        // Exception 1: skip failed services
        fn skip_failed_services(view: View<Service>) -> bool {
            view.install_failed
        }

        // Exception 2: skip readonly configs
        fn skip_readonly_configs(view: View<Config>) -> bool {
            view.readonly
        }

        let domain = Domain::new()
            .job(
                "/services/{service}",
                update(start_service).with_description(|Args(service): Args<String>| {
                    format!("start service {service}")
                }),
            )
            .job(
                "/config/{key}",
                update(update_config)
                    .with_description(|Args(key): Args<String>| format!("update config {key}")),
            )
            .exception(
                "/services/{service}",
                crate::exception::update(skip_failed_services),
            )
            .exception(
                "/config/{key}",
                crate::exception::update(skip_readonly_configs),
            );

        let initial = serde_json::from_value::<Device>(json!({
            "services": {
                "web": { "running": false, "install_failed": true },
                "api": { "running": false, "install_failed": false }
            },
            "config": {
                "host": { "value": "old", "readonly": true },
                "port": { "value": "8080", "readonly": false }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<DeviceTarget>(json!({
            "services": {
                "web": { "running": true },
                "api": { "running": true }
            },
            "config": {
                "host": { "value": "new" },
                "port": { "value": "9090" }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Service "web" and config "host" should be skipped
        // Check that both tasks are present (order may vary)
        let workflow_str = workflow.to_string();
        assert!(workflow_str.contains("start service api"));
        assert!(workflow_str.contains("update config port"));
        assert_eq!(workflow.ignored.len(), 2);
        assert_eq!(
            workflow
                .ignored
                .iter()
                .find(|op| op.path().starts_with("/services/web")),
            Some(&Operation::Update {
                path: Path::from_static("/services/web/running"),
                value: json!(true)
            })
        );
        assert_eq!(
            workflow
                .ignored
                .iter()
                .find(|op| op.path().starts_with("/config/host")),
            Some(&Operation::Update {
                path: Path::from_static("/config/host/value"),
                value: json!("new")
            })
        );
    }

    #[test]
    fn test_exception_with_system_extractor() {
        init();

        #[derive(Serialize, Deserialize)]
        struct App {
            image: String,
        }

        impl State for App {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Image {
            available: bool,
        }

        impl State for Image {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Device {
            apps: Map<String, App>,
            images: Map<String, Image>,
        }

        impl State for Device {
            type Target = DeviceTarget;
        }

        #[derive(Serialize, Deserialize)]
        struct DeviceTarget {
            apps: Map<String, App>,
        }

        fn update_app(mut app: View<App>, Target(tgt): Target<App>) -> View<App> {
            app.image = tgt.image;
            app
        }

        // Exception: skip apps if their image is not available
        fn skip_if_image_unavailable(
            view: View<App>,
            crate::extract::System(device): crate::extract::System<Device>,
        ) -> bool {
            device
                .images
                .get(&view.image)
                .map(|img| !img.available)
                .unwrap_or(true)
        }

        let domain = Domain::new()
            .job(
                "/apps/{app}",
                update(update_app)
                    .with_description(|Args(app): Args<String>| format!("update app {app}")),
            )
            .exception(
                "/apps/{app}",
                crate::exception::update(skip_if_image_unavailable),
            );

        let initial = serde_json::from_value::<Device>(json!({
            "apps": {
                "web": { "image": "nginx:1.0" },
                "api": { "image": "node:14" },
                "worker": { "image": "python:3.9" }
            },
            "images": {
                "nginx:1.0": { "available": true },
                "node:14": { "available": false },
                "python:3.9": { "available": true }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<DeviceTarget>(json!({
            "apps": {
                "web": { "image": "nginx:2.0" },
                "api": { "image": "node:16" },
                "worker": { "image": "python:3.10" }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // App "api" should be skipped because node:14 image is not available
        let expected: Dag<&str> = par!("update app web", "update app worker");
        assert_eq!(expected.to_string(), workflow.to_string());
        assert_eq!(workflow.ignored.len(), 1);
        assert_eq!(
            workflow.ignored[0],
            Operation::Update {
                path: Path::from_static("/apps/api/image"),
                value: json!("node:16")
            }
        );
    }

    #[test]
    fn test_exception_preserves_parent_path_skip() {
        init();

        #[derive(Serialize, Deserialize)]
        struct NestedValue {
            inner: String,
        }

        impl State for NestedValue {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Container {
            name: String,
            nested: NestedValue,
        }

        impl State for Container {
            type Target = Self;
        }

        #[derive(Serialize, Deserialize)]
        struct Root {
            containers: Map<String, Container>,
        }

        impl State for Root {
            type Target = Self;
        }

        fn update_name(mut name: View<String>, Target(tgt): Target<String>) -> View<String> {
            *name = tgt;
            name
        }

        fn update_inner(mut inner: View<String>, Target(tgt): Target<String>) -> View<String> {
            *inner = tgt;
            inner
        }

        // Exception: skip containers with name "disabled"
        fn skip_disabled_containers(view: View<Container>) -> bool {
            view.name == "disabled"
        }

        let domain = Domain::new()
            .job(
                "/containers/{id}/name",
                update(update_name).with_description(|Args(id): Args<String>| {
                    format!("update container {id} name")
                }),
            )
            .job(
                "/containers/{id}/nested/inner",
                update(update_inner).with_description(|Args(id): Args<String>| {
                    format!("update container {id} nested value")
                }),
            )
            .exception(
                "/containers/{id}",
                crate::exception::update(skip_disabled_containers),
            );

        let initial = serde_json::from_value::<Root>(json!({
            "containers": {
                "a": {
                    "name": "disabled",
                    "nested": { "inner": "old1" }
                },
                "b": {
                    "name": "enabled",
                    "nested": { "inner": "old2" }
                }
            }
        }))
        .unwrap();

        let target = serde_json::from_value::<Root>(json!({
            "containers": {
                "a": {
                    "name": "still-disabled",
                    "nested": { "inner": "new1" }
                },
                "b": {
                    "name": "still-enabled",
                    "nested": { "inner": "new2" }
                }
            }
        }))
        .unwrap();

        let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

        // Container "a" and all its nested fields should be skipped
        let expected: Dag<&str> =
            par!("update container b name", "update container b nested value");
        assert_eq!(expected.to_string(), workflow.to_string());

        // The parent path /containers/a should be on the ignore list
        assert_eq!(
            workflow.ignored[0],
            Operation::Update {
                path: Path::from_static("/containers/a/name"),
                value: json!("still-disabled")
            }
        );
    }
}
