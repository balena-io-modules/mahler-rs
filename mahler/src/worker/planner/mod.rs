mod distance;
mod path_utils;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use tracing::{field, instrument, trace, trace_span, warn, Span};

use crate::dag::Dag;
use crate::error::{Error, ErrorKind};
use crate::json::{OperationMatcher, Patch, PatchOperation, Path, Value};
use crate::result::Result;
use crate::runtime::{Context, System};
use crate::state::State;
use crate::system_ext::SystemExt;
use crate::task::{Method, Task};

use super::domain::Domain;
use super::workflow::{Ignored, WorkUnit, Workflow};
use distance::Distance;
use path_utils::{
    domains_are_conflicting, longest_common_prefix, select_non_conflicting_prefer_prefixes,
};

/// Maximum number of planning steps
/// TODO: make this user configurable
const MAX_DEPTH: usize = 256;

/// Get a hash for the system state
fn hash_state(state: &System) -> u64 {
    let mut hasher = DefaultHasher::new();
    state.inner_state().hash(&mut hasher);
    hasher.finish()
}

/// Build the list of candidate plans for one search iteration by evaluating each
/// distance operation against matching jobs in the domain.
fn build_candidates(
    distance_operations: Vec<(Path, OperationMatcher)>,
    db: &Domain,
    cur_state: &mut System,
    tgt: &Arc<Value>,
    path_exceptions: &mut Vec<(Path, Option<String>)>,
) -> Result<Vec<Candidate>> {
    let mut candidates: Vec<Candidate> = Vec::new();

    'ops: for (path, kind) in distance_operations {
        let path = &path;

        // skip the path if any parent path has an exception
        if path_exceptions.iter().any(|(p, _)| p.is_prefix_of(path)) {
            continue;
        }

        // Look for matching exceptions for the path and operation
        if let Some((args, exceptions)) = db.find_matching_exceptions(path.as_str()) {
            let context = Context {
                path: path.clone(),
                args,
                target: tgt.clone(),
                target_override: None,
            };

            // if there are any exceptions that apply to the context, add the path to
            // the list and skip the path
            for exception in exceptions.filter(|e| e.operation().allows(&kind)) {
                if exception.test(cur_state, &context)? {
                    path_exceptions.push((path.clone(), exception.description(&context)?));
                    continue 'ops;
                }
            }
        }

        // Retrieve matching jobs at this path
        if let Some((args, jobs)) = db.find_matching_jobs(path.as_str()) {
            let context = Context {
                path: path.clone(),
                args,
                target: tgt.clone(),
                target_override: None,
            };

            // Look for jobs matching the operation
            for job in jobs.filter(|j| j.operation().allows(&kind)) {
                let task = job.new_task(context.clone());

                // Try applying this task to the current state
                match try_task_into_workflow(&task, db, cur_state) {
                    Ok(plan) if !plan.changes.is_empty() => candidates.push(Candidate {
                        plan,
                        path: task.path().clone(),
                        is_method: task.is_method(),
                        operation: job.operation().clone(),
                    }),
                    // Skip the task if its condition is not met or it makes no changes
                    Ok(_) => {}
                    Err(e) if matches!(e.kind(), ErrorKind::ConditionNotMet) => {}
                    Err(e) => return Err(e),
                }
            }
        }
    }

    Ok(candidates)
}

/// Find a workflow that takes the system from its current state
/// to the target state using the tasks in the provided Domain
///
/// The search is greedy: at each step the best candidate is applied and the
/// search never backtracks.
///
/// The final state of the system can be obtained with [`Workflow::apply_changes`]
#[instrument(level = "trace", skip_all, err(level = "trace"))]
pub fn find_workflow_to_target<T>(
    db: &Domain,
    system: &System,
    tgt: &T::Target,
) -> Result<Option<Workflow>>
where
    T: State,
{
    let tgt = &Arc::new(crate::json::to_value(tgt)?);

    trace!(initial=%system, target=%tgt, "searching for workflow");

    let find_workflow_span = Span::current();

    let mut cur_state = system.clone();
    let mut cur_plan = Dag::default();

    // path exceptions and the corresponding reason
    let mut path_exceptions: Vec<(Path, Option<String>)> = Vec::new();

    // Keep track of visited states
    let mut visited_states = HashSet::new();

    // Set when no candidate could be applied. The goal is checked once more
    // because evaluating candidates may have added new path exceptions.
    let mut stalled = false;

    // Number of steps applied to the plan so far
    let mut depth = 0;
    loop {
        // Prevent infinite recursion (e.g., from buggy tasks or recursive methods)
        if depth >= MAX_DEPTH {
            warn!(parent: &find_workflow_span, "reached max search depth ({MAX_DEPTH})");
            return Err(ErrorKind::PlanningOverflow.into());
        }

        visited_states.insert(hash_state(&cur_state));

        // Compute the difference between current and target state. The state is normalized
        // first: deserialized into the target type to remove internal fields
        let distance = {
            let cur = cur_state.state::<T::Target>()?;
            let skipped: Vec<Path> = path_exceptions.iter().map(|(p, _)| p.clone()).collect();
            Distance::new(&cur, tgt, &skipped)?
        };

        // If there are no more operations, we've reached the goal
        if distance.is_empty() {
            let mut workflow = Workflow::new(cur_plan);
            workflow.ignored = distance
                .ignored
                .into_iter()
                .map(|operation| {
                    let reason = path_exceptions
                        .iter()
                        .find(|(p, _)| p.is_prefix_of(operation.path()))
                        .and_then(|(_, reason)| reason.clone());
                    Ignored { operation, reason }
                })
                .collect();

            return Ok(Some(workflow));
        }

        if stalled {
            trace!(last_evaluated_state=%cur_state, "no plan was found");
            return Ok(None);
        }

        let next_span = trace_span!("find_next", distance = %distance, cur_plan=field::Empty);
        let _enter = next_span.enter();

        let candidates = build_candidates(
            distance.operations,
            db,
            &mut cur_state,
            tgt,
            &mut path_exceptions,
        )?;
        let mut candidates = merge_concurrent_candidates(candidates);

        // Record candidates found for this planning step
        trace!(candidates=%candidates.len());

        // Only the most qualified candidate is considered (greedy search). Candidates that
        // take us to a previously visited state are skipped. This avoids the planner looping
        // forever jumping between a few visited states
        candidates.sort();
        let mut next = None;
        for Candidate {
            plan,
            path,
            operation,
            ..
        } in candidates.into_iter().rev()
        {
            // apply the candidate in place, reverting it if the state was visited
            let mut undo = Vec::new();
            cur_state.patch_with_undo(plan.changes, &mut undo)?;

            if !visited_states.contains(&hash_state(&cur_state)) {
                trace!(path=%path,op=%operation,partial_plan=%plan.dag, "selected candidate");
                next = Some(plan.dag);
                break;
            }

            cur_state.revert(undo)?;
        }

        match next {
            Some(partial_plan) => {
                cur_plan = cur_plan.concat(partial_plan);
                depth += 1;
                next_span.record("cur_plan", field::display(&cur_plan));
            }
            None => stalled = true,
        }
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
/// * `system` - the state of the system before running the task
pub fn find_workflow_for_task(mut task: Task, db: &Domain, system: &System) -> Result<Workflow> {
    // Look-up the task on the domain to find its path
    let task_id = task.id();
    let Context { args, .. } = task.context_mut();
    let path = db.find_path(task_id, args)?;

    // Set the correct path for the task
    let task = task.with_path(path);

    let PartialPlan { dag, .. } = try_task_into_workflow(&task, db, &mut system.clone())?;

    Ok(Workflow::new(dag))
}

/// The result of simulating a task against a state
struct PartialPlan {
    /// Workflow for the task
    dag: Dag<WorkUnit>,
    /// Cumulative changes performed by the task and its sub-tasks (if any)
    changes: Vec<PatchOperation>,
    /// The task operational domain, used for conflict detection
    domain: BTreeSet<Path>,
}

/// Convert the task into a workflow if possible
///
/// For primitive tasks, the workflow will just contain a single node, however
/// for composite tasks, the workflow will create a Dag with concurrent branches
/// depending on the operational domain of each sub-task.
///
/// The state is used to simulate the tasks of a method and is restored before returning.
fn try_task_into_workflow(task: &Task, db: &Domain, cur_state: &mut System) -> Result<PartialPlan> {
    match task {
        Task::Action(action) => {
            // Simulate the task and get the list of changes
            let Patch(changes) = action.dry_run(cur_state)?;

            // Create a single node workflow, including a copy of the
            // changes for validation during runtime
            let dag = Dag::from(WorkUnit::new(action.clone(), changes.clone()));

            Ok(PartialPlan {
                dag,
                changes,
                domain: BTreeSet::from([action.domain()]),
            })
        }
        Task::Method(method) => {
            // child tasks are simulated in place and reverted once the method is expanded
            let mut undo = Vec::new();
            let plan = try_method_into_workflow(method, db, cur_state, &mut undo);
            cur_state.revert(undo)?;
            plan
        }
    }
}

/// Convert the method into a workflow, recording in `undo` the changes made to the state
/// while simulating its child tasks
fn try_method_into_workflow(
    method: &Method,
    db: &Domain,
    cur_state: &mut System,
    undo: &mut Vec<PatchOperation>,
) -> Result<PartialPlan> {
    let mut plan_branches = Vec::new();
    let mut domain = BTreeSet::new();
    let mut changes = Vec::new();

    for task in expand_method_tasks(method, db, cur_state)? {
        // Run the task as if it was a sequential plan. Child methods record their changes
        // in the same undo log, so the changes are applied once and reverted by the caller
        let task_plan = match &task {
            Task::Method(child) => try_method_into_workflow(child, db, cur_state, undo)?,
            Task::Action(_) => {
                let task_plan = try_task_into_workflow(&task, db, cur_state)?;
                cur_state.patch_with_undo(task_plan.changes.iter().cloned(), undo)?;
                task_plan
            }
        };

        // If the task domain conflicts with the cumulative domain from the
        // branches, join the existing branches before continuing
        let dag = if domains_are_conflicting(&domain, task_plan.domain.iter()) {
            Dag::new(std::mem::take(&mut plan_branches)).concat(task_plan.dag)
        } else {
            task_plan.dag
        };

        changes.extend(task_plan.changes);
        domain.extend(task_plan.domain);
        plan_branches.push(dag);
    }

    Ok(PartialPlan {
        dag: Dag::new(plan_branches),
        changes,
        domain,
    })
}

/// Expand a method into its enriched child tasks, propagating arguments and resolving
/// each task against the domain to attach the correct path and description.
fn expand_method_tasks(method: &Method, db: &Domain, cur_state: &System) -> Result<Vec<Task>> {
    // Get the list of referenced tasks
    let tasks = method.expand(cur_state)?;

    // Extended tasks will store the correct references from the domain with the
    // right path and description
    let mut extended_tasks = Vec::new();

    for mut t in tasks.into_iter() {
        let task_id = t.id();

        let Context {
            args: m_args,
            target: m_target,
            ..
        } = method.context();
        let Context {
            args: t_args,
            target: t_target,
            ..
        } = t.context_mut();

        // Propagate arguments from the method into the child tasks.
        // This is just for better user experience as it avoids having to define
        // arguments for each sub-task in the method
        for (k, v) in m_args.iter() {
            if !t_args.contains_key(k) {
                t_args.insert(k, v);
            }
        }

        // The global target is the same for every task so we propagate it
        // to the child task
        *t_target = m_target.clone();

        // Find the job path on the domain list, pass the argument for path matching
        // this will remove any unused arguments in the path
        let path = db.find_path(task_id, t_args)?;

        // Using the path, now find the actual job on the domain.
        // The domain job includes additional configuration like the description that
        // we want to use in the workflow
        let job = db
            .find_job(&path, task_id)
            // this should never happen since the path was returned by the domain
            .ok_or(Error::internal(format!("should find a job for {path}")))?;

        // Create the domain task with the child context
        let task = job.new_task(t.into_context()).with_path(path);

        extended_tasks.push(task);
    }

    Ok(extended_tasks)
}

struct Candidate {
    plan: PartialPlan,
    path: Path,
    operation: OperationMatcher,
    is_method: bool,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Candidate {}

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

/// Merge concurrent non-conflicting candidates into a single combined candidate when possible.
///
/// When more than one candidate can run without conflicts, they are combined into a single
/// concurrent candidate backed by a parallel DAG branch. If only one (or zero) candidates
/// are non-conflicting, the list is returned unchanged.
fn merge_concurrent_candidates(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let selected = select_concurrent(&candidates);
    if selected.len() < 2 {
        return candidates;
    }

    let mut concurrent = Vec::with_capacity(selected.len());
    let mut remaining = Vec::with_capacity(candidates.len() - selected.len());
    for (i, candidate) in candidates.into_iter().enumerate() {
        if selected.contains(&i) {
            concurrent.push(candidate);
        } else {
            remaining.push(candidate);
        }
    }

    remaining.push(Candidate::concurrent(concurrent));
    remaining
}

/// Select the indexes of the candidates that can be run concurrently.
///
/// The selection has at most one candidate per path (the highest priority one), the
/// paths do not conflict with each other and the task domains do not overlap.
fn select_concurrent(candidates: &[Candidate]) -> BTreeSet<usize> {
    // Find the longest list of non-conflicting tasks based on paths (for prioritization)
    let non_conflicting_paths = select_non_conflicting_prefer_prefixes(
        candidates.iter().map(|Candidate { path, .. }| path),
    );

    // Highest priority candidate for each path
    let mut best: BTreeMap<&Path, usize> = BTreeMap::new();
    for (i, candidate) in candidates.iter().enumerate() {
        if non_conflicting_paths.contains(&candidate.path) {
            let prev = best.entry(&candidate.path).or_insert(i);
            if candidates[*prev] < *candidate {
                *prev = i;
            }
        }
    }

    // Keep the best candidates whose domains do not overlap, in the original order
    let mut best: Vec<usize> = best.into_values().collect();
    best.sort_unstable();

    let mut selected = BTreeSet::new();
    let mut cumulative_domain = BTreeSet::new();
    for i in best {
        let domain = &candidates[i].plan.domain;
        if !domains_are_conflicting(&cumulative_domain, domain.iter()) {
            cumulative_domain.extend(domain.iter().cloned());
            selected.insert(i);
        }
    }
    selected
}

impl Candidate {
    /// Combine independent candidates into a single candidate with a parallel DAG
    fn concurrent(candidates: Vec<Candidate>) -> Self {
        // The path is the longest common prefix between the child paths
        let path = longest_common_prefix(candidates.iter().map(|c| &c.path));
        // Treat the candidate as a method when sorting if all children are methods
        let is_method = candidates.iter().all(|c| c.is_method);

        let mut branches = Vec::with_capacity(candidates.len());
        let mut changes = Vec::new();
        let mut domain = BTreeSet::new();
        for Candidate { plan, .. } in candidates {
            branches.push(plan.dag);
            changes.extend(plan.changes);
            domain.extend(plan.domain);
        }

        Candidate {
            plan: PartialPlan {
                dag: Dag::new(branches),
                changes,
                domain,
            },
            path,
            is_method,
            operation: OperationMatcher::Update,
        }
    }
}

#[cfg(test)]
mod tests;
