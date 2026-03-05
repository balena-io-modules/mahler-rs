use std::collections::HashSet;

use tracing::{field, instrument, trace, trace_span, warn, Span};

use crate::dag::Dag;
use crate::error::{Error, ErrorKind};
use crate::json::{Patch, Path, Value};
use crate::result::Result;
use crate::runtime::{Context, System};
use crate::state::State;
use crate::system_ext::SystemExt;

use super::candidate::{merge_concurrent_candidates, Candidate};
use super::distance::Distance;
use super::path_utils::{hash_state, try_find};
use super::task_workflow::try_task_into_workflow;
use super::{Domain, Ignored, WorkUnit, Workflow};

pub fn prepare_workflow(
    cur_plan: Dag<WorkUnit>,
    distance: Distance,
    path_exceptions: &[(Path, Option<String>)],
) -> Workflow {
    // we need to reverse the plan before returning
    let mut workflow = Workflow::new(cur_plan.reverse());
    let mut exceptions = Vec::new();
    for op in distance.ignored {
        let ex = if let Some((_, reason)) = path_exceptions
            .iter()
            .find(|(p, _)| p.is_prefix_of(op.path()))
        {
            Ignored {
                operation: op,
                reason: reason.clone(),
            }
        } else {
            Ignored {
                operation: op,
                reason: None,
            }
        };

        exceptions.push(ex);
    }

    workflow.ignored = exceptions;
    workflow
}

pub fn skipped_paths(path_exceptions: &[(Path, Option<String>)]) -> Vec<Path> {
    path_exceptions
        .iter()
        .map(|(path, _)| Path::new(path.as_ref()))
        .collect()
}

/// Build the list of candidate plans for one search iteration by evaluating each
/// distance operation against matching jobs in the domain.
fn build_candidates(
    distance_operations: Vec<crate::json::Operation>,
    db: &Domain,
    cur_state: &System,
    tgt: &Value,
    path_exceptions: &mut Vec<(Path, Option<String>)>,
) -> Result<Vec<Candidate>> {
    let mut candidates: Vec<Candidate> = Vec::new();

    for op in distance_operations {
        let path = op.path();

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
            if let Some(exception) = try_find(
                exceptions.filter(|j| j.operation().matches(&op)),
                |exception| exception.test(cur_state, &context),
            )? {
                path_exceptions.push((path.clone(), exception.description(&context)?));
                continue;
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
            for job in jobs.filter(|j| j.operation().matches(&op)) {
                let task = job.new_task(context.clone());
                let mut changes = Vec::new();
                let mut domain = std::collections::BTreeSet::new();

                // Try applying this task to the current state
                match try_task_into_workflow(&task, db, cur_state, &mut domain, &mut changes) {
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
                        // Other job errors also terminate the search
                        _ => return Err(e),
                    },

                    // ignore if changes is empty
                    _ => {}
                }
            }
        }
    }

    Ok(candidates)
}

/// Sort candidates and push the best valid one (that doesn't introduce cycles or revisit
/// a previously visited state) onto the search stack.
fn select_best_candidate(
    mut candidates: Vec<Candidate>,
    cur_state: &System,
    cur_plan: &Dag<WorkUnit>,
    visited_states: &HashSet<u64>,
    stack: &mut Vec<(System, Dag<WorkUnit>, usize)>,
    depth: usize,
) -> Result<()> {
    // sort candidates
    candidates.sort();

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

    Ok(())
}

/// Find a workflow that takes the system from its current state
/// to the target state using the tasks in the provided Domain
///
/// If found, this will replace the value of system with the final state of the system
#[instrument(level = "trace", skip_all, err(level = "trace"))]
pub fn find_workflow_to_target<T>(
    db: &Domain,
    system: &mut System,
    tgt: &Value,
) -> Result<Option<Workflow>>
where
    T: State,
{
    trace!(initial=%system, target=%tgt, "searching for workflow");

    // The search stack stores (current_state, current_plan, depth)
    let mut stack = vec![(system.clone(), Dag::default(), 0)];

    let find_workflow_span = Span::current();

    // path exceptions and the corresponding reason
    let mut path_exceptions: Vec<(Path, Option<String>)> = Vec::new();

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
        let distance = Distance::new(&cur, tgt, &skipped_paths(&path_exceptions));

        // If there are no more operations, we've reached the goal
        if distance.is_empty() {
            let workflow = prepare_workflow(cur_plan, distance, &path_exceptions);

            // update the system
            *system = cur_state;
            return Ok(Some(workflow));
        }

        let next_span = trace_span!("find_next", distance = %distance, cur_plan=field::Empty);
        next_span.in_scope(|| {
            // make a copy of the plan for the logs if in the tracing scope
            next_span.record("cur_plan", field::display(cur_plan.clone().reverse()));
        });
        let _enter = next_span.enter();

        let candidates = build_candidates(
            distance.operations,
            db,
            &cur_state,
            tgt,
            &mut path_exceptions,
        )?;
        let candidates = merge_concurrent_candidates(candidates);

        // Record candidates found for this planning step
        trace!(candidates=%candidates.len());

        select_best_candidate(
            candidates,
            &cur_state,
            &cur_plan,
            &visited_states,
            &mut stack,
            depth,
        )?;

        if stack.is_empty() {
            // Compute the difference between current and target state
            // one last time in case some new paths were ignored in the
            // last iteration
            let distance = Distance::new(&cur, tgt, &skipped_paths(&path_exceptions));

            // If there are no more operations, we've reached the goal
            if distance.is_empty() {
                let workflow = prepare_workflow(cur_plan, distance, &path_exceptions);

                // update the system
                *system = cur_state;
                return Ok(Some(workflow));
            }

            trace!(last_evaluated_state=%cur_state, "no plan was found");
        }
    }

    // No candidate plan reached the goal state
    Ok(None)
}
