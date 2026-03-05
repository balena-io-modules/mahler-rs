use std::collections::BTreeSet;

use crate::dag::Dag;
use crate::error::Error;
use crate::json::{Patch, PatchOperation, Path};
use crate::result::Result;
use crate::runtime::{Context, System};
use crate::system_ext::SystemExt;
use crate::task::Task;

use super::path_utils::domains_are_conflicting;
use super::{Domain, WorkUnit, Workflow};

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
/// * `changes` - the cumulative changes performed by the task and its sub-tasks (if any)
pub(super) fn try_task_into_workflow(
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
            let Patch(ops) = action.dry_run(cur_state)?;

            // Prepend a new node to the workflow, include a copy
            // of the changes for validation during runtime
            let new_plan = Dag::from(WorkUnit::new(work_id, action.clone(), ops.clone()));

            domain.insert(action.domain());
            changes.extend(ops);

            Ok(new_plan)
        }
        Task::Method(method) => {
            let extended_tasks = expand_method_tasks(method, db, cur_state)?;
            build_method_plan(extended_tasks, db, cur_state, domain, changes)
        }
    }
}

/// Expand a method into its enriched child tasks, propagating arguments and resolving
/// each task against the domain to attach the correct path and description.
fn expand_method_tasks(
    method: &crate::task::Method,
    db: &Domain,
    cur_state: &System,
) -> Result<Vec<Task>> {
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

        // Get a copy of the task for the final list
        let task = job.new_task(t.context().to_owned()).with_path(path);

        extended_tasks.push(task);
    }

    Ok(extended_tasks)
}

/// Build a DAG plan from an ordered list of expanded tasks, grouping non-conflicting tasks
/// into concurrent branches and sequencing conflicting groups.
fn build_method_plan(
    tasks: Vec<Task>,
    db: &Domain,
    cur_state: &System,
    domain: &mut BTreeSet<Path>,
    changes: &mut Vec<PatchOperation>,
) -> Result<Dag<WorkUnit>> {
    let mut plan_branches = Vec::new();
    let mut cumulative_domain = BTreeSet::new();
    let mut cur_state = cur_state.clone();

    // Iterate over the list of sub-tasks
    for task in tasks {
        // Run the task as if it was a sequential plan
        let mut task_domain = BTreeSet::new();
        let mut task_changes = Vec::new();
        let partial_plan =
            try_task_into_workflow(&task, db, &cur_state, &mut task_domain, &mut task_changes)?;

        // Check if the task domain conflicts with the cumulative domain from branches
        let partial_plan = if domains_are_conflicting(&cumulative_domain, task_domain.iter()) {
            // If so, join the existing branches and concatenate the returned workflow
            // we reverse the branches to preserve the expected order of tasks in the plan
            let dag = Dag::new(plan_branches).prepend(partial_plan);
            plan_branches = Vec::new();

            dag
        } else {
            partial_plan
        };

        // move task changes temporarily to a patch
        let patch = Patch(task_changes);

        // Apply the task changes
        cur_state.patch(&patch)?;

        let Patch(task_changes) = patch;

        // Append the changes to the parent list, then move task_changes into the patch
        changes.extend(task_changes);

        // Add the new dag to the list of branches and update the cumulative domain
        plan_branches.push(partial_plan);
        cumulative_domain.extend(task_domain);
    }

    // After all tasks are evaluated, join remaining branches
    let new_plan = Dag::new(plan_branches);
    domain.extend(cumulative_domain);

    // Include changes in the returned plan
    Ok(new_plan)
}
