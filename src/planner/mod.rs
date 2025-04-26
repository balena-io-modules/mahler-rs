use std::fmt::Debug;

use anyhow::{anyhow, Context as AnyhowCtx};
use json_patch::Patch;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tracing::{debug, debug_span, error, field, instrument, warn, Level, Span};

use crate::path::Path;
use crate::system::System;
use crate::task::{self, Context, Operation, Task};
use crate::workflow::{WorkUnit, Workflow};

mod distance;
mod domain;

use distance::*;
pub use domain::*;

#[derive(Debug)]
pub struct Planner(Domain);

#[derive(Debug, Error)]
enum SearchError {
    #[error("method error: {0}")]
    CannotExpand(#[source] PathSearchError),

    #[error("task error: {0}")]
    Task(#[from] task::Error),

    #[error("task condition failed")]
    ConditionFailed,

    #[error("loop detected")]
    LoopDetected,

    // this is probably a bug if this error
    // happens
    #[error("unexpected error: {0}")]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct TaskError(#[from] anyhow::Error);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Task(#[from] TaskError),

    #[error("workflow not found")]
    NotFound,

    #[error("max search depth reached")]
    MaxDepthReached,

    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl Planner {
    pub fn new(domain: Domain) -> Self {
        Self(domain)
    }

    pub fn domain(&self) -> &Domain {
        &self.0
    }

    #[instrument(level = "trace", skip_all, fields(task=?task, changes=field::Empty), err(level=Level::TRACE))]
    fn try_task(
        &self,
        task: Task,
        cur_state: &System,
        cur_plan: Workflow,
        stack_len: u32,
    ) -> Result<Workflow, SearchError> {
        match task {
            Task::Action(action) => {
                let work_id = WorkUnit::new_id(&action, cur_state.root());

                // Detect loops in the plan
                if cur_plan.as_dag().some(|a| a.id == work_id) {
                    return Err(SearchError::LoopDetected)?;
                }

                // Test the task
                let Patch(changes) = action.dry_run(cur_state).map_err(SearchError::Task)?;

                // if we are at the top of the stack and no changes are introduced
                // then assume the condition has failed
                if stack_len == 0 && changes.is_empty() {
                    return Err(SearchError::ConditionFailed);
                }

                // Otherwise add the task to the plan
                let Workflow { dag, pending } = cur_plan;
                let dag = dag.with_head(WorkUnit::new(work_id, action, changes.clone()));
                Span::current().record("changes", format!("{:?}", changes));
                let pending = [pending, changes].concat();

                Span::current().record("selected", true);
                Ok(Workflow { dag, pending })
            }
            Task::Method(method) => {
                let tasks = method.expand(cur_state).map_err(SearchError::Task)?;

                let mut cur_state = cur_state.clone();
                let mut cur_plan = cur_plan;
                let mut changes = Vec::new();
                for mut t in tasks.into_iter() {
                    // A task cannot override the context set by the
                    // parent task. For instance a method for `/a/b` cannot
                    // have a sub-task for `/a/c`, it can however have a task
                    // for `/a/b` or `/a/b/c`. This is to ensure we can parallelize
                    // tasks
                    for (k, v) in method.context().args.iter() {
                        t = t.with_arg(k, v)
                    }
                    let task_id = t.id().to_string();
                    let Context { args, .. } = t.context_mut();
                    let path = self
                        .0
                        .find_path_for_job(task_id.as_str(), args)
                        // The user may have not have put the child task in the
                        // domain, or failed to account for all args in the path
                        // in which case we need to return an error
                        .map_err(SearchError::CannotExpand)?;

                    // Now that we have the proper path, look
                    // up the job at the domain to get any additional metadata
                    // from the job (such as the description)
                    let job = self
                        .0
                        .find_job(&path, &task_id)
                        // this really should not happen
                        .ok_or(anyhow!("failed to find job for path {path}"))?;

                    let task = job.clone_task(t.context().to_owned()).with_path(path);

                    let Workflow { dag, pending } =
                        self.try_task(task, &cur_state, cur_plan, stack_len + 1)?;

                    // Apply changes to the local copy of the state
                    cur_state
                        .patch(Patch(pending.clone()))
                        // not sure how this can happen, perhaps a bad method definition?
                        .context(format!("failed to apply patch {pending:?}"))?;

                    // But accumulate the changes separately to avoid
                    // applying them twice
                    changes = [changes, pending].concat();
                    cur_plan = Workflow {
                        dag,
                        pending: vec![],
                    };
                }

                // if we are at the top of the stack and no changes are introduced
                // then assume the condition has failed
                if stack_len == 0 && changes.is_empty() {
                    return Err(SearchError::ConditionFailed)?;
                }

                let mut cur_plan = cur_plan;
                cur_plan.pending = changes;

                Span::current().record("selected", true);
                Ok(cur_plan)
            }
        }
    }

    #[instrument(skip_all, fields(ini=%system.root(), tgt=%tgt), err, ret(Display))]
    pub(crate) fn find_workflow<T>(&self, system: &System, tgt: &Value) -> Result<Workflow, Error>
    where
        T: Serialize + DeserializeOwned,
    {
        // Store the initial state and an empty plan on the stack
        let mut stack = vec![(system.clone(), Workflow::default(), 0)];
        let find_worflow_span = Span::current();

        // TODO: we should merge non conflicting workflows
        // for parallelism
        while let Some((cur_state, cur_plan, depth)) = stack.pop() {
            // we need to limit the search depth to avoid following
            // a buggy task forever
            // TODO: make this configurable
            if depth >= 256 {
                return Err(Error::MaxDepthReached)?;
            }

            // There may be fields in internal state that we don't want
            // to use when comparing with the target. For this reason, we
            // deserialize the state into the input type T and then re-serialize
            // to Value it before comparing
            let cur = cur_state
                .state::<T>()
                .and_then(System::try_from)
                // TODO: return seriaization error
                .with_context(|| "failed to serialize state")?;

            let distance = Distance::new(&cur, tgt);

            // we reached the target
            if distance.is_empty() {
                // return the existing plan reversing it first
                return Ok(cur_plan.reverse());
            }

            let next_span = debug_span!("find_next", cur = %&cur_state.root());
            let mut candidates = Vec::new();
            let _enter = next_span.enter();
            for op in distance.iter().rev() {
                let path = Path::new(op.path());
                // Find applicable jobs
                let matching = self.0.find_matching_jobs(path.to_str());

                if let Some((args, jobs)) = matching {
                    // Calculate the target for the job path
                    let pointer = path.as_ref();

                    // The target will not be resolvable on `delete` operations
                    // in which case we need to assign it to null, this will
                    // cause tasks trying to extract a Target to fail, which is
                    // the right behavior
                    let target = pointer.resolve(tgt).unwrap_or(&Value::Null);

                    // Create the calling context for the job
                    let context = Context {
                        path,
                        args,
                        target: target.clone(),
                    };

                    // Go through the list of usable jobs
                    for job in jobs.filter(|i| i.operation() != &Operation::None).rev() {
                        // If the job is applicable to the operation
                        if op.matches(job.operation()) || job.operation() == &Operation::Any {
                            let task = job.clone_task(context.clone());
                            let task_id = task.id().to_string();
                            let task_description = task.to_string();

                            // apply the task to the state, if it progresses the plan, then select
                            // it and put the new state with the new plan on the stack
                            match self.try_task(task, &cur_state, cur_plan.clone(), 0) {
                                Ok(Workflow { dag, pending }) => {
                                    // If the task is not progressing the plan, we can ignore it
                                    // this should never happen
                                    if pending.is_empty() {
                                        continue;
                                    }

                                    // If we got here, the task is part of a new potential workflow
                                    // so we to make a copy of the system
                                    let mut new_sys = cur_state.clone();

                                    // Update the state and the workflow
                                    new_sys
                                        .patch(Patch(pending))
                                        .with_context(|| "failed to apply system patch")?;
                                    let new_plan = Workflow {
                                        dag,
                                        pending: vec![],
                                    };

                                    // add the new initial state and plan to the stack
                                    candidates.push(task_description);
                                    stack.push((new_sys, new_plan, depth + 1));
                                }
                                // Ignore harmless errors
                                Err(SearchError::Task(task::Error::ConditionFailed)) => {}
                                Err(SearchError::LoopDetected) => {}
                                Err(SearchError::ConditionFailed) => {}
                                // this is probably a bug so we terminate the search
                                Err(SearchError::Unexpected(err)) => {
                                    return Err(Error::Unexpected(err))
                                }
                                Err(err) => {
                                    if cfg!(debug_assertions) {
                                        return Err(TaskError(anyhow!(err)))?;
                                    }
                                    warn!(parent: &find_worflow_span, "task {} failed during planning: {} ... ignoring", task_id, err);
                                }
                            }
                        }
                    }
                }
            }
            debug!(parent: &next_span, candidates=%candidates
                    .iter()
                    .map(|task| task.to_string())
                    .rev()
                    .collect::<Vec<_>>()
                    .join(", "));
        }

        Err(Error::NotFound)?
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::fmt::Display;

    use super::*;
    use crate::extract::{Args, System, Target, View};
    use crate::task::*;
    use crate::{seq, Dag};

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
            "gustav::planner::tests::plus_one()",
            "gustav::planner::tests::plus_one()"
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
            "gustav::planner::tests::plus_one()",
            "gustav::planner::tests::plus_one()"
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

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "gustav::planner::tests::plus_one(/counters/one)",
            "gustav::planner::tests::plus_one(/counters/one)",
            "gustav::planner::tests::plus_one(/counters/two)",
            "gustav::planner::tests::plus_one(/counters/two)",
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
            counters: HashMap::from([("one".to_string(), 2), ("two".to_string(), 0)]),
        };

        let domain = Domain::new()
            .job("/counters/{counter}", none(plus_one))
            .job("/counters/{counter}", update(plus_two));

        let planner = Planner::new(domain);
        let workflow = find_plan(planner, initial, target).unwrap();

        // We expect a linear DAG with two tasks
        let expected: Dag<&str> = seq!(
            "gustav::planner::tests::plus_one(/counters/one)",
            "gustav::planner::tests::plus_one(/counters/one)",
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
            "gustav::planner::tests::plus_one(/counters/one)",
            "gustav::planner::tests::plus_one(/counters/one)",
            "gustav::planner::tests::plus_one(/counters/one)",
        );

        assert_eq!(workflow.to_string(), expected.to_string(),);
    }

    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::{prelude::*, EnvFilter};
    #[test]
    fn test_stacking_problem() {
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
        ) -> View<Location> {
            // if the block is clear and we are not holding any other blocks
            // we can grab the block
            if loc.is_block()
                && is_clear(&sys.blocks, &Location::Blk(block))
                && !is_holding(&sys.blocks)
            {
                *loc = Location::Hand;
            }

            loc
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
                } else if *loc != Location::Hand {
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
