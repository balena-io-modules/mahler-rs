use crate::dag::Dag;
use crate::task::Task;

pub(crate) struct Action<S> {
    /**
     * Unique id for the action. This is calculated from the
     * task is and the current runtime state expected
     * by the planner. This is used for loop detection in the plan.
     */
    id: String,

    /**
     * The task to execute
     *
     * Only atomic tasks should be added to a worflow item
     */
    task: Task<S>,
}

pub struct Workflow<S>(pub(crate) Dag<Action<S>>);

impl<S> Default for Workflow<S> {
    fn default() -> Self {
        Workflow(Dag::default())
    }
}
