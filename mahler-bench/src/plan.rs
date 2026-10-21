//! Helpers to describe a workflow produced by the planner
//!
//! The benchmarks use these to check they are measuring the plan they intend to, and
//! the memory report prints them alongside the allocation counts so that numbers can
//! be compared across runs.

use mahler::worker::Workflow;

/// Shape of a planned workflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanStats {
    /// Number of tasks (actions) in the workflow
    pub tasks: usize,
    /// Number of parallel branching points in the workflow
    pub forks: usize,
}

impl PlanStats {
    /// Derive the stats from the workflow rendering
    ///
    /// The `Workflow` `Display` impl renders every action as `- <description>` and every
    /// fork as `+ `, followed by one `~ ` per branch, so tasks and forks can be counted
    /// from the rendered plan. Job descriptions in the benchmark domains never contain
    /// those markers. `Workflow` has no public API to count them directly.
    pub fn of(workflow: &Workflow) -> Self {
        let rendered = workflow.to_string();
        Self {
            tasks: rendered.matches("- ").count(),
            forks: rendered.matches("+ ").count(),
        }
    }
}
