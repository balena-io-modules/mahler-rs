use serde::Serialize;
use serde_json::Value;
use std::fmt::{self, Display};

use crate::error::{Error, IntoError};
use crate::path::Path;
use crate::task::{Context, Task};

use super::domain::Domain;
use super::target::Target;
use super::workflow::Workflow;
use super::Operation;

pub struct Planner<S> {
    domain: Domain<S>,
}

pub enum Plan<S> {
    Found { workflow: Workflow<S>, state: S },
    NotFound,
}

#[derive(Debug)]
pub enum PlanSearchError {
    SerializationError(serde_json::error::Error),
    PathNotFound,
}

impl std::error::Error for PlanSearchError {}

impl Display for PlanSearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanSearchError::SerializationError(err) => err.fmt(f),
            PlanSearchError::PathNotFound => write!(f, "not found"),
        }
    }
}

impl IntoError for PlanSearchError {
    fn into_error(self) -> Error {
        Error::PlanNotFound(self)
    }
}

impl<S> Planner<S> {
    pub fn new(domain: Domain<S>) -> Self {
        Self { domain }
    }

    fn try_task(&self, task: Task<S>, state: &Value, initial_plan: Workflow<S>) {
        match task {
            Task::Atom { .. } => {
                // Detect loops in the plan
                // if (initial_plan.0.some())
            }
            Task::List { .. } => unimplemented!(),
        }
    }

    pub fn find_plan(&self, cur: S, tgt: S) -> Result<Workflow<S>, Error>
    where
        S: Serialize + Clone,
    {
        let initial = serde_json::to_value(cur).map_err(PlanSearchError::SerializationError)?;
        let target = Target::try_from(tgt.clone()).map_err(PlanSearchError::SerializationError)?;
        let initial_plan = Workflow::<S>::default();

        let mut stack = vec![(initial, initial_plan)];

        while let Some((state, plan)) = stack.pop() {
            let distance = target.distance(&state);

            // we reached the target
            if distance.is_empty() {
                // TODO: return the proper plan
                return Ok(Workflow::default());
            }

            for op in distance.iter() {
                // Find applicable tasks
                let path = Path::new(op.path());
                let matching = self.domain.at(path.to_str());
                if let Some((args, intents)) = matching {
                    // Create the calling context for the job
                    let context = Context {
                        path,
                        args,
                        target: Some(tgt.clone()),
                    };
                    for intent in intents {
                        // If the intent is applicable to the operation
                        if op.matches(&intent.operation) || intent.operation != Operation::Any {
                            let task = intent.job.into_task(context.clone());

                            // apply the task to the state, if it progresses the plan, then select
                            // it and put the new state with the new plan on the stack

                            // try to apply the job
                            // if successful, put the original state on the stack, followed by the
                            // new state after applying
                        }
                    }
                }
            }
        }

        Err(PlanSearchError::PathNotFound)?
    }
}
