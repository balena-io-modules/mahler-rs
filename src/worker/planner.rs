use serde::Serialize;
use serde_json::Value;
use std::fmt::{self, Display};

use crate::error::{Error, IntoError};
use crate::path::Path;
use crate::task::{Context, Task};

use super::distance::Distance;
use super::domain::Domain;
use super::workflow::Workflow;
use super::Operation;

pub struct Planner {
    domain: Domain,
}

#[derive(Debug)]
pub enum PlanSearchError {
    SerializationError(serde_json::error::Error),
    CannotResolveTarget {
        path: String,
        reason: jsonptr::resolve::ResolveError,
    },
    PathNotFound,
}

impl std::error::Error for PlanSearchError {}

impl Display for PlanSearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanSearchError::SerializationError(err) => err.fmt(f),
            PlanSearchError::PathNotFound => write!(f, "not found"),
            PlanSearchError::CannotResolveTarget { path, reason } => {
                write!(f, "cannot resolve target path `{}`: {}", path, reason)
            }
        }
    }
}

impl IntoError for PlanSearchError {
    fn into_error(self) -> Error {
        Error::PlanSearchFailed(self)
    }
}

impl Planner {
    pub fn new(domain: Domain) -> Self {
        Self { domain }
    }

    fn try_task(&self, task: Task, state: &Value, initial_plan: Workflow) {
        match task {
            Task::Atom { .. } => {
                // Detect loops in the plan
                // if (initial_plan.0.some())
            }
            Task::List { .. } => unimplemented!(),
        }
    }

    pub fn find_plan<S>(&self, cur: S, tgt: S) -> Result<Workflow, Error>
    where
        S: Serialize,
    {
        let cur = serde_json::to_value(cur).map_err(PlanSearchError::SerializationError)?;
        let tgt = serde_json::to_value(tgt).map_err(PlanSearchError::SerializationError)?;

        // Store the initial state and an empty plan on the stack
        let mut stack = vec![(cur, Workflow::default())];

        while let Some((cur, plan)) = stack.pop() {
            let distance = Distance::new(&cur, &tgt);

            // we reached the target
            if distance.is_empty() {
                // return the existing plan
                return Ok(plan);
            }

            for op in distance.iter() {
                // Find applicable tasks
                let path = Path::new(op.path());
                let matching = self.domain.at(path.to_str());
                if let Some((args, intents)) = matching {
                    // Calculate the target for the job path
                    let pointer = path.as_ref();
                    let target = pointer.resolve(&tgt).map_err(|e| {
                        PlanSearchError::CannotResolveTarget {
                            path: path.to_string(),
                            reason: e,
                        }
                    })?;

                    // Create the calling context for the job
                    let context = Context {
                        path,
                        args,
                        target: target.clone(),
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
