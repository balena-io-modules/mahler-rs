use std::fmt::{self, Display};
use thiserror::Error;

/// A workflow error aggregating multiple errors
/// in the execution of parallel branches of the
/// graph
#[derive(Error, Debug)]
pub struct AggregateError<E>(#[from] pub Vec<E>);

impl<E> Display for AggregateError<E>
where
    E: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for e in &self.0 {
            writeln!(f, "- {}", e)?;
        }
        Ok(())
    }
}
