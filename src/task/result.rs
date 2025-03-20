use crate::{system::System, task::TaskError};

/// The task outcome is a type alias
/// of Result
pub type Result<O> = core::result::Result<O, TaskError>;

pub trait IntoResult<O> {
    fn into_result(self, system: &System) -> Result<O>;
}
