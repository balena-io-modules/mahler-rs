use super::context::Context;

use crate::result::Result;

/// Trait for types that can be initialized from a context
///
/// See [`crate::extract`] for more info.
///
/// Types created from the context do not need to know the runtime state of the system and only use
/// the task configuration for initialization. They can be used on this crate to create a task [description](`super::Description`).
pub trait FromContext: Sized {
    fn from_context(context: &Context) -> Result<Self>;
}
