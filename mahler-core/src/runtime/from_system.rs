use super::context::Context;
use super::system::System;

use crate::result::Result;

/// Trait for types that can be initialized from a system state and a given context
pub trait FromSystem: Sized {
    /// Try to initialize an extractor from the system state and context
    fn from_system(state: &System, context: &Context) -> Result<Self>;

    /// Return true if the extractor is scoped
    ///
    /// The extractor is scoped if it only grants access to some
    /// part of the system state rather than to the global state.
    ///
    /// All extractors are scoped by default, but implementors can override this method to indicate
    /// that the extractor is not scoped.
    fn is_scoped() -> bool {
        true
    }
}
