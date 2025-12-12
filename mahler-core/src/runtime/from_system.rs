use super::context::Context;
use super::error::Error;
use super::system::System;

/// Trait for types that can be initialized from a system state and a given context
///
/// See [`crate::extract`] for more info.
pub trait FromSystem: Sized {
    type Error: Into<Error> + 'static;

    /// Try to initialize an extractor from the system state and context
    fn from_system(state: &System, context: &Context) -> Result<Self, Self::Error>;

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
