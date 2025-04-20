use std::{convert::Infallible, future::Future};

use super::effect::Effect;
use crate::extract::{Pointer, View};

/// Creates an effect from a pure value and an I/O function
///
/// This function is useful for creating an effect that
/// combines a pure value with an I/O operation.
pub fn with_io<T, E, F, Res>(pure: T, io: F) -> Effect<T, E>
where
    T: Send + 'static,
    F: FnOnce(T) -> Res + Send + 'static,
    Res: Future<Output = Result<T, E>> + Send,
{
    Effect::of(pure).with_io(io)
}

/// Convenience aliases for the most common return types from effectful
/// tasks
pub type IO<T, E = Infallible> = Effect<View<T>, E>;

pub type Update<T, E = Infallible> = Effect<View<T>, E>;
pub type Delete<T, E = Infallible> = Effect<Pointer<T>, E>;
pub type Create<T, E = Infallible> = Effect<Pointer<T>, E>;
pub type Any<T, E = Infallible> = Effect<Pointer<T>, E>;
