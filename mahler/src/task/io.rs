use std::{convert::Infallible, future::Future};

use json_patch::Patch;

use super::effect::Effect;
use super::into_result::IntoResult;

use crate::error::{Error, ErrorKind};
use crate::extract::View;
use crate::serde::Serialize;

/// An opaque type wrapping the outcome of an IO operation
pub struct Outcome<T, E>(Effect<View<T>, E>);

/// A type representing a lazy I/O operation producing a value of type `T` or an error of type `E`.
///
/// `IO<T, E>` represents an operation that:
/// - Accesses a mutable reference to state of type `T` via [`View<T>`]
/// - Can perform asynchronous I/O operations
/// - May fail with an error of type `E`
///
/// Internally, the IO type combines a "pure" computation, which is used during planning and an
/// effectful computation that will be used at runtime.
///
/// This type is primarily used as the return type for job handlers that need to:
/// 1. Modify system state
/// 2. Perform side effects (like network calls, file I/O, etc.)
/// 3. Handle potential errors
///
/// # Type Parameters
///
/// - `T`: The type of value being operated on in the system state
/// - `E`: The error type for I/O operations (defaults to [`Infallible`] for infallible operations)
///
/// # Example
///
/// ```
/// use mahler::{
///     extract::{View, Target},
///     task::{with_io, IO},
/// };
///
/// // A simple job that increments a counter
/// fn plus_one(
///     mut counter: View<i32>,
///     Target(target): Target<i32>
/// ) -> IO<i32> {
///     if *counter < target {
///         *counter += 1;
///     }
///
///     // Perform async I/O operation
///     with_io(counter, |counter| async move {
///         // Simulate some async work
///         tokio::time::sleep(std::time::Duration::from_millis(10)).await;
///         Ok(counter)
///     })
/// }
/// ```
pub enum IO<T, E = Infallible> {
    /// There is an IO result from the task
    Result(Outcome<T, E>),
    /// The task ahas
    Aborted(String),
}

impl<T, E> IO<T, E> {
    pub fn abort(msg: impl Into<String>) -> Self {
        IO::Aborted(msg.into())
    }
}

impl<T: Send + 'static, E: 'static> IO<T, E> {
    /// Transform the output returned by the operation
    ///
    /// Applies a function over both the pure and effectful parts of the type.
    ///
    /// The function receives a [View](`crate::extract::View`) as input
    ///
    /// # Examples
    ///
    /// ```
    /// # use mahler::{extract::View, task::{with_io, IO}};
    /// # use std::convert::Infallible;
    /// fn double_counter(mut counter: View<i32>) -> IO<i32, Infallible> {
    ///     with_io(counter, |counter| async move {
    ///         Ok(counter)
    ///     })
    ///     .map(|mut counter| {
    ///         *counter = *counter * 2;
    ///         counter
    ///     })
    /// }
    /// ```
    pub fn map<F>(self, fu: F) -> Self
    where
        F: FnOnce(View<T>) -> View<T> + Clone + Send + 'static,
    {
        use IO::*;
        match self {
            Result(Outcome(eff)) => Result(Outcome(eff.map(fu))),
            Aborted(..) => self,
        }
    }

    /// Chain a fallible operation that can transform the value or produce an error.
    ///
    /// Similar to `map`, but the transformation function can fail. If the function
    /// returns an `Err`, the entire IO operation fails with that error.
    ///
    /// # Examples
    ///
    /// ```
    /// # use mahler::{extract::View, task::{with_io, IO}};
    /// fn validate_positive(mut value: View<i32>) -> IO<i32, String> {
    ///     with_io(value, |value| async move {
    ///         Ok(value)
    ///     })
    ///     .and_then(|val| {
    ///         if *val >= 0 {
    ///             Ok(val)
    ///         } else {
    ///             Err("Value must be non-negative".to_string())
    ///         }
    ///     })
    /// }
    /// ```
    pub fn and_then<F>(self, fu: F) -> Self
    where
        F: FnOnce(View<T>) -> Result<View<T>, E> + Clone + Send + 'static,
    {
        use IO::*;
        match self {
            Result(Outcome(eff)) => Result(Outcome(eff.and_then(fu))),
            Aborted(..) => self,
        }
    }

    /// Transform the error type of this IO operation.
    ///
    /// Maps any error that occurs during the operation to a new error type.
    /// This is useful for converting between different error types or adding
    /// context to errors.
    ///
    /// # Examples
    ///
    /// ```
    /// # use mahler::{extract::View, task::{with_io, IO}};
    /// fn convert_error(view: View<String>) -> IO<String, String> {
    ///     with_io(view, |view| async move {
    ///         // Simulate an operation that might fail
    ///         if view.is_empty() {
    ///             Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "empty"))
    ///         } else {
    ///             Ok(view)
    ///         }
    ///     })
    ///     .map_err(|io_err| format!("I/O error: {}", io_err))
    /// }
    /// ```
    pub fn map_err<E1, F>(self, fe: F) -> IO<T, E1>
    where
        F: FnOnce(E) -> E1 + Clone + Send + 'static,
    {
        use IO::*;
        match self {
            Result(Outcome(eff)) => Result(Outcome(eff.map_err(fe))),
            Aborted(msg) => Aborted(msg),
        }
    }
}

/// Convert an IO operation into the internal effect representation.
///
/// This conversion allows IO operations to be executed by the workflow engine.
/// Any I/O errors are wrapped as [`Error`] with [`ErrorKind::Runtime`] and the final
/// result is converted to a JSON patch representing the state changes.
impl<T, E> From<IO<T, E>> for Effect<Patch, Error, View<T>>
where
    T: Serialize + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(io: IO<T, E>) -> Self {
        use IO::*;
        match io {
            Result(Outcome(eff)) => eff
                .map_err(Error::runtime)
                .and_then(|view| view.into_result()),
            Aborted(msg) => Effect::from_error(Error::new(ErrorKind::ConditionNotMet, msg)),
        }
    }
}

/// Creates an [`IO`] operation from a [`View`] and an asynchronous I/O function.
///
/// This function combines a pure state modification (via the `View`) with an
/// asynchronous I/O operation. The I/O function receives the view and must
/// return a `Future` that resolves to a `Result<View<T>, E>`.
///
/// This is the primary way to create IO operations that perform side effects
/// like network requests, file operations, or other async work.
///
/// # Parameters
///
/// - `pure`: The initial [`View<T>`] containing the state to operate on
/// - `io`: An async function that performs the I/O operation and returns the modified view
///
/// # Examples
///
/// ```
/// use mahler::{
///     extract::{View, Target},
///     task::{with_io, IO},
/// };
/// use std::time::Duration;
///
/// fn plus_one(
///     mut counter: View<i32>,
///     Target(target): Target<i32>
/// ) -> IO<i32, Box<dyn std::error::Error + Send + Sync>> {
///     if *counter < target {
///         *counter += 1;
///     }
///     
///     // Perform async I/O (e.g., save to database, send notification, etc.)
///     with_io(counter, |counter| async move {
///         // Simulate async work
///         tokio::time::sleep(Duration::from_millis(100)).await;
///         
///         // Could perform actual I/O here:
///         // - Database operations
///         // - HTTP requests  
///         // - File I/O
///         // - etc.
///         
///         Ok(counter)
///     })
/// }
/// ```
pub fn with_io<T, E, F, Res>(pure: View<T>, io: F) -> IO<T, E>
where
    T: Send + 'static,
    F: FnOnce(View<T>) -> Res + Send + 'static,
    Res: Future<Output = Result<View<T>, E>> + Send,
{
    IO::Result(Outcome(Effect::of(pure).with_io(io)))
}

/// Enforces a condition and returns early with an aborted IO if the condition is false.
///
/// This macro is similar to `assert!` but instead of panicking, it returns an
/// [`IO::Aborted`] with a formatted error message.
///
/// # Examples
///
/// ```
/// # use mahler::{extract::{View, Target}, task::{with_io, IO, enforce}};
/// fn increment_if_below_limit(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
///     // With a custom message
///     enforce!(*counter < tgt, "counter {} exceeds limit {}", *counter, tgt);
///
///     *counter += 1;
///     with_io(counter, |counter| async move { Ok(counter) })
/// }
///
/// fn another_example(mut value: View<i32>) -> IO<i32> {
///     // Without a message - uses default
///     enforce!(*value >= 0);
///
///     with_io(value, |value| async move { Ok(value) })
/// }
/// ```
#[macro_export]
macro_rules! enforce {
    ($cond:expr) => {
        if !($cond) {
            return $crate::task::IO::abort("condition failed");
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            return $crate::task::IO::abort(format!($($arg)+));
        }
    };
}
