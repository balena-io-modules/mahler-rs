//! Types for defining planning exceptions
//!
//! Exceptions are the mechanism in Mahler to implement partial planning. Sometimes when seeking a
//! target state, it may be desirable to skip some operations that are expected to fail. For
//! instance, a working managing downloads may want to skip fetching an HTTP artifact if the last
//! request returned a 404. By default, the planner will look for full plan to the target state, meaning
//! the default behavior would be to download all missing images.
//!
//! Using exceptions, it is possible to tell the planner to skip certain operations if a condition
//! is met.
//!
//! An exception is a function that accepts zero or more extractors and returns a boolean. If the
//! function evaluates to true, that tells the planner to skip the path assigned to the operation
//! and look for a workflow for the remainder of the target state.
//!
//! # Example
//!
//! ```rust
//! use mahler::state::{State, Map, List};
//! use mahler::extract::{Target, View};
//! use mahler::exception;
//! use mahler::worker::Worker;
//!
//! #[derive(State)]
//! struct Service {
//!     cmd: List<String>,
//!     force: bool,
//!     
//!     // the service is dead for an unknown reason
//!     #[mahler(internal)]
//!     dead: bool
//! }
//!
//! #[derive(State, Default)]
//! struct Services(Map<String, Service>);
//!
//! // Skip dead services unless target.force
//! fn only_alive(view: View<Service>, Target(tgt): Target<Service>) -> bool {
//!     view.dead && !tgt.force
//! }
//!
//! # tokio_test::block_on(async {
//! // Add exceptions to the worker domain
//! let worker = Worker::new()
//!         // ... add jobs
//!         // add `only_alive` as an exception for update operations on all services
//!         .exception("/{service_name}", exception::update(only_alive))
//!         .initial_state(Services::default())
//!         .unwrap();
//! # })
//! ```

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::json::OperationMatcher;
use crate::result::Result;
use crate::runtime::{Context, System};

mod handler;

pub use handler::*;

type ExceptionFn = Arc<dyn Fn(&System, &Context) -> Result<bool> + Send + Sync>;

/// An atomic task
#[derive(Clone)]
pub struct Exception {
    id: &'static str,
    operation: OperationMatcher,
    exception: ExceptionFn,
}

impl Exception {
    // Create a new exception for the given operation matcher
    fn new<H, T>(exception: H, operation: OperationMatcher) -> Self
    where
        H: ExceptionHandler<T>,
    {
        Self {
            id: std::any::type_name::<H>(),
            operation,
            exception: Arc::new(move |system: &System, context: &Context| {
                exception.call(system, context)
            }),
        }
    }

    /// Test if the exception applies to the given state and context
    pub fn test(&self, system: &System, context: &Context) -> Result<bool> {
        (self.exception)(system, context)
    }

    /// Get the operation assigned to the exception
    pub fn operation(&self) -> &OperationMatcher {
        &self.operation
    }
}

impl fmt::Display for Exception {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl fmt::Debug for Exception {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exception")
            .field("id", &self.id)
            .field("operation", &self.operation)
            .finish()
    }
}

impl PartialEq for Exception {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.operation == other.operation
    }
}
impl Eq for Exception {}

impl PartialOrd for Exception {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Exception {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id
            .cmp(&other.id)
            .then(self.operation.cmp(&other.operation))
    }
}

macro_rules! define_exception {
    ($func_name:ident, $operation:expr) => {
        #[doc = concat!("Create a new `Exception` for the [`", stringify!($operation), "`] operation.")]
        pub fn $func_name<H, T>(handler: H) -> Exception
        where
            H: ExceptionHandler<T>,
        {
            Exception::new(handler, $operation)
        }
    };
}

define_exception!(create, OperationMatcher::Create);
define_exception!(update, OperationMatcher::Update);
define_exception!(delete, OperationMatcher::Delete);
define_exception!(any, OperationMatcher::Any);
