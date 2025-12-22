use std::sync::Arc;

use crate::result::Result;
use crate::runtime::{Context, FromSystem, System};

/// Extracts a shared system resource
///
/// This extractor will look for a resource of type `<R>` for the [Worker](`crate::worker::Worker`) and
/// provides read-only access to a resource reference if it exists.
///
/// # Example
///
/// Multiple `Res` extractors can be used in the same handler, but since `Worker` indexes resources
/// by `TypeId`, extracting the same resource twice will result in two references of the same
/// resource.
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::Res,
///     job::update,
///     worker::{Worker, Ready}
/// };
///
/// // A shared resource type
/// struct MyConnection {/* ... */};
///
/// // Another resource
/// struct MyConfig {/* ... */};
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// fn multiple_resources(conn: Res<MyConnection>, config: Res<MyConfig>) {
///     // a reference to the resources configured in the Worker below
///     // can be access within the Job handler
/// }
///
/// let mut worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(multiple_resources))
///     .resource(MyConnection {/* .. */})
///     .resource(MyConfig {/* .. */})
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
///
/// There is nothing that prevents you from making a resource editable behind a `RwLock` (for
/// instance), however, this may interfere with workflow execution.
///
/// ```rust,no_run
/// use tokio::sync::RwLock;
/// use std::ops::Deref;
/// use mahler::{
///     state::State,
///     extract::{View, Res},
///     task::{IO, with_io},
///     job::update,
///     worker::{Worker, Ready}
/// };
///
/// // An editable resource
/// struct MyConfig(RwLock<String>);
/// impl MyConfig {
///     pub fn new(s: impl Into<String>) -> Self {
///         MyConfig(RwLock::new(s.into()))
///     }
/// }
///
/// impl Deref for MyConfig {
///     type Target = RwLock<String>;
///
///     fn deref(&self) -> &Self::Target {
///         &self.0
///     }
/// }
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// fn edit_resources(view: View<i32>, config: Res<MyConfig>) -> IO<i32> {
///     // update view
///     with_io(view, |view| async move {
///         if let Some(config) = config.as_ref() {
///             // this is possible but it may interfere with the workflow execution
///             // if there are multiple writers running concurrently
///             let mut conf = config.write().await;
///             *conf = String::from("bar");
///         }
///
///         Ok(view)
///     })
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(edit_resources))
///     .resource(MyConfig::new("foo"))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct Res<R>(Option<Arc<R>>);

impl<R> Res<R> {
    /// Returns a reference to the resource wrapped in an Option
    ///
    /// Returns None if the resource is not defined on the system
    pub fn as_ref(&self) -> Option<&R> {
        self.0.as_deref()
    }
}

impl<R: Send + Sync + 'static> FromSystem for Res<R> {
    fn from_system(system: &System, _: &Context) -> Result<Self> {
        let arc = system.resource::<R>();
        Ok(Res(arc))
    }
}
