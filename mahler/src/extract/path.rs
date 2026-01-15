use std::fmt::{Display, Formatter};

use crate::result::Result;
use crate::runtime::{Channel, Context, FromContext, FromSystem, System};

/// Extracts the full path that the Task is being applied to
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::Path,
///     job::update,
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// fn foo_bar(Path(path): Path) {
///     // ...
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(foo_bar))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
pub struct Path(pub String);

impl FromContext for Path {
    fn from_context(context: &Context) -> Result<Self> {
        Ok(Path(context.path.to_string()))
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromSystem for Path {
    fn from_system(_: &System, context: &Context, _: &Channel) -> Result<Self> {
        Self::from_context(context)
    }
}

impl From<Path> for String {
    fn from(path: Path) -> Self {
        path.0
    }
}
