use std::ops::Deref;

use crate::error::{Error, ErrorKind};
use crate::result::Result;
use crate::runtime::{Channel, Context, FromContext, FromSystem, System};
use crate::serde::de::DeserializeOwned;
use crate::state::State;

/// Extracts the target state for tasks created from the job handler
///
/// Allows the Job to see the desired state for the path assigned to the task.
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::Target,
///     job::update,
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct MySystem {/* ... */};
///
/// fn with_target(Target(tgt): Target<i32>) {
///     // ...
/// }
///
/// let worker = Worker::new()
///     .job("/{foo}/{bar}", update(with_target))
///     .initial_state(MySystem {/* ... */})
///     .unwrap();
/// ```
///
/// Note that for non-primitive types, this extractor requires that the type implements
/// [State](`crate::state::State`)
///
/// ```rust,no_run
/// use mahler::{
///     state::{State, List},
///     extract::Target,
///     job::update,
///     worker::Worker
/// };
///
/// #[derive(State)]
/// struct Service {
///     name: String,
/// };
///
/// #[derive(State)]
/// struct App {
///     services: List<Service>,
/// }
///
/// // Service needs to implement State to use the target extractor
/// fn with_target(Target(tgt): Target<Service>) {
///     // ...
/// }
///
/// let worker = Worker::new()
///     .job("/{foo}/{bar}", update(with_target))
///     .initial_state(App {services: List::new()})
///     .unwrap();
/// ```
///
/// # Errors
///
/// Initializing the extractor will fail if the target value for the task cannot be deserialized
/// into the given type `<T>`. This includes the case where no target exists, for instance, for
/// `delete` type jobs.
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::Target,
///     job::delete,
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// // this task is applicable to `delete` operations which do not
/// // have a target, thus this extractor will always fail to be initialized
/// fn invalid_task(Target(tgt): Target<i32>) {
///     // ...
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     // `invalid_task` will be called if a delete operation on path `/{foo}/{bar}`
///     // is required
///     .job("/{foo}/{bar}", delete(invalid_task))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct Target<T: State>(pub T::Target);

impl<T: State> FromContext for Target<T> {
    fn from_context(context: &Context) -> Result<Self> {
        // resolve the context path on the target state unless the user set
        // an override via `with_target`
        let value = if let Some(target) = context.target_override.as_ref() {
            target
        } else {
            context
                .path
                .as_ref()
                .resolve(&context.target)
                // the path is assumed to be correct at this point, so
                // unwrap the result to Null if a resolution error happens
                .unwrap_or(&serde_json::Value::Null)
        };

        // This will fail if the value cannot be deserialized into the target type
        let target = serde_json::from_value::<T::Target>(value.clone())
            .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?;

        Ok(Target(target))
    }
}

impl<T: State> FromSystem for Target<T> {
    fn from_system(_: &System, context: &Context, _: &Channel) -> Result<Self> {
        Self::from_context(context)
    }
}

impl<T: State> Deref for Target<T> {
    type Target = T::Target;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Extracts the target state for tasks created from the job handler
///
/// Allows the Job to see the desired state for the path assigned to the task.
///
/// Note that unlike [`crate::extract::Target`], this extractor doesn't require the inner type
/// to implement `State`, only `Deserialize`
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::RawTarget,
///     job::update,
///     worker::{Worker, Ready}
/// };
/// use serde::Deserialize;
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// #[derive(Deserialize)]
/// struct Num(i32);
///
/// // MyApp does not need to implement `State`
/// fn with_target(RawTarget(tgt): RawTarget<Num>) {
///     // ...
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(with_target))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct RawTarget<T>(pub T);

impl<T: DeserializeOwned> FromContext for RawTarget<T> {
    fn from_context(context: &Context) -> Result<Self> {
        // resolve the context path on the target state unless the user set
        // an override via `with_target`
        let value = if let Some(target) = context.target_override.as_ref() {
            target
        } else {
            context
                .path
                .as_ref()
                .resolve(&context.target)
                // the path is assumed to be correct at this point, so
                // unwrap the result to Null if a resolution error happens
                .unwrap_or(&serde_json::Value::Null)
        };

        // This will fail if the value cannot be deserialized into the target type
        let target = serde_json::from_value::<T>(value.clone())
            .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?;

        Ok(Self(target))
    }
}

impl<T: DeserializeOwned> FromSystem for RawTarget<T> {
    fn from_system(_: &System, context: &Context, _: &Channel) -> Result<Self> {
        Self::from_context(context)
    }
}

impl<T> Deref for RawTarget<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Extracts the global target given to the planner for the current task
///
/// # Example
///
/// ```rust,no_run
/// use mahler::{
///     state::State,
///     extract::SystemTarget,
///     job::update,
///     worker::{Worker, Ready}
/// };
///
/// #[derive(State)]
/// struct SystemState {/* ... */};
///
/// // Perform an operation based on the global target state
/// fn with_system_target(SystemTarget(tgt): SystemTarget<SystemState>) {
///     // ...
/// }
///
/// let worker: Worker<SystemState, Ready> = Worker::new()
///     .job("/{foo}/{bar}", update(with_system_target))
///     .initial_state(SystemState {/* ... */})
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct SystemTarget<T: State>(pub T::Target);

impl<T: State> FromContext for SystemTarget<T> {
    fn from_context(context: &Context) -> Result<Self> {
        let value = &context.target;

        // This will fail if the value cannot be deserialized into the target type
        let target = serde_json::from_value::<T::Target>(value.clone())
            .map_err(|e| Error::new(ErrorKind::CannotDeserializeArg, e))?;

        Ok(Self(target))
    }
}

impl<T: State> FromSystem for SystemTarget<T> {
    fn from_system(_: &System, context: &Context, _: &Channel) -> Result<Self> {
        Self::from_context(context)
    }
}

impl<T: State> Deref for SystemTarget<T> {
    type Target = T::Target;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
