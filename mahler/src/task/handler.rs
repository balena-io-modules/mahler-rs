use super::effect::Effect;
use super::id::Id;
use super::{Action, Method, Task};

use crate::error::Error;
use crate::json::Patch;
use crate::runtime::{Channel, Context, FromSystem, System};
use crate::serde::Serialize;

/// Trait for functions that can be converted into tasks
///
/// A Handler is any function that accepts zero or more "[extractors](`crate::extract`)" as
/// arguments and returns something that can be converted into an Effect on the
/// system.
pub trait Handler<T, O, I = O>: Clone + Sync + Send + 'static {
    /// Execute the handler
    ///
    /// This is never called directly by the library users but it is used by mahler to simulate or
    /// run the handler on the system in the right context
    fn call(&self, system: &System, context: &Context, channel: &Channel) -> Effect<O, Error, I>;

    /// Get the unique identifier of the job handler
    ///
    /// The identifier is given by the type name of `Self`
    ///
    /// This is used accross the library to search for tasks for tracing purposes and human
    /// readable logs.
    fn id(&self) -> Id {
        Id::from(std::any::type_name::<Self>())
    }

    /// Create a task from the handler using the default [Context](`crate::runtime::Context`)
    ///
    /// The generated task can be modified using [`Task::with_target`] and [`Task::with_arg`]
    fn into_task(self) -> Task;

    /// Create a task from the handler with a specific target
    ///
    /// This is a convenience method that is equivalent to calling
    /// `handler.into_task().with_target(target)`.
    ///
    /// Important: This function will panic if serialization of the target into JSON fails
    /// Use [`Task::try_target`] if you want to handle the error. This is done for convenience as
    /// serialization errors should be rare and this makes the code more concise.
    ///
    /// ```rust
    /// use mahler::task::{Handler, Task};
    ///
    /// fn foo() {}
    ///
    /// // Assign the value of the `foo` path argument to the task.
    /// let task = foo.with_target(10);
    /// ```
    fn with_target<S: Serialize>(self, target: S) -> Task {
        self.into_task().with_target(target)
    }

    /// Create a task from the handler with a specific path argument
    ///
    /// This is a convenience method that is equivalent to calling
    /// `handler.into_task().with_arg()`.
    ///
    /// ```rust
    /// use mahler::task::Handler;
    ///
    /// fn foo() {}
    ///
    /// // Assign the value of the `foo` path argument to the task.
    /// let task = foo.with_arg("foo", "123");
    /// ```
    fn with_arg(self, key: impl AsRef<str>, value: impl Into<String>) -> Task {
        self.into_task().with_arg(key, value)
    }
}

macro_rules! impl_action_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, $($ty,)* Res, I> Handler<($($ty,)*), Patch, I> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: Into<Effect<Patch, Error, I>> + Send,
            $($ty: FromSystem,)*
            I: Send + 'static
        {

            fn call(&self, system: &System, context: &Context, channel: &Channel) -> Effect<Patch, Error, I>{
                $(
                    let $ty = match $ty::from_system(system, context, channel) {
                        Ok(value) => value,
                        Err(failure) => {
                            return Effect::from_error(failure.into())
                        }
                    };
                )*

                let res = (self)($($ty,)*);

                // Convert to effect
                res.into()
            }

            fn into_task(self) -> Task {
                let is_scoped = true $(&& $ty::is_scoped())*;
                Action::new(self, Context::default(), is_scoped).into()
            }
        }
    };
}

impl_action_handler!(T1,);
impl_action_handler!(T1, T2);
impl_action_handler!(T1, T2, T3);
impl_action_handler!(T1, T2, T3, T4);
impl_action_handler!(T1, T2, T3, T4, T5);
impl_action_handler!(T1, T2, T3, T4, T5, T6);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_action_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

macro_rules! impl_method_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, $($ty,)* Res> Handler<($($ty,)*), Vec<Task>> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: Into<Effect<Vec<Task>, Error>>,
            $($ty: FromSystem,)*
        {

            fn call(&self, system: &System, context: &Context, channel: &Channel) -> Effect<Vec<Task>, Error> {
                $(
                    let $ty = match $ty::from_system(system, context, channel) {
                        Ok(value) => value,
                        Err(failure) => {
                            return Effect::from_error(failure.into())
                        }
                    };
                )*

                let res = (self)($($ty,)*);

                // Convert to effect
                res.into()
            }


            fn into_task(self) -> Task {
                Method::new(self, Context::default()).into()
            }
        }
    };
}

impl_method_handler!(T1,);
impl_method_handler!(T1, T2);
impl_method_handler!(T1, T2, T3);
impl_method_handler!(T1, T2, T3, T4);
impl_method_handler!(T1, T2, T3, T4, T5);
impl_method_handler!(T1, T2, T3, T4, T5, T6);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_method_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
