use crate::runtime::{Context, Error, FromContext};

/// Trait for functions that can be used to describe a Job/Task
///
/// A description is any function that accepts zero or more context "[extractors](`crate::extract`)" as
/// arguments and returns a String. A context extractor is a type that implements
/// [FromContext](`super::FromContext`)
///
/// Supported context extractors are
///
/// - [`crate::extract::Target`] to extract the task target
/// - [`crate::extract::Args`] to extract the task args
/// - [`crate::extract::Path`] to extract the task path
pub trait Description<T>: Clone + Sync + Send + 'static {
    fn call(&self, context: &Context) -> Result<String, Error>;
}

macro_rules! impl_description {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, Res, $($ty,)*> Description<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: Into<String>,
            $($ty: FromContext,)*
        {
            fn call(&self, context: &Context) -> Result<String, Error> {
                $(
                    let $ty = match $ty::from_context(context) {
                        Ok(value) => value,
                        Err(failure) => {
                            return Err(failure.into())
                        }
                    };
                )*

                Ok((self)($($ty,)*).into())
            }

        }
    };
}

impl_description!(T1,);
impl_description!(T1, T2);
impl_description!(T1, T2, T3);
impl_description!(T1, T2, T3, T4);
impl_description!(T1, T2, T3, T4, T5);
impl_description!(T1, T2, T3, T4, T5, T6);
impl_description!(T1, T2, T3, T4, T5, T6, T7);
impl_description!(T1, T2, T3, T4, T5, T6, T7, T8);
