use crate::error::Error;
use crate::runtime::{Context, FromSystem, System};

/// Trait for functions that can be used to define planning exceptions
///
/// An exception is any function that accepts zero or more "[extractors](`crate::extract`)" as
/// arguments and returns a boolean. An extractor is a type that implements
/// [FromSystem](`crate::runtime::FromSystem`)
pub trait ExceptionHandler<T>: Clone + Sync + Send + 'static {
    fn call(&self, system: &System, context: &Context) -> Result<bool, Error>;
}

macro_rules! impl_exception {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<F, Res, $($ty,)*> ExceptionHandler<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> Res + Clone + Send + Sync +'static,
            Res: Into<bool>,
            $($ty: FromSystem,)*
        {
            fn call(&self, system: &System, context: &Context) -> Result<bool, Error> {
                $(
                    let $ty = match $ty::from_system(system, context) {
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

impl_exception!(T1,);
impl_exception!(T1, T2);
impl_exception!(T1, T2, T3);
impl_exception!(T1, T2, T3, T4);
impl_exception!(T1, T2, T3, T4, T5);
impl_exception!(T1, T2, T3, T4, T5, T6);
impl_exception!(T1, T2, T3, T4, T5, T6, T7);
impl_exception!(T1, T2, T3, T4, T5, T6, T7, T8);
