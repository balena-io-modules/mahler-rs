use super::result::{IntoResult, Result};
use super::{Job, Task};
use crate::error::IntoError;
use crate::system::{Context, FromSystem, System};

pub trait Method<S, T>: Clone + Send + 'static {
    fn call(self, system: System, context: Context<S>) -> Result<Vec<Task<S>>>;

    fn into_job(self) -> Job<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
    {
        Job::from_method(self.clone())
    }

    fn into_task(self, context: Context<S>) -> Task<S>
    where
        S: Send + Sync + 'static,
        T: Send + 'static,
    {
        Job::from_method(self.clone()).into_task(context)
    }
}

impl<S> IntoResult<Vec<Task<S>>> for Vec<Task<S>> {
    fn into_result(self, _: &System) -> Result<Vec<Task<S>>> {
        Ok(self)
    }
}

macro_rules! impl_method_handler {
    (
        $first:ident, $($ty:ident),*
    ) => {
        #[allow(non_snake_case, unused)]
        impl<S, F, $($ty,)* Res> Method<S, ($($ty,)*)> for F
        where
            F: FnOnce($($ty,)*) -> Res + Clone + Send +'static,
            Res: IntoResult<Vec<Task<S>>>,
            $($ty: FromSystem<S>,)*
        {

            fn call(self, system: System, context: Context<S>) -> Result<Vec<Task<S>>> {
                $(
                    let $ty = match $ty::from_system(&system, &context) {
                        Ok(value) => value,
                        Err(failure) => return Err(failure.into_error())
                    };
                )*

                let res = (self)($($ty,)*);

                // Update the system
                res.into_result(&system)
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
