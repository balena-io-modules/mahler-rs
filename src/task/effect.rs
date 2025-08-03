use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;

type IOResult<O, E> = Pin<Box<dyn Future<Output = Result<O, E>> + Send>>;
type IO<O, E = Infallible, I = O> = Box<dyn FnOnce(I) -> IOResult<O, E> + Send>;
type Pure<O, E, I> = Box<dyn FnOnce(I) -> Result<O, E> + Send>;

/// Encode pure and IO operations on a single type
///
/// An `Effect` is a way to define an effectful operation on a system, but to also provide a
/// way to simulate/test the changes the operation will have on the system (the `pure` part of the
/// effect).
///
/// `Effect` is used as a return type for the Job handler to allow the planner to safely test the
/// Job effect without actually having to run the function.
///
/// An effect may be `Pure`, meaning it has no side-effects (is just a value), or `IO`, meaning it
/// has pure and effectful computations.
pub enum Effect<O, E = Infallible, I = O> {
    /// Pure computation with no side-effects
    Pure(Result<O, E>),
    /// Effectful computation
    IO {
        /// Input type
        input: Result<I, E>,
        /// Pure computation on the input type
        pure: Pure<O, E, I>,
        /// Effectful operation for the given input
        io: IO<O, E, I>,
    },
}

impl<O, E> Effect<O, E> {
    /// Create a pure `Effect`
    pub fn of(o: O) -> Self {
        Effect::Pure(Ok(o))
    }

    /// Create a pure `Effect` from a `Result`
    pub(crate) fn from_result(res: Result<O, E>) -> Effect<O, E> {
        Effect::Pure(res)
    }

    /// Convert the effect to an effectful computation
    ///
    /// Allows providing an computation to apply to the input value that performs side-effects on the
    /// underlying system.
    pub fn with_io<
        F: FnOnce(O) -> Res + Send + 'static,
        Res: Future<Output = Result<O, E>> + Send,
    >(
        self,
        f: F,
    ) -> Effect<O, E>
    where
        O: Send + 'static,
    {
        let io: IO<O, E> = Box::new(|o| Box::pin(async { f(o).await }));
        let pure = Box::new(|o| Ok(o));
        match self {
            Effect::Pure(output) => Effect::IO {
                input: output,
                pure,
                io,
            },
            Effect::IO { input, .. } => Effect::IO { input, pure, io },
        }
    }
}

impl<T: 'static, E: 'static, I: Send + 'static> Effect<T, E, I> {
    /// Create a pure effect from an `Error`
    pub(crate) fn from_error(e: E) -> Self {
        Effect::Pure(Err(e))
    }

    /// Transform the effect output type using a pure function
    pub fn map<O, F: FnOnce(T) -> O + Clone + Send + 'static>(self, fu: F) -> Effect<O, E, I> {
        match self {
            Effect::Pure(output) => Effect::Pure(output.map(fu)),
            Effect::IO { input, pure, io } => {
                let fc = fu.clone();
                Effect::IO {
                    input,
                    pure: Box::new(|i| pure(i).map(fc)),
                    io: Box::new(|i| {
                        Box::pin(async {
                            let o = io(i).await?;
                            Ok(fu(o))
                        })
                    }),
                }
            }
        }
    }

    /// Apply a transformation function to the effectful part of the Effect
    pub fn map_io<F: FnOnce(T) -> T + Send + 'static>(self, fu: F) -> Effect<T, E, I> {
        match self {
            Effect::Pure(output) => Effect::Pure(output),
            Effect::IO { input, pure, io } => Effect::IO {
                input,
                pure,
                io: Box::new(|i| {
                    Box::pin(async {
                        let o = io(i).await?;
                        Ok(fu(o))
                    })
                }),
            },
        }
    }

    /// Transform the effect output type using a pure function returning a Result
    pub fn and_then<O, F: FnOnce(T) -> Result<O, E> + Clone + Send + 'static>(
        self,
        fu: F,
    ) -> Effect<O, E, I> {
        match self {
            Effect::Pure(output) => Effect::Pure(output.and_then(fu)),
            Effect::IO { input, pure, io } => {
                let fc = fu.clone();
                Effect::IO {
                    input,
                    pure: Box::new(|i| pure(i).and_then(fc)),
                    io: Box::new(|i| {
                        Box::pin(async {
                            let o = io(i).await?;
                            fu(o)
                        })
                    }),
                }
            }
        }
    }

    /// Transform the error type of the Effect
    pub fn map_err<E1, F: FnOnce(E) -> E1 + Clone + Send + 'static>(
        self,
        fe: F,
    ) -> Effect<T, E1, I> {
        match self {
            Effect::Pure(output) => Effect::Pure(output.map_err(fe)),
            Effect::IO { input, pure, io } => {
                let fc1 = fe.clone();
                let fc2 = fe.clone();
                Effect::IO {
                    input: input.map_err(fc1),
                    pure: Box::new(|i| pure(i).map_err(fc2)),
                    io: Box::new(|i| Box::pin(async { io(i).await.map_err(fe) })),
                }
            }
        }
    }

    /// Run the *pure* part of the Effect
    pub fn pure(self) -> Result<T, E> {
        match self {
            Effect::Pure(output) => output,
            Effect::IO { input, pure, .. } => input.and_then(pure),
        }
    }
    /// Run the effectful (IO) part of the effect
    pub async fn run(self) -> Result<T, E> {
        match self {
            Effect::Pure(output) => output,
            Effect::IO { input, io, .. } => {
                let i = input?;
                io(i).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn it_allows_wrapping_a_value() {
        let effect: Effect<i32> = Effect::of(0);
        assert_eq!(effect.pure(), Ok(0))
    }

    #[tokio::test]
    async fn it_allows_async_executions() {
        let effect = Effect::of(0)
            .map(|x| x + 1)
            .with_io(|x| async move {
                sleep(Duration::from_millis(10)).await;
                Ok(x + 1) as Result<i32, ()>
            })
            .map(|x| format!("result: {x}"));

        assert_eq!(effect.run().await, Ok("result: 2".to_string()))
    }

    #[test]
    fn it_allows_sync_executions() {
        let effect = Effect::of(0)
            .map(|x| x + 1)
            .with_io(|x| async move {
                sleep(Duration::from_millis(10)).await;
                Ok(x + 1) as Result<i32, ()>
            })
            .map(|x| format!("result: {x}"));

        assert_eq!(effect.pure(), Ok("result: 1".to_string()))
    }

    #[test]
    fn it_allows_errors_in_sync_executions() {
        let effect =
            Effect::from_result(Err("ERROR") as Result<i32, &str>).with_io(|x| async move {
                sleep(Duration::from_millis(10)).await;
                Ok(x + 1)
            });

        assert_eq!(effect.pure(), Err("ERROR"))
    }

    #[tokio::test]
    async fn it_propagates_errors_in_async_calls() {
        let effect = Effect::of(0).with_io(|_| async move {
            sleep(Duration::from_millis(10)).await;
            Err("this is an error")
        });

        assert_eq!(effect.run().await, Err("this is an error"))
    }
}
