use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;

use crate::system::System;

pub(crate) trait IntoEffect<O, E, I = O> {
    fn into_effect(self, system: &System) -> Effect<O, E, I>;
}

impl<O, E, I> IntoEffect<O, E, I> for Effect<O, E, I> {
    fn into_effect(self, _: &System) -> Effect<O, E, I> {
        self
    }
}

type IOResult<O, E> = Pin<Box<dyn Future<Output = Result<O, E>>>>;
type IO<O, E = Infallible, I = O> = Box<dyn FnOnce(I) -> IOResult<O, E>>;
type Pure<O, E, I> = Box<dyn FnOnce(I) -> Result<O, E>>;

pub enum Effect<O, E = Infallible, I = O> {
    Pure(Result<O, E>),
    IO {
        input: Result<I, E>,
        pure: Pure<O, E, I>,
        io: IO<O, E, I>,
    },
}

impl<O, E> Effect<O, E> {
    pub fn of(o: O) -> Self {
        Effect::Pure(Ok(o))
    }

    pub fn with_io<F: FnOnce(O) -> Res + 'static, Res: Future<Output = Result<O, E>>>(
        self,
        f: F,
    ) -> Effect<O, E>
    where
        O: 'static,
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

impl<T: 'static, E: 'static, I: 'static> Effect<T, E, I> {
    pub fn from_error(e: E) -> Self {
        Effect::Pure(Err(e))
    }

    pub fn map<O, F: FnOnce(T) -> O + Clone + 'static>(self, fu: F) -> Effect<O, E, I> {
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

    pub fn map_io<F: FnOnce(T) -> T + 'static>(self, fu: F) -> Effect<T, E, I> {
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

    pub fn and_then<O, F: FnOnce(T) -> Result<O, E> + Clone + 'static>(
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

    pub fn map_err<E1, F: FnOnce(E) -> E1 + Clone + 'static>(self, fe: F) -> Effect<T, E1, I> {
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

    pub fn pure(self) -> Result<T, E> {
        match self {
            Effect::Pure(output) => output,
            Effect::IO { input, pure, .. } => input.and_then(pure),
        }
    }

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

impl<T: 'static, E: 'static> From<Result<T, E>> for Effect<T, E> {
    fn from(res: Result<T, E>) -> Effect<T, E> {
        Effect::Pure(res)
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
            .map(|x| format!("result: {}", x));

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
            .map(|x| format!("result: {}", x));

        assert_eq!(effect.pure(), Ok("result: 1".to_string()))
    }

    #[test]
    fn it_allows_errors_in_sync_executions() {
        let effect = Effect::from(Err("ERROR") as Result<i32, &str>).with_io(|x| async move {
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
