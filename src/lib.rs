pub struct Context<S>
where
    S: Clone,
{
    target: S,
}

pub trait FromContext<S>
where
    S: Clone,
{
    fn from_context(state: &S, context: &Context<S>) -> Self;
}

pub struct State<T>(pub T);
pub struct Target<T>(pub T);

impl<S> FromContext<S> for State<S>
where
    S: Clone,
{
    fn from_context(state: &S, _: &Context<S>) -> Self {
        State(state.clone())
    }
}

impl<S> FromContext<S> for Target<S>
where
    S: Clone,
{
    fn from_context(_: &S, context: &Context<S>) -> Self {
        Target(context.target.clone())
    }
}

pub trait Handler<T, S>
where
    S: Clone,
{
    fn call(&self, state: S, context: Context<S>);
}

impl<F, S> Handler<(), S> for F
where
    F: Fn(),
    S: Clone,
{
    fn call(&self, _: S, _: Context<S>) {
        (self)();
    }
}

impl<F, S, T1> Handler<(T1,), S> for F
where
    F: Fn(T1),
    S: Clone,
    T1: FromContext<S>,
{
    fn call(&self, state: S, context: Context<S>) {
        (self)(T1::from_context(&state, &context));
    }
}

impl<F, S, T1, T2> Handler<(T1, T2), S> for F
where
    F: Fn(T1, T2),
    S: Clone,
    T1: FromContext<S>,
    T2: FromContext<S>,
{
    fn call(&self, state: S, context: Context<S>) {
        (self)(
            T1::from_context(&state, &context),
            T2::from_context(&state, &context),
        );
    }
}

pub struct Action<S, T, H>
where
    S: Clone,
    H: Handler<T, S>,
{
    handler: H,
    context: Context<S>,
    _marker: std::marker::PhantomData<T>,
}

impl<S, T, H> Action<S, T, H>
where
    S: Clone,
    H: Handler<T, S>,
{
    pub fn from(effect: H, context: Context<S>) -> Self {
        Action {
            handler: effect,
            context,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn run(self, state: S) {
        self.handler.call(state, self.context);
    }
}

pub struct Task<S, T, H>
where
    S: Clone,
    H: Handler<T, S>,
{
    pub handler: H,
    state: std::marker::PhantomData<S>,
    _marker: std::marker::PhantomData<T>,
}

impl<S, T, H> Task<S, T, H>
where
    S: Clone,
    H: Handler<T, S>,
{
    pub fn from(handler: H) -> Self {
        Task {
            handler,
            state: std::marker::PhantomData,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn bind(self, context: Context<S>) -> Action<S, T, H> {
        Action::from(self.handler, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let task = Task::from(|State(counter): State<i32>, Target(tgt): Target<i32>| {
            if counter < tgt {
                println!("counter: {}", counter);
            }

            assert_eq!(counter, 1);
        });
        let action = task.bind(Context { target: 1 });
        action.run(1)
    }
}
