mod action;
mod handler;

pub use action::*;

use crate::context::Context;
use handler::Handler;

pub trait Task<'system, S, T, E>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
{
    fn get_effect(&self) -> E;

    fn bind(&self, context: Context<S>) -> Action<'system, S, T, E> {
        Action::from(self.get_effect(), self.get_effect(), context)
    }
}

pub struct ActionTask<'system, S, T, E> {
    effect: E,
    _system: std::marker::PhantomData<&'system S>,
    _args: std::marker::PhantomData<T>,
}

impl<'system, S, T, E> ActionTask<'system, S, T, E> {
    pub fn from(effect: E) -> Self {
        ActionTask {
            effect,
            _system: std::marker::PhantomData::<&'system S>,
            _args: std::marker::PhantomData::<T>,
        }
    }
}

impl<'system, S, T, E> Task<'system, S, T, E> for ActionTask<'system, S, T, E>
where
    S: Clone,
    E: Handler<'system, S, T, ()>,
{
    fn get_effect(&self) -> E {
        self.effect.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::context::{Context, Target};
    use crate::system::System;
    use crate::task::{ActionTask, Task};

    fn my_task(mut counter: System<i32>, Target(tgt): Target<i32>) {
        if *counter < tgt {
            *counter = *counter + 1;
        }
    }

    #[test]
    fn it_works() {
        let task = ActionTask::from(my_task);
        let action = task.bind(Context { target: 1 });
        let mut counter = 0;
        action.run(&mut counter);

        // The referenced value was modified
        assert_eq!(counter, 1);
    }
}
