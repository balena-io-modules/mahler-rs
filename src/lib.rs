pub mod context;
pub mod system;
pub mod task;

#[cfg(test)]
mod tests {
    use crate::task::{ActionTask, Task};

    use super::*;
    use context::{Context, Target};
    use system::System;

    #[test]
    fn it_works() {
        let task = ActionTask::from(|mut counter: System<i32>, Target(tgt): Target<i32>| {
            if *counter < tgt {
                *counter = *counter + 1;
            }
        });
        let action = task.bind(Context { target: 1 });
        let mut counter = 0;
        action.run(&mut counter);

        // The referenced value was modified
        assert_eq!(counter, 1);
    }
}
