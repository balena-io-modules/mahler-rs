pub mod context;
pub mod state;
pub mod task;

#[cfg(test)]
mod tests {
    use super::*;
    use context::{Context, Target};
    use state::State;
    use task::Task;

    #[test]
    fn it_works() {
        let task = Task::from(|mut counter: State<i32>, Target(tgt): Target<i32>| {
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
