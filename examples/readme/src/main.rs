use std::time::Duration;
use tokio::time::sleep;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use mahler::extract::{Args, Target, View};
use mahler::job::update;
use mahler::result::Result;
use mahler::state::{Map, State};
use mahler::task::{enforce, with_io, Handler, Task, IO};
use mahler::worker::{SeekStatus, Worker};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

// The state model needs to be Serializable and Deserializable
// since the library uses JSON internally to access parts
// of the state
#[derive(State, Debug, PartialEq, Eq)]
struct Counters(Map<String, i32>);

// `plus_one` defines a job that updates a counter if it is below some target.
// The job makes use of two extractors:
// - `View`, that provides a mutable view into the system state. By modifying the view,
// the job task can affect the global state
// - `Target`, providing a read only view to the target being seeked by the planner
fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
    // abort the task if the target has already been reached
    enforce!(*counter < tgt);

    // Modify the counter value if we are below the target
    *counter += 1;

    // The task is called at planning and at runtime, the `with_io` function
    // allows us to define what is returned by the function at each context.
    // The first argument of the function is what the planner receives,
    // the right side of the call is what will be executed at runtime if the
    // task is selected.
    with_io(counter, |counter| async {
        // The async call can be used to actually make changes to the underlying system.
        // It could be writing the counter to a database or a file. In this
        // case we just add some timer
        sleep(Duration::from_millis(10)).await;
        Ok(counter)
    })
}

// `plus_two` is a compound job. Compound job do not modify the state directly
// but return combination of sub-tasks that are applicable to a certain target
fn plus_two(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
    // If the difference between the current state and target is >1
    if tgt - *counter > 1 {
        // Then return a sequence of two tasks. The target for the tasks is
        // automatically inherited from the method
        return vec![plus_one.into_task(), plus_one.into_task()];
    }

    // Otherwise do nothing
    vec![]
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber with custom formatting
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            fmt::layer()
                .with_writer(std::io::stderr)
                .with_span_events(FmtSpan::CLOSE)
                .event_format(fmt::format().compact().with_target(false)),
        )
        .init();

    let worker = Worker::new()
        // The jobs are applicable to `UPDATE` operations
        // on individual counters
        .job(
            "/{counter}",
            update(plus_one)
                // we can add a description to the job for the logs
                .with_description(|Args(counter): Args<String>| format!("{counter}++")),
        )
        .job("/{counter}", update(plus_two))
        // We initialize the worker with two counters
        // `a` and `b` with value 0
        .initial_state(Counters(Map::from([
            ("a".to_string(), 0),
            ("b".to_string(), 0),
        ])))?;

    // Tell the worker to find a plan from the initial state (a:0, b:0)
    // to the target state (a:1, b:2) and execute it
    let (state, status) = worker
        .seek_target(CountersTarget(Map::from([
            ("a".to_string(), 1),
            ("b".to_string(), 2),
        ])))
        .await?;

    assert_eq!(status, SeekStatus::Success);
    assert_eq!(
        state,
        Counters(Map::from([("a".to_string(), 1), ("b".to_string(), 2),]))
    );

    println!("The system state is now {state:?}");
    Ok(())
}
