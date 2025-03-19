use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

use gustav::extract::{Target, View};
use gustav::task::*;
use gustav::worker::Worker;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Counters(HashMap<String, i32>);

fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> Effect<View<i32>> {
    if *counter < tgt {
        // Modify the counter if we are below target
        *counter += 1;
    }

    // Return the updated counter. The I/O part of the
    // effect will only be called if the job is chosen
    // in the workflow which will only happens if there are
    // changes
    Effect::of(counter).with_io(|counter| async {
        sleep(Duration::from_millis(10)).await;
        Ok(counter)
    })
}

#[tokio::test]
// this is just a placeholder test to check that library modules
// are accessible
async fn test_worker() {
    let worker = Worker::new()
        .job("/{counter}", update(plus_one))
        .initial_state(Counters(HashMap::from([
            ("one".to_string(), 0),
            ("two".to_string(), 0),
        ])))
        .seek_target(Counters(HashMap::from([
            ("one".to_string(), 2),
            ("two".to_string(), 0),
        ])));

    let worker = worker.wait(None).await.unwrap();
    let state = worker.state();
    assert_eq!(
        state,
        Counters(HashMap::from([
            ("one".to_string(), 2),
            ("two".to_string(), 0),
        ]))
    );
}
