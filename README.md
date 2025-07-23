# mahler

`mahler` is an automated job orchestration library that builds and executes dynamic workflows.

[![Crates.io](https://img.shields.io/crates/v/mahler)](https://crates.io/crates/mahler)
[![Documentation](https://docs.rs/mahler/badge.svg)](https://docs.rs/mahler)

More information about this crate can be found in the [crate documentation](https://docs.rs/mahler/latest/mahler/).

> [!WARNING]  
> This project is still quite experimental, while the API is reaching a relatively stable state, it may still be subjected to change.
> Same thing with the internal planning and execution model, it is very much still in a _make it work_ stage and there are still
> a lot of optimizations that can be made.

## Overview

Modern infrastructure requires intelligent automation that can adapt to changing conditions. Traditional static workflows become unmanageable as system complexity grows - you end up with brittle scripts that break when conditions change.

Mahler solves this by using [automated planning](https://en.wikipedia.org/wiki/Automated_planning_and_scheduling) to generate workflows dynamically. Instead of writing complex conditional logic, you define simple jobs with their constraints. The planner automatically discovers the right sequence of operations to reach your target state.

## Core Features

- **Job-based architecture** - Define operations as Rust functions that operate on typed state
- **Parallel execution** - Planner detects when jobs can run concurrently based on state paths
- **JSON state model** - System state represented as JSON with path-based job targeting
- **Effect isolation** - Jobs separate planning logic from actual I/O operations
- **Automatic re-planning** - Re-computes workflow when runtime conditions change
- **Structured logging** - Built-in tracing integration for workflow execution monitoring

## Basic Usage

In Mahler, a **Job** is a reusable operation you define (like "increment counter"). A **Task** is that job applied to a specific context (like "increment counter 'a' to value 5").

This separation lets the planner compose your jobs into workflows that achieve complex state transitions. Jobs are evaluated during planning to determine applicability to a given target and later executed at runtime. This duality needs to be built into the job definition.

We'll create a system controller for a simple counting system. Let's define a job that operates on i32

```rust
use std::time::Duration;
use tokio::time::sleep;

use mahler::task::prelude::*;
use mahler::extract::{Target, View};

// `plus_one` defines a job that updates a counter if it is below some target.
// The job makes use of two extractors:
// - `View`, that provides a mutable view into the system state. By modifying the view,
// the job task can affect the global state
// - `Target`, providing a read only view to the target being seeked by the planner
fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> Update<i32> {
    if *counter < tgt {
        // Modify the counter value if we are below the target
        *counter += 1;
    }

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
```

The job above updates the counter if it is below the target, otherwise it returns the same value that it currently has. When planning, a job/task that perform no changes on the system state is not selected, which allows us to control when the job is considered applicable.

The job above defines an atomic task, but we can also define compound tasks, that allow to bias the planner to certain workflows depending on the conditions. Let's define job to increase the counter by `two`.

```rust
use mahler::task::prelude::*;
use mahler::extract::{Target, View};

// `plus_two` is a compound job. Compound job do not modify the state directly
// but return combination of sub-tasks that are applicable to a certain target
fn plus_two(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
    // If the difference between the current state and target is >1
    if tgt - *counter > 1 {
        // Then return a sequence of two tasks with the same target
        return vec![plus_one.with_target(tgt), plus_one.with_target(tgt)];
    }

    // Otherwise do nothing
    vec![]
}
```

To use the jobs, we need to create a system model where the jobs will be applied. We'll use
a HashMap.

```rust
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

// The state model needs to be Serializable and Deserializable
// since the library uses JSON internally to access parts
// of the state
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Counters(HashMap<String, i32>);
```

Finally in order to create and run workflows we need a `Worker`:

```rust
use mahler::worker::Worker;
use mahler::task::prelude::*;

let mut worker = Worker::new()
    .job("/{counter}", update(plus_one))
    .job("/{counter}", update(plus_two))
    .initial_state(Counters(HashMap::from([
        ("a".to_string(), 0),
        ("b".to_string(), 0),
    ])))
    .unwrap();

worker.seek_target(Counters(HashMap::from([
    ("a".to_string(), 1),
    ("b".to_string(), 2),
])))
.await?;

println!("Final state: {:?}", worker.state().await?);
```

### Complete Example

Here's the full runnable example with logging and error handling:

```rust
use anyhow::{Context, Result};
use mahler::worker::Worker;
use mahler::task::prelude::*;
use mahler::extract::Args;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize structured logging with tracing-subscriber
    // This provides human-readable logs for workflow execution monitoring
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            fmt::layer()
                .with_span_events(FmtSpan::CLOSE)
                .event_format(fmt::format().compact().with_target(false)),
        )
        .init();

    let mut worker = Worker::new()
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
        .initial_state(Counters(HashMap::from([
            ("a".to_string(), 0),
            ("b".to_string(), 0),
        ])))
        .with_context(|| "failed to serialize initial state")?;

    // Tell the worker to find a plan from the initial state (a:0, b:0)
    // to the target state (a:1, b:2) and execute it
    worker
        .seek_target(Counters(HashMap::from([
            ("a".to_string(), 1),
            ("b".to_string(), 2),
        ])))
        .await
        .with_context(|| "failed to reach target state")?;

    // Get the internal state from the Worker. The worker
    // is idle but the state may not be static so we need
    // to use an await to get the current state.
    let state = worker
        .state()
        .await
        .with_context(|| "failed to deserialize state")?;

    assert_eq!(
        state,
        Counters(HashMap::from([("a".to_string(), 1), ("b".to_string(), 2),]))
    );

    println!("The system state is now {:?}", state);
    Ok(())
}
```

When receiving a call to `seek_target`, the worker looks for a plan to get the system to the given target state. The plan can be seen in the logs further down, but its representation is

```
+ ~ - a++
  ~ - b++
- b++
```

which is equivalent to the following graph

```mermaid
graph TD
    start(( ))
    fork(( ))
    start --> fork
    a0(a++)
    fork --> a0
    fork --> b0
    b0(b++)
    join(( ))
    a0 --> join
    b0 --> join
    b1(b++)
    join --> b1
    stop(( ))
    b1 --> stop
    start:::initial
    stop:::finish
    classDef initial stroke:#000,fill:#fff
    classDef finish stroke:#000,fill:#000
```

The full logs generated by the worker are below.

```sh
> RUST_LOG=debug cargo run
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.74s
     Running `target/debug/mahler-examples-readme`
2025-07-23T19:55:39.163508Z  INFO seek_target: applying target state
2025-07-23T19:55:39.163574Z  INFO seek_target: searching workflow
2025-07-23T19:55:39.163662Z DEBUG seek_target: pending changes:
2025-07-23T19:55:39.163672Z DEBUG seek_target: - {"op":"replace","path":"/a","value":1}
2025-07-23T19:55:39.163706Z DEBUG seek_target: - {"op":"replace","path":"/b","value":2}
2025-07-23T19:55:39.163858Z  INFO seek_target: workflow found time=276.708µs
2025-07-23T19:55:39.163871Z DEBUG seek_target: will execute the following tasks:
2025-07-23T19:55:39.163894Z DEBUG seek_target: + ~ - a++
2025-07-23T19:55:39.163900Z DEBUG seek_target:   ~ - b++
2025-07-23T19:55:39.163905Z DEBUG seek_target: - b++
2025-07-23T19:55:39.163911Z  INFO seek_target: executing workflow
2025-07-23T19:55:39.163938Z  INFO seek_target:run_task: starting task=a++
2025-07-23T19:55:39.163976Z  INFO seek_target:run_task: starting task=b++
2025-07-23T19:55:39.177845Z  INFO seek_target:run_task: close time.busy=55.8µs time.idle=13.8ms task=a++
2025-07-23T19:55:39.177908Z  INFO seek_target:run_task: close time.busy=22.8µs time.idle=13.9ms task=b++
2025-07-23T19:55:39.178029Z  INFO seek_target:run_task: starting task=b++
2025-07-23T19:55:39.191978Z  INFO seek_target:run_task: close time.busy=77.2µs time.idle=13.9ms task=b++
2025-07-23T19:55:39.192052Z  INFO seek_target: workflow executed successfully time=28.139666ms
2025-07-23T19:55:39.192064Z  INFO seek_target: searching workflow
2025-07-23T19:55:39.192111Z DEBUG seek_target: nothing to do
2025-07-23T19:55:39.192119Z  INFO seek_target: target state applied
2025-07-23T19:55:39.192140Z  INFO seek_target: close time.busy=958µs time.idle=27.7ms result=success
The system state is now Counters({"a": 1, "b": 2})
```

The source code for the example can be seen at [examples/readme](./examples/readme/src/main.rs). A more advanced example can be seen in the [example/composer](./examples/composer/) directory.

## Contributing

Thank you for your interest in contributing!

### Issues

Feature requests and bug reports should be submitted via issues. For bug reports, including a reproduction will make the issue
more likely to be prioritized and resolved. For feature requests, providing a description of the use case and the reasoning behind
the feature request will encourage discussion and help prioritization.

### Pull requests

Pull requests are the way concrete changes are made to the code in this repository. Here are a few guidelines to make the process easier.

- Every PR _should_ have an associated issue, and the PR's opening comment should say "Fixes #issue" or "Closes #issue".
- We use [Versionist](https://github.com/resin-io/versionist) to manage versioning (and in particular, [semantic versioning](https://semver.org)) and generate the changelog for this project.
- At least one commit in a PR should have a `Change-Type: type` footer, where `type` can be `patch`, `minor` or `major`. The subject of this commit will be added to the changelog.
- Commits should be squashed as much as makes sense.

## License

This project is licensed under the [Apache License](https://www.apache.org/licenses/LICENSE-2.0.html)
