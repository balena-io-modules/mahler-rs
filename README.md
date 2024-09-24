# gustav

An automated job orchestration library to build and execute dynamic workflows.

Gustav uses [automated planning](https://en.wikipedia.org/wiki/Automated_planning_and_scheduling) (heavily based on [HTNs](https://en.wikipedia.org/wiki/Hierarchical_task_network)) to compose user defined jobs into a task workflow (represented as a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)) to achieve a desired target system state. If failures happen during execution it can adapt the workflow to perform corrective actions or automatically retry certain operations. It is meant for more than job orchestration, but as a library to build autonomous system agents in Rust.

NOTE: this project is still in very early stage, but its design is guided by [Mahler](https://github.com/balena-io-modules/mahler), it's NodeJS counterpart, and will share many of the same design concepts, with an API more suited for Rust that is heavily inspired by [Axum](https://github.com/tokio-rs/axum).

## Features

- Simple API. Just write your jobs as Rust functions and link them to a worker. The planner will determine the best way to compose them into a workflow to achieve a specific system state. Jobs can be referenced by compound jobs to guide the planner towards a desired behavior.
- Observable runtime. The runtime state of the agent can be monitored at all times with different levels of detail. Human readable metadata for indidividual tasks is generated from the function names and execution context.
- Parallel execution of tasks. The planner automatically detects when operations can be performed in parallel and creates branches in the plan to tell the agent to run concurrent operations.
- Easy to debug. Agent observable state and known goals allow easy replicability when issues occur. The planning decision tree and resulting plans can be diagrammed to visually inspect where planning is failing.

# Core concepts

- **Job** a repeatable operation defined as a function that operates on the system state and may or may not perform IO operations. Jobs are generic, and depend on a context to be selected by the planner. Jobs can be _atomic_, meaning they define a concrete action (e.g. download an artifact), or _compound_, meaning they require to execute several tasks to be achieved.
- **Task** is a concrete operation to be used for planning or as part of a workflow. When a Job is given a context, it becomes a task and can be used for planning.
- **Effect** is the outcome on the system that will happen after executing a task. It is used by the planner to safely find paths between the current state and the target state.
- **Context** is metadata about the execution environment where the Task is being executed, the current state of the system, the target, the result from a previous run, etc., can be used in the Job definition to tell the planner whether or not to chose it for the workflow.
- **Workflow** encodes a plan of what actions need to be executed by the worker in order to reach a certain targets. Workflows are represented as Directed Acyclic Graphs (DAG).
- **Target** is a desired system state, e.g. "temperature of the room should be 25 degrees celsius".
- **Worker** is an autonomous agent that can perform changes on a system using the knowledge encoded as jobs and a given target. A worker will keep trying to reach the target until the target changes or some other exit conditions are met.
- **Sensor** is an observer of the system state, the worker can subscribe to one or more sensors in order to keep its local view of the state up-to-date and trigger re-planning if necessary.

## Basic Usage

Let's create a system controller for a simple counting system. Let's define a Job that operates on i32

```rust
use gustav::*;
use gustav::extract::{Target, Update};

/// Plus one is a job that updates a counter if it is below some target
fn plus_one(mut counter: Update<i32>, tgt: Target<i32>) -> Update<i32> {
    if *counter < *tgt {
        // Modify the counter value if we are below the target
        *counter += 1;
    }

    // We need to return the modified value so the planner
    // can identify the changes to the state. If there are no
    // changes made, then the job will not be used
    counter
}
```

The job above updates the counter if it is below the target, otherwise it returs the same value that it currently has. When planning, a task that perform no changes on the system state is not selected, which allows us to control when jobs are applicable. To use the task, we need to create a worker.

```rust
#[tokio::main]
async fn main() {
    // build our agent using the plus one job
    let agent = Worker::new()
        // The plus_one job is applicable to UPDATE operation
        // for global state
        .job("/", update(plus_one))
        // The initial state of the worker
        .with_state(0)


    // Tell the agent to find a plan from the initial state (0)
    // to the target state (3) and execute it
    agent.seek(3);

    // Wait for the agent to return a result
    let res = agent.wait().await;


    if let Some(state) = res {
      println!("The system state is now {}", state);
    }
}
```

When receiving a call to `seek`, the worker looks for a plan to get the system to a state equal to 3 target and then executes it.
In this case it will identify that 3 sequential calls to `plus_one` are necessary to reach the target.

## Performing IO

The code above is a bit too simple, however. It only operates on the Worker in-memory state but does not perform any IO. In order to interact with the actual system state, we need to use an `Effect`.

```rust
use tokio::fs;
use tokio::io::Error;

/// Plus one is a job that updates a counter if it is below some target
fn plus_one(mut counter: Update<i32>, tgt: Target<i32>) -> Effect<Update<i32>, Error>  {
    if *counter < *tgt {
        // Modify the counter value if we are below the target
        *counter += 1;
    }

    // An Effect type allows us to isolate IO from the outcome
    // that the job will have on the system. It allows the planner
    // to test tasks safely
    Effect.of(counter)
          // This will not be executed at planning
          .with_io(|counter| async move {
              fs::write("/etc/counter", &counter.to_le_bytes()).await?;
              Ok(counter)
          })
}
```
