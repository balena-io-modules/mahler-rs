#![cfg_attr(docsrs, feature(doc_cfg))]
//! mahler is an automated job orchestration library that builds and executes dynamic workflows.
//!
//! The library uses [automated planning](https://en.wikipedia.org/wiki/Automated_planning_and_scheduling) (heavily based on [HTNs](https://en.wikipedia.org/wiki/Hierarchical_task_network)) to compose user defined jobs into a workflow (represented as a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)) to achieve a desired target system state.
//!
//! This library's API is heavily inspired by [Axum's](https://docs.rs/axum/latest/axum/).
//!
//! # Features
//!
//! - Simple API - system state is defined as Rust structs with the help of the provided [derive](https://crates.io/crates/mahler-derive) crate.
//!   Allowed tasks are defined as pure Rust functions acting on a part or the whole system state.
//! - State engine with integrated planner - tasks are configured as jobs in a `Worker` domain. On a new target state, the worker will look for necessary changes to reach the target and look for a workflow that allows to reach the target from the current state.
//! - Concurrent execution - the internal planner detects when tasks can run concurrently based on state paths
//! - Automatic re-planning - re-computes workflow when runtime conditions change
//! - Observable runtime - monitor the evolving state of the system from the Worker API. For more detailed logging, the library uses the [tracing crate](https://crates.io/crates/tracing).
//! - Easy to debug - worker observable state and known goals allow easy replicability when issues occur.
//!
//! # Worker
//!
//! A [Worker](`worker::Worker`) orchestrates jobs into a workflow and executes the workflow tasks
//! when given a target. The worker also manages the system state model, making changes as the
//! workflow is executed.
//!
//! When creating a Worker, the first step is to assign jobs to specific routes and operations within the system
//! state. Once the jobs are set-up, providing the worker with an initial state makes the worker
//! ready to operate on the system.
//!
//! The only way to control the worker and effect changes into the controlled system is to provide
//! the worker with a target state.
//!
//! ```rust
//! use mahler::state::State;
//! use mahler::worker::Worker;
//! use mahler::job::{create, update};
//!
//! #[derive(State)]
//! struct MySystem;
//!
//! fn global() {}
//! fn foo() {}
//! fn foo_bar() {}
//!
//! # async fn test_worker_example_0() -> Result<(), Box<dyn std::error::Error>> {
//! let mut worker = Worker::new()
//!         // assign possible jobs to worker
//!         .job("", update(global))
//!         .job("/{foo}", update(foo))
//!         .job("/{foo}/{bar}", create(foo_bar))
//!         // initialize the worker state
//!         .initial_state(MySystem {/* .. */})?;
//!
//! // Control the system by providing a new target state
//! let (state, workflow_status) = worker.seek_target(MySystemTarget { /* .. */ }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! When comparing the internal state with the target, the Worker's planner will generate a list of
//! differences between the states (see [JSON Patch](https://datatracker.ietf.org/doc/html/rfc6902)) and try
//! to find a plan within the jobs that are applicable to a path.
//!
//! For instance, in the worker defined above, if the target finds a new value is created in the path `/a/b`
//! it will try jobs from more general to more specific, meaning it will try
//!
//! - global
//! - foo
//! - foo_bar
//!
//! ## Operations
//!
//! Jobs may be applicable to operations `create` (add), `update` (replace), `delete` (remove), `any` and `none`,
//! meaning they may be selected when a new property is created/updated or removed from the system
//! state. A task assigned to `none` is never selected by the planner, but may be used as part of
//! [compound tasks](#compound-tasks). All potentially runnable jobs need to be linked to the worker, hence the
//! need for `none` jobs.
//!
//! See [Operation](`crate::json::Operation`) for more information.
//!
//! ## State
//!
//! The library relies internally on [JSON values](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) for state
//! representation. Parts of the state can be referenced using [JSON pointers](https://docs.rs/jsonptr/latest/jsonptr/)
//! (see [RFC 6901](https://datatracker.ietf.org/doc/html/rfc6901)) and state differences are calculated using
//! [JSON patch](https://crates.io/crates/json_patch) (see [RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902)).
//!
//! For this reason, the system state can only be modelled using [serializable data structures](https://serde.rs).
//!
//! The worker and planner make distinction between the **internal** state of the system, and what
//! can be used as target state. For instance, the start date of a process is good for reference
//! information, but it doesn't make a good target. When modelling state, internal state can be
//! annotated using `#[mahler(internal)]` which means these properties will not be used in the
//! comparison of with the target state.
//!
//! ```rust
//! use std::time::SystemTime;
//! use mahler::state::{State, List};
//!
//! // the `State` macro implements `Serialize` and `Deserialize` for
//! // the struct and creates an associated type `ServiceTarget` without
//! // any internal properties
//! #[derive(State)]
//! struct Service {
//!     // will be used when planning
//!     cmd: List<String>,
//!
//!     // will not be used for planning
//!     #[mahler(internal)]
//!     start_time: SystemTime,
//! }
//! ```
//!
//! For accessing read-only, non-serializable resources from tasks, Mahler provides a separate mechanism.
//!
//! ## Shared resources
//!
//! Sometimes it may be desirable for jobs to access a shared resource (e.g. database connection,
//! file descriptor, etc.). These can be provided when creating the worker, with the only
//! restriction is that these structures must be `Send` and `Sync`.
//!
//! ```rust
//! use mahler::state::State;
//! use mahler::extract::Res;
//! use mahler::job::update;
//! use mahler::worker::Worker;
//!
//! // the system state
//! #[derive(State)]
//! struct MySystem;
//!
//! // MyConnection represents a shared resource
//! struct MyConnection;
//!
//! // Tasks can make use of resources via the `Res` extractor
//! fn some_task(conn: Res<MyConnection>) {}
//!
//! # async fn test_worker_example_0() -> Result<(), Box<dyn std::error::Error>> {
//! let conn = MyConnection {/* .. */};
//!
//! let worker = Worker::new()
//!         .resource::<MyConnection>(conn)
//!         .job("/", update(some_task))
//!         .initial_state(MySystem {/* .. */})?;
//! # Ok(())
//! # }
//! ```
//!
//! Note that only one resource of each type can be provided to the worker.
//!
//! # Tasks and Jobs
//!
//! A [Task](`task::Task`) in Mahler is an operation on a part of the system state that may chose
//! to make changes to the state given some target. It is defined as a pure Rust handler  and it may
//! or may not perform IO. A [Job](`job::Job`) is the configuration of a task to an operation on
//! the system state.
//!
//! A task is defined via a [Handler](`task::Handler`) and is applied to a specific [Context](`runtime::Context`), which is composed of
//! an application path (a [JSON pointer](https://www.rfc-editor.org/rfc/rfc6901) to a part of the
//! system state), an optional target and zero or more path arguments.
//!
//! A [handler](`task::Handler`) in mahler is any function that accepts zero or more "[extractors](`extract`)" as
//! arguments and returns something that can be converted into an effect on the
//! system.
//!
//! ## Extractors
//!
//! An extractor is a type that implements [FromSystem](`runtime::FromSystem`). Extractors are how
//! the planning/execution context is passed to the handler.
//!
//! ```rust
//! use mahler::state::State;
//! use mahler::extract::{View, Args, Target, System, Res};
//!
//! struct MyConnection;
//!
//! #[derive(State)]
//! struct MySystem;
//!
//!  // `View` provides a view into the relevant part of the
//! // state for the handler and allows making changes to the state.
//! fn view(state: View<u32>) {}
//!
//! // For nullable values, use `View<Option<T>>`  
//! // for instance, in the case of `create` operations
//! fn nullable_view(state: View<Option<u32>>) {}
//!
//! // `Args` gives you the path arguments and deserializes them
//! fn args(Args(counter_name): Args<String>) {}
//!
//! // `Target` gives you the target value for the Job operation
//! // note that `delete` operations do not have a target.
//! fn target(Target(tgt): Target<u32>) {}
//!
//! // `System` provides a view into the top level system state.
//! // A Job using the System extractor cannot run concurrently to other jobs
//! fn system(System(state): System<MySystem>) {}
//!
//! // `Res` allows to access a shared resource
//! fn res(res: Res<MyConnection>) {}
//! ```
//!
//! For extractors using generics, using a type that cannot be deserialized from the internal worker state will
//! result in an [Error](`error::ErrorKind::CannotDeserializeArg`). This means the task won't be usable by the planner, resulting
//! in a warning (or a failure in debug builds).
//!
//! ## Modifying the system state
//!
//! The [View](`extract::View`) extractor provides a mechanism to modify the system state by
//! returning the modified view.
//!
//! ```rust
//! use mahler::extract::{View, Target};
//!
//! // create a task to update a counter
//! fn plus_one(mut counter: View<u32>, Target(tgt): Target<u32>) -> View<u32> {
//!     // `View` implements Deref and DerefMut to
//!     // operate on the internal value
//!     if *counter < tgt {
//!         // update the counter if below the target
//!         *counter += 1;
//!     }
//!
//!     // if the counter has not changed, then the task won't
//!     // be selected by the planner
//!     counter
//! }
//!
//! // remove the counter
//! fn delete_counter(mut view: View<Option<u32>>) -> View<Option<u32>> {
//!         view.take();
//!         view
//!     }
//! ```
//!
//! Internally, the cumulative changes to the `View` extractors are converted by the planner to a
//! [Patch](https://docs.rs/json-patch/latest/json_patch/struct.Patch.html) and used to determine
//! the applicability of the task to a given target (if no changes are performed by the task at planning,
//! then the task is not applicable). At runtime, the same patch is used first to
//! determine if the task is safe to apply, and later to update the internal worker state.
//!
//! ## System Effects (I/O)
//!
//! In mahler, the task handler needs to be executed in two different contexts:
//! - At planning, the context for the job is determined (current state, path, target) and the
//!   corresponding task is tested to simulate the changes it introduces without actually modifying
//!   the underlying system. The same job may be tested multiple times while planning.
//! - At runtime, the tasks composing the workflow are executed in the corresponding order
//!   determined by the planner and changes to the underlying system are performed.
//!
//! This 2-in-1 function evaluation is enabled by the introduction of the [IO](`task::IO`)
//! type. An `IO` combines both a *pure* operation on an input and an effectful or `IO`
//! operation.
//!
//! ```rust
//! use mahler::task::{with_io, IO};
//! use mahler::extract::View;
//! use tokio::time::{sleep, Duration};
//!
//! fn plus_one(mut view: View<i32>) -> IO<i32> {
//!     // Pure modification
//!     *view += 1;
//!     
//!     // Combine with IO operation
//!     with_io(view, |view| async move {
//!         // system changes should only be performed
//!         // within the IO part
//!         sleep(Duration::from_millis(10)).await;
//!
//!         // return the view
//!         Ok(view)
//!     })
//! }
//!
//! #
//! ```
//!
//! This type is what can be used in mahler jobs to isolate effects on the underlying system.
//!
//! ```rust
//! use tokio::time::{sleep, Duration};
//!
//! use mahler::extract::{View, Target};
//! use mahler::task::{with_io, IO};
//!
//! // create a Job to update a counter
//! fn plus_one(mut counter: View<u32>, Target(tgt): Target<u32>) -> IO<u32> {
//!     if *counter < tgt {
//!         // update the counter if below the target
//!         *counter += 1;
//!     }
//!
//!     // return an IO type to isolate system changes
//!     with_io(counter, |counter| async move {
//!         // the IO portion will only ever be called if the job is
//!         // selected by the planner
//!         // perform IO here
//!         sleep(Duration::from_millis(10)).await;
//!
//!         // with_io expects a Result output
//!         Ok(counter)
//!     })
//! }
//! ```
//!
//! <div class="warning">
//! It is critical that system changes are isolated with `IO`. There is nothing that prevents
//! a task from performing blocking I/O, but that could potentially be harmful as the task may be
//! tried multiple times during planning.
//! </div>
//!
//! ```rust
//! use tokio::time::{sleep, Duration};
//! use tokio::runtime::Runtime;
//!
//! use mahler::extract::{View, Target};
//!
//! fn plus_one(mut counter: View<u32>, Target(tgt): Target<u32>) -> View<u32> {
//!     if *counter < tgt {
//!         // update the counter if below the target
//!         *counter += 1;
//!     }
//!
//!     Runtime::new().unwrap().block_on(async {
//!         // This is a footgun as it adds 100ms every time
//!         // the task is tested by the planner
//!         sleep(Duration::from_millis(100)).await;
//!     });
//!
//!     counter
//! }
//! ```
//!
//! ## Compound tasks
//!
//! Sometimes it may be desirable to re-use tasks in different contexts, or combine multiple tasks in
//! order to guide the planner. This can be achieved by the use of compound tasks, called
//! [Methods](`task::Method`) in the Mahler API.
//!
//! A `Method` [handler](`task::Handler`) is any function that receives zero or more extractors and
//! returns something that can be converted to a `Vec` of tasks.
//!
//! ```rust
//! use mahler::extract::{View, Target};
//! use mahler::task::{Task, Handler};
//!
//! fn plus_one() {}
//!
//! // define a method
//! fn plus_two(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
//!     if tgt - *counter > 1 {
//!         // Return two instances of the `plus_one` task
//!         return vec![
//!             // Provide a target for the task.
//!             // `with_target` assigns a target to the task
//!             plus_one.with_target(tgt),
//!             plus_one.with_target(tgt)
//!         ];
//!     }
//!
//!     // returning nothing means the method will not be picked
//!     // by the planner
//!     vec![]
//! }
//!
//! // methods can also call other methods
//! fn plus_three(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
//!     if tgt - *counter > 2 {
//!         return vec![plus_two.with_target(tgt), plus_one.with_target(tgt)];
//!     }
//!
//!     vec![]
//! }
//! ```
//!
//! A task [Handler](`task::Handler`) may be converted to a [Task](task::Task) by using the [into_task](`task::Handler::into_task`)
//! method or using one of the helper methods [with_target](`task::Handler::with_target`) or
//! [with_arg](`task::Handler::with_arg`).
//!
//! <div class="warning">
//! It is important that all tasks are registered as jobs when setting up a Worker. Internally, the Worker
//! has a database linking tasks and paths and when expanding a method it performs
//! a reverse search for the paths corresponding to the tasks returned by the
//! method. Failing to do this will result on an error during planning.
//! </div>
//!
//! # Error handling
//!
//! All possible errors by Mahler operations are defined by the [ErrorKind](`error::ErrorKind`)
//! type.
//!
//! When calling [Worker::seek_target](`worker::Worker::seek_target`), the worker will return an
//! error if there is a problem with serialization or if an error happens at planning or there is
//! an internal error. If an error happens at workflow execution, the method will not return an
//! error but terminate with an [Aborted](`worker::SeekStatus::Aborted`) value, which will include
//! a [`Vec`] of all errors that happened during the workflow execution with the either
//! [ErrorKind::Runtime](`error::ErrorKind::Runtime`) type or the
//! [ErrorKind:ConditionNotMet](`error::ErrorKind::ConditionNotMet`) type.
//!
//! # Observability
//!
//! The [Worker::seek_target](`worker::Worker::seek_target`) method is instrumented using the [tracing](https://crates.io/crates/tracing)
//! crate to report on the operation and progress of the Worker planning and workflow execution. These events can be processed using the
//! [tracing_subscriber](https://crates.io/crates/tracing_subscriber) crate to produce structured or human readable logs.
//!
//! For additional control and a more granular view of system changes during Worker operation, the [find_workflow](`worker::Worker::find_workflow`) and
//! [run_workflow](`worker::Worker::run_workflow`) [Worker](`worker::Worker`) methods can be used.
//!
//! - [find_workflow](`worker::Worker::find_workflow`) looks up a workflow to a given target.
//! - [run_workflow](`worker::Worker::run_workflow`) consumes the worker and the given workflow and
//!   return a stream of [events](`worker::WorkerEvent`), indicating a new system state or the
//!   workflow execution completion. Note that if the worker has [sensors](`sensor`) defined, the
//!   stream will continue producing values even after workflow completion.
//!
//! Similarly, to only monitor the system and not produce changes, the [Worker](`worker::Worker`)
//! exposes a [listen](`worker::Worker::listen`) method that produces values from sensors as a
//! stream (if any).
//!
//! # Testing
//!
//! The [find_workflow](`worker::Worker::find_workflow`) and [run_task](`worker::Worker::run_task`)
//! methods of [Worker](`worker::Worker`) can also be used for respectively testing the configuration of the
//! workerby comparing the generated plans against some expectations and implementation of tasks.
//!
//! The library exposes the following workflow types and macros for this purpose
//!
//! - [Dag](`struct@dag::Dag`) an DAG implementation used internally by mahler.
//! - [dag](`dag::dag!`) a declarative macro to combine DAGs into branches
//! - [seq](`dag::seq`) a declarative macro to create a linear DAG from a list of values
//! - [par](`dag::par`) a declarative macro to create a branching DAG with single value
//!   branches
//!
//! ```rust
//! use mahler::task::{IO, with_io};
//! use mahler::extract::{View, Target};
//! use mahler::worker::Worker;
//! use mahler::dag::{Dag, seq};
//! use mahler::job::update;
//!
//! fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> IO<i32> {
//!    if *counter < tgt {
//!        // Modify the counter if we are below target
//!        *counter += 1;
//!    }
//!
//!    // Return the updated counter
//!    with_io(counter, |counter| async {
//!        Ok(counter)
//!    })
//! }
//!
//! // Setup the worker domain and resources
//! let worker = Worker::new()
//!                 .job("", update(plus_one).with_description(|| "+1"))
//!                 .initial_state(0).unwrap();
//! let workflow = worker.find_workflow(2).unwrap().unwrap();
//!
//! // We expect a linear DAG with two tasks
//! let expected: Dag<&str> = seq!("+1", "+1");
//! assert_eq!(workflow.to_string(), expected.to_string());
//! ```
pub use mahler_core::error;
pub use mahler_core::json;
pub use mahler_core::result;
pub use mahler_core::serde;

use mahler_core::runtime;

mod system_ext;

pub mod exception;
pub mod extract;
pub mod job;
pub mod sensor;
pub mod task;
pub mod worker;

pub mod sync {
    //! State synchronization and runtime control

    // Only expose Interrupt publicly
    pub use mahler_core::sync::Interrupt;

    pub(crate) use mahler_core::sync::{channel, rw_lock, Reader, Sender, WithAck, Writer};
}

pub mod dag {
    //! Directed Acyclic Graph implementation and methods
    pub use mahler_core::dag::*;
    pub use mahler_core::{dag, par, seq};
}

pub mod state {
    //! State trait and standardized serialization along with State procedural macro
    //!
    //! This module provides the State trait which defines how types are serialized
    //! and deserialized in a standardized way, including the halted state.

    pub use mahler_core::state::*;

    // Expose collection macros under the state module
    pub use mahler_core::{list, map, set};

    #[cfg(feature = "derive")]
    #[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
    pub use mahler_derive::*;
}
