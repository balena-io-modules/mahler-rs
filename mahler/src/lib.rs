#![cfg_attr(docsrs, feature(doc_cfg))]
//! mahler is an automated job orchestration library that builds and executes dynamic workflows.
//!
//! The library uses [automated planning](https://en.wikipedia.org/wiki/Automated_planning_and_scheduling) (heavily based on [HTNs](https://en.wikipedia.org/wiki/Hierarchical_task_network)) to compose user defined jobs into a workflow (represented as a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)) to achieve a desired target system state.
//!
//! This library's API is heavily inspired by [Axum's](https://docs.rs/axum/latest/axum/).
//!
//! # Features
//!
//! - Simple API. Jobs can be targeted to specific paths and operations within the state model for targeted operations.
//! - Declaratively access System state and resources using extractors.
//! - Intelligent planner. Automatically discover a workflow to transition from the current system state to a given target state.
//! - Concurrent execution of jobs. The planner automatically detects when operations can be performed concurrently and adjusts the execution graph accorddingly.
//! - Observable runtime. Monitor the evolving state of the system from the Worker API. For more detailed logging, the library uses the [tracing crate](https://crates.io/crates/tracing).
//! - Easy to debug. Agent observable state and known goals allow easy replicability when issues occur.
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
//! ```rust,no_run
//! use mahler::state::State;
//! use mahler::worker::Worker;
//! use mahler::task::prelude::*;
//!
//! #[derive(State)]
//! struct MySystem;
//!
//! let mut worker = Worker::new()
//!         // assign possible jobs to worker
//!         .job("", update(global))
//!         .job("/{foo}", update(foo))
//!         .job("/{foo}/{bar}", create(foo_bar))
//!         // initialize the worker state
//!         .initial_state(MySystem {/* .. */})
//!         .unwrap();
//!
//! fn global() {}
//! fn foo() {}
//! fn foo_bar() {}
//!
//! # tokio_test::block_on(async {
//! // Control the system by providing a new target state
//! worker.seek_target(MySystemTarget { /* .. */ }).await.unwrap();
//! # })
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
//! state. A job assigned to `none` is never selected by the planner, but may be used as part of
//! [compound jobs](#compound-jobs). All potentially runnable jobs need to be linked to the worker, hence the
//! need for `none` jobs.
//!
//! See [Operation](`task::Operation`) for more information.
//!
//! ## State
//!
//! The library relies on [JSON values](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) for internal state
//! representation. Parts of the state can be referenced using [JSON pointers](https://docs.rs/jsonptr/latest/jsonptr/)
//! (see [RFC 6901](https://datatracker.ietf.org/doc/html/rfc6901)) and state differences are calculated using
//! [JSON patch](https://crates.io/crates/json_patch) (see [RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902)).
//!
//! For this reason, the system state can only be modelled using [serializable data structures](https://serde.rs). Moreover,
//! non-serializable fields (annotated with [skip_serializing](https://serde.rs/attr-skip-serializing.html) will be
//! lost when passing data to the jobs.
//!
//! ```rust
//! use mahler::state::State;
//!
//! #[derive(State)]
//! struct MySystem {
//!     // can be accessed from jobs
//!     some_value: String,
//!
//!     // this will never be passed to jobs
//!     #[mahler(internal)]
//!     other_value: String,
//! }
//! ```
//!
//! Mahler provides another mechanism for accessing read-only, non-serializable resources from
//! jobs.
//!
//! ## Shared resources
//!
//! Sometimes it may be desirable for jobs to access a shared resource (e.g. database connection,
//! file descriptor, etc.). These can be provided when creating the worker, with the only
//! restriction is that these structures must be `Send` and `Sync`.
//!
//! ```rust,no_run
//! use mahler::state::State;
//! use mahler::worker::Worker;
//!
//! #[derive(State)]
//! struct MySystem;
//!
//! // MyConnection represents a shared resource
//! struct MyConnection;
//! let conn = MyConnection {/* .. */};
//!
//! let worker = Worker::new()
//!         .resource::<MyConnection>(conn)
//!         .initial_state(MySystem {/* .. */})
//!         .unwrap();
//! ```
//!
//! Note that only one resource of each type can be provided to the worker.
//!
//! # Jobs and Tasks
//!
//! A [Job](`task::Job`) in Mahler is a repeatable operation defined as a Rust handler that operates on the
//! system state and may or may not perform IO operations. A Job describes a generic operation and
//! can be converted to a concrete [Task](`task::Task`) by assigning a context. The context is composed of
//! an application path (a [JSON pointer](https://www.rfc-editor.org/rfc/rfc6901) to a part of the
//! system state), an optional target and zero or more path arguments.
//!
//! A [handler](`task::Handler`) in mahler is any function that accepts zero or more "[extractors](`extract`)" as
//! arguments and returns something that can be converted into an effect on the
//! system.
//!
//! ## Extractors
//!
//! An extractor is a type that implements [FromSystem](`task::FromSystem`). Extractors are how
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
//!  // `View` gives you a view into the relevant part of the
//! // state for the handler.
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
//! Because of the way extractors work, an improperly used extractor with fail at runtime
//! with an [ExtractionError](`errors::ExtractionError`). For instance, using the wrong type may
//! result in a deserialization error, meaning the task won't be usable by the planner, resulting
//! in a warning (or a failure in debug builds).
//!
//! ## Modifying the system state
//!
//! The [View](`extract::View`) extractor provides a mechanism to modify the system state.
//!
//! ```rust
//! use mahler::extract::{View, Target};
//!
//! // create a Job to update a counter
//! fn plus_one(mut counter: View<u32>, Target(tgt): Target<u32>) -> View<u32> {
//!     // `View` implements Deref and DerefMut to access
//!     // operate the internal value
//!     if *counter < tgt {
//!         // update the counter if below the target
//!         *counter += 1;
//!     }
//!
//!     // if the counter has not changed, then the job won't
//!     // be selected by the planner
//!     counter
//! }
//!
//! // remove the counter (delete type job)
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
//! In mahler, the function defined by the job needs to be executed in two different contexts:
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
//! fn increment_job(mut view: View<i32>) -> IO<i32> {
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
//! a `Job` from performing blocking I/O, but that could potentially be harmful as the job may be
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
//!         // the job is tested by the planner
//!         sleep(Duration::from_millis(100)).await;
//!     });
//!
//!     counter
//! }
//! ```
//!
//! ## Compound Jobs
//!
//! Sometimes it may be desirable to re-use jobs in different contexts, or combine multiple jobs in
//! order to guide the planner. This can be achieved by the use of compound jobs, called
//! [Methods](`task::Method`) in the mahler API.
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
//!         // Return two instances of the `plus_one` job
//!         return vec![
//!             // Provide a target for the job.
//!             // `with_target` converts the job into a `Task`
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
//! A Job handler may be converted to a task by using the [into_task](`task::Handler::into_task`)
//! method or using one of the helper methods [with_target](`task::Handler::with_target`) or
//! [with_arg](`task::Handler::with_arg`).
//!
//! <div class="warning">
//! It is important that all jobs are registered when setting up a Worker. Internally, the Worker
//! has a database linking jobs and paths and when expanding a method it performs
//! a reverse search for the paths corresponding to the tasks returned by the
//! method. Failing to do this will result in an failure when planning.
//! </div>
//!
//! # Error handling
//!
//! When calling [Worker::seek_target](`worker::Worker::seek_target`), there are two types of
//! errors that may happen.
//! - A [SeekError](`worker::SeekError`) is an unrecoverable error that occurs if there is some
//!   [issue serializing](`errors::SerializationError`) the current or target state, there is a problem
//!   when [initializing extractors](`errors::ExtractionError`), or [expanding a method](`errors::MethodError`)
//!   or there is an unexpected [internal error](`errors::InternalError`).
//! - An [IO error](`errors::IOError`) may happen on a task when executing the workflow. In that
//!   case, the `seek_target` method will return an [Aborted
//!   status](`worker::SeekStatus::Aborted`), including the list of errors that happened during the
//!   run.
//!
//! # Monitoring system state
//!
//! The [Worker](`worker::Worker`) provides a [follow](`worker::Worker::follow`) method, returning a
//! stream of state updates. A new value will be produced on the stream every time the internal
//! worker state changes.
//!
//! # Observability
//!
//! For detailed Worker observability, mahler is instrumented with the [tracing](https://crates.io/crates/tracing)
//! crate to report on the operation and progress of the Worker planning and workflow execution
//! stages. These events can be processed using the [tracing_subscriber](https://crates.io/crates/tracing_subscriber) crate
//! to produce structured or human readable logs.
//!
//! Key log levels:
//! - **INFO**: Workflow events, task execution
//! - **DEBUG**: Detailed planning information, state changes
//! - **WARN**: Task failures, interruptions
//! - **ERROR**: Fatal errors
//! - **TRACE**: Planner and internal worker operation
//!
//! ```rust,no_run
//! use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
//!
//! // Initialize tracing subscriber with recommended formatting
//! tracing_subscriber::registry()
//!     .with(EnvFilter::from_default_env())
//!     .with(
//!         fmt::layer()
//!             .with_span_events(fmt::format::FmtSpan::CLOSE)
//!             .event_format(fmt::format().compact().with_target(false)),
//!     )
//!     .init();
//! ```
//!
//! # Testing
//!
//! When `debug_assertions` is enabled, mahler exposes two testing methods for [Worker](`worker::Worker`).
//! - [find_workflow](`worker::Worker::find_workflow`) allows to generate a Workflow for a given
//!   initial and target state. The workflow can be compared with a manually created [DAG](`workflow::Dag`) to
//!   test against an expected plan.
//! - [run_task](`worker::Worker::run_task`) allows to run a task in the context of a worker. This
//!   may be helpful to diagnose any extraction/expansion errors with the task definition or for
//!   debugging of a specific task.
//!
//! It also exposes the following workflow types and macros
//! - [Dag](`workflow::Dag`) an DAG implementation used internally by mahler.
//! - [dag](`workflow::dag`) a declarative macro to combine DAGs into branches
//! - [seq](`workflow::seq`) a declarative macro to create a linear DAG from a list of values
//! - [par](`workflow::par`) a declarative macro to create a branching DAG with single value
//!   branches
//!   
//! These utils can be used for testing and comparing generated workflows with specific DAGs. See
//! [find_workflow](`worker::Worker::find_workflow`) for more info.
pub use mahler_core::errors;
pub use mahler_core::extract;
pub use mahler_core::task;
pub use mahler_core::worker;

pub mod workflow {
    pub use mahler_core::workflow::Interrupt;

    #[cfg(debug_assertions)]
    pub use mahler_core::workflow::{Dag, Workflow};

    #[cfg(debug_assertions)]
    pub use mahler_core::{dag, par, seq};
}

pub mod state {
    //! State trait and standardized serialization along with State procedural macro
    //!
    //! This module provides the State trait which defines how types are serialized
    //! and deserialized in a standardized way, including the halted state.

    pub use mahler_core::state::*;
    pub use mahler_core::{list, map, set};

    #[cfg(feature = "derive")]
    #[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
    pub use mahler_derive::*;
}
