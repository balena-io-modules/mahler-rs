use super::*;
use crate::json::Path;
use crate::result::Result;

use pretty_assertions::assert_eq;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{prelude::*, EnvFilter};

use crate::dag::{dag, par, seq, Dag};
use crate::extract::{Args, System, Target, View};
use crate::job::*;
use crate::json::Operation;
use crate::state::{Map, State};
use crate::task::prelude::*;

fn init() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .pretty()
                .with_target(false)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
        )
        .with(EnvFilter::from_default_env())
        .try_init()
        .unwrap_or(());
}

fn plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
    if *counter < tgt {
        *counter += 1;
    }

    counter
}

fn buggy_plus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
    if *counter < tgt {
        // This is the wrong operation
        *counter -= 1;
    }

    counter
}

fn plus_two(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
    if tgt - *counter > 1 {
        return vec![plus_one.into_task(), plus_one.into_task()];
    }

    vec![]
}

fn plus_three(counter: View<i32>, Target(tgt): Target<i32>) -> Vec<Task> {
    if tgt - *counter > 2 {
        return vec![plus_two.with_target(tgt), plus_one.with_target(tgt)];
    }

    vec![]
}

fn minus_one(mut counter: View<i32>, Target(tgt): Target<i32>) -> View<i32> {
    if *counter > tgt {
        *counter -= 1;
    }

    counter
}

pub fn find_plan<T>(db: &Domain, cur: T, tgt: T::Target) -> Result<Option<Workflow>>
where
    T: State,
{
    let tgt = serde_json::to_value(tgt).expect("failed to serialize target state");

    let mut system =
        crate::runtime::System::try_from(cur).expect("failed to serialize current state");

    let res = find_workflow_to_target::<T>(db, &mut system, &tgt)?;
    Ok(res)
}

#[test]
fn it_calculates_a_linear_workflow() {
    let domain = Domain::new()
        .job("", update(plus_one))
        .job("", update(minus_one));

    let workflow = find_plan(&domain, 0, 2).unwrap().unwrap();

    // We expect a linear DAG with two tasks
    let expected: Dag<&str> = seq!(
        "mahler::worker::planner::tests::plus_one()",
        "mahler::worker::planner::tests::plus_one()"
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_ignores_none_jobs() {
    let domain = Domain::new().job("", none(plus_one));

    let workflow = find_plan(&domain, 0, 2);

    assert!(matches!(workflow, Ok(None)));
}

#[test]
fn it_aborts_search_if_plan_length_grows_too_much() {
    let domain = Domain::new()
        .job("", update(buggy_plus_one))
        .job("", update(minus_one));

    let workflow = find_plan(&domain, 0, 2);
    assert!(workflow.is_err());
}

#[test]
fn it_calculates_a_linear_workflow_with_compound_tasks() {
    init();
    let domain = Domain::new()
        .job("", update(plus_two))
        .job("", none(plus_one));

    let workflow = find_plan(&domain, 0, 2).unwrap().unwrap();

    // We expect a linear DAG with two tasks
    let expected: Dag<&str> = seq!(
        "mahler::worker::planner::tests::plus_one()",
        "mahler::worker::planner::tests::plus_one()"
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_calculates_a_linear_workflow_on_a_complex_state() {
    #[derive(Serialize, Deserialize)]
    struct MyState {
        counters: HashMap<String, i32>,
    }

    impl State for MyState {
        type Target = Self;
    }

    let initial = MyState {
        counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
    };

    let target = MyState {
        counters: HashMap::from([("one".to_string(), 2), ("two".to_string(), 2)]),
    };

    let domain = Domain::new()
        .job("/counters/{counter}", update(minus_one))
        .job("/counters/{counter}", update(plus_one));

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // We expect counters to be updated concurrently
    let expected: Dag<&str> = par!(
        "mahler::worker::planner::tests::plus_one(/counters/one)",
        "mahler::worker::planner::tests::plus_one(/counters/two)",
    ) + par!(
        "mahler::worker::planner::tests::plus_one(/counters/one)",
        "mahler::worker::planner::tests::plus_one(/counters/two)",
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_calculates_a_linear_workflow_on_a_complex_state_with_compound_tasks() {
    #[derive(Serialize, Deserialize)]
    struct MyState {
        counters: HashMap<String, i32>,
    }

    impl State for MyState {
        type Target = Self;
    }

    let initial = MyState {
        counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
    };

    let target = MyState {
        counters: HashMap::from([("one".to_string(), 2), ("two".to_string(), 2)]),
    };

    let domain = Domain::new()
        .job("/counters/{counter}", none(plus_one))
        .job("/counters/{counter}", update(plus_two));

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // We expect a concurrent dag with two tasks on each branch
    let expected: Dag<&str> = dag!(
        seq!(
            "mahler::worker::planner::tests::plus_one(/counters/one)",
            "mahler::worker::planner::tests::plus_one(/counters/one)",
        ),
        seq!(
            "mahler::worker::planner::tests::plus_one(/counters/two)",
            "mahler::worker::planner::tests::plus_one(/counters/two)",
        )
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_calculates_a_linear_workflow_on_a_complex_state_with_deep_compound_tasks() {
    #[derive(Serialize, Deserialize)]
    struct MyState {
        counters: HashMap<String, i32>,
    }

    impl State for MyState {
        type Target = Self;
    }

    let initial = MyState {
        counters: HashMap::from([("one".to_string(), 0), ("two".to_string(), 0)]),
    };

    let target = MyState {
        counters: HashMap::from([("one".to_string(), 3), ("two".to_string(), 0)]),
    };

    let domain = Domain::new()
        .job("/counters/{counter}", none(plus_one))
        .job("/counters/{counter}", none(plus_two))
        .job("/counters/{counter}", update(plus_three));

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // We expect a linear DAG with two tasks
    let expected: Dag<&str> = seq!(
        "mahler::worker::planner::tests::plus_one(/counters/one)",
        "mahler::worker::planner::tests::plus_one(/counters/one)",
        "mahler::worker::planner::tests::plus_one(/counters/one)",
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_avoids_conflicts_from_methods() {
    init();
    let initial = Map::from([("one".to_string(), 0), ("two".to_string(), 0)]);
    let target = Map::from([("one".to_string(), 1), ("two".to_string(), 1)]);

    fn plus_other(Target(tgt): Target<i32>) -> Vec<Task> {
        vec![
            // should work with or without target
            plus_one.with_arg("counter", "one"),
            plus_one.with_arg("counter", "two").with_target(tgt),
        ]
    }

    let domain = Domain::new()
        .job("/{counter}", none(plus_one))
        .job("/{counter}", update(plus_other));

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // We expect a parallel dag for this specific target
    let expected: Dag<&str> = par!(
        "mahler::worker::planner::tests::plus_one(/one)",
        "mahler::worker::planner::tests::plus_one(/two)",
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn it_allows_empty_tasks_as_part_of_methods() {
    init();

    #[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct App {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    }

    impl State for App {
        type Target = Self;
    }

    type Config = Map<String, String>;

    #[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct Device {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default)]
        apps: HashMap<String, App>,
        #[serde(default)]
        config: Config,
        #[serde(default)]
        needs_cleanup: bool,
    }

    impl State for Device {
        type Target = Self;
    }

    /// Store configuration in memory
    fn store_config(mut config: View<Config>, Target(tgt_config): Target<Config>) -> View<Config> {
        // If a new config received, just update the in-memory state, the config will be handled
        // by the legacy supervisor
        *config = tgt_config;
        config
    }

    fn set_device_name(
        mut name: View<Option<String>>,
        Target(tgt): Target<Option<String>>,
    ) -> View<Option<String>> {
        *name = tgt;
        name
    }

    fn ensure_cleanup(mut device: View<Device>) -> View<Device> {
        device.needs_cleanup = true;
        device
    }

    fn complete_cleanup(mut device: View<Device>) -> View<Device> {
        device.needs_cleanup = false;
        device
    }

    fn dummy_task() {}

    fn do_cleanup(System(device): System<Device>, Target(tgt_device): Target<Device>) -> Vec<Task> {
        let into_tgt = Device {
            needs_cleanup: false,
            ..device.clone()
        };
        if into_tgt != tgt_device || !device.needs_cleanup {
            return vec![];
        }

        // dummy task is always empty but it should be accepted as part of the
        // method
        vec![dummy_task.into_task(), complete_cleanup.into_task()]
    }

    fn prepare_app(mut app: View<Option<App>>, Target(tgt_app): Target<App>) -> View<Option<App>> {
        app.replace(tgt_app);
        app
    }

    let domain = Domain::new()
        .job(
            "/name",
            any(set_device_name).with_description(|| "set device name"),
        )
        .job(
            "/config",
            update(store_config).with_description(|| "store configuration"),
        )
        .jobs(
            "",
            [
                update(ensure_cleanup).with_description(|| "ensure cleanup"),
                update(do_cleanup),
                none(complete_cleanup).with_description(|| "complete cleanup"),
            ],
        )
        .job("", none(dummy_task).with_description(|| "dummy task"))
        .job(
            "/apps/{app_uuid}",
            create(prepare_app)
                .with_description(|Args(app_uuid): Args<String>| format!("prepare app {app_uuid}")),
        );

    let initial = serde_json::from_value::<Device>(json!({})).unwrap();
    let target = serde_json::from_value::<Device>(json!({
        "name": "my-device",
        "apps": {
            "my-app": {"name": "my-app-name"}
        },
        "config": {
            "some-var": "some-value"
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();
    let expected: Dag<&str> = seq!("ensure cleanup")
        + par!(
            "store configuration",
            "set device name",
            "prepare app my-app"
        )
        + seq!("dummy task", "complete cleanup");
    assert_eq!(expected.to_string(), workflow.to_string());
}

#[test]
fn it_avoids_conflict_in_tasks_returned_from_methods() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        image: String,
    }

    impl State for Service {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Image {}

    #[derive(Serialize, Deserialize)]
    struct MySys {
        services: Map<String, Service>,
        images: Map<String, Image>,
    }

    impl State for MySys {
        type Target = MySysTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct MySysTarget {
        services: Map<String, Service>,
    }

    fn create_image(mut view: View<Option<Image>>) -> View<Option<Image>> {
        *view = Some(Image {});
        view
    }

    fn create_service_image(
        Target(tgt): Target<Service>,
        System(state): System<MySys>,
    ) -> Option<Task> {
        if !state.images.contains_key(&tgt.image) {
            return Some(create_image.with_arg("image_name", tgt.image));
        }
        None
    }

    fn create_service(
        mut view: View<Option<Service>>,
        Target(tgt): Target<Service>,
        System(state): System<MySys>,
    ) -> View<Option<Service>> {
        if state.images.contains_key(&tgt.image) {
            *view = Some(tgt);
        }
        view
    }

    let domain = Domain::new()
        .job(
            "/images/{image_name}",
            none(create_image).with_description(|Args(image_name): Args<String>| {
                format!("create image '{image_name}'")
            }),
        )
        .jobs(
            "/services/{service_name}",
            [
                create(create_service).with_description(|Args(service_name): Args<String>| {
                    format!("create service '{service_name}'")
                }),
                create(create_service_image),
            ],
        );

    let initial = serde_json::from_value::<MySys>(json!({ "images": {}, "services": {} })).unwrap();
    let target = serde_json::from_value::<MySysTarget>(
        json!({ "services": {"one":{"image": "ubuntu"}, "two": {"image": "ubuntu"}} }),
    )
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    let expected: Dag<&str> = seq!(
        "create image 'ubuntu'",
        "create service 'one'",
        "create service 'two'",
    );
    assert_eq!(expected.to_string(), workflow.to_string());
}

#[test]
fn it_calculates_concurrent_workflows_from_non_conflicting_paths() {
    init();
    type Config = Map<String, String>;

    #[derive(Serialize, Deserialize)]
    struct MyState {
        config: Config,
        counters: Map<String, i32>,
    }

    impl State for MyState {
        type Target = Self;
    }

    fn new_counter(mut counter: View<Option<i32>>, Target(tgt): Target<i32>) -> View<Option<i32>> {
        counter.replace(tgt);
        counter
    }

    fn update_config(mut config: View<Config>, Target(tgt): Target<Config>) -> View<Config> {
        *config = tgt;
        config
    }

    fn new_config(
        mut config: View<Option<String>>,
        Target(tgt): Target<String>,
    ) -> View<Option<String>> {
        config.replace(tgt);
        config
    }

    let domain = Domain::new()
        .job(
            "/counters/{counter}",
            create(new_counter).with_description(|Args(counter): Args<String>| {
                format!("create counter '{counter}'")
            }),
        )
        .job(
            "/config/{config}",
            create(new_config)
                .with_description(|Args(config): Args<String>| format!("create config '{config}'")),
        )
        .job(
            "/config",
            update(update_config).with_description(|| "update configurations"),
        );

    let initial =
        serde_json::from_value::<MyState>(json!({ "config": {}, "counters": {} })).unwrap();
    let target = serde_json::from_value::<MyState>(
        json!({ "config": {"some_var":"one", "other_var": "two"}, "counters": {"one": 0} }),
    )
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    let expected: Dag<&str> = par!("update configurations", "create counter 'one'");
    assert_eq!(expected.to_string(), workflow.to_string());
}

#[test]
fn it_finds_concurrent_plans_with_nested_forks() {
    init();
    type Counters = Map<String, i32>;

    #[derive(Serialize, Deserialize, Debug)]
    struct MyState {
        counters: Counters,
    }

    impl State for MyState {
        type Target = Self;
    }

    // This is a very dumb example to test how the planner
    // choses concurrent methods over automated concurrency
    fn multi_increment(counters: View<Counters>, target: Target<Counters>) -> Vec<Task> {
        counters
            .keys()
            .filter(|k| {
                target.get(k.as_str()).unwrap_or(&0) - counters.get(k.as_str()).unwrap_or(&0) > 1
            })
            .map(|k| {
                plus_two
                    .with_arg("counter", k)
                    .with_target(target.get(k.as_str()))
            })
            .collect::<Vec<Task>>()
    }

    fn chunker(counters: View<Counters>, target: Target<Counters>) -> Vec<Task> {
        let mut tasks = Vec::new();
        for k in counters
            .keys()
            .filter(|k| {
                target.get(k.as_str()).unwrap_or(&0) - counters.get(k.as_str()).unwrap_or(&0) > 1
            })
            .take(2)
        // take at most 2 changes and create a multi_increment_step
        {
            let mut tgt = (*counters).clone();
            if target.contains_key(k.as_str()) {
                tgt.insert(k.to_string(), *target.get(k.as_str()).unwrap_or(&0));
            }
            tasks.push(multi_increment.with_target(tgt));
        }

        tasks
    }

    let domain = Domain::new()
        .job(
            "/counters/{counter}",
            update(plus_one).with_description(|Args(counter): Args<String>| format!("{counter}++")),
        )
        .job("/counters/{counter}", update(plus_two))
        .job("/counters", update(chunker))
        .job("/counters", none(multi_increment));

    let initial = MyState {
        counters: Map::from([
            ("a".to_string(), 0),
            ("b".to_string(), 0),
            ("c".to_string(), 0),
            ("d".to_string(), 0),
        ]),
    };

    let target = MyState {
        counters: Map::from([
            ("a".to_string(), 3),
            ("b".to_string(), 2),
            ("c".to_string(), 2),
            ("d".to_string(), 2),
        ]),
    };

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // We expect a concurrent dag with two tasks on each branch
    let expected: Dag<&str> = dag!(seq!("a++", "a++"), seq!("b++", "b++"))
        + dag!(seq!("c++", "c++"), seq!("d++", "d++"))
        + seq!("a++");

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn test_array_element_conflicts() {
    init();

    #[derive(Serialize, Deserialize)]
    struct MySys {
        items: Vec<String>,
        configs: HashMap<String, String>,
    }

    impl State for MySys {
        type Target = Self;
    }

    fn update_item(mut item: View<String>, Target(tgt): Target<String>) -> View<String> {
        *item = tgt;
        item
    }

    fn update_config(mut config: View<String>, Target(tgt): Target<String>) -> View<String> {
        *config = tgt;
        config
    }

    fn create_item(
        mut item: View<Option<String>>,
        Target(tgt): Target<String>,
    ) -> View<Option<String>> {
        *item = Some(tgt);
        item
    }

    fn create_config(
        mut config: View<Option<String>>,
        Target(tgt): Target<String>,
    ) -> View<Option<String>> {
        *config = Some(tgt);
        config
    }

    fn non_conflicting_updates(Target(tgt): Target<MySys>) -> Vec<Task> {
        vec![
            update_item
                .with_arg("index", "0")
                .with_target(tgt.items[0].clone()),
            update_item
                .with_arg("index", "1")
                .with_target(tgt.items[1].clone()),
            update_config
                .with_arg("key", "server")
                .with_target(tgt.configs.get("server").unwrap().clone()),
            update_config
                .with_arg("key", "database")
                .with_target(tgt.configs.get("database").unwrap().clone()),
        ]
    }

    let domain = Domain::new()
        .job("/items/{index}", update(update_item))
        .job("/configs/{key}", update(update_config))
        .job("/items/{index}", create(create_item))
        .job("/configs/{key}", create(create_config))
        .job("/", update(non_conflicting_updates));

    let initial = MySys {
        items: vec!["old1".to_string(), "old2".to_string()],
        configs: HashMap::from([
            ("server".to_string(), "oldserver".to_string()),
            ("database".to_string(), "olddatabase".to_string()),
        ]),
    };

    let target = MySys {
        items: vec!["new1".to_string(), "new2".to_string()],
        configs: HashMap::from([
            ("server".to_string(), "newserver".to_string()),
            ("database".to_string(), "newdatabase".to_string()),
        ]),
    };

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Should run concurrently because different array elements and map keys don't conflict
    let expected: Dag<&str> = par!("mahler::worker::planner::tests::test_array_element_conflicts::update_config(/configs/database)",
            "mahler::worker::planner::tests::test_array_element_conflicts::update_config(/configs/server)",
            "mahler::worker::planner::tests::test_array_element_conflicts::update_item(/items/0)",
            "mahler::worker::planner::tests::test_array_element_conflicts::update_item(/items/1)",
        );

    assert_eq!(workflow.to_string(), expected.to_string());
}

#[test]
fn test_stacking_problem() {
    init();

    #[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
    enum Block {
        A,
        B,
        C,
    }

    impl State for Block {
        type Target = Self;
    }

    impl Display for Block {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    enum Location {
        Blk(Block),
        Table,
        Hand,
    }

    impl State for Location {
        type Target = Self;
    }

    impl Location {
        fn is_block(&self) -> bool {
            matches!(self, Location::Blk(_))
        }
    }

    type Blocks = Map<Block, Location>;

    #[derive(Serialize, Deserialize, Debug)]
    struct World {
        blocks: Blocks,
    }

    impl State for World {
        type Target = Self;
    }

    fn is_clear(blocks: &Blocks, loc: &Location) -> bool {
        if loc.is_block() || loc == &Location::Hand {
            // No block is on top of the location
            return blocks.iter().all(|(_, l)| l != loc);
        }
        // the table is always clear
        true
    }

    fn is_holding(blocks: &Blocks) -> bool {
        !is_clear(blocks, &Location::Hand)
    }

    fn all_clear(blocks: &Blocks) -> Vec<&Block> {
        blocks
            .iter()
            .filter(|(b, _)| is_clear(blocks, &Location::Blk((*b).clone())))
            .map(|(b, _)| b)
            .collect()
    }

    // Get a block from the table
    fn pickup(
        mut loc: View<Location>,
        System(sys): System<World>,
        Args(block): Args<Block>,
    ) -> View<Location> {
        // if the block is clear and we are not holding any other blocks
        // we can grab the block
        if *loc == Location::Table
            && is_clear(&sys.blocks, &Location::Blk(block))
            && !is_holding(&sys.blocks)
        {
            *loc = Location::Hand;
        }

        loc
    }

    // Unstack a block from other block
    fn unstack(
        mut loc: View<Location>,
        System(sys): System<World>,
        Args(block): Args<Block>,
    ) -> Option<View<Location>> {
        // if the block is clear and we are not holding any other blocks
        // we can grab the block
        if loc.is_block()
            && is_clear(&sys.blocks, &Location::Blk(block))
            && !is_holding(&sys.blocks)
        {
            *loc = Location::Hand;
            return Some(loc);
        }

        None
    }

    // There is really not that much of a difference between putdown and stack
    // this is just to test that the planner can work with nested methods
    fn putdown(mut loc: View<Location>) -> View<Location> {
        // If we are holding the block and the target is clear
        // then we can modify the block location
        if *loc == Location::Hand {
            *loc = Location::Table
        }

        loc
    }

    fn stack(
        mut loc: View<Location>,
        Target(tgt): Target<Location>,
        System(sys): System<World>,
    ) -> View<Location> {
        // If we are holding the block and the target is clear
        // then we can modify the block location
        if *loc == Location::Hand && is_clear(&sys.blocks, &tgt) {
            *loc = tgt
        }

        loc
    }

    fn take(
        loc: View<Location>,
        System(sys): System<World>,
        Args(block): Args<Block>,
    ) -> Option<Task> {
        if is_clear(&sys.blocks, &Location::Blk(block)) {
            if *loc == Location::Table {
                return Some(pickup.into_task());
            } else {
                return Some(unstack.into_task());
            }
        }
        None
    }

    fn put(loc: View<Location>, Target(tgt): Target<Location>) -> Option<Task> {
        if *loc == Location::Hand {
            if tgt == Location::Table {
                return Some(putdown.into_task());
            } else {
                return Some(stack.with_target(tgt));
            }
        }
        None
    }

    //
    //  This method implements the following block-stacking algorithm [1]:
    //
    //  - If there's a clear block x that can be moved to a place where it won't
    //    need to be moved again, then return a todo list that includes goals to
    //    move it there, followed by mgoal (to achieve the remaining goals).
    //    Otherwise, if there's a clear block x that needs to be moved out of the
    //    way to make another block movable, then return a todo list that includes
    //    goals to move x to the table, followed by mgoal.
    //  - Otherwise, no blocks need to be moved.
    //    [1] N. Gupta and D. S. Nau. On the complexity of blocks-world
    //    planning. Artificial Intelligence 56(2-3):223–254, 1992.
    //
    //  Source: https://github.com/dananau/GTPyhop/blob/main/Examples/blocks_hgn/methods.py
    //
    fn move_blks(blocks: View<Blocks>, Target(target): Target<Blocks>) -> Vec<Task> {
        for blk in all_clear(&blocks) {
            // we assume that the target is well formed
            let tgt_loc = target.get(blk).unwrap();
            let cur_loc = blocks.get(blk).unwrap();

            // The block is free and it can be moved to the final location (another block or the table)
            if cur_loc != tgt_loc && is_clear(&blocks, tgt_loc) {
                return vec![
                    take.with_arg("block", blk.to_string()),
                    put.with_arg("block", blk.to_string()).with_target(tgt_loc),
                ];
            }
        }

        // If we get here, no blocks can be moved to the final location so
        // we move them to the table
        let mut to_table: Vec<Task> = vec![];
        for b in all_clear(&blocks) {
            to_table.push(take.with_arg("block", b.to_string()));
            to_table.push(
                put.with_target(Location::Table)
                    .with_arg("block", b.to_string()),
            );
        }

        to_table
    }
    let domain = Domain::new()
        .jobs(
            "/blocks/{block}",
            [
                update(pickup)
                    .with_description(|Args(block): Args<String>| format!("pick up block {block}")),
                update(unstack)
                    .with_description(|Args(block): Args<String>| format!("unstack block {block}")),
                update(putdown).with_description(|Args(block): Args<String>| {
                    format!("put down block {block}")
                }),
                update(stack).with_description(
                    |Args(block): Args<String>, Target(tgt): Target<Location>| {
                        let tgt_block = match tgt {
                            Location::Blk(block) => format!("{block:?}"),
                            _ => format!("{tgt:?}"),
                        };

                        format!("stack block {block} on top of block {tgt_block}")
                    },
                ),
                update(take),
                update(put),
            ],
        )
        .job("/blocks", update(move_blks));

    let initial = World {
        blocks: Map::from([
            (Block::A, Location::Table),
            (Block::B, Location::Blk(Block::A)),
            (Block::C, Location::Blk(Block::B)),
        ]),
    };
    let target = World {
        blocks: Map::from([
            (Block::A, Location::Blk(Block::B)),
            (Block::B, Location::Blk(Block::C)),
            (Block::C, Location::Table),
        ]),
    };

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();
    let expected: Dag<&str> = seq!(
        "unstack block C",
        "put down block C",
        "unstack block B",
        "stack block B on top of block C",
        "pick up block A",
        "stack block A on top of block B",
    );

    assert_eq!(workflow.to_string(), expected.to_string(),);
}

#[test]
fn test_exception_skips_path_with_simple_condition() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        running: bool,
        #[serde(default)]
        failed: bool,
    }

    impl State for Service {
        type Target = ServiceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ServiceTarget {
        running: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        services: Map<String, Service>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        services: Map<String, ServiceTarget>,
    }

    fn start_service(mut service: View<Service>) -> View<Service> {
        service.running = true;
        service
    }

    // Exception: skip failed services
    fn skip_failed(view: View<Service>) -> bool {
        view.failed
    }

    let domain = Domain::new()
        .job(
            "/services/{service}",
            update(start_service)
                .with_description(|Args(service): Args<String>| format!("start service {service}")),
        )
        .exception("/services/{service}", crate::exception::update(skip_failed));

    let initial = serde_json::from_value::<Device>(json!({
        "services": {
            "one": { "running": false, "failed": false },
            "two": { "running": false, "failed": true },
            "three": { "running": false, "failed": false }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "services": {
            "one": { "running": true },
            "two": { "running": true },
            "three": { "running": true }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Service "two" should be skipped because it failed
    let expected: Dag<&str> = par!("start service one", "start service three");
    assert_eq!(expected.to_string(), workflow.to_string());

    // Verify that the ignored paths include service two
    assert_eq!(workflow.ignored.len(), 1);
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/services/two/running"),
            value: json!(true)
        }
    );
}

#[test]
fn test_exception_with_target_extractor() {
    init();

    #[derive(Serialize, Deserialize)]
    struct App {
        version: String,
        #[serde(default)]
        pinned_version: Option<String>,
    }

    impl State for App {
        type Target = AppTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct AppTarget {
        version: String,
    }

    #[derive(Serialize, Deserialize)]
    struct System {
        apps: Map<String, App>,
    }

    impl State for System {
        type Target = SystemTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct SystemTarget {
        apps: Map<String, AppTarget>,
    }

    fn update_app(mut app: View<App>, Target(tgt): Target<App>) -> View<App> {
        app.version = tgt.version;
        app
    }

    // Exception: skip if target version doesn't match pinned version
    fn skip_if_pinned(view: View<App>, Target(tgt): Target<App>) -> bool {
        if let Some(ref pinned) = view.pinned_version {
            pinned != &tgt.version
        } else {
            false
        }
    }

    let domain = Domain::new()
        .job(
            "/apps/{app}",
            update(update_app)
                .with_description(|Args(app): Args<String>| format!("update app {app}")),
        )
        .exception("/apps/{app}", crate::exception::update(skip_if_pinned));

    let initial = serde_json::from_value::<System>(json!({
        "apps": {
            "foo": { "version": "1.0", "pinned_version": "1.0" },
            "bar": { "version": "1.0", "pinned_version": null },
            "baz": { "version": "1.0" }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<SystemTarget>(json!({
        "apps": {
            "foo": { "version": "2.0" },
            "bar": { "version": "2.0" },
            "baz": { "version": "2.0" }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // App "foo" should be skipped because it's pinned to 1.0
    let expected: Dag<&str> = par!("update app bar", "update app baz");
    assert_eq!(expected.to_string(), workflow.to_string());
    assert_eq!(workflow.ignored.len(), 1);
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/apps/foo/version"),
            value: json!("2.0")
        }
    );
}

#[test]
fn test_exception_only_applies_to_matching_operation() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Resource {
        value: i32,
    }

    impl State for Resource {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Container {
        resources: Map<String, Resource>,
    }

    impl State for Container {
        type Target = Self;
    }

    fn create_resource(
        mut res: View<Option<Resource>>,
        Target(tgt): Target<Resource>,
    ) -> View<Option<Resource>> {
        *res = Some(tgt);
        res
    }

    fn update_resource(mut res: View<Resource>, Target(tgt): Target<Resource>) -> View<Resource> {
        res.value = tgt.value;
        res
    }

    // Exception: skip updates when value is negative
    fn skip_negative_updates(Target(tgt): Target<Resource>) -> bool {
        tgt.value < 0
    }

    let domain = Domain::new()
        .job(
            "/resources/{name}",
            create(create_resource)
                .with_description(|Args(name): Args<String>| format!("create resource {name}")),
        )
        .job(
            "/resources/{name}",
            update(update_resource)
                .with_description(|Args(name): Args<String>| format!("update resource {name}")),
        )
        .exception(
            "/resources/{name}",
            crate::exception::update(skip_negative_updates),
        );

    // Test 1: Update operation should be skipped for negative values
    let initial = serde_json::from_value::<Container>(json!({
        "resources": {
            "a": { "value": 10 },
            "b": { "value": 20 }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<Container>(json!({
        "resources": {
            "a": { "value": -5 },
            "b": { "value": 30 }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Resource "a" update should be skipped, but "b" should be updated
    let expected: Dag<&str> = seq!("update resource b");
    assert_eq!(expected.to_string(), workflow.to_string());
    assert_eq!(workflow.ignored.len(), 1);

    // Test 2: Create operation should NOT be skipped even for negative values
    // because the exception only applies to update operations
    let initial = serde_json::from_value::<Container>(json!({
        "resources": {}
    }))
    .unwrap();

    let target = serde_json::from_value::<Container>(json!({
        "resources": {
            "c": { "value": -10 }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Resource "c" should be created even though value is negative
    let expected: Dag<&str> = seq!("create resource c");
    assert_eq!(expected.to_string(), workflow.to_string());
    assert!(workflow.ignored.is_empty());
}

#[test]
fn test_multiple_exceptions_on_different_paths() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        running: bool,
        #[serde(default)]
        install_failed: bool,
    }

    impl State for Service {
        type Target = ServiceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ServiceTarget {
        running: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct Config {
        value: String,
        #[serde(default)]
        readonly: bool,
    }

    impl State for Config {
        type Target = ConfigTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ConfigTarget {
        value: String,
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        services: Map<String, Service>,
        config: Map<String, Config>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        services: Map<String, ServiceTarget>,
        config: Map<String, ConfigTarget>,
    }

    fn start_service(mut service: View<Service>) -> View<Service> {
        service.running = true;
        service
    }

    fn update_config(mut config: View<Config>, Target(tgt): Target<Config>) -> View<Config> {
        config.value = tgt.value;
        config
    }

    // Exception 1: skip failed services
    fn skip_failed_services(view: View<Service>) -> bool {
        view.install_failed
    }

    // Exception 2: skip readonly configs
    fn skip_readonly_configs(view: View<Config>) -> bool {
        view.readonly
    }

    let domain = Domain::new()
        .job(
            "/services/{service}",
            update(start_service)
                .with_description(|Args(service): Args<String>| format!("start service {service}")),
        )
        .job(
            "/config/{key}",
            update(update_config)
                .with_description(|Args(key): Args<String>| format!("update config {key}")),
        )
        .exception(
            "/services/{service}",
            crate::exception::update(skip_failed_services),
        )
        .exception(
            "/config/{key}",
            crate::exception::update(skip_readonly_configs),
        );

    let initial = serde_json::from_value::<Device>(json!({
        "services": {
            "web": { "running": false, "install_failed": true },
            "api": { "running": false, "install_failed": false }
        },
        "config": {
            "host": { "value": "old", "readonly": true },
            "port": { "value": "8080", "readonly": false }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "services": {
            "web": { "running": true },
            "api": { "running": true }
        },
        "config": {
            "host": { "value": "new" },
            "port": { "value": "9090" }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Service "web" and config "host" should be skipped
    // Check that both tasks are present (order may vary)
    let workflow_str = workflow.to_string();
    assert!(workflow_str.contains("start service api"));
    assert!(workflow_str.contains("update config port"));
    assert_eq!(workflow.ignored.len(), 2);
    assert_eq!(
        workflow
            .ignored
            .iter()
            .find(|op| op.operation.path().starts_with("/services/web"))
            .map(|op| &op.operation),
        Some(&Operation::Update {
            path: Path::from_static("/services/web/running"),
            value: json!(true)
        })
    );
    assert_eq!(
        workflow
            .ignored
            .iter()
            .find(|op| op.operation.path().starts_with("/config/host"))
            .map(|op| &op.operation),
        Some(&Operation::Update {
            path: Path::from_static("/config/host/value"),
            value: json!("new")
        })
    );
}

#[test]
fn test_exception_with_system_extractor() {
    init();

    #[derive(Serialize, Deserialize)]
    struct App {
        image: String,
    }

    impl State for App {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Image {
        available: bool,
    }

    impl State for Image {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        apps: Map<String, App>,
        images: Map<String, Image>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        apps: Map<String, App>,
    }

    fn update_app(mut app: View<App>, Target(tgt): Target<App>) -> View<App> {
        app.image = tgt.image;
        app
    }

    // Exception: skip apps if their image is not available
    fn skip_if_image_unavailable(
        view: View<App>,
        crate::extract::System(device): crate::extract::System<Device>,
    ) -> bool {
        device
            .images
            .get(&view.image)
            .map(|img| !img.available)
            .unwrap_or(true)
    }

    let domain = Domain::new()
        .job(
            "/apps/{app}",
            update(update_app)
                .with_description(|Args(app): Args<String>| format!("update app {app}")),
        )
        .exception(
            "/apps/{app}",
            crate::exception::update(skip_if_image_unavailable),
        );

    let initial = serde_json::from_value::<Device>(json!({
        "apps": {
            "web": { "image": "nginx:1.0" },
            "api": { "image": "node:14" },
            "worker": { "image": "python:3.9" }
        },
        "images": {
            "nginx:1.0": { "available": true },
            "node:14": { "available": false },
            "python:3.9": { "available": true }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "apps": {
            "web": { "image": "nginx:2.0" },
            "api": { "image": "node:16" },
            "worker": { "image": "python:3.10" }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // App "api" should be skipped because node:14 image is not available
    let expected: Dag<&str> = par!("update app web", "update app worker");
    assert_eq!(expected.to_string(), workflow.to_string());
    assert_eq!(workflow.ignored.len(), 1);
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/apps/api/image"),
            value: json!("node:16")
        }
    );
}

#[test]
fn test_exception_preserves_parent_path_skip() {
    init();

    #[derive(Serialize, Deserialize)]
    struct NestedValue {
        inner: String,
    }

    impl State for NestedValue {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Container {
        name: String,
        nested: NestedValue,
    }

    impl State for Container {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct Root {
        containers: Map<String, Container>,
    }

    impl State for Root {
        type Target = Self;
    }

    fn update_name(mut name: View<String>, Target(tgt): Target<String>) -> View<String> {
        *name = tgt;
        name
    }

    fn update_inner(mut inner: View<String>, Target(tgt): Target<String>) -> View<String> {
        *inner = tgt;
        inner
    }

    // Exception: skip containers with name "disabled"
    fn skip_disabled_containers(view: View<Container>) -> bool {
        view.name == "disabled"
    }

    let domain = Domain::new()
        .job(
            "/containers/{id}/name",
            update(update_name)
                .with_description(|Args(id): Args<String>| format!("update container {id} name")),
        )
        .job(
            "/containers/{id}/nested/inner",
            update(update_inner).with_description(|Args(id): Args<String>| {
                format!("update container {id} nested value")
            }),
        )
        .exception(
            "/containers/{id}",
            crate::exception::update(skip_disabled_containers),
        );

    let initial = serde_json::from_value::<Root>(json!({
        "containers": {
            "a": {
                "name": "disabled",
                "nested": { "inner": "old1" }
            },
            "b": {
                "name": "enabled",
                "nested": { "inner": "old2" }
            }
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<Root>(json!({
        "containers": {
            "a": {
                "name": "still-disabled",
                "nested": { "inner": "new1" }
            },
            "b": {
                "name": "still-enabled",
                "nested": { "inner": "new2" }
            }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Container "a" and all its nested fields should be skipped
    let expected: Dag<&str> = par!("update container b name", "update container b nested value");
    assert_eq!(expected.to_string(), workflow.to_string());

    // The parent path /containers/a should be on the ignore list
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/containers/a/name"),
            value: json!("still-disabled")
        }
    );
}

#[test]
fn test_exception_last_iteration() {
    #[derive(Serialize, Deserialize, PartialEq, Eq)]
    enum AppInstallStatus {
        Created,
        Installed,
        Running,
    }

    impl State for AppInstallStatus {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct App {
        cmd: String,
        status: AppInstallStatus,
    }

    impl State for App {
        type Target = Self;
    }

    #[derive(Serialize, Deserialize)]
    struct MySys {
        apps: Map<String, App>,
    }

    impl State for MySys {
        type Target = Self;
    }

    fn create_app(app: View<Option<App>>, Target(t_app): Target<App>) -> View<App> {
        app.create(App {
            status: AppInstallStatus::Created,
            ..t_app
        })
    }

    fn install_app(mut app: View<App>) -> View<App> {
        app.status = AppInstallStatus::Installed;
        app
    }

    let domain = Domain::new()
        .jobs(
            "/apps/{app_name}",
            [
                create(create_app)
                    .with_description(|Args(name): Args<String>| format!("create app {name}")),
                update(install_app)
                    .with_description(|Args(name): Args<String>| format!("install app {name}")),
            ],
        )
        .exception(
            "/apps/{app_name}",
            crate::exception::update(|app: View<App>| app.status == AppInstallStatus::Installed),
        );

    let initial = serde_json::from_value::<MySys>(json!({"apps": {}})).unwrap();
    let target = serde_json::from_value::<MySys>(json!({
        "apps": {
            "my-app": {
                "cmd": "sleep infinity",
                "status": "Running",
            }
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // my-app should be installed, but it won't be started until an external
    // event happens (like a reboot), so it should find a workflow
    let expected: Dag<&str> = seq!("create app my-app", "install app my-app");
    assert_eq!(expected.to_string(), workflow.to_string());

    // The status update should be in the ignored list
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/apps/my-app/status"),
            value: json!("Running")
        }
    );
}

#[test]
fn test_exception_description_populates_reason() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        running: bool,
        #[serde(default)]
        failed: bool,
    }

    impl State for Service {
        type Target = ServiceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ServiceTarget {
        running: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        services: Map<String, Service>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        services: Map<String, ServiceTarget>,
    }

    fn start_service(mut service: View<Service>) -> View<Service> {
        service.running = true;
        service
    }

    fn skip_failed(view: View<Service>) -> bool {
        view.failed
    }

    let domain = Domain::new()
        .job(
            "/services/{service}",
            update(start_service)
                .with_description(|Args(s): Args<String>| format!("start service {s}")),
        )
        .exception(
            "/services/{service}",
            crate::exception::update(skip_failed)
                .with_description(|Args(s): Args<String>| format!("service '{s}' is failed")),
        );

    let initial = serde_json::from_value::<Device>(json!({
        "services": {
            "one": { "running": false, "failed": false },
            "two": { "running": false, "failed": true },
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "services": {
            "one": { "running": true },
            "two": { "running": true },
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Service "one" should be started, service "two" should be skipped
    let expected: Dag<&str> = seq!("start service one");
    assert_eq!(expected.to_string(), workflow.to_string());

    assert_eq!(workflow.ignored.len(), 1);
    assert_eq!(
        workflow.ignored[0].operation,
        Operation::Update {
            path: Path::from_static("/services/two/running"),
            value: json!(true)
        }
    );
    // The reason should be populated from with_description
    assert_eq!(
        workflow.ignored[0].reason,
        Some("service 'two' is failed".to_string())
    );
}

#[test]
fn test_exception_without_description_has_no_reason() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        running: bool,
        #[serde(default)]
        failed: bool,
    }

    impl State for Service {
        type Target = ServiceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ServiceTarget {
        running: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        services: Map<String, Service>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        services: Map<String, ServiceTarget>,
    }

    fn start_service(mut service: View<Service>) -> View<Service> {
        service.running = true;
        service
    }

    fn skip_failed(view: View<Service>) -> bool {
        view.failed
    }

    // Exception without with_description
    let domain = Domain::new()
        .job(
            "/services/{service}",
            update(start_service)
                .with_description(|Args(s): Args<String>| format!("start service {s}")),
        )
        .exception("/services/{service}", crate::exception::update(skip_failed));

    let initial = serde_json::from_value::<Device>(json!({
        "services": {
            "one": { "running": false, "failed": false },
            "two": { "running": false, "failed": true },
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "services": {
            "one": { "running": true },
            "two": { "running": true },
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    assert_eq!(workflow.ignored.len(), 1);
    // reason should be None when no description is set
    assert_eq!(workflow.ignored[0].reason, None);
}

#[test]
fn test_multiple_skipped_services_each_get_correct_reason() {
    init();

    #[derive(Serialize, Deserialize)]
    struct Service {
        running: bool,
        #[serde(default)]
        failed: bool,
    }

    impl State for Service {
        type Target = ServiceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct ServiceTarget {
        running: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct Device {
        services: Map<String, Service>,
    }

    impl State for Device {
        type Target = DeviceTarget;
    }

    #[derive(Serialize, Deserialize)]
    struct DeviceTarget {
        services: Map<String, ServiceTarget>,
    }

    fn start_service(mut service: View<Service>) -> View<Service> {
        service.running = true;
        service
    }

    fn skip_failed(view: View<Service>) -> bool {
        view.failed
    }

    let domain = Domain::new()
        .job(
            "/services/{service}",
            update(start_service)
                .with_description(|Args(s): Args<String>| format!("start service {s}")),
        )
        .exception(
            "/services/{service}",
            crate::exception::update(skip_failed)
                .with_description(|Args(s): Args<String>| format!("service '{s}' is failed")),
        );

    let initial = serde_json::from_value::<Device>(json!({
        "services": {
            "one": { "running": false, "failed": false },
            "two": { "running": false, "failed": true },
            "three": { "running": false, "failed": true },
        }
    }))
    .unwrap();

    let target = serde_json::from_value::<DeviceTarget>(json!({
        "services": {
            "one": { "running": true },
            "two": { "running": true },
            "three": { "running": true },
        }
    }))
    .unwrap();

    let workflow = find_plan(&domain, initial, target).unwrap().unwrap();

    // Only service "one" should be started
    let expected: Dag<&str> = seq!("start service one");
    assert_eq!(expected.to_string(), workflow.to_string());

    assert_eq!(workflow.ignored.len(), 2);

    // Each skipped service must have its own reason, not the other service's reason
    let two = workflow
        .ignored
        .iter()
        .find(|op| op.operation.path().starts_with("/services/two"));
    assert_eq!(
        two.and_then(|op| op.reason.as_deref()),
        Some("service 'two' is failed")
    );

    let three = workflow
        .ignored
        .iter()
        .find(|op| op.operation.path().starts_with("/services/three"));
    assert_eq!(
        three.and_then(|op| op.reason.as_deref()),
        Some("service 'three' is failed")
    );
}
