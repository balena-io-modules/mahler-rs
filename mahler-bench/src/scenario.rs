//! The list of benchmark scenarios
//!
//! Every scenario builds a worker and the target it has to converge to. The criterion
//! benchmarks and the memory report both walk this list, so they always measure the
//! same workloads.

use std::future::Future;
use std::pin::Pin;

use mahler::state::State;
use mahler::worker::{Ready, SeekStatus, Worker, Workflow};

use crate::domain::{blocks, logistics, satellite};

/// A worker with a target, ready to be planned for or run
pub trait Case: Send {
    /// Search for a plan to reach the target, without running anything
    fn plan(&self) -> Option<Workflow>;

    /// Number of state operations between the current state and the target
    fn distance(&self) -> usize;

    /// Plan and run until the target is reached
    fn seek(self: Box<Self>) -> Pin<Box<dyn Future<Output = SeekStatus> + Send>>;
}

struct WorkerCase<O: State> {
    worker: Worker<O, Ready>,
    target: O::Target,
}

impl<O> Case for WorkerCase<O>
where
    O: State + Send + Sync + Unpin + std::fmt::Debug + 'static,
    O::Target: Send + Sync + 'static,
{
    fn plan(&self) -> Option<Workflow> {
        self.worker
            .find_plan(&self.target)
            .expect("planning should not fail")
    }

    fn distance(&self) -> usize {
        self.worker
            .distance(&self.target)
            .expect("distance should not fail")
            .len()
    }

    fn seek(self: Box<Self>) -> Pin<Box<dyn Future<Output = SeekStatus> + Send>> {
        let WorkerCase { worker, target } = *self;
        Box::pin(async move {
            let (_, status) = worker
                .seek_target(target)
                .await
                .expect("running the workflow should not fail");
            status
        })
    }
}

/// A named benchmark scenario
pub struct Scenario {
    /// Domain the scenario belongs to, used to group the benchmark results
    pub group: &'static str,
    /// Name of the scenario within its domain
    pub name: String,
    build: Box<dyn Fn() -> Box<dyn Case> + Send + Sync>,
}

impl Scenario {
    /// Build a fresh worker for the scenario
    ///
    /// Building is not part of what the benchmarks measure: a worker is set up once per
    /// benchmark and reused for planning, and rebuilt between runs for execution, since
    /// running consumes it.
    pub fn build(&self) -> Box<dyn Case> {
        (self.build)()
    }
}

fn scenario(
    group: &'static str,
    name: String,
    build: impl Fn() -> Box<dyn Case> + Send + Sync + 'static,
) -> Scenario {
    Scenario {
        group,
        name,
        build: Box::new(build),
    }
}

/// Blocks world scenarios, over the number of blocks and the number of hands
pub fn blocks() -> Vec<Scenario> {
    let mut scenarios = Vec::new();

    for hands in [1usize, 2] {
        for count in [8usize, 16, 32] {
            scenarios.push(scenario(
                "blocks",
                format!("tower_reversal/blocks={count}/hands={hands}"),
                move || {
                    let (initial, target) = blocks::tower_reversal(count, hands);
                    Box::new(WorkerCase {
                        worker: blocks::worker(initial),
                        target,
                    })
                },
            ));
        }

        for count in [8usize, 16] {
            scenarios.push(scenario(
                "blocks",
                format!("tower_rebuild/blocks={count}/hands={hands}"),
                move || {
                    let (initial, target) = blocks::tower_rebuild(count, hands);
                    Box::new(WorkerCase {
                        worker: blocks::worker(initial),
                        target,
                    })
                },
            ));
        }

        for towers in [2usize, 4, 8] {
            scenarios.push(scenario(
                "blocks",
                format!("independent_towers/towers={towers}/hands={hands}"),
                move || {
                    let (initial, target) = blocks::independent_towers(towers, 4, hands);
                    Box::new(WorkerCase {
                        worker: blocks::worker(initial),
                        target,
                    })
                },
            ));
        }
    }

    scenarios
}

/// Logistics scenarios, over the number of cities (width) and packages (depth)
///
/// With more packages than cities, trucks and airplanes have to go back to a place
/// they already left to pick up the next package.
pub fn logistics() -> Vec<Scenario> {
    let mut scenarios = Vec::new();

    for cities in [4usize, 8, 16] {
        scenarios.push(deliveries(16, cities));
    }

    for packages in [4usize, 8, 32] {
        scenarios.push(deliveries(packages, 4));
    }

    scenarios
}

fn deliveries(packages: usize, cities: usize) -> Scenario {
    scenario(
        "logistics",
        format!("deliveries/cities={cities}/packages={packages}"),
        move || {
            let (initial, target) = logistics::deliveries(packages, cities, 4, 2);
            Box::new(WorkerCase {
                worker: logistics::worker(initial),
                target,
            })
        },
    )
}

/// Satellite scenarios, over the number of satellites (width) and images (depth)
pub fn satellite() -> Vec<Scenario> {
    let mut scenarios = Vec::new();

    for satellites in [1usize, 2, 4, 8] {
        scenarios.push(scenario(
            "satellite",
            format!("observations/satellites={satellites}/images=4"),
            move || {
                let (initial, target) = satellite::observations(satellites, 2, 4);
                Box::new(WorkerCase {
                    worker: satellite::worker(initial),
                    target,
                })
            },
        ));
    }

    for images in [2usize, 8, 16] {
        scenarios.push(scenario(
            "satellite",
            format!("observations/satellites=4/images={images}"),
            move || {
                let (initial, target) = satellite::observations(4, 2, images);
                Box::new(WorkerCase {
                    worker: satellite::worker(initial),
                    target,
                })
            },
        ));
    }

    scenarios
}

/// Every scenario, in a stable order
pub fn all() -> Vec<Scenario> {
    let mut scenarios = blocks();
    scenarios.extend(logistics());
    scenarios.extend(satellite());
    scenarios
}

/// A small representative subset, one scenario per shape
///
/// The execution benchmarks and the secondary planning groups use this: running a
/// workflow is far slower than planning it, so measuring every scenario would make the
/// benchmark suite take minutes without adding information.
pub fn representative() -> Vec<Scenario> {
    let wanted = [
        "tower_reversal/blocks=16/hands=1",
        "tower_reversal/blocks=16/hands=2",
        "independent_towers/towers=4/hands=2",
        "deliveries/cities=4/packages=8",
        "observations/satellites=4/images=4",
    ];

    let mut all = all();
    wanted
        .iter()
        .map(|name| {
            let index = all
                .iter()
                .position(|s| s.name == *name)
                .unwrap_or_else(|| panic!("no scenario named {name}"));
            all.swap_remove(index)
        })
        .collect()
}
