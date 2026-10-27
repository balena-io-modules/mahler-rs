//! Checks on the benchmark scenarios
//!
//! A benchmark that measures a failed search, or a search over an empty plan, measures
//! nothing, so every scenario is checked here for the plan it produces. The plans are
//! deterministic, which makes the task counts a regression test for the domains.

use mahler::worker::SeekStatus;
use mahler_bench::domain::{blocks, logistics, satellite};
use mahler_bench::plan::PlanStats;
use mahler_bench::scenario;

/// A benchmark of `seek_target` that stops early would measure less work, not a faster
/// worker
#[test]
fn every_scenario_reaches_its_target() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime should build");

    for scenario in scenario::all() {
        let status = runtime.block_on(scenario.build().seek());
        assert!(
            matches!(status, SeekStatus::Success),
            "{} did not reach its target: {status:?}",
            scenario.name
        );
    }
}

#[test]
fn every_scenario_has_a_plan() {
    for scenario in scenario::all() {
        let case = scenario.build();
        let workflow = case
            .plan()
            .unwrap_or_else(|| panic!("no plan found for {}", scenario.name));

        let stats = PlanStats::of(&workflow);
        assert!(
            stats.tasks > 0,
            "{} planned an empty workflow",
            scenario.name
        );
        assert!(
            case.distance() > 0,
            "{} is already at its target",
            scenario.name
        );
    }
}

/// Turning a tower upside down takes one move, two tasks, per block
#[test]
fn blocks_tower_reversal_takes_one_move_per_block() {
    for hands in [1, 2] {
        for count in [4usize, 8, 16] {
            let (initial, target) = blocks::tower_reversal(count, hands);
            let workflow = blocks::worker(initial).find_plan(&target).unwrap().unwrap();

            assert_eq!(
                PlanStats::of(&workflow).tasks,
                count * 2,
                "tower_reversal with {count} blocks and {hands} hands"
            );
        }
    }
}

/// Rebuilding a tower takes the tower apart first: two moves per block, minus the two
/// blocks that are moved only once
#[test]
fn blocks_tower_rebuild_takes_the_tower_apart() {
    for hands in [1, 2] {
        for count in [4usize, 8, 16] {
            let (initial, target) = blocks::tower_rebuild(count, hands);
            let workflow = blocks::worker(initial).find_plan(&target).unwrap().unwrap();

            assert_eq!(
                PlanStats::of(&workflow).tasks,
                (count * 2 - 2) * 2,
                "tower_rebuild with {count} blocks and {hands} hands"
            );
        }
    }
}

#[test]
fn blocks_plan_is_the_expected_sequence() {
    let (initial, target) = blocks::tower_reversal(3, 1);
    let workflow = blocks::worker(initial).find_plan(&target).unwrap().unwrap();

    assert_eq!(
        workflow.to_string(),
        [
            "- unstack b002",
            "- put down b002",
            "- unstack b001",
            "- stack b001 on b002",
            "- pick up b000",
            "- stack b000 on b001",
        ]
        .join("\n")
    );
}

/// With two hands the planner picks two blocks up before putting either down
#[test]
fn blocks_two_hands_hold_two_blocks_at_once() {
    let (initial, target) = blocks::independent_towers(2, 2, 2);
    let workflow = blocks::worker(initial).find_plan(&target).unwrap().unwrap();

    assert_eq!(
        workflow.to_string(),
        [
            "- unstack t0b001",
            "- unstack t1b001",
            "- put down t0b001",
            "- put down t1b001",
            "- pick up t0b000",
            "- pick up t1b000",
            "- stack t0b000 on t0b001",
            "- stack t1b000 on t1b001",
        ]
        .join("\n")
    );

    // the same moves, one at a time, with a single hand
    let (initial, target) = blocks::independent_towers(2, 2, 1);
    let one_hand = blocks::worker(initial).find_plan(&target).unwrap().unwrap();
    assert_eq!(
        PlanStats::of(&one_hand).tasks,
        PlanStats::of(&workflow).tasks
    );
}

/// A delivery to another city is a truck leg, a flight and another truck leg
#[test]
fn logistics_delivers_through_the_airport() {
    let (initial, target) = logistics::deliveries(1, 2, 3, 1);
    let workflow = logistics::worker(initial)
        .find_plan(&target)
        .unwrap()
        .unwrap();

    assert_eq!(
        workflow.to_string(),
        [
            "- drive t0 to c0l1",
            "- load pkg000 into truck",
            "- drive t0 to c0l0",
            "- unload pkg000 from truck",
            "- load pkg000 into plane",
            "- fly p0 to c1l0",
            "- unload pkg000 from plane",
            "- load pkg000 into truck",
            "- drive t1 to c1l2",
            "- unload pkg000 from truck",
        ]
        .join("\n")
    );
}

/// Trucks and airplanes are shared, so with more packages than cities they have to
/// leave the same place more than once
#[test]
fn logistics_scales_with_the_number_of_deliveries() {
    for (packages, cities) in [(3usize, 4usize), (4, 4), (8, 4), (8, 2)] {
        let (initial, target) = logistics::deliveries(packages, cities, 4, 2);
        let workflow = logistics::worker(initial)
            .find_plan(&target)
            .unwrap()
            .unwrap();

        // ten tasks per delivery, but every package after the first starts where the
        // previous one was dropped, so its truck is already there and needs no drive
        assert_eq!(
            PlanStats::of(&workflow).tasks,
            packages * 9 + 1,
            "{packages} deliveries over {cities} cities"
        );
    }
}

/// Every image comes after its instrument is powered and calibrated and the satellite
/// is turned towards it; only the turn towards the calibration direction runs alongside
/// the instrument switches
#[test]
fn satellite_plan_respects_the_preconditions() {
    let (initial, target) = satellite::observations(1, 2, 4);
    let workflow = satellite::worker(initial)
        .find_plan(&target)
        .unwrap()
        .unwrap();

    assert_eq!(
        workflow.to_string(),
        [
            "+ ~ - turn s0 to cal-s0-i0",
            "  ~ - switch on s0 i0",
            "- calibrate s0 i0",
            "- turn s0 to dir-s0-0",
            "- take image i0-img000 with s0",
            "- turn s0 to dir-s0-2",
            "- take image i0-img002 with s0",
            "+ ~ - turn s0 to cal-s0-i1",
            "  ~ - switch off s0 i0",
            "- switch on s0 i1",
            "- calibrate s0 i1",
            "- turn s0 to dir-s0-1",
            "- take image i1-img001 with s0",
            "- turn s0 to dir-s0-3",
            "- take image i1-img003 with s0",
        ]
        .join("\n")
    );
}

/// Satellites do not share any state, so the planner runs them concurrently
#[test]
fn satellite_plans_are_concurrent() {
    let single = satellite::observations(1, 2, 4);
    let single = satellite::worker(single.0)
        .find_plan(&single.1)
        .unwrap()
        .unwrap();
    let single = PlanStats::of(&single);

    for satellites in [2usize, 4, 8] {
        let (initial, target) = satellite::observations(satellites, 2, 4);
        let workflow = satellite::worker(initial)
            .find_plan(&target)
            .unwrap()
            .unwrap();
        let stats = PlanStats::of(&workflow);

        // the same work as one satellite, once per satellite ...
        assert_eq!(
            stats.tasks,
            single.tasks * satellites,
            "observations with {satellites} satellites"
        );
        // ... and it does not end up in a single sequence
        assert!(
            stats.forks > single.forks,
            "observations with {satellites} satellites planned {} forks",
            stats.forks
        );
    }
}
