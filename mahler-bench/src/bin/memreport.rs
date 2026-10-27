//! Memory report for the benchmark scenarios
//!
//! Criterion measures time; this binary measures the heap traffic of the same
//! scenarios, using the counting allocator from [`mahler_bench::alloc`]. For each
//! scenario it reports what building the worker retains, and what planning (and
//! optionally running) allocates.
//!
//! ```text
//! cargo run --release --bin mahler-memreport -- [OPTIONS]
//!
//!   --run              also plan and run the workflow, not just plan it
//!   --json             print JSON instead of a table
//!   --filter <text>    only run scenarios whose name contains <text>
//! ```
//!
//! The elapsed times printed here are indicative only: accounting for every allocation
//! adds overhead and each scenario is measured once. Use `cargo bench` for timings.

use std::time::{Duration, Instant};

use mahler_bench::alloc::{measure, AllocStats, CountingAllocator};
use mahler_bench::plan::PlanStats;
use mahler_bench::scenario;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

struct Row {
    group: &'static str,
    name: String,
    plan: PlanStats,
    distance: usize,
    /// heap held by the worker once it is built
    retained_bytes: isize,
    plan_time: Duration,
    plan_alloc: AllocStats,
    run: Option<(Duration, AllocStats)>,
}

fn kib(bytes: isize) -> f64 {
    bytes as f64 / 1024.0
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn print_table(rows: &[Row], with_run: bool) {
    println!(
        "{:<44} {:>6} {:>6} {:>6} {:>10} {:>10} {:>11} {:>11}",
        "scenario", "tasks", "forks", "dist", "plan ms", "allocs", "alloc MiB", "peak KiB"
    );
    println!("{}", "-".repeat(114));

    let mut group = "";
    for row in rows {
        if row.group != group {
            group = row.group;
            println!("{group}:");
        }

        println!(
            "  {:<42} {:>6} {:>6} {:>6} {:>10.2} {:>10} {:>11.2} {:>11.1}",
            row.name,
            row.plan.tasks,
            row.plan.forks,
            row.distance,
            row.plan_time.as_secs_f64() * 1e3,
            row.plan_alloc.allocations,
            mib(row.plan_alloc.allocated_bytes),
            kib(row.plan_alloc.peak_bytes as isize),
        );
    }

    println!();
    println!(
        "{:<44} {:>14} {:>14}",
        "worker (built, before planning)", "retained KiB", "state ops"
    );
    println!("{}", "-".repeat(74));
    for row in rows {
        println!(
            "  {:<42} {:>14.1} {:>14}",
            row.name,
            kib(row.retained_bytes),
            row.distance
        );
    }

    if with_run {
        println!();
        println!(
            "{:<44} {:>10} {:>10} {:>11} {:>11}",
            "seek_target (plan and run)", "ms", "allocs", "alloc MiB", "peak KiB"
        );
        println!("{}", "-".repeat(90));
        for row in rows {
            if let Some((elapsed, stats)) = &row.run {
                println!(
                    "  {:<42} {:>10.2} {:>10} {:>11.2} {:>11.1}",
                    row.name,
                    elapsed.as_secs_f64() * 1e3,
                    stats.allocations,
                    mib(stats.allocated_bytes),
                    kib(stats.peak_bytes as isize),
                );
            }
        }
    }
}

fn print_json(rows: &[Row]) {
    let entries = rows
        .iter()
        .map(|row| {
            let mut entry = serde_json::json!({
                "group": row.group,
                "scenario": row.name,
                "tasks": row.plan.tasks,
                "forks": row.plan.forks,
                "distance": row.distance,
                "worker_retained_bytes": row.retained_bytes,
                "plan": {
                    "elapsed_ns": row.plan_time.as_nanos() as u64,
                    "allocations": row.plan_alloc.allocations,
                    "allocated_bytes": row.plan_alloc.allocated_bytes,
                    "peak_bytes": row.plan_alloc.peak_bytes,
                },
            });

            if let Some((elapsed, stats)) = &row.run {
                entry["run"] = serde_json::json!({
                    "elapsed_ns": elapsed.as_nanos() as u64,
                    "allocations": stats.allocations,
                    "allocated_bytes": stats.allocated_bytes,
                    "peak_bytes": stats.peak_bytes,
                });
            }

            entry
        })
        .collect::<Vec<_>>();

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Array(entries))
            .expect("report should serialize")
    );
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let json = args.iter().any(|a| a == "--json");
    let with_run = args.iter().any(|a| a == "--run");
    let filter = args
        .iter()
        .position(|a| a == "--filter")
        .and_then(|i| args.get(i + 1))
        .cloned();

    // a current thread runtime keeps every allocation on the measuring thread
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime should build");

    let mut rows = Vec::new();
    for scenario in scenario::all() {
        if let Some(filter) = &filter {
            if !scenario.name.contains(filter.as_str()) {
                continue;
            }
        }

        let (case, build_stats) = measure(|| scenario.build());
        let distance = case.distance();

        let start = Instant::now();
        let (workflow, plan_alloc) = measure(|| case.plan());
        let plan_time = start.elapsed();
        let workflow = workflow.unwrap_or_else(|| panic!("no plan found for {}", scenario.name));
        let plan = PlanStats::of(&workflow);
        drop(workflow);
        drop(case);

        let run = with_run.then(|| {
            let case = scenario.build();
            let start = Instant::now();
            let (_, stats) = measure(|| runtime.block_on(case.seek()));
            (start.elapsed(), stats)
        });

        rows.push(Row {
            group: scenario.group,
            name: scenario.name,
            plan,
            distance,
            retained_bytes: build_stats.retained_bytes,
            plan_time,
            plan_alloc,
            run,
        });
    }

    if json {
        print_json(&rows);
    } else {
        print_table(&rows, with_run);
    }
}
