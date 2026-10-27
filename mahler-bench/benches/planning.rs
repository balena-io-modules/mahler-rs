//! Planning benchmarks
//!
//! `find_plan` is the whole HTN search: diff the current state against the target,
//! expand the matching jobs into candidates, pick one, and repeat until the distance
//! is empty. Throughput is reported in plan tasks, so the results can be read as time
//! per planned task as well as time per plan.
//!
//! Two smaller groups isolate the parts of the search that dominate the cost:
//! `distance` only diffs the state against the target, and `build` only constructs the
//! worker and loads the initial state.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mahler_bench::plan::PlanStats;
use mahler_bench::scenario::{self, Scenario};

fn plan_group(c: &mut Criterion, name: &str, scenarios: Vec<Scenario>) {
    let mut group = c.benchmark_group(format!("plan/{name}"));

    for scenario in scenarios {
        let case = scenario.build();
        let workflow = case
            .plan()
            .unwrap_or_else(|| panic!("no plan found for {}", scenario.name));
        let stats = PlanStats::of(&workflow);

        group.throughput(Throughput::Elements(stats.tasks as u64));
        group.bench_function(scenario.name.clone(), |b| b.iter(|| black_box(case.plan())));
    }

    group.finish();
}

fn planning(c: &mut Criterion) {
    plan_group(c, "blocks", scenario::blocks());
    plan_group(c, "logistics", scenario::logistics());
    plan_group(c, "satellite", scenario::satellite());
}

/// State diffing on its own, without any search
fn distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance");

    for scenario in scenario::representative() {
        let case = scenario.build();
        group.throughput(Throughput::Elements(case.distance() as u64));
        group.bench_function(scenario.name.clone(), |b| {
            b.iter(|| black_box(case.distance()))
        });
    }

    group.finish();
}

/// Worker construction: job registration and loading of the initial state
fn build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");

    for scenario in scenario::representative() {
        group.bench_function(scenario.name.clone(), |b| {
            b.iter(|| black_box(scenario.build()))
        });
    }

    group.finish();
}

criterion_group!(benches, planning, distance, build);
criterion_main!(benches);
