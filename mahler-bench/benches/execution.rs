//! Execution benchmarks
//!
//! `seek_target` plans and then runs the workflow, re-planning if the state drifts.
//! The jobs of the benchmark domains are pure, so what is measured here is the runtime
//! overhead of the worker itself: dry running each task, applying its patch, pushing
//! the new state to the subscribers and walking the DAG, sequentially for blocks and
//! logistics, concurrently for satellite.
//!
//! The worker is consumed by a run, so each iteration gets a freshly built one. That
//! setup is not measured.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mahler_bench::plan::PlanStats;
use mahler_bench::scenario;

fn execution(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("tokio runtime should build");

    let mut group = c.benchmark_group("seek_target");

    for scenario in scenario::representative() {
        let workflow = scenario
            .build()
            .plan()
            .unwrap_or_else(|| panic!("no plan found for {}", scenario.name));
        let stats = PlanStats::of(&workflow);

        group.throughput(Throughput::Elements(stats.tasks as u64));
        group.bench_function(scenario.name.clone(), |b| {
            b.iter_batched(
                || scenario.build(),
                |case| black_box(runtime.block_on(case.seek())),
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, execution);
criterion_main!(benches);
