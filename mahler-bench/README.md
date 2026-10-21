# mahler-bench

Speed and memory benchmarks for the planner and the runtime of
[mahler](https://crates.io/crates/mahler).

Like the crates under `examples/`, this is a standalone crate rather than a member of
the workspace, so its dependencies never constrain the library: criterion needs a newer
toolchain than mahler's MSRV of 1.81. Run everything from this directory.

## Running

```bash
cd mahler-bench

# every benchmark (a few minutes)
cargo bench

# one group, or one scenario
cargo bench --bench planning -- plan/blocks
cargo bench --bench planning -- 'satellites=8'

# rough numbers while iterating on a change
cargo bench -- --quick --noplot
```

Benchmarks are written with [criterion](https://bheisler.github.io/criterion.rs/book/),
so a change can be measured against a saved baseline:

```bash
git checkout main
cargo bench -- --save-baseline main

git checkout my-optimisation
cargo bench -- --baseline main
```

Criterion writes its reports to `target/criterion`.

## What is measured

| Bench       | Group           | What it covers                                                                                    |
| ----------- | --------------- | ------------------------------------------------------------------------------------------------- |
| `planning`  | `plan/<domain>` | `Worker::find_plan`: the whole HTN search, per domain and size                                    |
| `planning`  | `distance`      | `Worker::distance`: only the JSON Patch diff between state and target                             |
| `planning`  | `build`         | `Worker::new().job(..).initial_state(..)`: job registration and state loading                     |
| `execution` | `seek_target`   | planning plus running the workflow, with pure jobs, so what is timed is the worker's own overhead |

Throughput is reported in plan tasks, so results read both as time per plan and as time
per task. Plans are deterministic, and `tests/scenarios.rs` asserts their size, so a
change in throughput is a change in speed and not in the amount of work planned.

### Memory

Criterion does not measure memory, so the same scenarios are driven by a separate binary
with an allocation-counting global allocator:

```bash
cargo run --release --bin mahler-memreport
cargo run --release --bin mahler-memreport -- --run
cargo run --release --bin mahler-memreport -- --filter satellites --json
```

It reports, per scenario: the heap the worker retains once built, and the allocation
count, total allocated bytes and peak live bytes of planning (and of `seek_target` with
`--run`). `--json` output is meant to be diffed between two revisions.

The allocator does not specialize `realloc`, so a growing `Vec` counts as a new
allocation. The numbers measure allocator traffic, which is what they are useful for
comparing; they are not what the system allocator would report for the same workload.
The times it prints are indicative only — use `cargo bench` for timings.

## Domains

Each domain is a full mahler worker: state model, jobs and scenario builders. They were
chosen to stress different parts of the planner.

### `blocks` — block stacking, generalized to several hands

The planning method is the algorithm from N. Gupta and D. S. Nau, _On the complexity of
blocks-world planning_, Artificial Intelligence 56(2-3):223-254, 1992, as implemented in
[GTPyhop](https://github.com/dananau/GTPyhop/blob/main/Examples/blocks_hgn/methods.py):
never move a block that is already where it belongs, move a clear block to its final
location when one can, otherwise move clear blocks out of the way onto the table.

The generalization to `h` hands batches up to `h` non-interfering moves per method
expansion — all of them picked up, then all of them put down — so with two hands the
planner reaches the same plan in half as many search iterations. With one hand the
method reduces to the original algorithm.

| Scenario             | Shape                                                                                                        |
| -------------------- | ------------------------------------------------------------------------------------------------------------ |
| `tower_reversal`     | one tower turned upside down: one move per block, no wasted moves                                            |
| `tower_rebuild`      | one tower rebuilt with its bottom block on top: the tower has to come apart first, about two moves per block |
| `independent_towers` | several towers reversed at once: the scenario where a second hand pays off                                   |

Every job here uses the `System<T>` extractor, because deciding whether a block is clear
needs the whole state. That makes the tasks unscoped, so blocks-world plans are always
sequential, however many hands the worker has. The actions are registered with `none()`,
so the planner only reaches them through the method and never tries them directly
against the per-block diff.

### `logistics` — the classic HTN transport domain

Packages are delivered between locations grouped in cities, with one truck per city and
airplanes between airports. This is the AIPS-98 competition domain and one of the
standard [SHOP2](https://www.cs.umd.edu/~nau/papers/nau2003shop2.pdf) examples. The
`deliver` method plans one leg at a time and the planner calls it again for the rest,
which is how a recursive HTN method is expressed against a diffing planner.

Only package locations are part of the target: trucks and airplanes are means, so no
diff is ever produced for them and their jobs are reachable only through the method.
With more packages than cities, a truck or an airplane has to go back to a place it
already left to pick up the next package.

### `satellite` — observation scheduling

Satellites acquire images, which means powering up an instrument, calibrating it against
its calibration direction, turning towards the image and shooting. This is the AIPS-2002
`Satellite` domain, also one of the domains SHOP2 was evaluated on.

Every job in this domain is _scoped_: handlers only use `View`, `Target`, `RawTarget` and
`Args` relative to their own path under `/satellites/{satellite}`. Satellites therefore never
conflict and the planner merges their work into concurrent branches, which makes this
the domain that exercises candidate merging and concurrent workflow execution.

Within a satellite, each action is registered at a path that covers all the state its
precondition reads: `calibrate` and `take image` act on the whole satellite, `switch on`
on all of its instruments. The planner only runs two steps of one satellite in parallel
when neither depends on the other, so the turn towards the calibration direction runs
alongside the instrument switches, and everything else is sequential.

## Indicative results

Release build, Apple M-series laptop, mahler 0.27.2. Planning only: times are the
criterion median, memory columns come from `mahler-memreport`.

| Scenario                              | Tasks |    Time | Allocations | Allocated | Peak live |
| ------------------------------------- | ----: | ------: | ----------: | --------: | --------: |
| `tower_reversal/blocks=8/hands=1`     |    16 | 0.56 ms |      14.0 k |   2.0 MiB |   170 KiB |
| `tower_reversal/blocks=16/hands=1`    |    32 |  2.6 ms |      63.3 k |  10.2 MiB |   544 KiB |
| `tower_reversal/blocks=32/hands=1`    |    64 |   13 ms |       341 k |  56.1 MiB |   1.8 MiB |
| `independent_towers/towers=8/hands=1` |    64 |   12 ms |       306 k |  46.1 MiB |   1.5 MiB |
| `independent_towers/towers=8/hands=2` |    64 |  7.6 ms |       190 k |  28.4 MiB |   1.5 MiB |
| `deliveries/cities=16/packages=16`    |   145 |   57 ms |      1.42 M |   168 MiB |   2.1 MiB |
| `deliveries/cities=4/packages=32`     |   289 |  232 ms |      5.67 M |   860 MiB |   7.2 MiB |
| `observations/satellites=8/images=4`  |   120 |  6.0 ms |       125 k |  23.2 MiB |   4.0 MiB |
| `observations/satellites=4/images=16` |   156 |   27 ms |       562 k |   113 MiB |   8.4 MiB |

Two things these say about where the cost is. Planning time grows faster than the plan:
doubling the blocks multiplies time by about five, and the allocated bytes grow with it,
because every search iteration clones the system state, re-serializes it into the target
type to diff it and hashes it to check whether it was visited. And the concurrent domain
is much cheaper per task than the sequential ones, because it reaches the goal in fewer,
wider iterations.

Running a workflow costs about 30–40 µs per task with jobs that do nothing (the
`seek_target` time minus the `plan` time of the same scenario), which is the worker's own
overhead: dry running the task, applying its patch and publishing the new state.

### Where the allocated bytes go

`allocated` is cumulative traffic over the whole search, not memory held at once: the
same scenario allocates 56 MiB in total while never holding more than 1.8 MiB live,
because almost all of it is freed within the search iteration that allocated it. Counting
handler invocations during the search attributes it as follows, for
`tower_reversal` with one hand:

| Blocks | Plan tasks | Handler invocations | Per invocation | Total allocated | Peak live | Held by the plan |
| -----: | ---------: | ------------------: | -------------: | --------------: | --------: | ---------------: |
|      8 |         16 |                  40 |       49.9 KiB |         1.9 MiB |   170 KiB |          107 KiB |
|     16 |         32 |                  80 |        130 KiB |        10.2 MiB |   544 KiB |          418 KiB |
|     32 |         64 |                 160 |        359 KiB |        56.1 MiB |   1.8 MiB |          1.5 MiB |

The number of handler invocations is linear in the plan, five per block moved, so the
superlinear growth is all in what the search does around each invocation, which grows
with the size of the state.
