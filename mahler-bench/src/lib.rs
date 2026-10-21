//! Speed and memory benchmarks for [mahler](https://crates.io/crates/mahler).
//!
//! The crate provides the benchmark domains (see [`domain`]), an allocation-counting
//! global allocator used by the memory report (see [`alloc`]) and helpers to describe
//! the plans the benchmarks operate on (see [`plan`]).
//!
//! See `README.md` for how to run the benchmarks.

pub mod alloc;
pub mod domain;
pub mod plan;
pub mod scenario;
