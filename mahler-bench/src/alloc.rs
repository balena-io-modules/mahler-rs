//! A global allocator that counts heap traffic
//!
//! Criterion measures time, not memory, so the memory report drives the same scenarios
//! through [`measure`] with this allocator installed:
//!
//! ```no_run
//! use mahler_bench::alloc::CountingAllocator;
//!
//! #[global_allocator]
//! static ALLOC: CountingAllocator = CountingAllocator::new();
//! ```
//!
//! The counters are process-wide, so a measurement is only meaningful while a single
//! thread is allocating. Every measurement in `mahler-memreport` runs on the main
//! thread with a current-thread tokio runtime for that reason.
//!
//! Note that `realloc` is not specialized: it falls back to the `GlobalAlloc` default of
//! allocate, copy and free, so growing a `Vec` is counted as a new allocation and can
//! never grow in place. This makes the numbers a measure of allocator traffic rather
//! than of what the system allocator would do for the same workload.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// A pass-through allocator that keeps track of the heap traffic it serves
pub struct CountingAllocator;

impl CountingAllocator {
    pub const fn new() -> Self {
        Self
    }
}

impl Default for CountingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            let size = layout.size();
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(size, Ordering::Relaxed);
            let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

/// Heap traffic observed while running a piece of code
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocStats {
    /// Number of allocation calls served
    pub allocations: usize,
    /// Total number of bytes allocated, including memory that was freed again
    pub allocated_bytes: usize,
    /// Largest amount of live bytes held at any point
    pub peak_bytes: usize,
    /// Live bytes still held once the measurement finished
    pub retained_bytes: isize,
}

/// Run `f` and report the heap traffic it caused
///
/// The counters are global: only one measurement may be in flight at a time and no
/// other thread may be allocating during it.
pub fn measure<T>(f: impl FnOnce() -> T) -> (T, AllocStats) {
    let live_before = LIVE.load(Ordering::Relaxed);
    let allocations_before = ALLOCATIONS.load(Ordering::Relaxed);
    let allocated_before = ALLOCATED.load(Ordering::Relaxed);
    PEAK.store(live_before, Ordering::Relaxed);

    let out = f();

    let live_after = LIVE.load(Ordering::Relaxed);
    let stats = AllocStats {
        allocations: ALLOCATIONS.load(Ordering::Relaxed) - allocations_before,
        allocated_bytes: ALLOCATED.load(Ordering::Relaxed) - allocated_before,
        peak_bytes: PEAK.load(Ordering::Relaxed).saturating_sub(live_before),
        retained_bytes: live_after as isize - live_before as isize,
    };

    (out, stats)
}
