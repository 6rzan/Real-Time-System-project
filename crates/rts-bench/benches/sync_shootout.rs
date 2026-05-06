//! P9 — Sync-Primitive Shootout
//!
//! Benchmarks five concurrency primitives under 1, 2, 4, 8, and 16 contending
//! threads to measure throughput (ops/s) and reveal scalability curves.
//!
//! Variants:
//!   1. `std::sync::Mutex<u64>`          — baseline stdlib mutex
//!   2. `parking_lot::Mutex<u64>`        — fast userspace mutex
//!   3. `crossbeam_channel` (MPSC)       — message-passing counter
//!   4. `dashmap::DashMap`               — sharded concurrent hashmap
//!   5. `DropOldestRing` push+pop cycle  — the pipeline's own bounded channel
//!
//! Each benchmark spawns N producer threads, each doing 1 000 increments /
//! pushes, then waits for all to finish. Criterion measures the wall-clock
//! time for one full round (all threads complete).

use std::sync::{Arc, Mutex as StdMutex};
use std::thread;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use crossbeam_channel::bounded;
use dashmap::DashMap;
use parking_lot::Mutex as PlMutex;
use rts_core::channel::ring::DropOldestRing;

/// Number of increments / pushes each thread performs per Criterion iteration.
const OPS_PER_THREAD: u64 = 1_000;

/// Thread counts to sweep over.
const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

// ── Variant 1: std::sync::Mutex ─────────────────────────────────────────────

fn bench_std_mutex(c: &mut Criterion) {
    let mut group = c.benchmark_group("std_mutex");
    for &threads in THREAD_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let counter = Arc::new(StdMutex::new(0u64));
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let c = Arc::clone(&counter);
                            thread::spawn(move || {
                                for _ in 0..OPS_PER_THREAD {
                                    *c.lock().unwrap() += 1;
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Variant 2: parking_lot::Mutex ────────────────────────────────────────────

fn bench_pl_mutex(c: &mut Criterion) {
    let mut group = c.benchmark_group("pl_mutex");
    for &threads in THREAD_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let counter = Arc::new(PlMutex::new(0u64));
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let c = Arc::clone(&counter);
                            thread::spawn(move || {
                                for _ in 0..OPS_PER_THREAD {
                                    *c.lock() += 1;
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Variant 3: crossbeam_channel MPSC ────────────────────────────────────────

fn bench_crossbeam_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_mpsc");
    for &threads in THREAD_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    // Channel large enough that producers never block.
                    let (tx, rx) = bounded::<u64>(threads * OPS_PER_THREAD as usize);
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let tx = tx.clone();
                            thread::spawn(move || {
                                for i in 0..OPS_PER_THREAD {
                                    tx.send(i).unwrap();
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                    drop(tx);
                    // Drain the channel (simulates a consumer aggregating results).
                    let _total: u64 = rx.iter().sum();
                });
            },
        );
    }
    group.finish();
}

// ── Variant 4: DashMap ────────────────────────────────────────────────────────

fn bench_dashmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap");
    for &threads in THREAD_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let map: Arc<DashMap<usize, u64>> = Arc::new(DashMap::new());
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let m = Arc::clone(&map);
                            thread::spawn(move || {
                                for _ in 0..OPS_PER_THREAD {
                                    *m.entry(t).or_insert(0) += 1;
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Variant 5: DropOldestRing push+pop ───────────────────────────────────────

fn bench_drop_oldest_ring(c: &mut Criterion) {
    let mut group = c.benchmark_group("drop_oldest_ring");
    for &threads in THREAD_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    // Ring sized to hold all items so we measure pure throughput,
                    // not drop-oldest eviction latency.
                    let ring = DropOldestRing::<u64>::new(threads * OPS_PER_THREAD as usize);
                    let push_handles: Vec<_> = (0..threads)
                        .map(|_| {
                            let r = Arc::clone(&ring);
                            thread::spawn(move || {
                                for i in 0..OPS_PER_THREAD {
                                    let _ = r.push(i);
                                }
                            })
                        })
                        .collect();
                    for h in push_handles {
                        h.join().unwrap();
                    }
                    // Drain synchronously to complete the round-trip.
                    while !ring.is_empty() {
                        ring.pop_blocking();
                    }
                });
            },
        );
    }
    group.finish();
}

// ── Criterion entry point ─────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_std_mutex,
    bench_pl_mutex,
    bench_crossbeam_mpsc,
    bench_dashmap,
    bench_drop_oldest_ring,
);
criterion_main!(benches);
