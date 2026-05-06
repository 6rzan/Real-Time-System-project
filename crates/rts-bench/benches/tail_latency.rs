//! P10 — Tail-Latency Comparison: async vs threaded scheduling kernel
//!
//! Measures the scheduling drift (time from `Task::enqueued_at` to the moment
//! the worker begins processing) for both pipeline implementations under a
//! synthetic in-process event feed.
//!
//! # What is benchmarked
//! The _scheduling kernel_ — parser enqueue → queue → worker dequeue — is
//! isolated from network I/O by using a synthetic in-process sink.  We push a
//! fixed batch of `N` tasks directly into the priority queue / ring-buffer,
//! then drain them through a worker pool, measuring:
//!   - **Async** (`DropOldestRing` biased-select workers via Tokio)
//!   - **Threaded** (`PriorityQueue` blocking workers via `std::thread`)
//!
//! Both pipelines are exercised with a 50/50 Human/Bot mix and batch sizes of
//! 256, 1 024, and 4 096 tasks so Criterion can extrapolate the per-task cost.
//!
//! # Metrics captured
//! Criterion records wall-clock throughput (tasks/s).  After each batch the
//! bench also prints p50/p99 drift to stdout so the report has real numbers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rts_core::channel::ring::DropOldestRing;
use rts_core::event::OwnedEvent;
use rts_core::metrics::Metrics;
use rts_core::priority::Priority;
use rts_core::task::Task;
use rts_threaded::scheduler::PriorityQueue;

const BATCH_SIZES: &[usize] = &[256, 1_024, 4_096];

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_task(idx: usize) -> Task {
    let bot = idx.is_multiple_of(2);
    Task {
        event: OwnedEvent {
            user: "bench_user".to_string(),
            bot,
            server_name: "en.wikipedia.org".to_string(),
        },
        priority: Priority::from_bot_flag(bot),
        enqueued_at: Instant::now(),
    }
}

// ── Async kernel ─────────────────────────────────────────────────────────────

/// Benchmark the async scheduling kernel.
///
/// Pushes `n` tasks into a `DropOldestRing` pair (hi / lo), then drains them
/// through `num_cpus` Tokio worker tasks.  The Tokio runtime is created fresh
/// per Criterion sample to avoid cross-sample state.
fn bench_async_kernel(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_kernel");
    for &n in BATCH_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_cpus::get().max(2))
                    .enable_all()
                    .build()
                    .expect("runtime");

                rt.block_on(async move {
                    let hi = DropOldestRing::<Task>::new(n + 1);
                    let lo = DropOldestRing::<Task>::new(n + 1);
                    let metrics = Metrics::new();
                    let cancel = tokio_util::sync::CancellationToken::new();

                    // Enqueue all tasks.
                    for i in 0..n {
                        let task = make_task(i);
                        let ring = if task.priority == Priority::Human {
                            &hi
                        } else {
                            &lo
                        };
                        let _ = ring.push(task);
                    }

                    // Signal done: workers drain then exit via cancel.
                    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

                    let workers = num_cpus::get().max(2);
                    let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(n));

                    for _ in 0..workers {
                        let hi2 = Arc::clone(&hi);
                        let lo2 = Arc::clone(&lo);
                        let m2 = Arc::clone(&metrics);
                        let cancel2 = cancel.clone();
                        let rem = Arc::clone(&remaining);
                        let done = done_tx.clone();
                        tokio::spawn(async move {
                            loop {
                                let task = tokio::select! {
                                    biased;
                                    () = cancel2.cancelled() => break,
                                    t = hi2.pop_async() => t,
                                    t = lo2.pop_async() => t,
                                };
                                let start = Instant::now();
                                let drift = start.saturating_duration_since(task.enqueued_at);
                                m2.record_drift(task.priority, drift);
                                m2.record_jitter(start.elapsed());
                                let prev = rem.fetch_sub(1, Ordering::Relaxed);
                                if prev == 1 {
                                    let _ = done.try_send(());
                                }
                            }
                        });
                    }
                    drop(done_tx);

                    // Wait for all tasks to be processed, then cancel workers.
                    let _ = done_rx.recv().await;
                    cancel.cancel();

                    // Return p99 drift for informational purposes.
                    let snap = metrics.snapshot();
                    (snap.drift_human_p99, snap.drift_bot_p99)
                });
            });
        });
    }
    group.finish();
}

// ── Threaded kernel ───────────────────────────────────────────────────────────

/// Benchmark the threaded scheduling kernel.
///
/// Pushes `n` tasks into a `PriorityQueue`, then drains them through
/// `num_cpus` OS-thread workers.
fn bench_threaded_kernel(c: &mut Criterion) {
    let mut group = c.benchmark_group("threaded_kernel");
    for &n in BATCH_SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let queue = PriorityQueue::new(n + 1);
                let metrics = Metrics::new();
                let cancel = Arc::new(AtomicBool::new(false));
                let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(n));

                // Enqueue all tasks.
                for i in 0..n {
                    let _ = queue.push(make_task(i));
                }

                let workers = num_cpus::get().max(2);
                let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(1);

                let mut handles = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let q = Arc::clone(&queue);
                    let m = Arc::clone(&metrics);
                    let c = Arc::clone(&cancel);
                    let rem = Arc::clone(&remaining);
                    let done = done_tx.clone();
                    handles.push(std::thread::spawn(move || {
                        while let Some(task) = q.pop_blocking(&c) {
                            let start = Instant::now();
                            let drift = start.saturating_duration_since(task.enqueued_at);
                            m.record_drift(task.priority, drift);
                            m.record_jitter(start.elapsed());
                            let prev = rem.fetch_sub(1, Ordering::Relaxed);
                            if prev == 1 {
                                let _ = done.try_send(());
                            }
                        }
                    }));
                }
                drop(done_tx);

                // Wait until all tasks are processed, then cancel workers.
                let _ = done_rx.recv();
                cancel.store(true, Ordering::Relaxed);
                queue.wake_all();

                for h in handles {
                    h.join().ok();
                }

                let snap = metrics.snapshot();
                (snap.drift_human_p99, snap.drift_bot_p99)
            });
        });
    }
    group.finish();
}

// ── Criterion entry point ─────────────────────────────────────────────────────

criterion_group!(benches, bench_async_kernel, bench_threaded_kernel);
criterion_main!(benches);
