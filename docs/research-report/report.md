# A Dual-Runtime Real-Time Pipeline for the Wikimedia SSE Firehose: Tokio Async vs. OS Threads in Rust

**Module:** RTS2601 Real-Time Systems  
**Student:** Taha Thabit  
**Date:** 6 May 2026  
**Word count:** ~3 600

---

## Abstract

This paper presents the design, implementation, and empirical evaluation of a soft real-time event-processing pipeline built in Rust against the Wikimedia Server-Sent Events (SSE) firehose. Two independent runtime implementations are compared: a Tokio-based cooperative async pipeline and a `std::thread` OS-thread pipeline. Both implementations share a common core library providing zero-copy JSON parsing via `Cow<'a, str>`, a drop-oldest ring-buffer backpressure primitive, per-priority drift measurement via HdrHistogram, and a hysteresis-controlled Degraded Mode fail-safe. A five-variant synchronisation shootout benchmarks `std::sync::Mutex`, `parking_lot::Mutex`, `crossbeam_channel`, `DashMap`, and `DropOldestRing` across 1–16 contending threads. Results show that the Tokio async pipeline halves p99 scheduling drift compared to the threaded implementation under 10× burst load, while the `parking_lot::Mutex` variant achieves approximately 2× the throughput of `std::sync::Mutex` at 16 threads. The zero-copy parser maintains a borrowed-field rate above 99.5% across the 3 078-event fixture, reducing per-event heap allocation by more than 60×.

---

## 1. Introduction

Real-time systems demand bounded, predictable latency, not merely high throughput (Liu & Layland, 1973). The Wikimedia SSE stream delivers a sustained firehose of encyclopaedia-edit events at roughly 50 events per second under normal conditions, spiking to over 500 events per second during mass-edit bot runs. Each event carries a `bot` flag that classifies it as either a human edit (high-priority, latency-sensitive) or a bot edit (low-priority, bulk throughput). A conforming pipeline must service human-priority events within a 2 ms micro-deadline and must not allow bot-volume spikes to crowd out human events — precisely the priority inversion hazard that brought down NASA's Mars Pathfinder spacecraft in 1997 (Reeves, 1997).

Rust is particularly well-suited to this problem: its ownership model eliminates data races at compile time, its zero-cost abstractions enable zero-copy parsing with compile-time lifetime enforcement, and its dual runtime ecosystem offers both a cooperative async runtime (Tokio) and OS threads (`std::thread`) within a single language, making head-to-head comparison straightforward.

This paper addresses two research questions:

**RQ1.** Under bursty real-time load, does Tokio async deliver lower tail latency than OS threads in Rust, and at what cost?

**RQ2.** Among Mutex, RwLock, Atomic, and sharded variants, which synchronisation primitive best supports a contended shared-counter workload?

---

## 2. Related Work

### 2.1 Real-Time Scheduling Theory

Liu and Layland (1973) derived the Rate Monotonic schedulability bound for *n* periodic tasks as U ≤ n(2^(1/n) − 1). As n → ∞ this bound converges to ln 2 ≈ 0.693, meaning a set of periodic tasks is guaranteed to be schedulable if total CPU utilisation is at most 69.3%. This paper measures end-to-end utilisation across the pipeline and evaluates whether the observed workload falls within the bound (§6.1).

Earliest Deadline First (EDF) is optimal in the sense that it can schedule any feasible task set, but it requires dynamic priority assignment proportional to absolute deadlines. The Tokio biased `select!` used in this pipeline instead implements **Static Priority Pre-emptive (SP-PE)** scheduling: the human-priority lane is always polled before the bot lane regardless of individual deadlines. This is simpler and sufficient when two priority classes are enough — the cost is slight under-utilisation compared to EDF under corner cases.

### 2.2 Priority Inversion

Priority inversion occurs when a high-priority task is blocked waiting for a resource held by a low-priority task. On Mars Pathfinder, a high-priority bus-management task and a low-priority meteorology task both competed for a shared `VxWorks` mutex; the low-priority task was preempted by a medium-priority task that did not need the mutex, starving the high-priority task until the watchdog reset the spacecraft (Reeves, 1997). Priority Ceiling Protocol (PCP) and Priority Inheritance Protocol (PIP) are the standard mitigations (Sha, Rajkumar, & Lehoczky, 1990).

This pipeline avoids the scenario architecturally: no shared mutex is ever held during the hot path. Leaderboard updates are sent via a non-blocking `mpsc` channel to a single owner task; workers never contend with each other on data structures.

### 2.3 Control Theory and Fault Tolerance

The Degraded Mode controller in this pipeline is a bang-bang (on/off) hysteresis controller — the same class of non-linear feedback used in thermostats since Schmitt (1938). The hysteresis band (enter at p99 jitter > 1.5 ms, recover at p99 jitter < 0.8 ms, 10-second dwell) prevents rapid oscillation between states (chattering). Avizienis, Laprie, Randell, and Landwehr (2004) define fault tolerance as the ability of a system to deliver correct service in the presence of faults; the watchdog timer (§4.5) is an example of their *error detection* mechanism class.

### 2.4 Backpressure and Drop-Oldest

The LMAX Disruptor (Thompson, Farley, Barker, & Gee, 2011) pioneered the idea of a lock-free ring buffer with a producer advancing a sequence number and consumers trailing behind. The `DropOldestRing<T>` in this pipeline uses a simpler mutex-protected `VecDeque` but shares the key design insight: **drop the oldest data rather than block the producer**. For live telemetry pipelines, a slightly stale leaderboard is preferable to a stalled ingest stream.

### 2.5 Cooperative vs. Preemptive Runtimes

Tokio's work-stealing scheduler multiplexes many async tasks onto a small pool of OS threads (Tokio, 2024). Each await point is a potential yield; a task that never yields monopolises its worker thread. The biased `select!` macro, introduced in Tokio 1.x, is a deliberate scheduling hint — it polls futures in declaration order rather than randomly, giving the high-priority lane a deterministic first-poll advantage. The threaded pipeline uses OS-level priority queues (`BinaryHeap`) and blocking pop operations; priority here is enforced at the data-structure level, not by the scheduler.

---

## 3. System Design

### 3.1 Workspace Architecture

The implementation is organised as a Cargo workspace with six crates:

```
rts-core      ← shared types, Cow parser, DropOldestRing,
                 HdrHistogram metrics, FailSafeController, watchdog
rts-async     ← Tokio ingest, biased-select scheduler, leaderboard actor
rts-threaded  ← ureq ingest, BinaryHeap priority queue, worker pool
rts-bench     ← Criterion harnesses (sync shootout, tail latency)
rts-cli       ← clap-derive binary; subcommands: run-async, run-threaded,
                 replay, analyse, bench, stress
rts-replay    ← axum SSE server for deterministic fixture replay
```

**Diagram 1 — Data-flow and communication between crates:**

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                         rts-cli (binary)                         │
 └───────────────┬──────────────────────────┬────────────────────────┘
                 │                          │
     ┌───────────▼──────────┐  ┌────────────▼─────────┐
     │      rts-async       │  │     rts-threaded      │
     │ ┌──────────────────┐ │  │ ┌──────────────────┐  │
     │ │  ingest (reqwest)│ │  │ │  ingest (ureq)   │  │
     │ └────────┬─────────┘ │  │ └────────┬─────────┘  │
     │          │ raw str   │  │          │ raw str     │
     │ ┌────────▼─────────┐ │  │ ┌────────▼─────────┐  │
     │ │  parser           │ │  │ │  parser           │  │
     │ │  parse_one(&str)  │ │  │ │  parse_one(&str)  │  │
     │ │  Event<'a>→Task   │ │  │ │  Event<'a>→Task   │  │
     │ └────┬──────────┬──┘ │  │ └──────┬─────────┘  │
     │      │ hi_lane  │lo  │  │        │ PriorityQueue│
     │ ┌────▼──┐  ┌────▼──┐ │  │ ┌──────▼──────────┐  │
     │ │DropOld│  │DropOld│ │  │ │BinaryHeap+Condvar│  │
     │ │Ring(H)│  │Ring(L)│ │  │ └──────┬──────────┘  │
     │ └──┬────┘  └──┬────┘ │  │        │              │
     │    │biased    │      │  │ ┌──────▼──────────┐  │
     │ ┌──▼──────────▼──┐   │  │ │  worker pool     │  │
     │ │  worker pool    │   │  │ │  (N std::threads)│  │
     │ │  (Tokio tasks)  │   │  │ └──────┬──────────┘  │
     │ └────────┬────────┘   │  │        │              │
     │          │ LbCmd      │  │        │ LbCmd        │
     │ ┌────────▼──────────┐ │  │ ┌──────▼──────────┐  │
     │ │  Leaderboard actor│ │  │ │  Leaderboard thd │  │
     │ └────────────────────┘ │  └─────────────────────┘
     └────────────────────────┘
                  ↑ both share
          ┌───────┴────────────┐
          │      rts-core      │
          │  Event<'a>         │
          │  OwnedEvent        │
          │  DropOldestRing<T> │
          │  Metrics           │
          │  FailSafeController│
          └────────────────────┘
```

The `rts-replay` crate provides an Axum HTTP server that replays recorded NDJSON fixtures as a live SSE stream, enabling fully deterministic benchmarks without network dependency.

### 3.2 Zero-Copy JSON Parsing

The `Event<'a>` struct uses `#[serde(borrow)]` to allow serde to produce a `Cow<'a, str>` that borrows from the input buffer when the field value contains no JSON escape sequences:

```rust
#[derive(serde::Deserialize, Debug)]
pub struct Event<'a> {
    #[serde(borrow)] pub user: Cow<'a, str>,
    pub bot: bool,
    #[serde(borrow)] pub server_name: Cow<'a, str>,
}
```

The key distinction from `&'a str` is that `Cow<'a, str>` correctly handles the minority case where a user name contains a Unicode escape (`\uXXXX`). In that case, serde must decode the escape into a fresh `String` (`Cow::Owned`); `&'a str` would be unsound because the decoded data has no backing buffer to borrow from. `Cow` handles both paths with a single field type.

Two `AtomicU64` counters (`BORROWED_COUNT`, `OWNED_COUNT`) are incremented at parse time. Across the 3 078-event fixture, more than 99.5% of parses take the borrowed path, validating the zero-copy design.

The `into_owned()` call — converting `Event<'a>` to `OwnedEvent` by heap-allocating each field — is performed exactly once, at the parser-to-queue boundary, after priority classification. This is the only mandatory allocation per event on the hot path.

### 3.3 Priority Scheduling

**Diagram 2 — Timing diagram showing Human vs. Bot scheduling drift:**

```
Time ──────────────────────────────────────────────────────────────►
         t₀              t₁              t₂
          │               │               │
Human ────┼───────────────┼───────────────┼── enqueued
          │←── drift H ──►│               │
Bot ──────┼────────────────────────────────┼── enqueued
                          │← drift B long►│

Async worker (biased select):
          [poll hi] ──────► Hi available? YES → serve Human (drift_H small)
                  [poll lo] ──────────────────────► Serve Bot (drift_B larger)

Threaded worker (BinaryHeap):
          [pop] ──────────────────────────► Priority::Human < Priority::Bot
                                           → Human dequeued first ✓
```

**Drift definition:** `drift = actual_start − enqueued_at`. This assumes an empty-system baseline where `expected_start = enqueued_at`. The assumption is explicitly stated and conservative — in a loaded system the true schedulable delay is longer, so this metric is a lower bound on actual drift.

In the async runtime, `tokio::select! { biased; t = hi.pop_async() => t, t = lo.pop_async() => t }` polls the human-priority channel before the bot channel in every loop iteration. This is SP-PE scheduling: the human lane is always given first-poll preference, regardless of waiting time in the bot lane. In the threaded runtime, the `BinaryHeap<Reverse<(Priority, Instant, Task)>>` pop is O(log n) and deterministically returns the highest-priority (Human < Bot in the enum ordering) task first, with FIFO tie-breaking within a priority via the `enqueued_at` timestamp.

The 2 ms micro-deadline per event is checked after the hot-path completes:

```rust
let deadline_miss = total.as_nanos() > 2_000_000; // 2 ms
if deadline_miss { metrics.record_deadline_miss(priority); }
```

### 3.4 Drop-Oldest Ring Buffer

`DropOldestRing<T>` is the sole backpressure primitive shared by both runtimes:

```
Push contract:
  if len == capacity → pop_front() (increment overflow counter) → push_back()
  else → push_back()
  notify_one() on both Tokio::Notify and parking_lot::Condvar

Pop async: loop { notified = notify.notified(); if queue.pop_front() = Some(x) return x; await notified; }
Pop blocking: loop { condvar.wait(&mut guard); if guard.pop_front() = Some(x) return x; }
```

The single struct exposes both `pop_async` and `pop_blocking` by embedding both a `tokio::sync::Notify` and a `parking_lot::Condvar`. The push path notifies both regardless of which consumer is active; the inactive notifier incurs a single no-op atomic write, which is acceptable.

Property tests (`proptest`) verify that for any sequence of pushes and pops with any capacity, the overflow counter equals `max(0, pushes − capacity)` and the surviving elements form a strictly ascending FIFO sequence.

### 3.5 Watchdog and Fail-Safe State Machine

**Diagram 3 — Control-loop state machine for Degraded Mode with hysteresis:**

```
                        ┌──────────────────────────────────────┐
                        │         Watchdog (separate task)      │
                        │  last_event_ns ──► elapsed > 10 s?    │
                        │                   YES → ResetSignal   │
                        └──────────────────┬───────────────────┘
                                           │
                                    Ingest reconnects
                                    (exponential backoff)

 ┌───────────────────────────────────────────────────────────────────┐
 │               FailSafeController (tick every 1 s)                  │
 │                                                                    │
 │  [Normal Mode]                      [Degraded Mode]                │
 │   All events pass through            Bot events dropped at parser  │
 │        │                                      │                   │
 │        │ p99_jitter > 1.5 ms                  │ p99_jitter < 0.8ms│
 │        │ for DWELL_TICKS = 10 s               │ for 10 s          │
 │        ▼                                      ▼                   │
 │   [Enter Degraded] ─────────────────► [Exit Degraded]             │
 │                                                                    │
 │  Rolling window: 5 × 1 s HdrHistogram buckets                     │
 │  p99 = value_at_quantile(0.99) across merged buckets               │
 └────────────────────────────────────────────────────────────────────┘
```

The `FailSafeController` maintains a ring of five 1-second `Histogram<u64>` buckets. On every worker hot-path completion it receives a jitter sample via `record_jitter(Duration)`. A background task calls `tick()` once per second; tick rotates the ring (clearing the oldest bucket) and evaluates the p99 across the merged 5-second window.

The hysteresis parameters are chosen to avoid chattering:
- **Enter threshold:** 1.5 ms (p99 jitter) — aggressive enough to protect Human-priority latency.
- **Recover threshold:** 0.8 ms — a 700 µs deadband prevents oscillation when jitter hovers near the entry threshold.
- **Dwell ticks:** 10 — the 10-second hold ensures recovery is stable, not a transient improvement.

The watchdog is independent of the fail-safe: it monitors time since last received event and fires a reconnection signal after 10 seconds of silence. This handles upstream disconnections and network partitions, not load-driven degradation.

---

## 4. Implementation Notes

**Non-blocking tracing.** Every `tracing::info!` event on the hot path is routed through `tracing_appender::non_blocking(RollingFileAppender)`. Synchronous writes to disk would dominate jitter measurements; the non-blocking appender offloads I/O to a dedicated writer thread, adding only an `Arc<Mutex<VecDeque<LogLine>>>` push (~200 ns) to the hot-path cost.

**`parking_lot` vs. `std::sync`.** Production code uses `parking_lot::Mutex` throughout. The `std::sync::Mutex` is benched separately in the shootout (§6.2). `parking_lot` avoids the `PoisonError` overhead on unlock and uses a more efficient inline fast-path that spins briefly before sleeping, reducing context-switch overhead under low-to-medium contention.

**`ureq` for the threaded ingest.** Using `reqwest::blocking` in the threaded pipeline would link in the Tokio runtime, making the "OS threads" comparison dishonest. `ureq` 2.10 is a genuinely blocking HTTP client with a 1-second socket read timeout configured for watchdog interaction.

**`CachePadded<AtomicU64>`.** In the sync shootout `AtomicFixedSlots` variant, each counter is wrapped in `crossbeam_utils::CachePadded` to prevent false sharing. Without padding, adjacent atomic counters land on the same 64-byte cache line; a write by one thread invalidates the line for all others, serialising what should be independent operations.

**dhat feature gating.** The `dhat-heap` feature installs `dhat::Alloc` as the global allocator and calls `dhat::Profiler::new_heap()` at startup, producing a `dhat-heap.json` profile on program exit. This feature is **never** active during latency benchmarks — the profiling allocator adds per-allocation bookkeeping that would inflate jitter measurements.

---

## 5. Results and Discussion

### 5.1 Scheduling Drift: Async vs. Threaded

*Measurement method:* The tail-latency bench (`benches/tail_latency.rs`) pushes synthetic tasks directly into each runtime's priority channel, bypassing network I/O, and measures `actual_start − enqueued_at` for each task. Three batch sizes (256, 1 024, 4 096) are used; results below are for the 4 096-task batch which represents the loaded case.

| Runtime | Priority | p50 drift (µs) | p90 drift (µs) | p99 drift (µs) | p99.9 drift (µs) | Sample n |
|---|---|---|---|---|---|---|
| Async (Tokio) | Human | 12 | 28 | 61 | 140 | 2 048 |
| Async (Tokio) | Bot | 38 | 95 | 380 | 720 | 2 048 |
| Threaded | Human | 18 | 52 | 118 | 290 | 2 048 |
| Threaded | Bot | 44 | 110 | 420 | 890 | 2 048 |

The async runtime achieves approximately 48% lower p99 for Human-priority events (61 µs vs. 118 µs) and 10% lower p99 for Bot events under the 4 096-task load. The improvement is larger at the p99.9 tail — 140 µs vs. 290 µs — indicating that the Tokio cooperative scheduler is better at absorbing tail-latency spikes than OS thread scheduling under burst load.

The advantage is consistent with the cooperative model: when a Tokio worker task yields (at an `await` point), the scheduler can immediately dispatch a pending high-priority task on the same OS thread. OS thread scheduling involves a kernel context switch (~1–5 µs overhead per switch on Linux) that adds a floor to minimum achievable drift.

**Liu & Layland utilisation check.** For two priority classes (Human, Bot) the schedulability bound is U ≤ 2(2^(1/2) − 1) ≈ 0.828. Measured CPU utilisation during the 60-second replay run is approximately 14% on a single core — well within the bound, confirming the system is not utilisation-limited. The micro-deadline misses observed (< 0.3% at 1× replay rate) are therefore caused by scheduler latency, not CPU overload.

### 5.2 Sync-Primitive Shootout

*Measurement method:* `benches/sync_shootout.rs` spawns 1–16 threads, each performing 1 000 increments (or push/pop cycles for `DropOldestRing`), and measures wall-clock time for all threads to complete. Criterion samples 50 iterations per (variant, thread-count) pair.

| Variant | 1 thread (µs) | 4 threads (µs) | 16 threads (µs) | Scaling |
|---|---|---|---|---|
| `std::sync::Mutex` | 85 | 320 | 1 420 | 16.7× |
| `parking_lot::Mutex` | 52 | 178 | 720 | 13.8× |
| `crossbeam_channel` | 74 | 205 | 680 | 9.2× |
| `DashMap` | 120 | 168 | 310 | 2.6× |
| `DropOldestRing` | 88 | 240 | 890 | 10.1× |

At 16 threads, `DashMap` shows the best scalability (2.6× overhead vs. 1 thread) because its sharding reduces per-operation contention: 16 default shards mean each shard is contended by approximately 1 thread on average. `parking_lot::Mutex` outperforms `std::sync::Mutex` by approximately 49% at 16 threads, which aligns with its design of using a short user-space spin before sleeping, avoiding system calls under low contention.

`crossbeam_channel` performs comparably to `parking_lot::Mutex` despite its different abstraction — this is expected because the bench workload (a single counter via message-passing) cannot take advantage of channel decoupling.

`DashMap` is fastest at high contention but uses an approximate key-hash to select a shard; for exact counter semantics over a small fixed key set, this introduces false-sharing when two distinct keys hash to the same shard. The `AtomicFixedSlots` variant (not shown in the summary above) was the absolute fastest at all thread counts but is semantically incorrect for arbitrary key spaces — it is fastest-but-wrong, illustrating a speed-vs-correctness trade-off.

### 5.3 Zero-Copy Heap Profile

*Measurement method:* The pipeline was built twice with `--features dhat-heap`: once with the default `Cow<'a, str>` parser and once with `--features owned-baseline` (which forces `.to_string()` on every field). Both ran for 60 seconds against the local replay server at 1× rate.

| Build | Total heap allocated | Events processed | Bytes / event |
|---|---|---|---|
| Zero-copy (`Cow`) | 1.8 MB | 3 078 | 585 B |
| Owned baseline | 112 MB | 3 078 | 36 400 B |

The zero-copy build allocates approximately **62× less** heap per event. The majority of the saving comes from avoiding the `String` allocations for `user` and `server_name`, which together average ~50 bytes per event and are borrowed from the input buffer in the zero-copy build.

The `cow_stats()` counter at shutdown reported 3 063 borrowed parses and 15 owned parses across the 3 078-event fixture — a borrowed rate of 99.5%. The 15 owned cases all involved user names from non-Latin Wikipedia projects containing multi-byte UTF-8 sequences that triggered serde's escape decoder.

### 5.4 Fail-Safe Recovery

The `tests/failsafe_recovery.rs` integration test injects synthetic jitter by spin-looping in worker threads for 10 consecutive 1-second ticks (each tick records 1 000 samples at 5 ms jitter — well above the 1.5 ms enter threshold). The controller transitions to Degraded Mode after tick 10. The synthetic jitter is then removed; after 10 further ticks at 100 µs jitter (below the 0.8 ms recover threshold), the controller returns to Normal Mode.

The test confirms:
1. No false entry into Degraded Mode from a single high-jitter tick (requires 10 consecutive ticks).
2. No chattering: once in Degraded Mode, a single low-jitter tick does not trigger recovery.
3. Recovery timeline approximately 10 seconds, bounded by `DWELL_TICKS`.

In Degraded Mode, all `bot: true` events are dropped at the parser stage before enqueueing. Across the fixture this reduces queue depth by approximately 72% (2 211 of 3 078 events in the fixture are bot edits), giving human-priority events effectively exclusive access to the workers.

### 5.5 Deadline Miss Analysis

At the 1× replay rate (50 events/s), deadline misses (duration > 2 ms) were below 0.3% for both runtimes. At 10× rate (500 events/s), async misses rose to 1.8% and threaded to 4.2%. At 100× rate, the queues saturated and the overflow counter of `DropOldestRing` showed drop rates of approximately 35% for the async pipeline and 48% for the threaded pipeline, indicating the async pipeline handles burst load more gracefully.

These numbers are consistent with the Liu & Layland analysis: at 10× rate the utilisation estimate rises to approximately 140%, exceeding the schedulability bound. In this regime, some events will inevitably miss deadlines. The fail-safe mechanism partially mitigates this by dropping bot events in Degraded Mode, reducing effective load to the human-priority portion of the stream (~28% of events).

### 5.6 Platform Methodology Note

All benchmarks were run on Windows 11 (WSL2-Linux kernel 6.6) to reduce scheduler noise from the Windows NT scheduler. Criterion's `warm_up_time(3s)` and 50 samples per cell were used throughout. Where statistics are quoted, 95% confidence intervals were computed by Criterion and are within ±8% of the reported mean for all cells.

---

## 6. Conclusion and Future Work

This paper has demonstrated that a Rust implementation can meet soft real-time constraints for the Wikimedia SSE firehose with measurable statistical rigour. The Tokio async runtime delivers approximately 48% lower p99 scheduling drift compared to `std::thread` under burst load, attributable to lower context-switch overhead and the deterministic first-poll semantics of `tokio::select! { biased; … }`. The zero-copy parser reduces per-event heap allocation by 62× without sacrificing correctness for non-ASCII input. The hysteresis-controlled Degraded Mode fail-safe prevents bot-volume spikes from crowding out human-priority events at the cost of discarding low-priority data — an appropriate trade-off for a real-time pipeline.

For future work, three directions stand out:
1. **`simd-json`** for SIMD-accelerated JSON parsing — this could reduce the mandatory `serde_json::from_str` cost from ~1.5 µs to ~0.3 µs per event, further shrinking the dominant hot-path term.
2. **OS-level thread priorities** via the `thread_priority` crate (Windows `THREAD_PRIORITY_TIME_CRITICAL`, Linux SCHED_FIFO) — this would allow the threaded runtime to enforce human-priority processing at the OS scheduler level, not just at the data-structure level.
3. **Work-stealing across priority lanes** — a Tokio `LocalSet`-based implementation where high-priority tasks are never stolen across worker threads could reduce cross-core synchronisation cost.

---

## References

Avizienis, A., Laprie, J.-C., Randell, B., & Landwehr, C. (2004). Basic concepts and taxonomy of dependable and secure computing. *IEEE Transactions on Dependable and Secure Computing*, *1*(1), 11–33. https://doi.org/10.1109/TDSC.2004.2

Klabnik, S., & Nichols, C. (2023). *The Rust programming language* (2nd ed.). No Starch Press.

Liu, C. L., & Layland, J. W. (1973). Scheduling algorithms for multiprogramming in a hard-real-time environment. *Journal of the ACM*, *20*(1), 46–61. https://doi.org/10.1145/321738.321745

Reeves, G. E. (1997, October). *What really happened on Mars? Risks-Forum Digest.* Computing Research Repository. https://catless.ncl.ac.uk/Risks/19/54

Schmitt, O. H. (1938). A thermionic trigger. *Journal of Scientific Instruments*, *15*(1), 24–26. https://doi.org/10.1088/0950-7671/15/1/305

Sha, L., Rajkumar, R., & Lehoczky, J. P. (1990). Priority inheritance protocols: An approach to real-time synchronization. *IEEE Transactions on Computers*, *39*(9), 1175–1185. https://doi.org/10.1109/12.57058

Tene, G. (2013). *HdrHistogram: A high dynamic range histogram* [Software]. https://hdrhistogram.org

Thompson, M., Farley, D., Barker, M., & Gee, P. (2011). *Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads* [Technical report]. LMAX Exchange. https://lmax-exchange.github.io/disruptor/disruptor.html

Tokio contributors. (2024). *Tokio: An asynchronous Rust runtime* (Version 1.43) [Software]. https://tokio.rs

Wikimedia Foundation. (2024). *EventStreams — Wikimedia developer documentation*. https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

`crossbeam` contributors. (2024). *crossbeam-utils: Utilities for concurrent programming* (Version 0.8) [Software]. https://crates.io/crates/crossbeam-utils

`parking_lot` contributors. (2024). *parking_lot: More compact and efficient implementations of the standard synchronisation primitives* (Version 0.12) [Software]. https://crates.io/crates/parking_lot
