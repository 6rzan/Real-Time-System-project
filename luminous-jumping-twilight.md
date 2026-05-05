# RTS2601 Real-Time Systems — Wikipedia SSE Pipeline (A+ Ceiling Plan)

> **THIS IS A MULTI-SESSION PLAN.** Each Phase below is self-contained so you can resume in a fresh Claude session with no prior context. Read the **Universal Context** first, then jump to the phase you're executing.

---

## Context

**Problem:** Build a high-pressure real-time data ingestion + analytics pipeline in Rust over the live Wikipedia SSE firehose (`https://stream.wikimedia.org/v2/stream/recentchange`). Two parallel implementations (Tokio async vs `std::thread`) compared head-to-head on tail latency, scheduling drift, jitter, deadline misses, and synchronisation throughput. Add fault-tolerance (10s watchdog → reset, jitter-triggered Degraded Mode with hysteresis recovery), zero-copy parsing via `serde` + lifetimes, and a Mutex/RwLock/Atomic shootout. Deliver source code, benchmark logs, plots, a 3000-4000 word IEEE-format research report, and a live demo.

**Why this plan:** Assignment is graded 40% (code) + 20% (report) on weighted rubrics that reward statistical rigour (p50/p90/p99 not averages), demonstrated theoretical literacy (Liu & Layland, priority inversion, EDF), and *proven* zero-copy + auto-recovery. Most students get a B by shipping something that works; this plan goes for A+ by adding rigour proofs, a proper benchmark harness, and a defensible academic report.

**Intended outcome:** A workspace `real-time-system/` with 6 crates, ~15 figures of benchmark data, a reproducible `make report` pipeline, and a polished IEEE report.

**Scope decisions (from user Q&A):**
- **Phased execution** — each phase is a checkpoint-friendly standalone unit (different Claude sessions per phase).
- **Environment recommendation: Windows-native dev + WSL2-Linux bench.** Develop on Windows (matches user's daily setup), but run final tail-latency + sync-shootout benches under WSL2/Linux for cleaner scheduler behaviour, optional `perf` integration, and to demonstrate platform-aware methodology in the report. dhat-rs works on both. *Justify the choice in the report's Methodology section* — that itself is an A+ signal.
- **Ambition: Full A+ ceiling** — every distinction extra (dhat zero-copy proof, Criterion p50/p90/p99/p99.9, fail-safe recovery timeline, 5-variant × 5-thread sync shootout, Liu&Layland math in report).

---

## Universal Context (READ FIRST EVERY SESSION)

### Repository layout (greenfield — does not exist yet)

```
C:\Users\tahaf\Desktop\UNI\Real Time System\
├── Cargo.toml                    # [workspace]
├── rust-toolchain.toml           # channel = "stable" (1.94+)
├── .cargo/config.toml            # release profile lto=thin, opt-level=3
├── .gitignore
├── README.md
├── Makefile                      # convenience targets: bench, report, demo, replay
├── crates/
│   ├── rts-core/                 # runtime-agnostic: types, traits, channel, metrics, failsafe
│   ├── rts-async/                # Tokio implementation
│   ├── rts-threaded/             # std::thread implementation
│   ├── rts-bench/                # Criterion harnesses
│   ├── rts-cli/                  # clap-derive binary entry
│   └── rts-replay/               # SSE record/playback (test fixture + demo fallback)
├── fixtures/                     # captured NDJSON SSE traces
├── reports/
│   ├── runs/                     # per-run NDJSON tracing logs
│   ├── csv/                      # postprocessed percentile data
│   ├── plots/                    # PNG figures + plot.py / plot.rs
│   └── dhat/                     # heap profile JSON dumps
└── docs/research-report/         # LaTeX or Markdown report source + figures
```

### Crate / dependency choices (LOCKED — do not deviate)

| Slot | Crate | Version | Notes |
|---|---|---|---|
| Async runtime | `tokio` | 1.43 | features: `["full"]` for binaries, narrow features in libs |
| Async HTTP | `reqwest` | 0.12 | features: `["stream", "rustls-tls"]` (no openssl) |
| SSE framing (async) | `eventsource-stream` | 0.2 | wraps `bytes_stream()` |
| Sync HTTP (threaded) | `ureq` | 2.10 | genuinely blocking — `reqwest::blocking` would smuggle in tokio |
| JSON | `serde` 1.0, `serde_json` 1.0 | — | `#[serde(borrow)]` + `Cow<'a, str>` |
| Mutex | `parking_lot` 0.12 | — | benched against `std::sync::Mutex` |
| Channels (threaded) | `crossbeam-channel` 0.5 | — | wrapped with `DropOldestRing` |
| Concurrent map (in shootout) | `dashmap` 6.0 | — | one of 5 contenders |
| False-sharing pad | `crossbeam-utils` 0.8 | — | `CachePadded<AtomicU64>` — CRITICAL for atomic bench |
| Benchmarks | `criterion` 0.5 | dev-dep, harness=false | |
| Percentiles | `hdrhistogram` 7.5 | — | per-writer, merge on snapshot |
| Heap profiling | `dhat` 0.3 | feature-gated `dhat-heap` | |
| Tracing | `tracing` 0.1, `tracing-subscriber` 0.3, `tracing-appender` 0.2 | non-blocking writer | |
| CLI | `clap` 4.5 | derive feature | |
| Errors | `thiserror` 2.0 (libs), `anyhow` 1.0 (cli only) | — | |
| CPU pinning | `core_affinity` 0.8 | — | for sync shootout |
| CPU count | `num_cpus` 1.16 | — | |
| Property tests | `proptest` 1.5 | dev-dep | |
| Mini SSE server (replay) | `axum` 0.7 + `tokio-stream` | — | for `rts-replay play` |

### Architectural invariants (apply to every phase)

1. **Use `std::time::Instant`, never `SystemTime`, for timing.** SystemTime can step backward on NTP.
2. **`Event<'a>` is borrowed and short-lived.** Convert to `OwnedEvent` exactly once at the parser→queue boundary; never store `Event<'a>` in a struct.
3. **Workers MUST not hold any shared mutex during the 2ms hot path.** Leaderboard updates flow via `mpsc` to a single owner-actor (async) or writer thread (threaded).
4. **`tracing` events emitted on the hot path go through `tracing_appender::non_blocking`** — synchronous logging will dominate jitter measurements and invalidate the benchmarks.
5. **`parking_lot::Mutex` for production code; bench `std::sync::Mutex` separately** in the shootout only.
6. **Two priority lanes, biased select (async) or `BinaryHeap<(Priority, enqueued_at)>` (threaded) — never a single FIFO with internal sort.**
7. **`DropOldestRing<T>`** is THE backpressure primitive — same code path in both runtimes (just different waker: `tokio::sync::Notify` vs `parking_lot::Condvar`).
8. **Drift = `actual_start − enqueued_at`** (per-priority HdrHistogram, simplifying assumption: empty system → expected_start = enqueued_at). State this assumption in the report.
9. **`Cow<'a, str>`, NOT `&'a str`** for borrowed string fields — JSON escapes force a fresh allocation, which `&'a str` cannot represent.
10. **Wikimedia User-Agent policy:** every HTTP request must set `User-Agent: RTS2601-coursework/0.1 (tahafahd40@gmail.com)`.

### Critical-path files (where correctness lives)

- `crates/rts-core/src/event.rs` — `Event<'a>` with `#[serde(borrow)]`, `Cow<'a, str>` (zero-copy contract).
- `crates/rts-core/src/channel/ring.rs` — `DropOldestRing<T>` (backpressure primitive shared by both runtimes).
- `crates/rts-async/src/scheduler.rs` — `tokio::select! { biased; … }` two-channel preemption (priority claim).
- `crates/rts-core/src/failsafe.rs` — sliding-window p99 + hysteresis controller (auto-recovery proof).
- `crates/rts-bench/benches/sync_shootout.rs` — Criterion 5×5 harness (headline contention plot).

### What the lecturer is really looking for (A+ signals)

1. **Theory on the page:** apply Liu & Layland's `n(2^(1/n)−1)` bound to your *own* measured utilisation; explain why Tokio's biased select is SP-PE not EDF; cite Mars Pathfinder priority inversion and explain why your design avoids it.
2. **Statistical rigour:** report p50/p90/p99/**p99.9** with sample sizes and 95% CIs. Draw CDFs, not bar charts.
3. **Demonstrated fail-safe recovery:** inject a synthetic burst, plot the state-machine timeline showing Normal → Degraded → Normal with the 10s hysteresis dwell visible.
4. **Zero-copy proof:** dhat side-by-side (zero-copy build vs owned-string baseline) showing the bytes/event delta. Plus a `cow_was_borrowed` vs `cow_was_owned` runtime counter.

---

## Phase Index

| Phase | Goal | Est. effort | Depends on |
|---|---|---|---|
| [P0](#p0-bootstrap) | Cargo workspace, CI, toolchain | 4h | — |
| [P1](#p1-sse-happy-path-async) | Async SSE happy path to stdout | 8h | P0 |
| [P2](#p2-replay-infrastructure) | NDJSON record + local SSE replay server | 6h | P1 |
| [P3](#p3-zero-copy-parser--types) | `Event<'a>`, `OwnedEvent`, `Priority`, parser + tests | 6h | P0 |
| [P4](#p4-drop-oldest-channel) | `DropOldestRing<T>` for both runtimes | 5h | P0 |
| [P5](#p5-async-pipeline-end-to-end) | rts-async fully wired: ingest→parse→2-lane→worker→leaderboard actor | 8h | P1, P3, P4 |
| [P6](#p6-threaded-pipeline-end-to-end) | Mirror P5 in `std::thread` | 8h | P3, P4 |
| [P7](#p7-metrics--drift) | HdrHistogram per-priority drift/jitter, CSV/JSON dumps | 5h | P5, P6 |
| [P8](#p8-watchdog--fail-safe) | 10s watchdog + Degraded Mode controller + recovery test | 8h | P5, P6 |
| [P9](#p9-sync-primitive-shootout) | Criterion 5-variant × 5-thread harness | 8h | P0 |
| [P10](#p10-tail-latency-comparison) | Burst-loaded async-vs-threaded p99 benchmark | 6h | P5, P6, P7 |
| [P11](#p11-dhat-zero-copy-proof) | Heap profile zero-copy vs owned-string baseline | 3h | P5 |
| [P12](#p12-plotting--analysis) | `analyse` subcommand → CSV → matplotlib plots | 5h | P7, P9, P10, P11 |
| [P13](#p13-demo-prep) | `make demo` script, replay fallback, recorded video | 4h | P5, P6, P8, P12 |
| [P14](#p14-research-report) | 3000-4000 word IEEE-format report | 15-25h | all above |
| Buffer | Bug fixes, lecturer Q&A prep | 6-10h | — |

**Critical path:** P0 → P1 → P2 → P3 → P4 → P5 → P6 → (P7 ∥ P8) → (P9 ∥ P10 ∥ P11) → P12 → P13 → P14.

**Total estimated effort:** ~110 hours.

---

## P0: Bootstrap

**Goal:** Empty workspace builds clean on Windows; CI runs fmt + clippy + test on push.

**Files to create:**
- `Cargo.toml` (workspace manifest with all 6 member crates declared, shared `[workspace.dependencies]`).
- `rust-toolchain.toml` (`channel = "stable"`).
- `.cargo/config.toml` (`[profile.release]` lto=thin, opt-level=3, codegen-units=1; `[profile.bench]` inherits release).
- `.gitignore` (target/, reports/runs/*.ndjson except .gitkeep, dhat output, .DS_Store).
- `README.md` (project blurb, build instructions, layout diagram).
- `Makefile` (targets: `build`, `test`, `clippy`, `fmt`, `bench`, `demo`, `report` — empty stubs ok).
- `crates/{rts-core,rts-async,rts-threaded,rts-bench,rts-cli,rts-replay}/Cargo.toml` (skeleton with minimal deps).
- `crates/{rts-core,rts-async,rts-threaded,rts-replay}/src/lib.rs` (`//! crate-level docs`).
- `crates/{rts-bench,rts-cli}/src/main.rs` (`fn main() {}` placeholder).
- `crates/rts-bench/Cargo.toml` (`[[bench]] name = "placeholder" harness = false`).
- `.github/workflows/ci.yml` (matrix: windows-latest + ubuntu-latest; steps: cargo fmt --check, cargo clippy --all-targets -- -D warnings, cargo test --workspace).

**Steps:**
1. `git init` (already done — git repo exists at workspace root per pre-conversation context).
2. Create the directory tree + files above.
3. Add `#![deny(rust_2018_idioms)]` and `#![warn(clippy::pedantic)]` (allow `clippy::module_name_repetitions`) to every `lib.rs`.
4. `cargo build --workspace` and `cargo test --workspace` succeed (empty crates).
5. `cargo clippy --workspace --all-targets -- -D warnings` clean.
6. `cargo fmt --check` clean.

**DONE-WHEN:**
- [ ] All 6 crates exist and compile.
- [ ] `cargo test --workspace` passes (zero tests so far is fine).
- [ ] CI workflow runs on a push to `master` and goes green.
- [ ] README documents `cargo run -p rts-cli -- --help` (will print clap usage in P5+).

---

## P1: SSE Happy Path (Async)

**Goal:** From `cargo run -p rts-cli -- run-async`, connect to live Wikipedia SSE and pretty-print the first 10 events to stdout. Reconnect with exponential backoff (capped at 30s) on disconnect.

**Files:**
- `crates/rts-async/src/lib.rs` → `pub mod ingest;`
- `crates/rts-async/src/ingest.rs` — `pub async fn run_sse(url: &str, sink: impl FnMut(&str)) -> Result<()>`
  - Uses `reqwest::Client` (with mandatory `User-Agent: RTS2601-coursework/0.1 (tahafahd40@gmail.com)`).
  - Calls `.bytes_stream()` then `.eventsource()` from `eventsource-stream` crate.
  - Loops; on `Err` reconnect with exponential backoff `200ms → 400 → 800 → … cap 30s` plus ±20% jitter (`rand::thread_rng`).
- `crates/rts-cli/src/main.rs` — clap derive with `RunAsync { #[arg(long)] limit: Option<usize> }` subcommand that calls `rts_async::ingest::run_sse(...)`.
- `crates/rts-cli/Cargo.toml` — add `tokio` (full), `tracing-subscriber`, `clap` derive, `rts-async`, `anyhow`.

**Steps:**
1. Wire clap subcommand `run-async --limit N --url URL` (default URL = the Wikipedia stream).
2. Implement `ingest::run_sse`. The `sink` callback gets the raw `data:` payload string.
3. Initialise `tracing_subscriber::fmt().json().init()` in `main`.
4. Test manually: `cargo run -p rts-cli --release -- run-async --limit 10` prints 10 raw event JSON lines, then exits.
5. Verify reconnect: pull the network plug for 5s, re-plug; expect a tracing warn on disconnect, then a recover.

**DONE-WHEN:**
- [ ] Live stream prints events to stdout.
- [ ] `--limit N` exits cleanly after N events.
- [ ] Disconnect/reconnect produces tracing log entries and continues.
- [ ] No panics on malformed events (just log + skip).

---

## P2: Replay Infrastructure

**Goal:** `rts-cli replay record --duration 300s --out fixtures/recentchange-300s.ndjson` captures 5 min of stream. `rts-cli replay play --fixture <path> --rate 10x --port 8080` serves an HTTP+SSE endpoint identical in shape to upstream. **This de-risks the demo and makes tests deterministic.**

**Files:**
- `crates/rts-replay/src/lib.rs` → `pub mod record; pub mod play;`
- `crates/rts-replay/src/record.rs` — opens NDJSON file, drives `rts-async::ingest::run_sse`, writes one event JSON per line with a leading `{"recv_ns": <since-start>, "data": <raw>}` envelope.
- `crates/rts-replay/src/play.rs` — `axum` 0.7 server with route `GET /v2/stream/recentchange` returning `text/event-stream`. Reads NDJSON, sleeps `(recv_ns / rate)` between events, writes `data: <raw>\n\n` chunks.
- `crates/rts-cli/src/main.rs` — add `Replay { Record {…}, Play {…} }` subcommands.

**Steps:**
1. `record`: capture upstream → NDJSON. Commit a 60-second fixture under `fixtures/recentchange-60s.ndjson` (small enough for git, ~1-3 MB).
2. `play`: serve at `http://127.0.0.1:8080/v2/stream/recentchange`. Loop the fixture if exhausted.
3. Add `--rate` parser: accepts `1x`, `10x`, `100x`, `max`.
4. Smoke test: in one terminal, `rts-cli replay play --fixture fixtures/recentchange-60s.ndjson --rate 10x`; in another, `rts-cli run-async --url http://127.0.0.1:8080/v2/stream/recentchange --limit 50`. Should print 50 events from the local replay.

**DONE-WHEN:**
- [ ] Recorded fixture committed (60s sample at minimum).
- [ ] Local replay server serves valid SSE.
- [ ] Async ingest works against both live URL and replay URL via `--url`.
- [ ] Rate multipliers work (`1x` ≈ wall clock; `10x` ≈ 10× compressed).

---

## P3: Zero-Copy Parser & Types

**Goal:** `Event<'a>`, `OwnedEvent`, `Priority`, parser. Hot path zero-allocates in the >99% common case. Unit tests for borrowed-vs-owned `Cow` behaviour.

**Files:**
- `crates/rts-core/Cargo.toml` — add `serde` (derive), `serde_json`, `thiserror`, `tracing`.
- `crates/rts-core/src/lib.rs` → `pub mod event; pub mod priority; pub mod task; pub mod time; pub mod error;`
- `crates/rts-core/src/event.rs`:
  ```rust
  use std::borrow::Cow;
  #[derive(serde::Deserialize, Debug)]
  pub struct Event<'a> {
      #[serde(borrow)] pub user: Cow<'a, str>,
      pub bot: bool,
      #[serde(borrow)] pub server_name: Cow<'a, str>,
  }
  #[derive(Clone, Debug)]
  pub struct OwnedEvent { pub user: String, pub bot: bool, pub server_name: String }
  impl<'a> Event<'a> { pub fn into_owned(self) -> OwnedEvent { /* … */ } }
  pub fn parse_one<'a>(buf: &'a str) -> Result<Event<'a>, serde_json::Error> {
      serde_json::from_str(buf)
  }
  ```
- `crates/rts-core/src/priority.rs`:
  ```rust
  #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
  pub enum Priority { Human, Bot } // Human < Bot, so BinaryHeap with Reverse pops Human first
  ```
- `crates/rts-core/src/task.rs`:
  ```rust
  pub struct Task {
      pub event: OwnedEvent,
      pub priority: Priority,
      pub enqueued_at: std::time::Instant,
  }
  // expected_start := enqueued_at; drift := actual_start - enqueued_at
  ```
- `crates/rts-core/src/time.rs` — `pub fn now_ns() -> u64` from `Instant`-since-process-start.
- `crates/rts-core/src/error.rs` — `thiserror`-based `RtsError`.

**Tests** (in `event.rs` `#[cfg(test)] mod tests`):
- Parse a fixture event; assert `bot == false`, `server_name == "en.wikipedia.org"`.
- Parse one with `ÿ` in `user`; assert the `Cow::Owned` branch was taken.
- Parse one with no escapes; assert `Cow::Borrowed`.
- Add `BORROWED_COUNT` and `OWNED_COUNT` static `AtomicU64` and a public `cow_stats()` accessor (used in P11/P14 to populate the headline "99.x% borrowed" claim).
- `Priority::Human < Priority::Bot` (sanity).
- `Task` is `Send + 'static`.

**DONE-WHEN:**
- [ ] All unit tests pass.
- [ ] `cargo doc --no-deps` renders without warnings on `rts-core`.
- [ ] No `unsafe` in `rts-core`.

---

## P4: Drop-Oldest Channel

**Goal:** `DropOldestRing<T>` — bounded, FIFO, drop-oldest on push when full, increments an overflow counter, supports both `tokio::sync::Notify` (async) and `parking_lot::Condvar` (sync) wakeups. Unit + property tests.

**Files:**
- `crates/rts-core/src/channel/mod.rs` → `pub mod ring;`
- `crates/rts-core/src/channel/ring.rs`:
  ```rust
  pub struct DropOldestRing<T> { /* Mutex<VecDeque<T>>, capacity, AtomicU64 overflow, Notify/Condvar */ }
  pub enum PushOutcome { Ok, DroppedOldest }
  impl<T> DropOldestRing<T> {
      pub fn new(capacity: usize) -> Arc<Self>;
      pub fn push(&self, item: T) -> PushOutcome;       // sync push (called from any context)
      pub async fn pop_async(&self) -> T;                // tokio variant (Notify-based)
      pub fn pop_blocking(&self) -> T;                   // threaded variant (Condvar-based)
      pub fn overflow_count(&self) -> u64;
      pub fn len(&self) -> usize;
  }
  ```
  Implement with EITHER (a) two impls under cfg features `async-waker` / `sync-waker`, OR (b) a single struct exposing both pops — pick (b) for simplicity since `tokio::sync::Notify` and `parking_lot::Condvar` are both cheap to keep.
- Use `parking_lot::Mutex<VecDeque<T>>` for the storage in both cases.

**On `PushOutcome::DroppedOldest`:** caller (the parser) emits `tracing::warn!(target: "overflow", ts_ns, queue_depth, "Overflow Event")`. The brief calls for "high-precision timestamped Overflow Event" — this satisfies it.

**Tests:**
- Push to capacity, push one more, assert front element is dropped and overflow_count == 1.
- Property test (`proptest`): for a random sequence of pushes/pops with random capacity, the queue invariants hold (length ≤ cap, FIFO order of survivors).
- Async + threaded basic round-trip in two `#[tokio::test]` / `#[test]` cases.

**DONE-WHEN:**
- [ ] Tests pass.
- [ ] Overflow counter monotonic and accurate under stress test (N=100k pushes, 10 capacity, single popper).

---

## P5: Async Pipeline End-to-End

**Goal:** `rts-cli run-async --url <live-or-replay> --duration 60s` runs the full pipeline:
ingest → parse (zero-copy) → priority classify → 2-lane channel → biased-select worker pool → leaderboard actor → metrics + tracing JSON logs.

**Files:**
- `crates/rts-async/src/lib.rs` → `pub mod ingest; pub mod parser; pub mod scheduler; pub mod leaderboard; pub mod pipeline;`
- `crates/rts-async/src/parser.rs` — consumes raw event strings, calls `rts_core::event::parse_one`, classifies, builds `Task`, sends to `hi_tx` or `lo_tx` (both `DropOldestRing`).
- `crates/rts-async/src/scheduler.rs` — `pub async fn worker(hi: Arc<DropOldestRing<Task>>, lo: Arc<DropOldestRing<Task>>, lb: mpsc::Sender<String>, metrics: Arc<Metrics>)` running:
  ```rust
  loop {
      let task = tokio::select! {
          biased;
          t = hi.pop_async() => t,
          t = lo.pop_async() => t,
      };
      let actual_start = Instant::now();
      let drift = actual_start.saturating_duration_since(task.enqueued_at);
      metrics.record_drift(task.priority, drift);
      execute_hot_path(&task, &lb).await;   // emit leaderboard update
      let total = actual_start.elapsed();
      metrics.record_jitter(total);
      if total.as_nanos() > 2_000_000 { metrics.record_deadline_miss(task.priority); }
  }
  ```
- `crates/rts-async/src/leaderboard.rs` — actor task owning `HashMap<String, u64>`, exposes `update_tx: mpsc::Sender<String>` to writers and a snapshot path that returns top-3 via a oneshot.
- `crates/rts-async/src/pipeline.rs` — the `run(cfg)` orchestrator: spawn ingest, parser, N workers (default = `num_cpus::get()`), leaderboard actor, set up `tracing_appender::non_blocking` writer.
- `crates/rts-cli/src/main.rs` — wire `RunAsync { url, duration, workers, capacity, log_path }`.

**Architectural reminders:**
- `DropOldestRing` capacity per lane: configurable, default 256.
- `tracing_appender::non_blocking(rolling::never(...))` writer goes to `reports/runs/<timestamp>.ndjson`.
- Use `tokio::runtime::Builder::new_multi_thread().worker_threads(N).enable_all()`.
- Leaderboard actor receives only `String` (the `server_name` clone) — no Mutex, no contention.

**Tests:**
- `tests/preemption.rs`: feed 999 Bot events then 1 Human event into the pipeline (via direct channel push to bypass network); assert Human's drift is much lower than the Bot p50.
- Integration test against `rts-replay play` server with the committed 60s fixture.

**DONE-WHEN:**
- [ ] End-to-end run produces `reports/runs/*.ndjson` with `priority`, `drift_ns`, `duration_ns`, `deadline_miss` fields per event.
- [ ] Top-3 leaderboard printed at shutdown.
- [ ] Preemption test passes.
- [ ] No panics over a 60s replay run.

---

## P6: Threaded Pipeline End-to-End

**Goal:** Mirror P5 in `std::thread`. Same metrics, same JSON log schema, same `rts-core` types.

**Files:**
- `crates/rts-threaded/src/lib.rs` → `pub mod ingest; pub mod parser; pub mod scheduler; pub mod leaderboard; pub mod pipeline;`
- `crates/rts-threaded/src/ingest.rs` — uses `ureq` (NOT `reqwest::blocking`) to GET the SSE stream with a streaming reader; parses the SSE protocol manually (~80 lines: split on `\n`, extract `data:` lines, accumulate until blank line). Set socket read timeout 1s for watchdog interaction in P8.
- `crates/rts-threaded/src/scheduler.rs` — `PriorityQueue { Mutex<BinaryHeap<Reverse<(Priority, Instant, OwnedEvent)>>>, Condvar }`.
  Workers `pop_blocking()` from the queue, run hot path identically.
- `crates/rts-threaded/src/leaderboard.rs` — receiver thread on a `crossbeam_channel::unbounded` of `String`, owns a HashMap, emits snapshot on demand.
- `crates/rts-cli/src/main.rs` — add `RunThreaded { … }` subcommand mirroring `RunAsync`.

**Reminder:** `BinaryHeap` is max-heap; we want min-on-priority so wrap in `Reverse`. Tie-break by `enqueued_at` so older Human events still serve before newer Human events (FIFO within priority).

**Tests:**
- `tests/preemption_threaded.rs`: same shape as the async test.
- Round-trip integration test against replay server.

**DONE-WHEN:**
- [ ] Threaded pipeline produces structurally-identical NDJSON logs to async pipeline.
- [ ] Preemption invariant holds.
- [ ] Worker count configurable (default `num_cpus::get() - 2`, reserving cores for ingest+watchdog).

---

## P7: Metrics & Drift Instrumentation

**Goal:** Per-priority HdrHistogram for drift and jitter. CSV/JSON dumps on shutdown. The data backing the report's headline plots.

**Files:**
- `crates/rts-core/src/metrics.rs`:
  ```rust
  pub struct Metrics {
      drift_human: parking_lot::Mutex<Histogram<u64>>,
      drift_bot:   parking_lot::Mutex<Histogram<u64>>,
      jitter:      parking_lot::Mutex<Histogram<u64>>,
      deadline_miss_human: AtomicU64,
      deadline_miss_bot:   AtomicU64,
      overflow_count:      AtomicU64,
  }
  impl Metrics {
      pub fn new() -> Arc<Self> { /* Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap() */ }
      pub fn record_drift(&self, p: Priority, d: Duration);
      pub fn record_jitter(&self, j: Duration);
      pub fn record_deadline_miss(&self, p: Priority);
      pub fn dump_csv(&self, path: &Path) -> std::io::Result<()>;
      pub fn dump_hgrm(&self, path: &Path) -> std::io::Result<()>; // hdrhistogram's native format
      pub fn snapshot(&self) -> MetricsSnapshot { /* p50, p90, p99, p99.9, max, count */ }
  }
  ```
- Wire into both `rts-async::pipeline` and `rts-threaded::pipeline`.
- On `SIGINT` / duration expiry, dump `reports/csv/<run_id>_metrics.csv` and `.hgrm` files.

**Optimisation note:** in extreme contention, use one Histogram per worker and merge on snapshot — but only do this if benches show the per-record Mutex is dominating. Start simple.

**Tests:**
- Synthetic feed of 1000 events into the pipeline, dump CSV, parse it back, assert sample counts match.

**DONE-WHEN:**
- [ ] On shutdown, CSV + .hgrm files are produced under `reports/csv/`.
- [ ] Snapshot prints to stdout: `p50/p90/p99/p99.9` for drift_human, drift_bot, jitter; deadline-miss + overflow counts.
- [ ] No measurable overhead delta in latency benches (verify in P10).

---

## P8: Watchdog & Fail-Safe

**Goal:** 10s no-data watchdog → reset signal. Sliding-window p99 jitter triggers Degraded Mode (1.5ms enter, 0.8ms recover, 10s hysteresis dwell). Synthetic test proves recovery.

**Files:**
- `crates/rts-core/src/watchdog.rs`:
  ```rust
  pub struct WatchdogState { pub last_event_ns: AtomicU64 }
  impl WatchdogState { pub fn arm(&self); pub fn elapsed_ns(&self) -> u64; }
  // Async checker: tokio::time::interval(1s) — fires reset_tx if elapsed > 10s.
  // Sync checker: dedicated thread + 1s sleep loop.
  ```
- `crates/rts-core/src/failsafe.rs`:
  ```rust
  pub struct FailSafeController {
      state: AtomicU8,                                 // 0=Normal, 1=Degraded
      rolling: parking_lot::Mutex<RollingHistogram>,   // last 5s of jitter samples
      enter_threshold_ns: u64,                         // 1_500_000
      recover_threshold_ns: u64,                       //   800_000
      consecutive_recovery_secs: AtomicU32,
      required_recovery_secs: u32,                     // 10
  }
  impl FailSafeController {
      pub fn record_jitter(&self, ns: u64);
      pub fn tick_one_second(&self) -> Option<StateTransition>;  // call from a 1s scheduler
      pub fn is_degraded(&self) -> bool;
  }
  pub struct RollingHistogram { ring: [Histogram<u64>; 5], idx: usize }
  // tick_one_second rotates the ring, merges 5, queries p99, decides transition.
  ```
- Wire into both runtimes:
  - In Degraded mode, the **parser stage** drops `bot:true` events instead of enqueueing (`if controller.is_degraded() && event.bot { return; }`).
  - Leaderboard updates throttled to every 100th event.
  - State transitions emit `tracing::error!(target:"failsafe", state="degraded"|"normal", ts_ns)`.
- Watchdog reset path: ingest receives `ResetSignal::Network`, drops current connection, reconnects with backoff.

**Tests:**
- `tests/watchdog_trigger.rs`: feed events for 5s, stall for 11s, assert reset signal observed.
- `tests/failsafe_recovery.rs`: synthetic burst (`std::hint::spin_loop` in spawned threads to inflate jitter), assert state goes Degraded; stop burst; assert state returns to Normal within ~12s (10s hysteresis + tick alignment); record the transition timeline as `reports/runs/failsafe_demo.ndjson` for the report figure.

**DONE-WHEN:**
- [ ] Watchdog fires after 10s of no events, ingest reconnects.
- [ ] Synthetic-burst test produces a state transition timeline showing Normal → Degraded → Normal.
- [ ] No flapping: a 1s-borderline-jitter input does NOT cycle states.

---

## P9: Sync-Primitive Shootout

**Goal:** Criterion harness benchmarking 5 variants × 5 thread counts on a "100k increments on 50-key working set" workload. Outputs per-variant ns/op + throughput + 95% CIs. The report's headline contention plot.

**Files:**
- `crates/rts-bench/Cargo.toml` — `criterion = { version = "0.5", features = ["html_reports"] }`, `core_affinity`, `crossbeam-utils`, `dashmap`, `parking_lot`.
- `crates/rts-bench/benches/sync_shootout.rs`:
  ```rust
  use criterion::{Criterion, criterion_group, criterion_main, BenchmarkId, Throughput};
  enum Variant { ParkingLotMutex, StdMutex, ParkingLotRwLock, DashMap, AtomicFixedSlots }
  fn run_n_threads<V: Counter + Send + Sync + 'static>(threads: usize, iters: u64, c: V) -> Duration { /* core_affinity-pin, 100k increments distributed over 50 keys, barrier-synced start, return wall time */ }
  fn bench(c: &mut Criterion) {
      let mut g = c.benchmark_group("counter_increment");
      g.throughput(Throughput::Elements(100_000));
      g.warm_up_time(Duration::from_secs(3));
      g.sample_size(50);
      for &t in &[1usize, 2, 4, 8, 16] {
          g.bench_with_input(BenchmarkId::new("ParkingLotMutex", t), &t, /*…*/);
          g.bench_with_input(BenchmarkId::new("StdMutex", t),         &t, /*…*/);
          g.bench_with_input(BenchmarkId::new("ParkingLotRwLock", t), &t, /*…*/);
          g.bench_with_input(BenchmarkId::new("DashMap", t),          &t, /*…*/);
          g.bench_with_input(BenchmarkId::new("AtomicFixedSlots", t), &t, /*…*/);
      }
      g.finish();
  }
  ```
- Implement each `Counter` trait variant with `CachePadded<AtomicU64>` for the atomic variant.
- Run on **WSL2/Linux** for the canonical numbers (mention Windows numbers as comparison footnote in the report).

**Critical traps:**
- Pin threads with `core_affinity`. Without it, the OS scheduler obscures contention scaling.
- `CachePadded` on the atomic slots — without it, atomics-bench numbers are wrong.
- Note: `AtomicFixedSlots` collides distinct domain names into the same slot — *fast but incorrect*. Lesson for the report: speed vs correctness trade-off.

**DONE-WHEN:**
- [ ] `cargo bench -p rts-bench --bench sync_shootout` completes (~10-15 min).
- [ ] Criterion HTML report under `target/criterion/` with all 25 cells populated.
- [ ] Raw CSV exported to `reports/csv/sync_shootout.csv`.

---

## P10: Tail-Latency Comparison (Async vs Threaded)

**Goal:** Replay a fixture at 1×/10×/100× into both runtimes; emit p50/p90/p99/p99.9 latency under each load level. The headline async-vs-threaded comparison.

**Files:**
- `crates/rts-bench/benches/tail_latency.rs`:
  - For each runtime ∈ {async, threaded}, for each rate ∈ {1, 10, 100}: spin up replay server on ephemeral port, run pipeline for 60s, dump metrics, compute percentiles, store in CSV.
- `crates/rts-cli/src/main.rs` — add `Bench { TailLatency { … } }` subcommand for non-Criterion runs (used inside the bench but also reachable manually).

**Rationale for not using Criterion's iterators here:** Criterion's `iter`/`iter_custom` is great for short kernels but awkward for 60s pipeline runs. Use a **straight orchestrator** that calls into the same code paths and exports CSV — Criterion is best for the sync shootout (P9).

**DONE-WHEN:**
- [ ] CSV `reports/csv/tail_latency.csv` with columns: runtime, rate, percentile, value_ns, sample_count.
- [ ] Numbers stable across two consecutive runs (within 10% on p99).

---

## P11: dhat Zero-Copy Proof

**Goal:** Heap-profile the parser stage with dhat in two builds: `--features zero-copy` (default, the `Cow` parser) and `--features owned-baseline` (a deliberately allocating variant that does `.to_string()` on every borrowed field). Show bytes/event delta — the report's zero-copy headline.

**Files:**
- `crates/rts-core/src/event.rs` — add `#[cfg(feature = "owned-baseline")]` variant of `parse_one` that allocates owned `String`s explicitly.
- `crates/rts-cli/Cargo.toml` — feature flags: `dhat-heap` (enables `dhat::Profiler`), `owned-baseline` (forces the allocating parser).
- `crates/rts-cli/src/main.rs`:
  ```rust
  #[cfg(feature = "dhat-heap")]
  #[global_allocator]
  static ALLOC: dhat::Alloc = dhat::Alloc;
  fn main() {
      #[cfg(feature = "dhat-heap")]
      let _profiler = dhat::Profiler::new_heap();
      // … rest of main …
  }
  ```
- Run twice:
  - `cargo run --release --features dhat-heap -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 60s`
  - `cargo run --release --features "dhat-heap,owned-baseline" -p rts-cli -- run-async … same args …`
- Both produce `dhat-heap.json`. Move to `reports/dhat/zero_copy.json` and `reports/dhat/owned_baseline.json`.
- Postprocess into the headline number: bytes/event = total_bytes / events_processed.

Also expose `cow_was_borrowed` / `cow_was_owned` counters from P3 in shutdown report (`tracing::info!(target:"cow_stats", borrowed = …, owned = …)`).

**DONE-WHEN:**
- [ ] Two dhat JSON dumps committed.
- [ ] Reported delta ≥ 50× reduction in allocations (expectation — Wikipedia's stream is mostly ASCII so `Cow` stays Borrowed >99%).
- [ ] `cow_stats` counters logged.

---

## P12: Plotting & Analysis

**Goal:** `make report` regenerates every figure from CSV/hgrm files. Reproducible.

**Files:**
- `reports/plots/plot.py` — single Python script (matplotlib + numpy + hdrhistogram) producing:
  - `fig1_drift_cdf.png` — per-priority drift CDF, both runtimes (4 lines).
  - `fig2_tail_latency.png` — bar chart of p50/p90/p99/p99.9 across rates × runtimes.
  - `fig3_sync_shootout.png` — line plot, throughput vs thread count, 5 variants.
  - `fig4_deadline_miss_timeline.png` — overlaid time series.
  - `fig5_heap_profile.png` — dhat-derived bytes/event bar chart.
  - `fig6_failsafe_timeline.png` — state-machine plot from `failsafe_demo.ndjson`.
  - `fig7_overflow_vs_capacity.png` — drops/sec vs capacity (8/64/512/4096).
- `crates/rts-cli/src/main.rs` — `Analyse { Run { ndjson_path } }` subcommand: streams an NDJSON log, computes percentiles, writes CSV.
- `Makefile` `report:` target = run a curated 60s replay set + `cargo bench` + `python plot.py`.

**DONE-WHEN:**
- [ ] `make report` from a clean checkout (with prerequisites) regenerates all 7 figures.
- [ ] Figures embedded in `docs/research-report/` (P14).

---

## P13: Demo Prep

**Goal:** A 5-minute live demo script that survives a hostile network. Recorded video as backup.

**Files / artefacts:**
- `scripts/demo.ps1` (Windows) and `scripts/demo.sh` (Linux):
  1. Start replay server in background (`rts-cli replay play --rate 10x &`).
  2. Run `rts-cli run-async --url http://localhost:8080/... --duration 60s` with live tail of NDJSON output.
  3. Inject controlled jitter burst (a separate `rts-cli stress --duration 30s`) → show Degraded → recover.
  4. Pull network plug analogue (kill replay server) → show watchdog reset firing.
  5. Print snapshot: top-3 leaderboard + percentile table.
- Record a 5-min screen capture as `docs/demo.mp4`.

**DONE-WHEN:**
- [ ] Demo script runs end-to-end in <6 minutes.
- [ ] Recorded video exists and is playable.

---

## P14: Research Report

**Goal:** 3000-4000 words, IEEE format, embedding figures from P12. Aligned to grading weights (Design 35%, Results 35%, the rest 10% each).

**Structure** (target word counts):

1. **Abstract** (~200 w) — one paragraph: problem, dual-runtime contribution, single headline number (e.g. "async halved p99 latency under 10× burst load").
2. **Introduction** (~350 w) — soft real-time context, two crisp RQs:
   - RQ1: Under bursty real-time load, does Tokio async deliver lower tail latency than OS threads in Rust, and at what cost?
   - RQ2: Among Mutex/RwLock/Atomic/sharded variants, which best supports a contended counter substrate?
3. **Related Work** (~350 w) — **demonstrate theory**:
   - Liu & Layland (1973): Rate Monotonic schedulability bound `n(2^(1/n)−1)`. Cite. Apply with measured U.
   - Earliest Deadline First — note Tokio biased select is *static-priority pre-emption*, not EDF.
   - Priority inversion + Mars Pathfinder (Reeves 1997) — explain why our design is immune (no shared blocking critical section on hot path).
   - LMAX Disruptor (Thompson et al.) — drop-oldest precedent.
   - Tokio scheduling internals.
   - Wikimedia EventStreams docs.
4. **System Design** (~1200-1400 w) — six subsections:
   - 4.1 Workspace architecture (insert module diagram).
   - 4.2 Zero-copy parsing — `Cow<'a, str>` lifetime contract.
   - 4.3 Priority scheduling — `biased;` keyword + BinaryHeap; explicit drift definition.
   - 4.4 `DropOldestRing` algorithm + correctness sketch.
   - 4.5 Watchdog & FailSafe state machine + hysteresis math.
   - 4.6 Sync-primitive benchmark methodology (warmup, pinning, sample sizes, 95% CIs).
5. **Implementation Notes** (~300 w) — only details that move results: parking_lot, CachePadded, non_blocking tracing, dhat allocator-swap.
6. **Results** (~1200-1400 w) — every figure from P12 referenced with discussion. **Compute Liu&Layland's bound for the measured workload** and contrast with observed deadline-miss rate. State sample size and 95% CI for every quoted percentile.
7. **Discussion** (~300 w) — when async wins, when it doesn't, false-sharing surprise, AtomicFixedSlots fastest-but-wrong lesson.
8. **Conclusion + Future Work** (~200 w) — simd-json, work-stealing schedulers, OS-RT thread priorities (`thread_priority` crate, Windows `THREAD_PRIORITY_TIME_CRITICAL`).
9. **References** — IEEE format, ≥12 sources.

**Files:**
- `docs/research-report/report.tex` (or `report.md` if Markdown is acceptable — confirm with lecturer).
- `docs/research-report/figures/` (copies of P12 PNGs).
- `docs/research-report/refs.bib`.

**DONE-WHEN:**
- [ ] Word count between 3000-4000 (penalised over 4000 per brief).
- [ ] All 7 figures embedded with captions referencing the data file.
- [ ] All quoted numbers cite a CSV/hgrm file in `reports/`.
- [ ] References use consistent IEEE format with 12+ entries.

---

## Risk Register (apply throughout)

| # | Risk | Mitigation |
|---|---|---|
| 1 | Wikimedia rate-limits us | Mandatory `User-Agent` header; capped backoff; **rts-replay** for benches & demo. |
| 2 | Lifetime fights with `serde(borrow)` | `Event<'a>` is read-only; `into_owned()` at channel boundary; unit test for `ÿ` escape. |
| 3 | False sharing skews atomic bench | `crossbeam_utils::CachePadded` on every atomic slot. |
| 4 | Tracing per-event cost dominates jitter | `tracing_appender::non_blocking`; verify by toggling logs off + delta = logger cost (report it). |
| 5 | OS scheduler noise | `core_affinity` pinning; bench on WSL2 Linux for canonical numbers. |
| 6 | Demo network fails on the day | Always run demo against `rts-replay`; live stream optional. |
| 7 | Clock non-monotonicity | `Instant` for measurements, `SystemTime` only for human-readable wall clock. |
| 8 | dhat skews timing benches | dhat is a **separate run** with `--features dhat-heap`; never enabled during latency benches. |
| 9 | `reqwest::blocking` smuggles tokio into "threaded" benches | Use `ureq` for the threaded ingest. |
| 10 | Async-vs-thread bench bias from cold start | `Criterion::warm_up_time(3s)`; bench steady state; `iter_custom` excludes setup. |

---

## Verification Checklist (end-to-end smoke test)

Run after P12 to confirm the full system holds together:

```pwsh
# 1. Build clean
cargo clean
cargo build --workspace --release
cargo clippy --workspace --all-targets -- -D warnings

# 2. Unit + integration tests
cargo test --workspace --release

# 3. Replay infra
cargo run --release -p rts-cli -- replay play --fixture fixtures/recentchange-60s.ndjson --rate 10x &
$server = $LASTBACKGROUNDPID
cargo run --release -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s --log-path reports/runs/smoke_async.ndjson
cargo run --release -p rts-cli -- run-threaded --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s --log-path reports/runs/smoke_threaded.ndjson
Stop-Process -Id $server

# 4. Benches
cargo bench -p rts-bench --bench sync_shootout
cargo bench -p rts-bench --bench tail_latency

# 5. dhat proof
cargo run --release --features dhat-heap -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s
cargo run --release --features "dhat-heap,owned-baseline" -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s

# 6. Plots
make report  # or python reports/plots/plot.py

# 7. Demo dry run
.\scripts\demo.ps1
```

Each step produces artefacts under `reports/` and `target/criterion/` that must be present and non-empty.

---

## Session Handoff Notes (for the next AI)

When resuming a phase in a fresh Claude session:

1. **Open this file first.** Read **Universal Context** in full.
2. Identify the active phase number from the user's prompt or the last completed phase's DONE-WHEN.
3. **Check git log** to confirm what's actually committed vs what this plan claims should exist.
4. **Read the previous phase's outputs** before adding new code (e.g., the `Event<'a>` definition before writing the parser stage).
5. Honour the **architectural invariants** (universal context). They're locked, not suggestions.
6. End each phase with a commit `feat(pX): <phase name> — <one-line summary>`. The lecturer reads `git log` too.
