# RTS2601 — Real-Time Wikipedia SSE Pipeline

A high-pressure real-time data ingestion + analytics pipeline in **Rust** consuming the live Wikipedia *Recent Changes* SSE firehose
(`https://stream.wikimedia.org/v2/stream/recentchange`). Built for the **CT087-3-3-RTS** assignment.

The system implements the same pipeline twice — once with **Tokio async/await**, once with **`std::thread`** — and benchmarks them
head-to-head on tail latency, scheduling drift, jitter, and synchronisation throughput.

## Features

- **Dual-runtime pipelines**: Tokio (`rts-async`) and `std::thread` (`rts-threaded`) sharing the same core types and metrics.
- **Zero-copy parsing**: `serde` with `Cow<'a, str>` lifetimes; allocations only at the priority-queue boundary. Heap profile vs.
  an owned-string baseline proves the saving via `dhat-rs`.
- **Priority scheduling**: `bot:true` events are low priority; human edits preempt via biased `tokio::select!` (async) or a
  `BinaryHeap<(Priority, Instant)>` (threaded). Per-priority drift histograms quantify the effect.
- **Drop-oldest backpressure**: bounded ring with a high-precision timestamped *Overflow Event* per drop.
- **Watchdog**: triggers a network reset after 10 s of silence.
- **Fail-Safe / Degraded Mode**: sliding-window p99 jitter trip with hysteresis recovery.
- **Sync-primitive shootout**: Criterion benches comparing `parking_lot::Mutex`, `std::sync::Mutex`, `RwLock`, `DashMap`, and
  cache-padded atomics across 1–16 threads.
- **Statistical rigour**: HdrHistogram p50/p90/p99/p99.9 with sample sizes and 95 % CIs.

## Workspace layout

```
Cargo.toml                 # workspace root
crates/
  rts-core/                # runtime-agnostic types, channel, metrics, fail-safe
  rts-async/               # Tokio implementation
  rts-threaded/            # std::thread implementation
  rts-bench/               # Criterion harnesses
  rts-cli/                 # clap entry point (run-async, run-threaded, replay, bench, analyse)
  rts-replay/              # SSE record / playback (deterministic test source + demo fallback)
fixtures/                  # captured NDJSON SSE traces
reports/                   # logs, CSVs, plots, dhat dumps
docs/research-report/      # IEEE-format report source + figures
```

## Build

Requires Rust 1.94+ stable (pinned via `rust-toolchain.toml`). On Windows, `rustls-tls` removes the need for OpenSSL.

```pwsh
cargo build --workspace --release
cargo test  --workspace --release
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## Run (live stream)

```pwsh
cargo run --release -p rts-cli -- run-async    --duration 60s
cargo run --release -p rts-cli -- run-threaded --duration 60s
```

## Run (deterministic replay — recommended for benches and demos)

```pwsh
# Record a fixture (one-off):
cargo run --release -p rts-cli -- replay record --duration 300s --out fixtures/recentchange-300s.ndjson

# Serve a local SSE replay:
cargo run --release -p rts-cli -- replay play --fixture fixtures/recentchange-60s.ndjson --rate 10x

# Point either runtime at the local replay:
cargo run --release -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 60s
```

## Benchmarks

```pwsh
cargo bench -p rts-bench --bench sync_shootout
cargo bench -p rts-bench --bench tail_latency
```

Criterion HTML reports land under `target/criterion/`; raw CSVs under `reports/csv/`.

## Heap profile (zero-copy proof)

```pwsh
cargo run --release --features dhat-heap -p rts-cli -- run-async --duration 30s
# Then with the deliberately-allocating baseline:
cargo run --release --features "dhat-heap,owned-baseline" -p rts-cli -- run-async --duration 30s
```

## Plots / report figures

```pwsh
make report
# or:
python reports/plots/plot.py
```

## License

MIT OR Apache-2.0.
