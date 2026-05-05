# Development Guide — RTS2601 Real-Time Systems Pipeline

Quick reference for building, testing, and running the project locally.

## Prerequisites

- **Rust 1.94+** (stable) — install via [rustup](https://rustup.rs/)
- **Git** — for version control
- **Python 3.9+** — for plotting scripts (in P12+)
- **Optional:** WSL2 or Linux VM — for canonical benchmarks (easier scheduler behavior)

## Quick Start

### Clone & Build

```bash
git clone <repo>
cd "Real Time System"
cargo build --workspace --release
```

### Verify Everything Builds & Passes Local Tests

```bash
# Format check
cargo fmt --check

# Lint (all targets, deny warnings)
cargo clippy --workspace --all-targets -- -D warnings

# Unit + integration tests
cargo test --workspace --release
```

If all three commands succeed, your code is CI-ready.

## Running the Pipeline

### 1. Async Runtime (Tokio)

**Live Wikipedia stream:**
```bash
cargo run --release -p rts-cli -- run-async \
  --url https://stream.wikimedia.org/v2/stream/recentchange \
  --duration 60s \
  --workers 8 \
  --capacity 256 \
  --log-path reports/runs/async_live.ndjson
```

**Local replay (recommended for testing):**
```bash
# Terminal 1: Start replay server
cargo run --release -p rts-cli -- replay play \
  --fixture fixtures/recentchange-60s.ndjson \
  --rate 10x \
  --port 8080

# Terminal 2: Run pipeline against replay
cargo run --release -p rts-cli -- run-async \
  --url http://127.0.0.1:8080/v2/stream/recentchange \
  --duration 60s \
  --log-path reports/runs/async_replay.ndjson
```

### 2. Threaded Runtime (std::thread)

Same flags as async, swap `run-async` → `run-threaded`:

```bash
cargo run --release -p rts-cli -- run-threaded \
  --url http://127.0.0.1:8080/v2/stream/recentchange \
  --duration 60s \
  --log-path reports/runs/threaded_replay.ndjson
```

## Benchmarking

### Sync-Primitive Shootout (5 variants × 5 thread counts)

```bash
cargo bench -p rts-bench --bench sync_shootout
```

Output: `target/criterion/counter_increment/` with HTML report + raw CSV.

### Tail-Latency Comparison (async vs threaded under burst)

```bash
cargo bench -p rts-bench --bench tail_latency
```

Output: `reports/csv/tail_latency.csv` with p50/p90/p99/p99.9 per runtime × rate.

## Memory Profiling (Zero-Copy Proof)

### Zero-Copy Build (default, uses `Cow<'a, str>`)

```bash
cargo run --release --features dhat-heap -p rts-cli -- run-async \
  --url http://127.0.0.1:8080/v2/stream/recentchange \
  --duration 60s
```

Produces: `dhat-heap.json` in the working directory. Move to `reports/dhat/zero_copy.json`.

### Owned-String Baseline (deliberately allocating)

```bash
cargo run --release --features "dhat-heap,owned-baseline" -p rts-cli -- run-async \
  --url http://127.0.0.1:8080/v2/stream/recentchange \
  --duration 60s
```

Produces: `dhat-heap.json`. Move to `reports/dhat/owned_baseline.json`.

**Compare side-by-side:**
- Open both JSON files in a text editor or dhat's web viewer.
- Look for `total_allocated_bytes` / `num_blocks` to compute bytes/event.
- Expect zero-copy to be **50-100× smaller** for the Wikipedia stream.

## Replay Fixture Recording

To capture a fresh fixture from the live stream:

```bash
cargo run --release -p rts-cli -- replay record \
  --duration 300s \
  --out fixtures/recentchange-300s.ndjson
```

Produces an NDJSON file with envelope `{ "recv_ns": <u64>, "data": "<raw json>" }`. Playback-ready.

## Analyzing Logs

Convert NDJSON logs to CSV for plotting:

```bash
cargo run --release -p rts-cli -- analyse \
  --run reports/runs/async_replay.ndjson \
  --output reports/csv/async_replay.csv
```

Emits: `async_replay.csv` with columns `ts_ns, priority, drift_ns, duration_ns, deadline_miss, …`.

## Plotting

Generate all figures (requires Python + matplotlib + numpy + hdrhistogram):

```bash
python reports/plots/plot.py
```

Outputs PNGs to `reports/plots/`:
- `fig1_drift_cdf.png`
- `fig2_tail_latency.png`
- `fig3_sync_shootout.png`
- `fig4_deadline_miss_timeline.png`
- `fig5_heap_profile.png`
- `fig6_failsafe_timeline.png`
- `fig7_overflow_vs_capacity.png`

## Demo

Run the 5-minute live demo (replay-safe):

```bash
./scripts/demo.sh       # Linux/WSL
.\scripts\demo.ps1      # Windows PowerShell
```

Includes:
- Async pipeline under 10× burst
- Threaded pipeline for comparison
- Synthetic jitter spike → Degraded Mode → recovery
- Watchdog reset trigger
- Final statistics snapshot

## Common Workflows

### Local Smoke Test (5 min)

```bash
# Terminal 1
cargo run --release -p rts-cli -- replay play --fixture fixtures/recentchange-60s.ndjson --rate 10x &

# Terminal 2
cargo run --release -p rts-cli -- run-async --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s
cargo run --release -p rts-cli -- run-threaded --url http://127.0.0.1:8080/v2/stream/recentchange --duration 30s
```

Verify: both produce NDJSON logs with no panics.

### Full Report Regeneration

```bash
make report
```

Runs:
1. Replay server capture
2. Async + threaded 60s runs
3. Bench suite (sync shootout + tail latency)
4. dhat profiles
5. Plot generation
6. Report PDF build (if using LaTeX)

Expect: ~30-40 minutes total.

### Pre-Submission Checklist

```bash
# 1. Format & lint
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings

# 2. Tests
cargo test --workspace --release

# 3. Quick bench sanity
cargo bench -p rts-bench --bench sync_shootout -- --warm-up-time 1

# 4. Git status clean
git status

# 5. Recent commits logged
git log --oneline -5
```

All green? Ready to push.

## Troubleshooting

### "User-Agent header rejected" or 429 errors

The Wikipedia stream rate-limits without a User-Agent. Every HTTP request includes:
```
User-Agent: RTS2601-coursework/0.1 (tahafahd40@gmail.com)
```

If you're still hitting 429s, wait 60s before retrying (exponential backoff is automatic).

### Latency numbers look wrong

Check:
1. Are you using `--release` optimisation? (`--dev` is 10-100× slower).
2. Is the replay server running on `--rate 1x` (wall-clock speed)? (10× speeds up time, inflating apparent latency).
3. Are you on Windows? (run on WSL2/Linux for canonical numbers; Windows adds scheduler noise).

### "0/2 jobs completed" in GitHub Actions

Either:
1. Check `.github/workflows/ci.yml` for YAML syntax errors.
2. Run `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` locally to verify the code is clean.
3. If GitHub CI still hangs, it's likely a free-tier minute cap — check **Settings → Billing and plans → Actions**.

### dhat profile is huge or empty

Confirm:
- Build was compiled with `--release` optimisation.
- The binary ran long enough (60s minimum).
- No `CARGO_INCREMENTAL=1` or debug flags interfering.

If still issues, manually check the dhat JSON:
```bash
cat dhat-heap.json | head -50
```

Should show `"mode": "heap"` and non-zero `"total_allocated_bytes"`.

---

## Architecture Quick Ref

| Crate | Purpose |
|---|---|
| `rts-core` | Shared types, traits, channel primitives, metrics — no runtime-specific code. |
| `rts-async` | Tokio runtime: ingest, parser, biased-select scheduler, leaderboard actor. |
| `rts-threaded` | std::thread runtime: blocking ingest, priority queue, worker pool. |
| `rts-cli` | clap-derive CLI entry; subcommands: `run-async`, `run-threaded`, `replay`, `analyse`, `bench`. |
| `rts-replay` | SSE record/playback; local server for deterministic testing. |
| `rts-bench` | Criterion benches (sync shootout, tail latency). |

## Next Steps

Refer to the **[PLAN.md](.claude/plans/read-the-rts2601-assignmentbrief-v1-docx-vectorized-book.md)** for phase-by-phase guidance, or jump to:
- **P0:** Workspace bootstrap (done — you're reading this).
- **P1:** Async SSE happy path.
- **P2:** Replay infrastructure.
- ... (14 phases total).

---

**Questions?** Check the error message, `git log` for recent changes, or review the **[PLAN.md](.claude/plans/read-the-rts2601-assignmentbrief-v1-docx-vectorized-book.md)** for the phase you're working on.
