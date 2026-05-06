# RTS2601 — Real-Time Wikipedia SSE Pipeline

A high-pressure real-time data ingestion + analytics pipeline in **Rust** consuming the live Wikipedia *Recent Changes* SSE firehose
(`https://stream.wikimedia.org/v2/stream/recentchange`). Built for the **CT087-3-3-RTS** assignment.

The system implements the same pipeline twice — once with **Tokio async/await**, once with **`std::thread`** — and benchmarks them
head-to-head on tail latency, scheduling drift, jitter, and synchronisation throughput.

---

## Table of Contents

1. [Features](#features)
2. [Prerequisites — install these first](#prerequisites)
3. [Getting the code onto your machine](#getting-the-code)
4. [Building the project](#building)
5. [Running the tests](#tests)
6. [Running the pipeline (Windows PowerShell)](#running-on-windows)
7. [Running the pipeline (WSL / Linux)](#running-on-wsl)
8. [Running the benchmarks](#benchmarks)
9. [Heap profiling (zero-copy proof)](#heap-profiling)
10. [Generating plots](#plots)
11. [Running the demo script](#demo)
12. [Workspace layout](#workspace-layout)
13. [Troubleshooting](#troubleshooting)

---

## Features

- **Dual-runtime pipelines**: Tokio (`rts-async`) and `std::thread` (`rts-threaded`) sharing the same core types and metrics.
- **Zero-copy parsing**: `serde` with `Cow<'a, str>` lifetimes; allocations only at the priority-queue boundary. Heap profile vs. an owned-string baseline proves the saving via `dhat-rs`.
- **Priority scheduling**: `bot:true` events are low priority; human edits preempt via biased `tokio::select!` (async) or a `BinaryHeap<(Priority, Instant)>` (threaded).
- **Drop-oldest backpressure**: bounded ring with a high-precision timestamped *Overflow Event* per drop.
- **Watchdog**: triggers a network reset after 10 s of silence.
- **Fail-Safe / Degraded Mode**: sliding-window p99 jitter trip with hysteresis recovery.
- **Sync-primitive shootout**: Criterion benches comparing `parking_lot::Mutex`, `std::sync::Mutex`, `RwLock`, `DashMap`, and cache-padded atomics across 1–16 threads.
- **Statistical rigour**: HdrHistogram p50/p90/p99/p99.9 with sample sizes and 95% CIs.

---

## Prerequisites

Install every item in this section before touching any code. Do them in order.

### 1. Rust toolchain

Open **PowerShell** (Windows) or a terminal (Linux/WSL) and run:

```powershell
# Windows PowerShell / WSL both:
winget install Rustlang.Rustup          # Windows only — skip on WSL/Linux
# OR visit https://rustup.rs and run the installer shown there
```

After the installer finishes, **close and reopen your terminal**, then verify:

```powershell
rustc --version     # should print  rustc 1.94.x  or newer
cargo --version     # should print  cargo 1.94.x  or newer
```

If `rustc` is not found after reopening, add `%USERPROFILE%\.cargo\bin` to your Windows PATH
(Settings → System → Advanced system settings → Environment Variables → Path → New).

### 1b. MSVC Build Tools (Windows — required for the linker)

Rust on Windows needs the Microsoft C++ linker (`link.exe`). VS Code is **not** enough — you need the actual build tools.

**Option A (recommended — smaller download, ~5 GB):**

1. Go to: https://visualstudio.microsoft.com/visual-cpp-build-tools/
2. Click **Download Build Tools**
3. Run the installer
4. In the installer, check **"Desktop development with C++"** then click Install
5. Wait for it to finish (~5–10 min), then **reboot**

**Option B — via winget:**

```powershell
winget install Microsoft.VisualStudio.2022.BuildTools
# After install completes, reboot, then verify:
where link.exe    # should print a path like C:\Program Files (x86)\Microsoft Visual Studio\...
```

After rebooting, run `cargo build --workspace --release` again — the linker error will be gone.

### 2. Git

```powershell
winget install Git.Git      # Windows
# WSL/Ubuntu:
sudo apt update && sudo apt install git -y
```

Verify: `git --version`

### 3. Python 3.9+ (for plots only)

```powershell
winget install Python.Python.3.12    # Windows
# WSL/Ubuntu:
sudo apt install python3 python3-pip -y
```

Then install the plotting libraries:

```powershell
pip install matplotlib numpy hdrh     # same command on Windows and WSL
```

### 4. WSL 2 (Windows only — needed for canonical benchmarks)

WSL is optional for running the pipeline but **required** if you want clean benchmark numbers (Windows adds scheduler noise). Skip this if you only want to run the code on Windows.

Open PowerShell **as Administrator**:

```powershell
wsl --install           # installs WSL2 + Ubuntu automatically
# Reboot when prompted
```

After rebooting, open the **Ubuntu** app from the Start menu, create your Linux username/password, then install Rust inside WSL exactly as above:

```bash
# Inside WSL Ubuntu terminal:
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Press 1 (default install), then:
source "$HOME/.cargo/env"
rustc --version
```

---

## Getting the Code

### If you already have the folder on your desktop (most likely)

Open PowerShell and navigate to it:

```powershell
cd "$env:USERPROFILE\Desktop\UNI\Real Time System"
```

Verify you're in the right place:

```powershell
ls Cargo.toml     # should print the file — you're good
```

### If you need to clone from GitHub

```powershell
cd "$env:USERPROFILE\Desktop\UNI"
git clone <your-repo-url> "Real Time System"
cd "Real Time System"
```

### Opening the same folder in WSL

WSL can access your Windows files under `/mnt/c/`. Your project is at:

```bash
# Inside WSL terminal:
cd "/mnt/c/Users/$USER/Desktop/UNI/Real Time System"
# OR use your Windows username explicitly:
cd "/mnt/c/Users/tahaf/Desktop/UNI/Real Time System"
```

---

## Building

Run this once to compile everything. It takes 2–5 minutes the first time (downloading + compiling ~80 dependencies). Subsequent builds are seconds.

```powershell
# Windows PowerShell  OR  WSL terminal — same command:
cargo build --workspace --release
```

You should see a long list of `Compiling ...` lines ending with:

```
Finished `release` profile [optimized] target(s) in Xs
```

If you see any red `error[...]` lines, jump to [Troubleshooting](#troubleshooting).

**What `--release` means:** without it, the binary is 10–100× slower and benchmark numbers will be wrong. Always use `--release` for anything other than iterating on code.

---

## Tests

Run the full test suite to verify everything is wired up correctly:

```powershell
cargo test --workspace --release
```

Expected output ends with something like:

```
test result: ok. 42 passed; 0 failed; 0 ignored
```

Also check code style (CI will fail if these fail):

```powershell
cargo fmt --check                                      # formatting
cargo clippy --workspace --all-targets -- -D warnings  # linting
```

---

## Running on Windows

All commands below are run from inside the project folder in **PowerShell**:

```powershell
cd "$env:USERPROFILE\Desktop\UNI\Real Time System"
```

### Option A — Live Wikipedia stream (requires internet)

This connects to the real Wikimedia SSE endpoint and processes events for 60 seconds.

**Async pipeline:**
```powershell
cargo run --release -p rts-cli -- run-async --duration 60s
```

**Threaded pipeline:**
```powershell
cargo run --release -p rts-cli -- run-threaded --duration 60s
```

You will see JSON log lines printed to the terminal. Press `Ctrl+C` to stop early.

### Option B — Local replay (recommended — no internet needed)

This uses the pre-recorded fixture file (`fixtures/recentchange-60s.ndjson`) so results are deterministic and network problems cannot affect you.

**Step 1 — Open a first PowerShell window and start the replay server:**

```powershell
cd "$env:USERPROFILE\Desktop\UNI\Real Time System"
cargo run --release -p rts-cli -- replay play `
    --fixture fixtures/recentchange-60s.ndjson `
    --rate 10x `
    --port 8080
```

Leave this window running. It will print `Replay server listening on 127.0.0.1:8080`.

**Step 2 — Open a second PowerShell window and run the pipeline:**

```powershell
cd "$env:USERPROFILE\Desktop\UNI\Real Time System"

# Async:
cargo run --release -p rts-cli -- run-async `
    --url http://127.0.0.1:8080/v2/stream/recentchange `
    --duration 60s `
    --log-path reports/runs/async_replay.ndjson

# Threaded (run after async finishes):
cargo run --release -p rts-cli -- run-threaded `
    --url http://127.0.0.1:8080/v2/stream/recentchange `
    --duration 60s `
    --log-path reports/runs/threaded_replay.ndjson
```

When done, go back to the first window and press `Ctrl+C` to stop the replay server.

### Saving logs to a file

Add `--log-path` to write the NDJSON event log to disk (used later for plots):

```powershell
cargo run --release -p rts-cli -- run-async `
    --url http://127.0.0.1:8080/v2/stream/recentchange `
    --duration 60s `
    --log-path reports/runs/async_run1.ndjson `
    --metrics-path reports/csv/async_run1
```

---

## Running on WSL

Open the **Ubuntu** (WSL) terminal. Navigate to the project:

```bash
cd "/mnt/c/Users/tahaf/Desktop/UNI/Real Time System"
```

Build (first time only — WSL has its own `target/` cache):

```bash
cargo build --workspace --release
```

### Replay server + pipeline (two terminals)

**WSL terminal 1 — replay server:**
```bash
cargo run --release -p rts-cli -- replay play \
    --fixture fixtures/recentchange-60s.ndjson \
    --rate 10x \
    --port 8080
```

**WSL terminal 2 — pipeline:**
```bash
cargo run --release -p rts-cli -- run-async \
    --url http://127.0.0.1:8080/v2/stream/recentchange \
    --duration 60s \
    --log-path reports/runs/async_wsl.ndjson

cargo run --release -p rts-cli -- run-threaded \
    --url http://127.0.0.1:8080/v2/stream/recentchange \
    --duration 60s \
    --log-path reports/runs/threaded_wsl.ndjson
```

**Why WSL for benchmarks?** The Windows NT scheduler adds ~50–200 µs of noise per context switch. WSL2 runs a real Linux kernel, so benchmark numbers are more stable and match what the report's methodology section describes.

---

## Benchmarks

Benchmarks take **10–15 minutes** each. Run them in WSL for canonical numbers.

### Sync-primitive shootout (5 variants × 5 thread counts)

```bash
cargo bench -p rts-bench --bench sync_shootout
```

Results go to:
- `target/criterion/` — HTML report (open `target/criterion/std_mutex/report/index.html` in a browser)
- `reports/csv/sync_shootout.csv` — raw numbers for the plots

### Tail-latency comparison (async vs. threaded, 3 batch sizes)

```bash
cargo bench -p rts-bench --bench tail_latency
```

Results go to:
- `target/criterion/` — HTML report
- `reports/csv/tail_latency.csv` — p50/p90/p99/p99.9 per runtime × batch size

### Quick sanity check (30 seconds instead of 15 minutes)

```bash
cargo bench -p rts-bench --bench sync_shootout -- --warm-up-time 1 --sample-size 10
```

---

## Heap Profiling

This proves the zero-copy saving by comparing two builds: one with `Cow<'a, str>` (default) and one that forces `.to_string()` on every field (baseline).

**Make sure the replay server is running in another terminal first** (see [Option B](#option-b----local-replay-recommended----no-internet-needed) above).

### Run 1 — Zero-copy build:

```powershell
# Windows PowerShell:
cargo run --release --features dhat-heap -p rts-cli -- run-async `
    --url http://127.0.0.1:8080/v2/stream/recentchange `
    --duration 60s
```

This produces `dhat-heap.json` in the project root. Move it:

```powershell
Move-Item dhat-heap.json reports/dhat/zero_copy.json
```

### Run 2 — Owned-string baseline:

```powershell
cargo run --release --features "dhat-heap,owned-baseline" -p rts-cli -- run-async `
    --url http://127.0.0.1:8080/v2/stream/recentchange `
    --duration 60s
Move-Item dhat-heap.json reports/dhat/owned_baseline.json
```

### Viewing the results

Open both JSON files in the [dhat viewer](https://nnethercote.github.io/dh_view/dh_view.html) (drag and drop). Compare `total_bytes` — zero-copy should be ~60× smaller.

---

## Plots

**Prerequisites:** Python 3 + matplotlib + numpy + hdrh installed (see [Prerequisites](#prerequisites)).

Make sure you have run the pipeline and benchmarks first so the CSV files exist under `reports/csv/`.

```powershell
# Windows PowerShell:
python scripts/plot_latency.py
python scripts/plot_shootout.py
```

```bash
# WSL:
python3 scripts/plot_latency.py
python3 scripts/plot_shootout.py
```

Output PNGs land in `reports/plots/`.

---

## Demo

The demo script runs the entire pipeline end-to-end in ~5 minutes using only the local replay server (no internet needed). It shows:

1. Async pipeline running under 10× burst load
2. Threaded pipeline for comparison
3. CPU stress injection → Degraded Mode activates → recovery
4. Replay server killed → watchdog reset fires
5. Final stats snapshot

### Windows (PowerShell):

```powershell
cd "$env:USERPROFILE\Desktop\UNI\Real Time System"
.\scripts\demo.ps1
```

If you already built the binary and want to skip the compile step:

```powershell
.\scripts\demo.ps1 -SkipBuild
```

### WSL / Linux:

```bash
cd "/mnt/c/Users/tahaf/Desktop/UNI/Real Time System"
chmod +x scripts/demo.sh
./scripts/demo.sh
# or skip build:
./scripts/demo.sh --skip-build
```

---

## Workspace Layout

```
Cargo.toml                     # workspace root — lists all 6 member crates
rust-toolchain.toml            # pins Rust stable channel
.cargo/config.toml             # release profile: lto=thin, opt-level=3
scripts/
  demo.ps1                     # Windows demo script
  demo.sh                      # WSL/Linux demo script
  plot_latency.py              # Python: tail-latency CDF plot
  plot_shootout.py             # Python: sync-shootout throughput plot
  analyse_bench.py             # Python: NDJSON log → CSV post-processor
crates/
  rts-core/                    # shared types, Cow parser, DropOldestRing,
  │                            #   HdrHistogram metrics, FailSafeController
  rts-async/                   # Tokio: ingest, biased-select scheduler,
  │                            #   leaderboard actor, pipeline orchestrator
  rts-threaded/                # std::thread: blocking ingest, BinaryHeap
  │                            #   priority queue, worker pool
  rts-bench/                   # Criterion harnesses
  │  benches/sync_shootout.rs  #   5 primitives × 5 thread counts
  │  benches/tail_latency.rs   #   async vs threaded drift comparison
  rts-cli/                     # clap entry point — all subcommands live here
  rts-replay/                  # axum SSE server for fixture replay
fixtures/
  recentchange-60s.ndjson      # 3 078 real Wikipedia events (pre-recorded)
reports/
  runs/                        # per-run NDJSON event logs
  csv/                         # post-processed percentile CSVs
  plots/                       # PNG figures
  dhat/                        # heap profile JSON dumps
docs/research-report/
  report.md                    # full written report
```

---

## Troubleshooting

### `cargo: command not found`

Rust is not on your PATH. Fix:
- **Windows:** close and reopen PowerShell, or add `%USERPROFILE%\.cargo\bin` to PATH manually.
- **WSL:** run `source "$HOME/.cargo/env"` then try again.

### `error[E0XXX]` compile errors

Run `cargo update` then `cargo build --workspace --release` again. If the error mentions a specific crate, check the error message — it usually says exactly which line is wrong.

### Port 8080 already in use

```powershell
# Windows — find and kill whatever is using 8080:
netstat -ano | findstr :8080
taskkill /PID <pid-from-above> /F

# WSL:
lsof -i :8080
kill <pid>
```

Or use a different port: add `--port 9090` to both the replay server and `--url http://127.0.0.1:9090/...` to the pipeline.

### Pipeline exits immediately with `connection refused`

The replay server is not running. Start it in a **separate terminal** first (see [Option B](#option-b----local-replay-recommended----no-internet-needed)).

### Benchmark numbers look very slow (>10× expected)

- Are you using `--release`? Without it the binary is in debug mode.
- Are you on Windows? Run in WSL for stable numbers.
- Is antivirus scanning `target/`? Add the project folder to Windows Defender exclusions.

### `dhat-heap.json` not produced

Ensure you passed `--features dhat-heap` **and** the binary ran for at least 10 seconds. The profile is written on clean exit — don't kill with `Ctrl+C` or the file won't flush.

### Python plots fail with `ModuleNotFoundError`

```powershell
pip install matplotlib numpy hdrh
```

On WSL use `pip3` if `pip` points to Python 2.

### GitHub Actions CI showing 0/2 jobs

Run locally to find the problem:
```powershell
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --release
```
Fix whatever fails, commit, and push again.

---

## License

MIT OR Apache-2.0.
