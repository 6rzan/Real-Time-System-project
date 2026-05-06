#!/usr/bin/env bash
# P13 Demo — 5-minute live walkthrough of the RTS2601 pipeline.
#
# Runs entirely against the local replay server (no live network required).
# Steps:
#   1. Build release binary (skipped with --skip-build).
#   2. Start replay server (10×) in background.
#   3. Run async pipeline for 60 s with live metric tail.
#   4. Inject CPU stress for 30 s → Degraded Mode → recovery.
#   5. Kill replay server → watchdog stale-stream warning fires.
#   6. Print final snapshot (top-3 leaderboard + percentiles).
#
# Usage:
#   ./scripts/demo.sh [--skip-build] [--fixture <path>] [--port <n>]

set -euo pipefail

FIXTURE="fixtures/recentchange-60s.ndjson"
PORT=8080
SKIP_BUILD=0
REPORTS="reports"

# ── Argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=1 ;;
        --fixture)    FIXTURE="$2"; shift ;;
        --port)       PORT="$2";    shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
    shift
done

URL="http://127.0.0.1:${PORT}/v2/stream/recentchange"
BIN="./target/release/rts"

# ── Helpers ───────────────────────────────────────────────────────────────────

banner() { echo ""; echo "$(printf '=%.0s' {1..60})"; echo "  $*"; echo "$(printf '=%.0s' {1..60})"; }
step()   { echo ""; echo "[STEP] $*"; }
info()   { echo "  $*"; }
ok()     { echo "  [OK] $*"; }
warn()   { echo "  [WARN] $*"; }

cleanup() {
    if [[ -n "${REPLAY_PID:-}" ]]; then
        kill "$REPLAY_PID" 2>/dev/null || true
    fi
    if [[ -n "${THREADED_PID:-}" ]]; then
        kill "$THREADED_PID" 2>/dev/null || true
    fi
    if [[ -n "${WATCHDOG_PID:-}" ]]; then
        kill "$WATCHDOG_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Step 0: Prerequisites ─────────────────────────────────────────────────────

banner "RTS2601 Pipeline Demo (P13)"
info "Fixture : $FIXTURE"
info "Port    : $PORT"
info "URL     : $URL"

[[ -f "$FIXTURE" ]] || { echo "ERROR: fixture not found: $FIXTURE" >&2; exit 1; }

mkdir -p "$REPORTS/runs" "$REPORTS/csv" "$REPORTS/plots"

# ── Step 1: Build ─────────────────────────────────────────────────────────────

if [[ $SKIP_BUILD -eq 0 ]]; then
    step "Building release binary"
    cargo build --workspace --release
    ok "Build succeeded"
else
    ok "Skipping build (--skip-build)"
fi

# ── Step 2: Start replay server ───────────────────────────────────────────────

step "Starting replay server (10×) on port $PORT"
"$BIN" replay play --fixture "$FIXTURE" --rate 10x --port "$PORT" &
REPLAY_PID=$!
sleep 2
ok "Replay server running (PID $REPLAY_PID)"

# ── Step 3: Async pipeline (60 s) ─────────────────────────────────────────────

step "Running ASYNC pipeline for 60 s"
info "Logs    → $REPORTS/runs/demo_async.ndjson"
info "Metrics → $REPORTS/csv/demo_async"
info "(watch stderr for Degraded / watchdog events)"

"$BIN" run-async \
    --url "$URL" \
    --duration 60s \
    --log-path "$REPORTS/runs/demo_async.ndjson" \
    --metrics-path "$REPORTS/csv/demo_async"

ok "Async pipeline finished"

# ── Step 4: Inject jitter burst + show Degraded Mode ─────────────────────────

step "Injecting CPU stress (30 s) to drive Degraded Mode"
info "Stress spawns spin-loop threads → p99 jitter spikes > 1.5 ms"
info "Running threaded pipeline concurrently with stress..."

"$BIN" run-threaded \
    --url "$URL" \
    --duration 50s \
    --metrics-path "$REPORTS/csv/demo_threaded" &
THREADED_PID=$!

sleep 2

"$BIN" stress --duration 30s
ok "Stress burst complete — check stderr for Degraded Mode ON/OFF transitions"

wait "$THREADED_PID" 2>/dev/null || true
unset THREADED_PID
ok "Threaded pipeline finished"

# ── Step 5: Kill replay server → watchdog fires ───────────────────────────────

step "Killing replay server to trigger watchdog stale-stream warning"
info "Pipeline waits >10 s then logs: 'no events received for >10 s'"

"$BIN" run-async --url "$URL" --duration 20s &
WATCHDOG_PID=$!

sleep 2

kill "$REPLAY_PID" 2>/dev/null || true
unset REPLAY_PID
warn "Replay server killed — watchdog should fire after 10 s of silence"

sleep 12
wait "$WATCHDOG_PID" 2>/dev/null || true
unset WATCHDOG_PID
ok "Watchdog demo complete"

# ── Step 6: Final snapshot ────────────────────────────────────────────────────

banner "Demo complete — final artefacts"

for f in \
    "$REPORTS/runs/demo_async.ndjson" \
    "$REPORTS/csv/demo_async.csv" \
    "$REPORTS/csv/demo_threaded.csv"
do
    [[ -f "$f" ]] && info "  $f" || info "  (missing) $f"
done

echo ""
ok "Run 'python scripts/plot_latency.py'  to generate latency plots."
ok "Run 'python scripts/plot_shootout.py' to generate shootout plots."
echo ""
