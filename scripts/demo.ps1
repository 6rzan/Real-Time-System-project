#!/usr/bin/env pwsh
<#
.SYNOPSIS
    P13 Demo — 5-minute live walkthrough of the RTS2601 pipeline.

.DESCRIPTION
    Runs entirely against the local replay server (no live network required).
    Steps:
      1. Build release binary (skipped if already built).
      2. Start replay server (10x) in background.
      3. Run async pipeline for 60 s with live metric tail.
      4. Inject CPU stress for 30 s → Degraded Mode → recovery.
      5. Kill replay server → watchdog stale-stream warning fires.
      6. Print final snapshot (top-3 leaderboard + percentiles).

.EXAMPLE
    .\scripts\demo.ps1
    .\scripts\demo.ps1 -SkipBuild
#>

param(
    [switch]$SkipBuild,
    [string]$Fixture = "fixtures/recentchange-60s.ndjson",
    [int]   $Port    = 8080
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RTS  = "cargo"
$BIN  = "target\release\rts.exe"
$URL  = "http://127.0.0.1:$Port/v2/stream/recentchange"
$REPORTS = "reports"

# ── Helpers ──────────────────────────────────────────────────────────────────

function Banner([string]$msg) {
    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host ("=" * 60) -ForegroundColor Cyan
}

function Step([string]$msg) {
    Write-Host ""
    Write-Host "[STEP] $msg" -ForegroundColor Yellow
}

function Info([string]$msg) {
    Write-Host "  $msg" -ForegroundColor Gray
}

function Ok([string]$msg) {
    Write-Host "  [OK] $msg" -ForegroundColor Green
}

function Warn([string]$msg) {
    Write-Host "  [WARN] $msg" -ForegroundColor Magenta
}

# ── Step 0: Prerequisites ─────────────────────────────────────────────────────

Banner "RTS2601 Pipeline Demo (P13)"
Info "Fixture : $Fixture"
Info "Port    : $Port"
Info "URL     : $URL"

if (-not (Test-Path $Fixture)) {
    Write-Error "Fixture not found: $Fixture  (run 'cargo run -p rts-cli -- replay record ...' first)"
}

# ── Step 1: Build ─────────────────────────────────────────────────────────────

if (-not $SkipBuild) {
    Step "Building release binary"
    & $RTS build --workspace --release
    if ($LASTEXITCODE -ne 0) { Write-Error "Build failed" }
    Ok "Build succeeded"
} else {
    Ok "Skipping build (-SkipBuild)"
}

# Ensure reports dirs exist.
New-Item -ItemType Directory -Force -Path "$REPORTS\runs"   | Out-Null
New-Item -ItemType Directory -Force -Path "$REPORTS\csv"    | Out-Null
New-Item -ItemType Directory -Force -Path "$REPORTS\plots"  | Out-Null

# ── Step 2: Start replay server ───────────────────────────────────────────────

Step "Starting replay server (10x) on port $Port"
$replayJob = Start-Job -ScriptBlock {
    param($bin, $fixture, $port)
    & $bin replay play --fixture $fixture --rate 10x --port $port
} -ArgumentList (Resolve-Path $BIN).Path, $Fixture, $Port

Start-Sleep -Seconds 2  # give the server time to bind
Ok "Replay server running (job id $($replayJob.Id))"

# ── Step 3: Async pipeline (60 s) ─────────────────────────────────────────────

Step "Running ASYNC pipeline for 60 s"
Info "Logs → $REPORTS\runs\demo_async.ndjson"
Info "Metrics → $REPORTS\csv\demo_async"
Info "(watch stderr for Degraded / watchdog events)"

& $BIN run-async `
    --url $URL `
    --duration 60s `
    --log-path "$REPORTS\runs\demo_async.ndjson" `
    --metrics-path "$REPORTS\csv\demo_async"

Ok "Async pipeline finished"

# ── Step 4: Inject jitter burst + show Degraded Mode ─────────────────────────

Step "Injecting CPU stress (30 s) to drive Degraded Mode"
Info "Stress spawns spin-loop threads → p99 jitter spikes > 1.5 ms"
Info "Pipeline must be running concurrently for this to have effect."
Info "Running threaded pipeline + stress in parallel for demo..."

# Start threaded pipeline in background.
$threadedJob = Start-Job -ScriptBlock {
    param($bin, $url)
    & $bin run-threaded --url $url --duration 50s `
        --metrics-path "reports/csv/demo_threaded"
} -ArgumentList (Resolve-Path $BIN).Path, $URL

Start-Sleep -Seconds 2

# Run stress concurrently.
& $BIN stress --duration 30s
Ok "Stress burst complete"

# Wait for threaded pipeline to finish.
Receive-Job $threadedJob -Wait | Out-Null
Remove-Job $threadedJob -Force
Ok "Threaded pipeline finished"

# ── Step 5: Kill replay server → watchdog fires ───────────────────────────────

Step "Killing replay server to trigger watchdog stale-stream warning"
Info "Pipeline waits 10 s then logs: 'no events received for >10 s'"
Info "Starting short async run to demonstrate watchdog..."

$watchdogJob = Start-Job -ScriptBlock {
    param($bin, $url)
    & $bin run-async --url $url --duration 20s
} -ArgumentList (Resolve-Path $BIN).Path, $URL

Start-Sleep -Seconds 2

# Kill the replay server.
Stop-Job  $replayJob -ErrorAction SilentlyContinue
Remove-Job $replayJob -Force -ErrorAction SilentlyContinue
Warn "Replay server killed — watchdog should fire after 10 s of silence"

Start-Sleep -Seconds 12
Receive-Job $watchdogJob -Wait | Out-Null
Remove-Job $watchdogJob -Force
Ok "Watchdog demo complete"

# ── Step 6: Final snapshot ────────────────────────────────────────────────────

Banner "Demo complete — final artefacts"

if (Test-Path "$REPORTS\csv\demo_async.csv") {
    Info "Async metrics CSV:"
    Get-Content "$REPORTS\csv\demo_async.csv" | Select-Object -First 6 | ForEach-Object { Info "  $_" }
}

Info ""
Info "All artefacts:"
Get-ChildItem "$REPORTS\runs\demo_async.ndjson",
              "$REPORTS\csv\demo_async.csv",
              "$REPORTS\csv\demo_threaded.csv" -ErrorAction SilentlyContinue |
    ForEach-Object { Info ("  " + $_.FullName) }

Info ""
Ok "Run 'python scripts/plot_latency.py' to generate latency plots."
Ok "Run 'python scripts/plot_shootout.py' to generate shootout plots."
Write-Host ""
