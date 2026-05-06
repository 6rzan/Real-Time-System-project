//! Pipeline orchestrator: wires ingest → parser → priority queue → workers
//! → leaderboard thread → metrics.
//!
//! Call [`run`] with a [`PipelineConfig`] to start the full threaded pipeline.
//! The function blocks until the ingest stage finishes (limit / cancel /
//! duration), then signals workers, drains the leaderboard, and prints a
//! shutdown summary to stdout.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rts_core::event::cow_stats;
use rts_core::failsafe::FailSafeController;
use rts_core::metrics::Metrics;
use rts_core::watchdog::WatchdogState;

use crate::ingest::{self, IngestError};
use crate::leaderboard::{LbCmd, Leaderboard};
use crate::scheduler::{self, PriorityQueue};

/// Error returned by [`run`].
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("ingest: {0}")]
    Ingest(#[from] IngestError),
}

/// Configuration for a single threaded pipeline run.
pub struct PipelineConfig {
    /// SSE endpoint (live firehose or local replay server).
    pub url: String,
    /// Stop after this many events.  `None` = run until `cancel` fires.
    pub limit: Option<usize>,
    /// Number of worker threads.
    pub workers: usize,
    /// Capacity of the shared `PriorityQueue`.
    pub capacity: usize,
    /// Set to `true` to signal cancellation from outside (Ctrl-C / timer).
    pub cancel: Arc<AtomicBool>,
    /// If set, dump `<stem>_threaded.csv` and `<stem>_threaded.hgrm` on shutdown.
    pub metrics_path: Option<PathBuf>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        // Reserve ~2 cores for ingest + watchdog (P8).
        let workers = num_cpus::get().saturating_sub(2).max(1);
        Self {
            url: ingest::DEFAULT_URL.to_string(),
            limit: None,
            workers,
            capacity: 256,
            cancel: Arc::new(AtomicBool::new(false)),
            metrics_path: None,
        }
    }
}

/// Run the full threaded pipeline.
///
/// Returns when ingest finishes.  Prints a metrics + leaderboard summary.
#[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
pub fn run(cfg: PipelineConfig) -> Result<(), PipelineError> {
    let queue = PriorityQueue::new(cfg.capacity);
    let metrics = Metrics::new();
    let watchdog = WatchdogState::new();
    let failsafe = FailSafeController::new();

    let (lb_tx, lb_rx) = crossbeam_channel::unbounded::<LbCmd>();

    // Spawn the leaderboard actor thread.
    let lb_handle = std::thread::spawn(move || Leaderboard::new(lb_rx).run());

    // Watchdog checker thread.
    {
        let wd = Arc::clone(&watchdog);
        let cancel = Arc::clone(&cfg.cancel);
        std::thread::spawn(move || {
            rts_core::watchdog::run_sync_checker(
                &wd,
                std::time::Duration::from_secs(5),
                &cancel,
            );
        });
    }

    // Fail-safe ticker thread.
    {
        let fs = Arc::clone(&failsafe);
        let cancel = Arc::clone(&cfg.cancel);
        std::thread::spawn(move || {
            rts_core::failsafe::run_sync_ticker(&fs, &cancel);
        });
    }

    // Spawn worker threads.
    let mut worker_handles = Vec::new();
    for _ in 0..cfg.workers {
        let q = Arc::clone(&queue);
        let lb = lb_tx.clone();
        let m = Arc::clone(&metrics);
        let fs = Arc::clone(&failsafe);
        let c = Arc::clone(&cfg.cancel);
        worker_handles.push(std::thread::spawn(move || {
            scheduler::worker(q, lb, m, fs, c);
        }));
    }

    // Run ingest on the current thread (blocks until done).
    let q2 = Arc::clone(&queue);
    let wd2 = Arc::clone(&watchdog);
    let fs2 = Arc::clone(&failsafe);
    let outcome = ingest::run_sse(
        &cfg.url,
        cfg.limit,
        Arc::clone(&cfg.cancel),
        move |raw| crate::parser::dispatch(raw, &q2, &wd2, &fs2),
    )?;

    tracing::info!(target: "rts.threaded.pipeline", outcome = ?outcome, "ingest finished");

    // Signal cancel and wake all sleeping workers so they can exit.
    cfg.cancel.store(true, Ordering::Relaxed);
    queue.wake_all();

    // Wait for all workers to exit.
    for h in worker_handles {
        h.join().ok();
    }

    // Request a leaderboard snapshot before dropping the last sender.
    let (snap_tx, snap_rx) = crossbeam_channel::bounded::<Vec<(String, u64)>>(1);
    let _ = lb_tx.send(LbCmd::Snapshot(snap_tx));
    drop(lb_tx); // dropping the last sender closes the channel
    lb_handle.join().ok();

    // ── Print leaderboard ────────────────────────────────────────────────────
    println!("\n=== Top Wikis (threaded) ===");
    match snap_rx.recv() {
        Ok(entries) => {
            for (i, (name, count)) in entries.iter().enumerate().take(3) {
                println!("  {}. {} — {} edits", i + 1, name, count);
            }
        }
        Err(_) => println!("  (no data)"),
    }

    // ── Print metrics ────────────────────────────────────────────────────────
    let s = metrics.snapshot();
    println!("\n=== Metrics (nanoseconds) ===");
    println!(
        "Drift  Human  p50={:>10}  p90={:>10}  p99={:>10}  p99.9={:>10}  n={}",
        s.drift_human_p50,
        s.drift_human_p90,
        s.drift_human_p99,
        s.drift_human_p999,
        s.sample_count_human
    );
    println!(
        "Drift  Bot    p50={:>10}  p90={:>10}  p99={:>10}  p99.9={:>10}  n={}",
        s.drift_bot_p50,
        s.drift_bot_p90,
        s.drift_bot_p99,
        s.drift_bot_p999,
        s.sample_count_bot
    );
    println!(
        "Jitter        p50={:>10}  p90={:>10}  p99={:>10}  p99.9={:>10}",
        s.jitter_p50, s.jitter_p90, s.jitter_p99, s.jitter_p999
    );
    println!(
        "Deadline misses: human={}  bot={}",
        s.deadline_miss_human, s.deadline_miss_bot
    );
    println!("Overflow: {}", queue.overflow_count());

    let (borrowed, owned) = cow_stats();
    let total = borrowed + owned;
    let pct = if total > 0 { (borrowed * 100) / total } else { 0 };
    println!("Cow stats: borrowed={borrowed}  owned={owned}  ({pct}% borrowed)");

    // ── Dump metrics files ───────────────────────────────────────────────────
    if let Some(stem) = cfg.metrics_path {
        if let Some(parent) = stem.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let csv_path = stem.with_extension("csv");
        let hgrm_path = stem.with_extension("hgrm");
        match metrics.dump_csv(&csv_path) {
            Ok(()) => println!("Metrics CSV: {}", csv_path.display()),
            Err(e) => tracing::warn!(target: "rts.threaded.pipeline", error = %e, "CSV dump failed"),
        }
        match metrics.dump_hgrm(&hgrm_path) {
            Ok(()) => println!("Metrics HGRM: {}", hgrm_path.display()),
            Err(e) => tracing::warn!(target: "rts.threaded.pipeline", error = %e, "HGRM dump failed"),
        }
    }

    Ok(())
}
