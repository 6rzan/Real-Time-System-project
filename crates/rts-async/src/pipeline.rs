//! Pipeline orchestrator: wires ingest → parser → priority lanes → workers
//! → leaderboard actor → metrics.
//!
//! Call [`run`] with a [`PipelineConfig`] to start the full async pipeline.
//! The function blocks until the ingest stage finishes (limit / cancel /
//! duration), then cancels workers, drains the leaderboard, and prints a
//! shutdown summary to stdout.

use std::path::PathBuf;
use std::sync::Arc;

use rts_core::channel::ring::DropOldestRing;
use rts_core::event::cow_stats;
use rts_core::failsafe::FailSafeController;
use rts_core::metrics::Metrics;
use rts_core::task::Task;
use rts_core::watchdog::WatchdogState;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ingest::IngestError;
use crate::leaderboard::{LbCmd, Leaderboard};
use crate::scheduler;

/// Error returned by [`run`].
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("ingest: {0}")]
    Ingest(#[from] IngestError),
}

/// Configuration for a single pipeline run.
pub struct PipelineConfig {
    /// SSE endpoint (live firehose or local replay server).
    pub url: String,
    /// Stop after this many events. `None` means run until `cancel` fires.
    pub limit: Option<usize>,
    /// Number of worker tasks (default: logical CPU count).
    pub workers: usize,
    /// Capacity of each priority lane's `DropOldestRing`.
    pub capacity: usize,
    /// External cancellation token (Ctrl-C / duration timer).
    pub cancel: CancellationToken,
    /// If set, dump `<stem>_async.csv` and `<stem>_async.hgrm` on shutdown.
    pub metrics_path: Option<PathBuf>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            url: crate::ingest::DEFAULT_URL.to_string(),
            limit: None,
            workers: num_cpus::get(),
            capacity: 256,
            cancel: CancellationToken::new(),
            metrics_path: None,
        }
    }
}

/// Run the full async pipeline.
///
/// Returns when ingest finishes. Prints a metrics + leaderboard summary.
#[allow(clippy::too_many_lines)]
pub async fn run(cfg: PipelineConfig) -> Result<(), PipelineError> {
    let hi = DropOldestRing::<Task>::new(cfg.capacity);
    let lo = DropOldestRing::<Task>::new(cfg.capacity);
    let metrics = Metrics::new();
    let watchdog = WatchdogState::new();
    let failsafe = FailSafeController::new();

    let (lb_tx, lb_rx) = mpsc::channel::<LbCmd>(1024);
    let lb_handle = tokio::spawn(Leaderboard::new(lb_rx).run());

    // Watchdog checker: warns every 5 s when no events arrive.
    {
        let wd = Arc::clone(&watchdog);
        let cancel = cfg.cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                        if wd.is_stale() {
                            tracing::warn!(
                                target: "rts.watchdog",
                                "no events received for >10 s — possible upstream stall"
                            );
                        }
                    }
                }
            }
        });
    }

    // Fail-safe ticker: advances the rolling jitter window every second.
    {
        let fs = Arc::clone(&failsafe);
        let cancel = cfg.cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => fs.tick(),
                }
            }
        });
    }

    // Spawn worker pool.
    for _ in 0..cfg.workers {
        let hi2 = Arc::clone(&hi);
        let lo2 = Arc::clone(&lo);
        let lb2 = lb_tx.clone();
        let m2 = Arc::clone(&metrics);
        let fs2 = Arc::clone(&failsafe);
        let c2 = cfg.cancel.clone();
        tokio::spawn(async move {
            scheduler::worker(hi2, lo2, lb2, m2, fs2, c2).await;
        });
    }

    // Run ingest (blocks until limit / cancel).
    let hi2 = Arc::clone(&hi);
    let lo2 = Arc::clone(&lo);
    let wd2 = Arc::clone(&watchdog);
    let fs2 = Arc::clone(&failsafe);
    let outcome = crate::ingest::run_sse(&cfg.url, cfg.limit, cfg.cancel.clone(), move |raw| {
        crate::parser::dispatch(raw, &hi2, &lo2, &wd2, &fs2);
    })
    .await?;

    tracing::info!(target: "rts.pipeline", outcome = ?outcome, "ingest finished");

    // Signal workers to exit, then request a leaderboard snapshot before the
    // actor sees all senders dropped and exits its loop.
    cfg.cancel.cancel();

    let (snap_tx, snap_rx) = tokio::sync::oneshot::channel();
    let _ = lb_tx.send(LbCmd::Snapshot(snap_tx)).await;
    drop(lb_tx);

    lb_handle.await.ok();

    // Print leaderboard.
    println!("\n=== Top Wikis ===");
    match snap_rx.await {
        Ok(entries) => {
            for (i, (name, count)) in entries.iter().enumerate().take(3) {
                println!("  {}. {} — {} edits", i + 1, name, count);
            }
        }
        Err(_) => println!("  (no data)"),
    }

    // Print metrics snapshot.
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
        s.drift_bot_p50, s.drift_bot_p90, s.drift_bot_p99, s.drift_bot_p999, s.sample_count_bot
    );
    println!(
        "Jitter        p50={:>10}  p90={:>10}  p99={:>10}  p99.9={:>10}",
        s.jitter_p50, s.jitter_p90, s.jitter_p99, s.jitter_p999
    );
    println!(
        "Deadline misses: human={}  bot={}",
        s.deadline_miss_human, s.deadline_miss_bot
    );
    println!(
        "Overflows: hi={}  lo={}",
        hi.overflow_count(),
        lo.overflow_count()
    );

    let (borrowed, owned) = cow_stats();
    let total = borrowed + owned;
    let pct = (borrowed * 100).checked_div(total).unwrap_or(0);
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
            Err(e) => tracing::warn!(target: "rts.pipeline", error = %e, "CSV dump failed"),
        }
        match metrics.dump_hgrm(&hgrm_path) {
            Ok(()) => println!("Metrics HGRM: {}", hgrm_path.display()),
            Err(e) => tracing::warn!(target: "rts.pipeline", error = %e, "HGRM dump failed"),
        }
    }

    Ok(())
}
