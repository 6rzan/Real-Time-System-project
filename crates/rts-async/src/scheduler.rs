//! Biased-priority worker: drains the human lane before the bot lane.
//!
//! `tokio::select! { biased; … }` is static-priority pre-emption, not EDF —
//! the human channel is always polled first. This is equivalent to SP-PE
//! (Static-Priority Pre-Emptive) scheduling, discussed in the report.
//!
//! The 2 ms deadline matches the brief's "high-pressure real-time" threshold.
//! Drift = `actual_start − enqueued_at` (per invariant 8 in the plan).

use std::sync::Arc;
use std::time::Instant;

use rts_core::channel::ring::DropOldestRing;
use rts_core::metrics::Metrics;
use rts_core::task::Task;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::leaderboard::LbCmd;

/// 2 ms real-time deadline in nanoseconds.
const DEADLINE_NS: u128 = 2_000_000;

/// Async worker task.
///
/// Loops with a biased `select!`: human-priority items preempt bot items.
/// Exits cleanly when `cancel` fires.
pub async fn worker(
    hi: Arc<DropOldestRing<Task>>,
    lo: Arc<DropOldestRing<Task>>,
    lb_tx: mpsc::Sender<LbCmd>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    loop {
        let task = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            t = hi.pop_async() => t,
            t = lo.pop_async() => t,
        };

        let actual_start = Instant::now();
        let drift = actual_start.saturating_duration_since(task.enqueued_at);
        metrics.record_drift(task.priority, drift);

        let server_name = task.event.server_name.clone();
        let priority = task.priority;

        // Non-blocking: a saturated leaderboard actor must never stall workers.
        let _ = lb_tx.try_send(LbCmd::Update(server_name));

        let total = actual_start.elapsed();
        let drift_ns = u64::try_from(drift.as_nanos()).unwrap_or(u64::MAX);
        let duration_ns = u64::try_from(total.as_nanos()).unwrap_or(u64::MAX);
        metrics.record_jitter(total);
        let deadline_miss = total.as_nanos() > DEADLINE_NS;
        if deadline_miss {
            metrics.record_deadline_miss(priority);
        }

        tracing::info!(
            target: "rts.worker",
            priority = ?priority,
            drift_ns,
            duration_ns,
            deadline_miss,
        );
    }
}
