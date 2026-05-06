//! Parser stage: decode raw SSE `data:` payloads into priority-classified tasks.
//!
//! `dispatch` is called synchronously from the ingest sink (on the Tokio
//! thread that owns the SSE stream). It parses, classifies, and pushes into
//! the appropriate `DropOldestRing` lane. On overflow it emits the
//! high-precision `"overflow"` tracing event required by the brief.
//!
//! In Degraded Mode ([`FailSafeController::is_degraded`]) Bot-priority events
//! are dropped before enqueueing to shed load.

use std::sync::Arc;
use std::time::Instant;

use rts_core::channel::ring::{DropOldestRing, PushOutcome};
use rts_core::event::parse_one;
use rts_core::failsafe::FailSafeController;
use rts_core::priority::Priority;
use rts_core::task::Task;
use rts_core::time::now_ns;
use rts_core::watchdog::WatchdogState;

/// Parse `raw`, classify priority, and push into the appropriate lane.
///
/// Skips unparseable events with a `warn` log. Drops Bot events when the
/// fail-safe controller is in Degraded Mode.
pub fn dispatch(
    raw: &str,
    hi: &Arc<DropOldestRing<Task>>,
    lo: &Arc<DropOldestRing<Task>>,
    watchdog: &Arc<WatchdogState>,
    failsafe: &Arc<FailSafeController>,
) {
    let event = match parse_one(raw) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(target: "rts.parser", error = %err, "parse error; skipping event");
            return;
        }
    };

    watchdog.touch();

    let priority = Priority::from_bot_flag(event.bot);

    // Degraded Mode: shed Bot events to protect Human-priority latency.
    if priority == Priority::Bot && failsafe.is_degraded() {
        tracing::debug!(target: "rts.parser", "degraded mode: dropping Bot event");
        return;
    }

    let task = Task {
        event: event.into_owned(),
        priority,
        enqueued_at: Instant::now(),
    };

    let ring = if priority == Priority::Human { hi } else { lo };
    if let PushOutcome::DroppedOldest = ring.push(task) {
        let ts_ns = now_ns();
        let queue_depth = ring.len();
        tracing::warn!(
            target: "overflow",
            ts_ns,
            queue_depth,
            "Overflow Event"
        );
    }
}
