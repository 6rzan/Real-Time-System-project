//! Parse a raw SSE payload and enqueue the resulting [`Task`].

use std::sync::Arc;
use std::time::Instant;

use rts_core::channel::ring::PushOutcome;
use rts_core::event::parse_one;
use rts_core::priority::Priority;
use rts_core::task::Task;
use rts_core::time::now_ns;

use crate::scheduler::PriorityQueue;

/// Parse `raw`, classify by priority, and push the resulting `Task` into
/// `queue`.  On overflow emits a `tracing::warn` to the `overflow` target.
pub fn dispatch(raw: &str, queue: &Arc<PriorityQueue>) {
    let event = match parse_one(raw) {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(
                target: "rts.threaded.parser",
                error = %err,
                "parse error; skipping event",
            );
            return;
        }
    };

    let priority = Priority::from_bot_flag(event.bot);
    let task = Task {
        event: event.into_owned(),
        priority,
        enqueued_at: Instant::now(),
    };

    if let PushOutcome::DroppedOldest = queue.push(task) {
        let ts_ns = now_ns();
        let queue_depth = queue.len();
        tracing::warn!(
            target: "overflow",
            ts_ns,
            queue_depth,
            "Overflow Event",
        );
    }
}
