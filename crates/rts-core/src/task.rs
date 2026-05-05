//! Work-queue task: an owned event with priority and enqueue timestamp.

use std::time::Instant;

use crate::event::OwnedEvent;
use crate::priority::Priority;

/// A schedulable task: an owned event paired with its priority class and the
/// wall-clock time it arrived at the queue.
///
/// The `enqueued_at` timestamp is set when the event is parsed and priority-
/// classified, *before* it enters the priority queue. The scheduler measures
/// `drift := (start_time - enqueued_at)` to quantify scheduling latency.
#[derive(Clone)]
pub struct Task {
    pub event: OwnedEvent,
    pub priority: Priority,
    pub enqueued_at: Instant,
}

// Verify that Task is safe to send across thread boundaries.
#[allow(dead_code)]
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}
    const fn check() {
        assert_send::<Task>();
        assert_sync::<Task>();
    }
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_is_send_and_sync() {
        // Compile-time check via const assertion above; this test documents
        // the requirement for the multi-threaded scheduler.
        let task = Task {
            event: OwnedEvent {
                user: "test".to_string(),
                bot: false,
                server_name: "en.wikipedia.org".to_string(),
            },
            priority: Priority::Human,
            enqueued_at: Instant::now(),
        };
        let _ = std::thread::spawn(move || {
            let _ = task;
        });
    }
}
