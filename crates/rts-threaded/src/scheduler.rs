//! Blocking priority queue and worker function.
//!
//! [`PriorityQueue`] wraps a `parking_lot::Mutex<BinaryHeap<…>>` + `Condvar`
//! so that OS threads can block efficiently while waiting for work.  The heap
//! is ordered so that **Human edits preempt Bot edits**, and older tasks in
//! the same priority class are served before newer ones (FIFO within class).
//!
//! On overflow (push when at capacity) the *worst* item — highest priority
//! ordinal (Bot), then oldest enqueue time — is evicted, mirroring the
//! `DropOldestRing` semantics used in the async pipeline.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtOrd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rts_core::channel::ring::PushOutcome;
use rts_core::event::OwnedEvent;
use rts_core::failsafe::FailSafeController;
use rts_core::metrics::Metrics;
use rts_core::priority::Priority;
use rts_core::task::Task;

use crate::leaderboard::LbCmd;

// ── PrioritisedTask ─────────────────────────────────────────────────────────

/// An `OwnedEvent` paired with its scheduling key `(priority, enqueued_at)`.
///
/// The natural ordering places Human-oldest at the *minimum*, so wrapping in
/// `std::cmp::Reverse` and storing in a `BinaryHeap` (max-heap) pops
/// Human-oldest first.
struct PrioritisedTask {
    priority: Priority,
    enqueued_at: Instant,
    event: OwnedEvent,
}

impl PartialEq for PrioritisedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.enqueued_at == other.enqueued_at
    }
}

impl Eq for PrioritisedTask {}

impl Ord for PrioritisedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.enqueued_at.cmp(&other.enqueued_at))
    }
}

impl PartialOrd for PrioritisedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ── PriorityQueue ────────────────────────────────────────────────────────────

/// Bounded, blocking, priority-ordered work queue.
///
/// Backed by a `BinaryHeap<Reverse<PrioritisedTask>>` so that the worker's
/// `pop` always returns the highest-priority (oldest) item.  When at
/// capacity, [`PriorityQueue::push`] evicts the *worst* item — the one with
/// the largest `(priority_ordinal, enqueued_at)` tuple — to maintain the
/// bound.
pub struct PriorityQueue {
    inner: Mutex<BinaryHeap<std::cmp::Reverse<PrioritisedTask>>>,
    capacity: usize,
    overflow: AtomicU64,
    condvar: Condvar,
}

impl PriorityQueue {
    /// Create a new queue with `capacity` max items, wrapped in an `Arc`.
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BinaryHeap::with_capacity(capacity.saturating_add(1))),
            capacity,
            overflow: AtomicU64::new(0),
            condvar: Condvar::new(),
        })
    }

    /// Push a task.  If at capacity, the worst item is evicted first.
    pub fn push(&self, task: Task) -> PushOutcome {
        let incoming = PrioritisedTask {
            priority: task.priority,
            enqueued_at: task.enqueued_at,
            event: task.event,
        };

        let mut guard = self.inner.lock();

        let outcome = if guard.len() >= self.capacity {
            // Evict the worst item: highest priority ordinal (Bot=1 > Human=0),
            // and among ties the oldest (smallest Instant).
            // BinaryHeap only exposes its max cheaply, so we drain to a Vec,
            // remove the worst, and rebuild — O(n) but overflow is exceptional.
            let mut items: Vec<std::cmp::Reverse<PrioritisedTask>> =
                std::mem::take(&mut *guard).into_vec();

            let worst_idx = items
                .iter()
                .enumerate()
                .max_by(|(_, std::cmp::Reverse(a)), (_, std::cmp::Reverse(b))| {
                    // Higher priority ordinal = worse; among ties, older = worse.
                    (a.priority as u8)
                        .cmp(&(b.priority as u8))
                        .then_with(|| b.enqueued_at.cmp(&a.enqueued_at))
                })
                .map(|(i, _)| i)
                .expect("heap was non-empty when len >= capacity > 0");

            items.swap_remove(worst_idx);
            *guard = items.into_iter().collect();
            self.overflow.fetch_add(1, AtOrd::Relaxed);
            PushOutcome::DroppedOldest
        } else {
            PushOutcome::Ok
        };

        guard.push(std::cmp::Reverse(incoming));
        self.condvar.notify_one();
        outcome
    }

    /// Block until a task is available or `cancel` is set.
    ///
    /// Returns `None` when `cancel` is observed.  Uses a 100 ms poll interval
    /// so cancellation is noticed promptly without spinning.
    pub fn pop_blocking(&self, cancel: &AtomicBool) -> Option<Task> {
        let mut guard = self.inner.lock();
        loop {
            if cancel.load(AtOrd::Relaxed) {
                return None;
            }
            if let Some(std::cmp::Reverse(pt)) = guard.pop() {
                return Some(Task {
                    event: pt.event,
                    priority: pt.priority,
                    enqueued_at: pt.enqueued_at,
                });
            }
            self.condvar.wait_for(&mut guard, Duration::from_millis(100));
        }
    }

    /// Number of items currently in the queue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Returns `true` if the queue contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Number of items dropped due to overflow since creation.
    #[must_use]
    pub fn overflow_count(&self) -> u64 {
        self.overflow.load(AtOrd::Relaxed)
    }

    /// Notify all blocked workers (used during shutdown to unblock them).
    pub fn wake_all(&self) {
        self.condvar.notify_all();
    }
}

// ── Worker ───────────────────────────────────────────────────────────────────

const DEADLINE_NS: u128 = 2_000_000; // 2 ms

/// Run one worker on the calling thread.
///
/// Pops tasks from `queue` (highest-priority oldest first), records drift +
/// jitter, forwards the wiki name to the leaderboard, and emits a
/// `rts.worker` tracing event per task.  Returns when `cancel` is observed.
#[allow(clippy::needless_pass_by_value)]
pub fn worker(
    queue: Arc<PriorityQueue>,
    lb_tx: crossbeam_channel::Sender<LbCmd>,
    metrics: Arc<Metrics>,
    failsafe: Arc<FailSafeController>,
    cancel: Arc<AtomicBool>,
) {
    while let Some(task) = queue.pop_blocking(&cancel) {
        let actual_start = Instant::now();
        let drift = actual_start.saturating_duration_since(task.enqueued_at);
        let priority = task.priority;

        metrics.record_drift(priority, drift);

        // Non-blocking send: drop update if the leaderboard channel is full.
        let _ = lb_tx.try_send(LbCmd::Update(task.event.server_name.clone()));

        let total = actual_start.elapsed();
        let drift_ns = u64::try_from(drift.as_nanos()).unwrap_or(u64::MAX);
        let duration_ns = u64::try_from(total.as_nanos()).unwrap_or(u64::MAX);
        metrics.record_jitter(total);
        failsafe.record_jitter(total);
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rts_core::event::OwnedEvent;

    fn make_task(priority: Priority) -> Task {
        Task {
            event: OwnedEvent {
                user: "u".into(),
                bot: matches!(priority, Priority::Bot),
                server_name: "en.wikipedia.org".into(),
            },
            priority,
            enqueued_at: Instant::now(),
        }
    }

    #[test]
    fn human_before_bot() {
        let q = PriorityQueue::new(10);
        let _ = q.push(make_task(Priority::Bot));
        let _ = q.push(make_task(Priority::Bot));
        let _ = q.push(make_task(Priority::Human));

        let cancel = Arc::new(AtomicBool::new(false));
        assert_eq!(q.pop_blocking(&cancel).unwrap().priority, Priority::Human);
        assert_eq!(q.pop_blocking(&cancel).unwrap().priority, Priority::Bot);
        assert_eq!(q.pop_blocking(&cancel).unwrap().priority, Priority::Bot);
    }

    #[test]
    fn overflow_evicts_worst() {
        let q = PriorityQueue::new(2);
        // Fill with two Bot tasks.
        assert_eq!(q.push(make_task(Priority::Bot)), PushOutcome::Ok);
        assert_eq!(q.push(make_task(Priority::Bot)), PushOutcome::Ok);
        // Overflow: Human pushed, a Bot should be evicted.
        assert_eq!(q.push(make_task(Priority::Human)), PushOutcome::DroppedOldest);

        assert_eq!(q.overflow_count(), 1);
        assert_eq!(q.len(), 2);

        // At least one Human task survives.
        let cancel = Arc::new(AtomicBool::new(false));
        let first = q.pop_blocking(&cancel).unwrap();
        assert_eq!(first.priority, Priority::Human);
    }

    #[test]
    fn cancel_unblocks_pop() {
        let q = PriorityQueue::new(4);
        let cancel = Arc::new(AtomicBool::new(false));
        let c2 = Arc::clone(&cancel);
        let q2 = Arc::clone(&q);

        let handle = std::thread::spawn(move || q2.pop_blocking(&c2));
        std::thread::sleep(Duration::from_millis(50));
        cancel.store(true, AtOrd::Relaxed);
        q.wake_all();

        assert!(handle.join().unwrap().is_none());
    }
}
