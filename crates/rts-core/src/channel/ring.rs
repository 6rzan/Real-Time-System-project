use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Result of pushing an item into the `DropOldestRing`.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Ok,
    DroppedOldest,
}

/// A thread-safe, multi-producer, multi-consumer ring buffer that drops the oldest
/// elements when full, maintaining a bounded capacity.
/// Supports both async (Tokio) and blocking (`std::thread`) pop operations.
#[derive(Debug)]
pub struct DropOldestRing<T> {
    queue: Mutex<VecDeque<T>>,
    capacity: usize,
    overflow: AtomicU64,
    notify: Notify,
    condvar: Condvar,
}

impl<T> DropOldestRing<T> {
    /// Creates a new `DropOldestRing` with the given maximum capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            overflow: AtomicU64::new(0),
            notify: Notify::new(),
            condvar: Condvar::new(),
        })
    }

    /// Pushes an item into the ring buffer. If the buffer is full, the oldest item
    /// is dropped to make space, and the overflow counter is incremented.
    pub fn push(&self, item: T) -> PushOutcome {
        let mut q = self.queue.lock();
        let outcome = if q.len() == self.capacity {
            q.pop_front();
            self.overflow.fetch_add(1, Ordering::Relaxed);
            PushOutcome::DroppedOldest
        } else {
            PushOutcome::Ok
        };
        q.push_back(item);

        self.notify.notify_one();
        self.condvar.notify_one();

        outcome
    }

    /// Asynchronously waits for an item to become available and pops it.
    /// Uses `tokio::sync::Notify` for wakeup.
    pub async fn pop_async(&self) -> T {
        loop {
            // Get the permit future *before* checking the queue to avoid missing wakeups
            let notified = self.notify.notified();
            if let Some(item) = self.queue.lock().pop_front() {
                return item;
            }
            notified.await;
        }
    }

    /// Blocks the current thread until an item becomes available and pops it.
    /// Uses `parking_lot::Condvar` for wakeup.
    pub fn pop_blocking(&self) -> T {
        let mut q = self.queue.lock();
        loop {
            if let Some(item) = q.pop_front() {
                return item;
            }
            self.condvar.wait(&mut q);
        }
    }

    /// Returns the number of items that have been dropped due to overflow.
    #[must_use]
    pub fn overflow_count(&self) -> u64 {
        self.overflow.load(Ordering::Relaxed)
    }

    /// Returns the current number of items in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Returns true if the buffer contains no elements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_push_drops_oldest() {
        let ring = DropOldestRing::new(2);
        assert_eq!(ring.push(1), PushOutcome::Ok);
        assert_eq!(ring.push(2), PushOutcome::Ok);
        assert_eq!(ring.push(3), PushOutcome::DroppedOldest);

        assert_eq!(ring.overflow_count(), 1);
        assert_eq!(ring.len(), 2);

        assert_eq!(ring.pop_blocking(), 2);
        assert_eq!(ring.pop_blocking(), 3);
    }

    #[tokio::test]
    async fn test_async_roundtrip() {
        let ring = DropOldestRing::new(10);
        assert_eq!(ring.push(42), PushOutcome::Ok);
        assert_eq!(ring.pop_async().await, 42);
    }

    #[test]
    fn test_threaded_roundtrip() {
        let ring = DropOldestRing::new(10);
        assert_eq!(ring.push(42), PushOutcome::Ok);
        assert_eq!(ring.pop_blocking(), 42);
    }

    #[test]
    fn test_stress_overflow() {
        let ring = DropOldestRing::new(10);
        for i in 0..100_000 {
            let _ = ring.push(i);
        }
        assert_eq!(ring.overflow_count(), 100_000 - 10);
        assert_eq!(ring.len(), 10);
        assert_eq!(ring.pop_blocking(), 100_000 - 10);
    }

    proptest! {
        #[test]
        fn test_queue_invariants(cap in 1..100usize, pushes in 0..200usize) {
            let ring = DropOldestRing::new(cap);
            let mut expected_drops = 0;
            for i in 0..pushes {
                if let PushOutcome::DroppedOldest = ring.push(i) {
                    expected_drops += 1;
                }
            }

            assert_eq!(ring.overflow_count(), u64::try_from(expected_drops).unwrap());
            assert!(ring.len() <= cap);

            let mut extracted = Vec::new();
            while !ring.is_empty() {
                extracted.push(ring.pop_blocking());
            }

            // Should be strictly ascending sequence
            for w in extracted.windows(2) {
                assert!(w[0] < w[1]);
            }
        }
    }
}
