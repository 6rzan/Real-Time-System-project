//! No-data watchdog: fires if no event is recorded within a fixed window.
//!
//! [`WatchdogState`] holds a single `AtomicU64` (nanoseconds since Unix epoch)
//! that is `touch()`ed on every parsed event. Both async and threaded runtimes
//! spawn a periodic checker that calls [`WatchdogState::is_stale`]; when it
//! returns `true` the pipeline should emit a warning and may reset soft state.
//!
//! # Stale threshold
//! The default window is **10 seconds**. No event within that window is
//! treated as a data-source failure (network drop, upstream silence).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Shared watchdog heartbeat state.
pub struct WatchdogState {
    pub(crate) last_ns: AtomicU64,
}

/// 10-second stale window in nanoseconds.
const STALE_NS: u64 = 10_000_000_000;

impl WatchdogState {
    /// Create a new `WatchdogState` initialised to the current time,
    /// wrapped in an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            last_ns: AtomicU64::new(crate::time::now_ns()),
        })
    }

    /// Record that an event was just received. Call from the parser on every
    /// successful dispatch.
    #[inline]
    pub fn touch(&self) {
        self.last_ns.store(crate::time::now_ns(), Ordering::Relaxed);
    }

    /// Returns `true` if no event has been received for longer than
    /// `STALE_NS` nanoseconds.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.is_stale_ns(STALE_NS)
    }

    /// Same as `is_stale` but with a caller-supplied threshold (for tests).
    #[must_use]
    pub(crate) fn is_stale_ns(&self, threshold_ns: u64) -> bool {
        let now = crate::time::now_ns();
        let last = self.last_ns.load(Ordering::Relaxed);
        now.saturating_sub(last) > threshold_ns
    }

    /// Reset the heartbeat to now (use after an intentional pipeline pause).
    pub fn reset(&self) {
        self.last_ns.store(crate::time::now_ns(), Ordering::Relaxed);
    }
}

/// Blocking watchdog checker for the threaded pipeline.
///
/// Polls every `check_interval`, logging a warning when stale.
/// Returns when `cancel` is set.
pub fn run_sync_checker(
    state: &Arc<WatchdogState>,
    check_interval: Duration,
    cancel: &Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        std::thread::sleep(check_interval);
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        if state.is_stale() {
            tracing::warn!(
                target: "rts.watchdog",
                "no events received for >10 s — possible upstream stall"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_is_not_stale() {
        let w = WatchdogState::new();
        assert!(!w.is_stale());
    }

    #[test]
    fn stale_after_backdating() {
        // now_ns() counts from process start — only ms in a fresh test process.
        // Store 0 and use a 1 µs threshold: elapsed = now_ns() which is always
        // several milliseconds, so this is reliably > 1_000 ns.
        let w = WatchdogState::new();
        w.last_ns.store(0, Ordering::Relaxed);
        assert!(w.is_stale_ns(1_000));
    }

    #[test]
    fn touch_resets_stale() {
        let w = WatchdogState::new();
        // Mark stale by setting last_ns to 0.
        w.last_ns.store(0, Ordering::Relaxed);
        assert!(w.is_stale_ns(1_000));
        // After touch(), last_ns = now_ns(). Check staleness with a 1-second
        // threshold — the time between touch() and is_stale_ns() is nanoseconds,
        // nowhere near 1 second.
        w.touch();
        assert!(!w.is_stale_ns(1_000_000_000));
    }
}
