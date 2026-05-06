//! Sliding-window p99 jitter Degraded-Mode controller.
//!
//! [`FailSafeController`] maintains a ring of five 1-second `Histogram`
//! buckets. On every worker hot-path the controller receives a jitter sample;
//! on each tick it evaluates the combined p99 jitter across all buckets:
//!
//! | Condition                              | Action                       |
//! |----------------------------------------|------------------------------|
//! | p99 > `ENTER_NS` for `DWELL_TICKS`     | Enter Degraded Mode          |
//! | p99 < `RECOVER_NS` for `DWELL_TICKS`   | Leave Degraded Mode          |
//!
//! In Degraded Mode the pipeline drops all Bot-priority events at the parser
//! stage (checked via [`FailSafeController::is_degraded`]).
//!
//! # Thresholds
//! - Enter:   p99 jitter > **1.5 ms** for `DWELL_TICKS` consecutive ticks
//! - Recover: p99 jitter < **0.8 ms** for `DWELL_TICKS` consecutive ticks
//! - Dwell:   **10 ticks** (each tick = 1 s → 10 s hysteresis)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hdrhistogram::Histogram;
use parking_lot::Mutex;

// ── Thresholds ───────────────────────────────────────────────────────────────

/// p99 jitter above this value triggers Degraded Mode entry.
const ENTER_NS: u64 = 1_500_000; // 1.5 ms
/// p99 jitter below this value allows Degraded Mode recovery.
const RECOVER_NS: u64 = 800_000; // 0.8 ms
/// Number of consecutive ticks required to change state (hysteresis).
const DWELL_TICKS: u8 = 10;
/// Number of histogram buckets in the rolling window (one per second).
const RING_LEN: usize = 5;

// ── RollingHistogram ─────────────────────────────────────────────────────────

struct RollingHistogram {
    buckets: Vec<Mutex<Histogram<u64>>>,
    idx: Mutex<usize>,
}

impl RollingHistogram {
    fn new() -> Self {
        let buckets = (0..RING_LEN)
            .map(|_| {
                Mutex::new(
                    Histogram::new_with_bounds(1, 60_000_000_000, 3)
                        .expect("valid histogram bounds"),
                )
            })
            .collect();
        Self {
            buckets,
            idx: Mutex::new(0),
        }
    }

    /// Record a jitter sample into the current bucket.
    fn record(&self, ns: u64) {
        let idx = *self.idx.lock();
        let _ = self.buckets[idx].lock().record(ns.max(1));
    }

    /// Advance to the next bucket (called once per tick) and clear it.
    fn advance(&self) {
        let mut idx = self.idx.lock();
        *idx = (*idx + 1) % RING_LEN;
        self.buckets[*idx].lock().reset();
    }

    /// Compute p99 across all buckets by merging into a scratch histogram.
    fn p99_ns(&self) -> u64 {
        let mut merged =
            Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("valid bounds");
        for b in &self.buckets {
            let _ = merged.add(&*b.lock());
        }
        if merged.is_empty() {
            return 0;
        }
        merged.value_at_quantile(0.99)
    }
}

// ── FailSafeController ───────────────────────────────────────────────────────

/// Sliding-window jitter controller shared between parser and worker threads.
pub struct FailSafeController {
    rolling: RollingHistogram,
    pub(crate) degraded: AtomicBool,
    dwell_counter: Mutex<u8>,
}

impl FailSafeController {
    /// Create a new controller wrapped in an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            rolling: RollingHistogram::new(),
            degraded: AtomicBool::new(false),
            dwell_counter: Mutex::new(0),
        })
    }

    /// Record a jitter sample from the worker hot-path.
    #[inline]
    pub fn record_jitter(&self, j: Duration) {
        let ns = u64::try_from(j.as_nanos()).unwrap_or(u64::MAX).max(1);
        self.rolling.record(ns);
    }

    /// Returns `true` when the pipeline is in Degraded Mode.
    ///
    /// The parser checks this on every event; Bot events are dropped when
    /// `true`.
    #[must_use]
    #[inline]
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Relaxed)
    }

    /// Advance the rolling window and evaluate the hysteresis automaton.
    ///
    /// Call once per second from a background task/thread.
    pub fn tick(&self) {
        self.rolling.advance();
        let p99 = self.rolling.p99_ns();
        let currently_degraded = self.degraded.load(Ordering::Relaxed);
        let mut dwell = self.dwell_counter.lock();

        if currently_degraded {
            if p99 < RECOVER_NS {
                *dwell += 1;
                if *dwell >= DWELL_TICKS {
                    self.degraded.store(false, Ordering::Relaxed);
                    *dwell = 0;
                    tracing::info!(
                        target: "rts.failsafe",
                        p99_jitter_ns = p99,
                        "Degraded Mode OFF — jitter recovered"
                    );
                }
            } else {
                *dwell = 0;
            }
        } else if p99 > ENTER_NS {
            *dwell += 1;
            if *dwell >= DWELL_TICKS {
                self.degraded.store(true, Ordering::Relaxed);
                *dwell = 0;
                tracing::warn!(
                    target: "rts.failsafe",
                    p99_jitter_ns = p99,
                    "Degraded Mode ON — dropping Bot events"
                );
            }
        } else {
            *dwell = 0;
        }
    }
}

/// Blocking ticker for the threaded pipeline. Returns when `cancel` is set.
pub fn run_sync_ticker(ctrl: &Arc<FailSafeController>, cancel: &Arc<AtomicBool>) {
    loop {
        std::thread::sleep(Duration::from_secs(1));
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        ctrl.tick();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_normal() {
        let ctrl = FailSafeController::new();
        assert!(!ctrl.is_degraded());
    }

    #[test]
    fn enters_degraded_after_dwell() {
        let ctrl = FailSafeController::new();
        for _ in 0..DWELL_TICKS {
            for _ in 0..1000 {
                ctrl.record_jitter(Duration::from_millis(5)); // >> 1.5 ms
            }
            ctrl.tick();
        }
        assert!(ctrl.is_degraded());
    }

    #[test]
    fn recovers_after_dwell() {
        let ctrl = FailSafeController::new();
        ctrl.degraded.store(true, Ordering::Relaxed);
        for _ in 0..DWELL_TICKS {
            for _ in 0..1000 {
                ctrl.record_jitter(Duration::from_micros(100)); // << 0.8 ms
            }
            ctrl.tick();
        }
        assert!(!ctrl.is_degraded());
    }
}
