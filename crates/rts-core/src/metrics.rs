//! Per-priority latency histograms and pipeline counters.
//!
//! `Metrics` is shared between all workers via `Arc`. Histograms are guarded
//! by `parking_lot::Mutex`; counters use `AtomicU64` (relaxed ordering is fine
//! because we only need eventual consistency for reporting).
//!
//! Full CSV / `.hgrm` dump functionality is completed in P7; this module
//! provides the snapshot path needed by the P5 pipeline.

use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hdrhistogram::Histogram;
use parking_lot::Mutex;

use crate::priority::Priority;

/// Point-in-time snapshot returned by [`Metrics::snapshot`].
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub drift_human_p50: u64,
    pub drift_human_p90: u64,
    pub drift_human_p99: u64,
    pub drift_human_p999: u64,
    pub drift_bot_p50: u64,
    pub drift_bot_p90: u64,
    pub drift_bot_p99: u64,
    pub drift_bot_p999: u64,
    pub jitter_p50: u64,
    pub jitter_p90: u64,
    pub jitter_p99: u64,
    pub jitter_p999: u64,
    pub deadline_miss_human: u64,
    pub deadline_miss_bot: u64,
    pub overflow_count: u64,
    pub sample_count_human: u64,
    pub sample_count_bot: u64,
}

/// Shared pipeline metrics store.
pub struct Metrics {
    drift_human: Mutex<Histogram<u64>>,
    drift_bot: Mutex<Histogram<u64>>,
    jitter: Mutex<Histogram<u64>>,
    pub deadline_miss_human: AtomicU64,
    pub deadline_miss_bot: AtomicU64,
    pub overflow_count: AtomicU64,
}

impl Metrics {
    /// Create a new metrics store wrapped in an `Arc`.
    ///
    /// Histogram bounds: 1 ns … 60 s, 3 significant figures.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            drift_human: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("valid histogram bounds"),
            ),
            drift_bot: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("valid histogram bounds"),
            ),
            jitter: Mutex::new(
                Histogram::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("valid histogram bounds"),
            ),
            deadline_miss_human: AtomicU64::new(0),
            deadline_miss_bot: AtomicU64::new(0),
            overflow_count: AtomicU64::new(0),
        })
    }

    /// Record a scheduling-drift sample (`actual_start − enqueued_at`).
    pub fn record_drift(&self, priority: Priority, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX).max(1);
        match priority {
            Priority::Human => {
                let _ = self.drift_human.lock().record(ns);
            }
            Priority::Bot => {
                let _ = self.drift_bot.lock().record(ns);
            }
        }
    }

    /// Record total hot-path execution time (jitter measure).
    pub fn record_jitter(&self, j: Duration) {
        let ns = u64::try_from(j.as_nanos()).unwrap_or(u64::MAX).max(1);
        let _ = self.jitter.lock().record(ns);
    }

    /// Increment the deadline-miss counter for `priority`.
    pub fn record_deadline_miss(&self, priority: Priority) {
        match priority {
            Priority::Human => {
                self.deadline_miss_human.fetch_add(1, Ordering::Relaxed);
            }
            Priority::Bot => {
                self.deadline_miss_bot.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Compute and return a point-in-time snapshot (locks all three histograms).
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        let dh = self.drift_human.lock();
        let db = self.drift_bot.lock();
        let j = self.jitter.lock();
        MetricsSnapshot {
            drift_human_p50: dh.value_at_quantile(0.50),
            drift_human_p90: dh.value_at_quantile(0.90),
            drift_human_p99: dh.value_at_quantile(0.99),
            drift_human_p999: dh.value_at_quantile(0.999),
            drift_bot_p50: db.value_at_quantile(0.50),
            drift_bot_p90: db.value_at_quantile(0.90),
            drift_bot_p99: db.value_at_quantile(0.99),
            drift_bot_p999: db.value_at_quantile(0.999),
            jitter_p50: j.value_at_quantile(0.50),
            jitter_p90: j.value_at_quantile(0.90),
            jitter_p99: j.value_at_quantile(0.99),
            jitter_p999: j.value_at_quantile(0.999),
            deadline_miss_human: self.deadline_miss_human.load(Ordering::Relaxed),
            deadline_miss_bot: self.deadline_miss_bot.load(Ordering::Relaxed),
            overflow_count: self.overflow_count.load(Ordering::Relaxed),
            sample_count_human: dh.len(),
            sample_count_bot: db.len(),
        }
    }

    /// Dump per-priority percentile rows to a CSV file.
    ///
    /// Full `.hgrm` export is added in P7; this stub writes the snapshot rows.
    pub fn dump_csv(&self, path: &Path) -> std::io::Result<()> {
        let snap = self.snapshot();
        let mut f = std::fs::File::create(path)?;
        writeln!(f, "metric,p50,p90,p99,p99.9,sample_count")?;
        writeln!(
            f,
            "drift_human,{},{},{},{},{}",
            snap.drift_human_p50,
            snap.drift_human_p90,
            snap.drift_human_p99,
            snap.drift_human_p999,
            snap.sample_count_human
        )?;
        writeln!(
            f,
            "drift_bot,{},{},{},{},{}",
            snap.drift_bot_p50,
            snap.drift_bot_p90,
            snap.drift_bot_p99,
            snap.drift_bot_p999,
            snap.sample_count_bot
        )?;
        writeln!(
            f,
            "jitter,{},{},{},{},{}",
            snap.jitter_p50,
            snap.jitter_p90,
            snap.jitter_p99,
            snap.jitter_p999,
            snap.sample_count_human + snap.sample_count_bot
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_after_records() {
        let m = Metrics::new();
        for _ in 0..1000 {
            m.record_drift(Priority::Human, Duration::from_micros(500));
            m.record_drift(Priority::Bot, Duration::from_micros(5_000));
            m.record_jitter(Duration::from_micros(100));
        }
        m.record_deadline_miss(Priority::Human);
        m.record_deadline_miss(Priority::Bot);

        let s = m.snapshot();
        assert_eq!(s.sample_count_human, 1000);
        assert_eq!(s.sample_count_bot, 1000);
        // Human drift ≈ 500 µs (500_000 ns); bot ≈ 5 ms (5_000_000 ns).
        assert!(s.drift_human_p99 < s.drift_bot_p50);
        assert_eq!(s.deadline_miss_human, 1);
        assert_eq!(s.deadline_miss_bot, 1);
    }
}
