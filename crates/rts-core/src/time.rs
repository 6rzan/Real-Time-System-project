//! High-resolution timing utilities.

use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// Return elapsed nanoseconds since process start.
///
/// Uses [`std::time::Instant`] internally, which is monotonic and immune to
/// NTP steps (unlike [`std::time::SystemTime`]).
#[must_use]
pub fn now_ns() -> u64 {
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ns_monotonic() {
        let a = now_ns();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let b = now_ns();
        assert!(b > a, "now_ns should be monotonic");
    }
}
