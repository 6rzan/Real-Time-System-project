//! Blocking SSE ingest using `ureq`.
//!
//! `run_sse` drives the Wikipedia (or replay) event stream on the calling
//! thread, invoking `sink` for every complete SSE data payload.  On
//! disconnect it reconnects with capped exponential back-off + ±20 % jitter.

use std::io::{BufRead as _, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng as _;
use thiserror::Error;

/// Default SSE endpoint — the live Wikimedia firehose.
pub const DEFAULT_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

const USER_AGENT: &str = "RTS2601-coursework/0.1 (tahafahd40@gmail.com)";

/// The reason `run_sse` returned successfully.
#[derive(Debug, Clone, Copy)]
pub enum IngestOutcome {
    LimitReached,
    Cancelled,
}

/// Errors that cause `run_sse` to abort (not just reconnect).
#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Drive the SSE stream on the calling thread.
///
/// Calls `sink` with each raw SSE `data:` payload string.  Returns when
/// `limit` events have been processed, `cancel` is set, or a non-recoverable
/// error occurs.  IO / network errors trigger an automatic reconnect with
/// exponential back-off (200 ms → 400 → … capped at 30 s, ±20 % jitter).
#[allow(clippy::needless_pass_by_value)]
pub fn run_sse(
    url: &str,
    limit: Option<usize>,
    cancel: Arc<AtomicBool>,
    mut sink: impl FnMut(&str),
) -> Result<IngestOutcome, IngestError> {
    let mut count = 0usize;
    let mut backoff = Duration::from_millis(200);
    let mut rng = rand::thread_rng();

    loop {
        if cancel.load(Ordering::Relaxed) {
            return Ok(IngestOutcome::Cancelled);
        }
        if limit.is_some_and(|l| count >= l) {
            return Ok(IngestOutcome::LimitReached);
        }

        match read_stream(url, limit, &cancel, &mut sink, &mut count) {
            Ok(()) => {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(IngestOutcome::Cancelled);
                }
                return Ok(IngestOutcome::LimitReached);
            }
            Err(e) if cancel.load(Ordering::Relaxed) => {
                let _ = e;
                return Ok(IngestOutcome::Cancelled);
            }
            Err(e) => {
                tracing::warn!(
                    target: "rts.threaded.ingest",
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "SSE disconnect; reconnecting",
                );
                let jitter: f64 = rng.gen_range(0.8..=1.2_f64);
                let sleep = Duration::from_secs_f64(backoff.as_secs_f64() * jitter);
                std::thread::sleep(sleep);
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Attempt one connection and read until end-of-stream, cancel, or IO error.
fn read_stream(
    url: &str,
    limit: Option<usize>,
    cancel: &AtomicBool,
    sink: &mut impl FnMut(&str),
    count: &mut usize,
) -> Result<(), IngestError> {
    let agent = ureq::AgentBuilder::new()
        .timeout_read(Duration::from_secs(30))
        .build();

    let response = match agent
        .get(url)
        .set("Accept", "text/event-stream")
        .set("User-Agent", USER_AGENT)
        .call()
    {
        Ok(r) => r,
        Err(e) => {
            // ureq errors are not std::io::Error; treat as a warn + reconnect.
            tracing::warn!(target: "rts.threaded.ingest", error = %e, "connect failed");
            // Return an IO error kind so the outer loop triggers back-off.
            return Err(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string()).into());
        }
    };

    let reader = BufReader::new(response.into_reader());
    let mut data_buf: Vec<String> = Vec::new();

    for line_res in reader.lines() {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        if limit.is_some_and(|l| *count >= l) {
            return Ok(());
        }

        let line = line_res?;

        if line.is_empty() {
            // Blank line = end of SSE event block.
            if !data_buf.is_empty() {
                let payload = data_buf.join("\n");
                sink(&payload);
                *count += 1;
                data_buf.clear();
            }
        } else if let Some(rest) = line.strip_prefix("data:") {
            // Both "data: foo" and "data:foo" are valid SSE.
            data_buf.push(rest.trim_start_matches(' ').to_owned());
        }
        // event:, id:, and `:` comment lines are silently ignored.
    }

    Ok(())
}
