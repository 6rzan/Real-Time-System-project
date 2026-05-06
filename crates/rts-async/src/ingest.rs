//! Live SSE ingestion: connect, frame, reconnect-with-backoff.
//!
//! The Wikipedia *Recent Changes* endpoint is `text/event-stream`; each event
//! has an `event:`, `data:`, `id:`, etc. prefix. We delegate framing to
//! [`eventsource_stream`] and forward each event's `data` payload (one full
//! JSON object per event) to the caller-supplied sink.
//!
//! Disconnects, HTTP errors, and per-event parse failures are non-fatal: we log
//! and continue. A capped exponential backoff with ±20 % jitter throttles
//! reconnect storms.

use std::time::Duration;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use rand::Rng;
use reqwest::header;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Default upstream — the live Wikimedia `EventStreams` firehose.
pub const DEFAULT_URL: &str = "https://stream.wikimedia.org/v2/stream/recentchange";

/// Wikimedia policy mandates an identifying `User-Agent`. We include the
/// project name and the maintainer e-mail so they can reach us if the load
/// pattern is problematic.
pub const USER_AGENT: &str =
    "RTS2601-coursework/0.1 (+https://github.com/6rzan/Real-Time-System-project; tahafahd40@gmail.com)";

/// The longest interval the backoff loop will sleep before retrying.
pub const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Initial backoff duration; doubles on each consecutive failure.
pub const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

/// Outcome of a single SSE session — distinguished so callers can tell the
/// difference between "we hit the limit" and "the upstream went away".
#[derive(Debug)]
pub enum IngestOutcome {
    /// `--limit N` reached; the configured number of events were forwarded.
    LimitReached(usize),
    /// External cancellation token tripped (e.g. Ctrl-C).
    Cancelled,
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("failed to build HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),
}

/// Connect to `url`, decode SSE frames, and forward each event's `data:`
/// payload string to `sink`. Reconnects with exponential backoff on transport
/// failures. Returns when `limit` events have been forwarded or `cancel`
/// fires.
pub async fn run_sse<F>(
    url: &str,
    limit: Option<usize>,
    cancel: CancellationToken,
    mut sink: F,
) -> Result<IngestOutcome, IngestError>
where
    F: FnMut(&str),
{
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .pool_idle_timeout(Some(Duration::from_secs(30)))
        // Wikipedia keeps the connection open indefinitely; we treat 60 s of
        // total silence as fatal at the HTTP layer (the watchdog enforces a
        // tighter 10 s SLA over the framed stream in P8).
        .read_timeout(Duration::from_mins(1))
        .connect_timeout(Duration::from_secs(15))
        .build()
        .map_err(IngestError::ClientBuild)?;

    let mut delivered: usize = 0;
    let mut backoff = INITIAL_BACKOFF;
    let mut consecutive_failures: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            return Ok(IngestOutcome::Cancelled);
        }

        tracing::info!(target: "rts.ingest", url = %url, "connecting to SSE upstream");
        let req = client
            .get(url)
            .header(header::ACCEPT, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache");

        let attempt = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(IngestOutcome::Cancelled),
            res = req.send() => res,
        };

        let response = match attempt {
            Ok(r) if r.status().is_success() => {
                consecutive_failures = 0;
                backoff = INITIAL_BACKOFF;
                r
            }
            Ok(r) => {
                let status = r.status();
                tracing::warn!(target: "rts.ingest", %status, "non-success status; will reconnect");
                consecutive_failures = consecutive_failures.saturating_add(1);
                sleep_with_jitter(&mut backoff, consecutive_failures, &cancel).await;
                if cancel.is_cancelled() {
                    return Ok(IngestOutcome::Cancelled);
                }
                continue;
            }
            Err(e) => {
                tracing::warn!(target: "rts.ingest", error = %e, "connect failed; will reconnect");
                consecutive_failures = consecutive_failures.saturating_add(1);
                sleep_with_jitter(&mut backoff, consecutive_failures, &cancel).await;
                if cancel.is_cancelled() {
                    return Ok(IngestOutcome::Cancelled);
                }
                continue;
            }
        };

        let mut stream = response.bytes_stream().eventsource();
        tracing::info!(target: "rts.ingest", "SSE stream open");

        loop {
            let frame = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(IngestOutcome::Cancelled),
                next = stream.next() => next,
            };

            match frame {
                Some(Ok(event)) => {
                    // The Wikimedia stream periodically emits a heartbeat with
                    // `event: message` and an empty data field; skip those.
                    if event.data.is_empty() {
                        continue;
                    }
                    sink(&event.data);
                    delivered = delivered.saturating_add(1);
                    if let Some(n) = limit {
                        if delivered >= n {
                            tracing::info!(target: "rts.ingest", delivered, "limit reached");
                            return Ok(IngestOutcome::LimitReached(delivered));
                        }
                    }
                }
                Some(Err(e)) => {
                    // Framing or decoding error on a single event — log and skip.
                    // We do not tear down the connection here; the underlying
                    // bytes stream is still healthy.
                    tracing::warn!(target: "rts.ingest", error = %e, "SSE frame error; skipping");
                }
                None => {
                    tracing::warn!(target: "rts.ingest", delivered, "SSE stream ended; will reconnect");
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    sleep_with_jitter(&mut backoff, consecutive_failures, &cancel).await;
                    if cancel.is_cancelled() {
                        return Ok(IngestOutcome::Cancelled);
                    }
                    break;
                }
            }
        }
    }
}

async fn sleep_with_jitter(
    backoff: &mut Duration,
    consecutive_failures: u32,
    cancel: &CancellationToken,
) {
    // ±20 % jitter applied as a multiplicative factor in [0.8, 1.2]. Working in
    // f64 keeps the cast surface small and stays well clear of overflow given
    // backoffs are bounded above by `MAX_BACKOFF` (30 s).
    let factor: f64 = rand::thread_rng().gen_range(0.8..=1.2);
    let actual = backoff.mul_f64(factor);
    let backoff_ms = u64::try_from(actual.as_millis()).unwrap_or(u64::MAX);

    tracing::info!(
        target: "rts.ingest",
        backoff_ms,
        consecutive_failures,
        "sleeping before reconnect"
    );

    tokio::select! {
        biased;
        () = cancel.cancelled() => {},
        () = tokio::time::sleep(actual) => {},
    }

    *backoff = (*backoff * 2).min(MAX_BACKOFF);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_includes_project() {
        assert!(USER_AGENT.contains("RTS2601"));
        assert!(USER_AGENT.contains('@')); // has contact e-mail
    }

    #[test]
    fn backoff_constants_sensible() {
        assert!(INITIAL_BACKOFF < MAX_BACKOFF);
        assert!(INITIAL_BACKOFF >= Duration::from_millis(50));
    }
}
