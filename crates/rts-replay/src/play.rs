//! Replay a recorded NDJSON fixture as a local SSE endpoint.
//!
//! Serves `GET /v2/stream/recentchange` with `Content-Type: text/event-stream`,
//! framing each recorded payload as `event: message\ndata: <payload>\n\n` to
//! match the upstream Wikimedia stream byte-for-byte from the consumer's
//! perspective. Inter-event gaps are honoured (scaled by `--rate`) using the
//! `recv_ns` deltas captured by the recorder, so a fixture replayed at `1x`
//! reproduces the original arrival cadence; `10x`, `100x` etc. compress it;
//! `max` removes the gap entirely (useful for stress tests). The fixture is
//! looped once exhausted, which keeps demos stable when they outlast the
//! recording.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use bytes::Bytes;
use futures::stream::{self, Stream};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::AsyncBufReadExt;
use tokio_util::sync::CancellationToken;

/// Replay rate. `Multiplier(m)` divides each inter-event gap by `m` (so `m > 1`
/// is faster than wall clock). [`Rate::Max`] eliminates gaps entirely.
#[derive(Debug, Clone, Copy)]
pub enum Rate {
    Multiplier(f64),
    Max,
}

#[derive(Debug, Error)]
#[error("invalid rate: {0}")]
pub struct ParseRateError(pub String);

impl Rate {
    /// Parse `1x`, `10x`, `100x`, `2.5`, `max` (case-insensitive).
    pub fn parse(s: &str) -> Result<Self, ParseRateError> {
        if s.eq_ignore_ascii_case("max") {
            return Ok(Rate::Max);
        }
        let stripped = s.strip_suffix(|c: char| c == 'x' || c == 'X').unwrap_or(s);
        let v: f64 = stripped
            .parse()
            .map_err(|_| ParseRateError(s.to_string()))?;
        if !v.is_finite() || v <= 0.0 {
            return Err(ParseRateError(s.to_string()));
        }
        Ok(Rate::Multiplier(v))
    }
}

#[derive(Debug, Deserialize)]
struct Envelope {
    recv_ns: u64,
    data: String,
}

#[derive(Debug, Clone)]
pub struct RecordedEvent {
    pub recv_ns: u64,
    pub data: String,
}

#[derive(Debug, Error)]
pub enum PlayError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("fixture is empty: {0}")]
    Empty(PathBuf),
    #[error("invalid envelope at line {line}: {source}")]
    BadEnvelope {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// Load a fixture into memory. Blank lines are ignored; non-JSON lines are an
/// error (better to fail fast on a corrupt fixture than silently truncate).
pub async fn load_fixture(path: &Path) -> Result<Vec<RecordedEvent>, PlayError> {
    let file = tokio::fs::File::open(path).await?;
    let reader = tokio::io::BufReader::new(file);
    let mut lines = reader.lines();
    let mut out = Vec::new();
    let mut line_no = 0_usize;
    while let Some(line) = lines.next_line().await? {
        line_no += 1;
        if line.trim().is_empty() {
            continue;
        }
        let env: Envelope = serde_json::from_str(&line).map_err(|e| PlayError::BadEnvelope {
            line: line_no,
            source: e,
        })?;
        out.push(RecordedEvent {
            recv_ns: env.recv_ns,
            data: env.data,
        });
    }
    if out.is_empty() {
        return Err(PlayError::Empty(path.to_path_buf()));
    }
    Ok(out)
}

#[derive(Clone)]
struct AppState {
    events: Arc<Vec<RecordedEvent>>,
    rate: Rate,
}

/// Bind on `addr` and serve the fixture until `cancel` fires.
pub async fn serve(
    fixture: &Path,
    rate: Rate,
    addr: SocketAddr,
    cancel: CancellationToken,
) -> Result<(), PlayError> {
    let events = Arc::new(load_fixture(fixture).await?);
    let state = AppState { events, rate };
    let app = Router::new()
        .route("/v2/stream/recentchange", get(stream_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    tracing::info!(
        target: "rts.replay.play",
        addr = %local,
        "replay server listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn stream_handler(State(state): State<AppState>) -> Response {
    let body = Body::from_stream(replay_stream(state.events, state.rate));
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"))
        // Disable proxy buffering for SSE behaviour parity. Harmless if absent.
        .header("x-accel-buffering", HeaderValue::from_static("no"))
        .body(body)
        .expect("static response builder cannot fail")
}

struct ReplayState {
    events: Arc<Vec<RecordedEvent>>,
    idx: usize,
    rate: Rate,
    /// Wall-clock anchor for the current pass through the fixture.
    loop_start: Instant,
    /// `recv_ns` of the first event in the current pass; subtracted from each
    /// subsequent `recv_ns` to compute its offset within the pass.
    loop_offset_ns: u64,
}

fn replay_stream(
    events: Arc<Vec<RecordedEvent>>,
    rate: Rate,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    let init = ReplayState {
        loop_offset_ns: events.first().map_or(0, |e| e.recv_ns),
        events,
        idx: 0,
        rate,
        loop_start: Instant::now(),
    };

    stream::unfold(init, |mut s| async move {
        if s.events.is_empty() {
            return None;
        }
        if s.idx >= s.events.len() {
            // Reached the end of the fixture — restart from the top with a
            // fresh anchor so timestamps remain monotonic relative to the
            // current loop's start.
            s.idx = 0;
            s.loop_start = Instant::now();
            s.loop_offset_ns = s.events[0].recv_ns;
        }
        let evt = &s.events[s.idx];
        let offset_ns = evt.recv_ns.saturating_sub(s.loop_offset_ns);
        let scaled_ns: u64 = match s.rate {
            Rate::Max => 0,
            Rate::Multiplier(m) => {
                let v = (offset_ns as f64) / m;
                if v.is_finite() && v >= 0.0 {
                    v as u64
                } else {
                    0
                }
            }
        };
        let target = s.loop_start + Duration::from_nanos(scaled_ns);
        let now = Instant::now();
        if target > now {
            tokio::time::sleep_until(tokio::time::Instant::from_std(target)).await;
        }

        // SSE frame. We mirror the upstream Wikimedia framing (`event: message`
        // + `data:` + blank line) so consumers cannot tell the difference
        // between live and replay.
        let mut buf = Vec::with_capacity(evt.data.len() + 24);
        buf.extend_from_slice(b"event: message\n");
        buf.extend_from_slice(b"data: ");
        buf.extend_from_slice(evt.data.as_bytes());
        buf.extend_from_slice(b"\n\n");

        s.idx += 1;
        Some((Ok::<Bytes, std::io::Error>(Bytes::from(buf)), s))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_parses_common_forms() {
        assert!(
            matches!(Rate::parse("1x").unwrap(), Rate::Multiplier(v) if (v - 1.0).abs() < 1e-9)
        );
        assert!(
            matches!(Rate::parse("10x").unwrap(), Rate::Multiplier(v) if (v - 10.0).abs() < 1e-9)
        );
        assert!(
            matches!(Rate::parse("2.5").unwrap(), Rate::Multiplier(v) if (v - 2.5).abs() < 1e-9)
        );
        assert!(matches!(Rate::parse("MAX").unwrap(), Rate::Max));
        assert!(matches!(Rate::parse("max").unwrap(), Rate::Max));
    }

    #[test]
    fn rate_rejects_garbage() {
        assert!(Rate::parse("").is_err());
        assert!(Rate::parse("abc").is_err());
        assert!(Rate::parse("0x").is_err());
        assert!(Rate::parse("-1x").is_err());
    }
}
