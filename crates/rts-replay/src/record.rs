//! Capture a live SSE trace into NDJSON.
//!
//! Each line in the output file is a JSON envelope of the form
//! `{"recv_ns": <u64>, "data": <string>}` where `recv_ns` is the wall-clock
//! delta since the start of the recording (measured with [`std::time::Instant`])
//! and `data` is the verbatim payload of the upstream SSE event.
//!
//! The recorder reuses [`rts_async::ingest::run_sse`] as the upstream client so
//! that the demo / replay pipeline shares its reconnect / backoff behaviour
//! with the real ingest path. Recording stops when `duration` elapses or
//! `cancel` fires (whichever comes first).

use std::path::Path;
use std::time::{Duration, Instant};

use rts_async::ingest::{self, IngestError};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ingest: {0}")]
    Ingest(#[from] IngestError),
    #[error("writer task panicked")]
    WriterPanic,
}

/// Record `url` to `out` for at most `duration` wall-clock time.
///
/// Returns the number of events written. `cancel` provides cooperative
/// cancellation (Ctrl-C). The output file's parent directory is created if
/// missing.
pub async fn record(
    url: &str,
    out: &Path,
    duration: Option<Duration>,
    cancel: CancellationToken,
) -> Result<usize, RecordError> {
    if let Some(parent) = out.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    let file = tokio::fs::File::create(out).await?;
    let mut writer = tokio::io::BufWriter::new(file);

    if let Some(d) = duration {
        let cancel_for_timer = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            tracing::info!(target: "rts.replay.record", "duration elapsed; cancelling");
            cancel_for_timer.cancel();
        });
    }

    // Unbounded mpsc lets the synchronous `sink` callback hand events to an
    // async writer task without ever blocking the ingest loop. The recorder is
    // not on the latency-sensitive hot path, so unbounded is fine and avoids
    // dropping events under transient stalls.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, String)>();
    let start = Instant::now();

    let writer_task: tokio::task::JoinHandle<Result<usize, std::io::Error>> =
        tokio::spawn(async move {
            let mut count: usize = 0;
            while let Some((recv_ns, data)) = rx.recv().await {
                let envelope = serde_json::json!({ "recv_ns": recv_ns, "data": data });
                // serde_json::to_vec on a Value never fails.
                let mut line = serde_json::to_vec(&envelope)
                    .expect("serialise NDJSON envelope");
                line.push(b'\n');
                writer.write_all(&line).await?;
                count = count.saturating_add(1);
            }
            writer.flush().await?;
            Ok(count)
        });

    let outcome = ingest::run_sse(url, None, cancel.clone(), |data| {
        let recv_ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        // If the receiver was dropped (writer task died), recording is
        // already finished — silently drop the event; the surrounding
        // `await` below will surface the writer task's error.
        let _ = tx.send((recv_ns, data.to_string()));
    })
    .await;

    drop(tx);

    let join = writer_task.await;
    let count = match join {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Err(RecordError::Io(e)),
        Err(_) => return Err(RecordError::WriterPanic),
    };

    // Surface ingest errors only after the writer has been drained, so the
    // partially-recorded fixture is still flushed to disk.
    let outcome = outcome?;

    tracing::info!(
        target: "rts.replay.record",
        events = count,
        ?outcome,
        "recording finished"
    );
    Ok(count)
}
