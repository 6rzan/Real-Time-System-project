//! `rts` — CLI entry point.
//!
//! Subcommands:
//! - `run-async`   — full Tokio async pipeline (ingest → parse → workers → leaderboard)
//! - `replay record` — capture live SSE to NDJSON fixture
//! - `replay play`   — serve a fixture over local HTTP+SSE

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use rts_async::ingest;
use rts_async::pipeline::PipelineConfig;
use rts_replay::Rate;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Debug, Parser)]
#[command(name = "rts", version, about = "RTS2601 real-time SSE pipeline")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the full Tokio async pipeline.
    RunAsync(RunAsyncArgs),
    /// Record a live SSE trace, or replay a recorded one as a local server.
    Replay {
        #[command(subcommand)]
        cmd: ReplayCmd,
    },
}

#[derive(Debug, Subcommand)]
enum ReplayCmd {
    /// Capture a live SSE trace into NDJSON (`{"recv_ns","data"}` per line).
    Record(RecordArgs),
    /// Serve a recorded fixture over HTTP+SSE on 127.0.0.1.
    Play(PlayArgs),
}

#[derive(Debug, clap::Args)]
struct RecordArgs {
    /// SSE endpoint to record. Defaults to the live Wikimedia firehose.
    #[arg(long, default_value = ingest::DEFAULT_URL)]
    url: String,

    /// Capture for this much wall-clock time, then stop.
    #[arg(long, value_parser = parse_duration)]
    duration: Duration,

    /// NDJSON output path (parent directories are created on demand).
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, clap::Args)]
struct PlayArgs {
    /// Recorded NDJSON fixture to replay.
    #[arg(long)]
    fixture: PathBuf,

    /// Replay rate: `1x`, `10x`, `100x`, `2.5`, or `max`.
    #[arg(long, default_value = "1x", value_parser = parse_rate)]
    rate: Rate,

    /// TCP port to bind on `127.0.0.1`.
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

fn parse_rate(raw: &str) -> Result<Rate, String> {
    Rate::parse(raw).map_err(|e| e.to_string())
}

#[derive(Debug, clap::Args)]
struct RunAsyncArgs {
    /// SSE endpoint to ingest from. Defaults to the live Wikimedia firehose.
    #[arg(long, default_value = ingest::DEFAULT_URL)]
    url: String,

    /// Stop after N events have been forwarded.
    #[arg(long)]
    limit: Option<usize>,

    /// Stop after this much wall-clock time (`ms`, `s`, `m`, `h` suffixes).
    #[arg(long, value_parser = parse_duration)]
    duration: Option<Duration>,

    /// Number of worker tasks. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,

    /// Capacity of each priority lane's drop-oldest ring buffer.
    #[arg(long, default_value_t = 256)]
    capacity: usize,

    /// Write per-event NDJSON logs to this file (creates parent dirs).
    /// Example: `reports/runs/async_run.ndjson`
    #[arg(long)]
    log_path: Option<PathBuf>,
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    let unit_start = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    let (num, unit) = raw.split_at(unit_start);
    if num.is_empty() {
        return Err(format!("invalid duration: {raw}"));
    }
    let value: f64 = num
        .parse()
        .map_err(|_| format!("invalid duration: {raw}"))?;
    let secs = match unit {
        "" | "s" => value,
        "ms" => value / 1000.0,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        other => return Err(format!("unknown duration unit: {other}")),
    };
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("invalid duration: {raw}"));
    }
    Ok(Duration::from_secs_f64(secs))
}

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let cli = Cli::parse();

    // Determine the per-event log path before initialising tracing so we can
    // add the file layer to the subscriber before calling set_global_default.
    let log_path: Option<PathBuf> = match &cli.command {
        Command::RunAsync(args) => args.log_path.clone(),
        Command::Replay { .. } => None,
    };
    let _tracing_guard = init_tracing(log_path.as_deref());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    runtime.block_on(async move {
        match cli.command {
            Command::RunAsync(args) => run_async(args).await,
            Command::Replay { cmd } => match cmd {
                ReplayCmd::Record(args) => replay_record(args).await,
                ReplayCmd::Play(args) => replay_play(args).await,
            },
        }
    })
}

/// Initialise the global tracing subscriber.
///
/// When `log_path` is `Some`, a second JSON layer writes per-event records
/// (target `rts.worker` and `overflow`) to that file via a non-blocking
/// background writer. The returned guard must be kept alive for the duration
/// of the program.
fn init_tracing(
    log_path: Option<&std::path::Path>,
) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stderr_layer = fmt::layer()
        .json()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_current_span(false);

    if let Some(path) = log_path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let dir = path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let file_name = path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("run.ndjson"));

        let (non_blocking, guard) =
            tracing_appender::non_blocking(tracing_appender::rolling::never(dir, file_name));

        // Only write per-event records to the NDJSON file; infrastructure logs
        // stay on stderr.
        let file_filter = tracing_subscriber::filter::Targets::new()
            .with_target("rts.worker", tracing::Level::INFO)
            .with_target("overflow", tracing::Level::WARN);

        let file_layer = fmt::layer()
            .json()
            .with_writer(non_blocking)
            .with_target(true)
            .with_current_span(false)
            .with_filter(file_filter);

        tracing_subscriber::registry()
            .with(filter)
            .with(stderr_layer)
            .with(file_layer)
            .init();

        Some(guard)
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(stderr_layer)
            .init();
        None
    }
}

async fn run_async(args: RunAsyncArgs) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    install_ctrl_c(&cancel);

    if let Some(d) = args.duration {
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            tracing::info!(target: "rts.cli", "duration elapsed; cancelling");
            c.cancel();
        });
    }

    let workers = args.workers.unwrap_or_else(num_cpus::get);
    let cfg = PipelineConfig {
        url: args.url,
        limit: args.limit,
        workers,
        capacity: args.capacity,
        cancel,
    };

    rts_async::pipeline::run(cfg)
        .await
        .context("async pipeline")?;
    Ok(())
}

async fn replay_record(args: RecordArgs) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    install_ctrl_c(&cancel);
    tracing::info!(
        target: "rts.cli",
        url = %args.url,
        out = %args.out.display(),
        duration_ms = u64::try_from(args.duration.as_millis()).unwrap_or(u64::MAX),
        "starting recording"
    );
    let count = rts_replay::record::record(&args.url, &args.out, Some(args.duration), cancel)
        .await
        .context("replay record")?;
    tracing::info!(
        target: "rts.cli",
        events = count,
        path = %args.out.display(),
        "recording complete"
    );
    Ok(())
}

async fn replay_play(args: PlayArgs) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    install_ctrl_c(&cancel);
    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    tracing::info!(
        target: "rts.cli",
        fixture = %args.fixture.display(),
        rate = ?args.rate,
        addr = %addr,
        "starting replay server"
    );
    rts_replay::play::serve(&args.fixture, args.rate, addr, cancel)
        .await
        .context("replay play")?;
    Ok(())
}

fn install_ctrl_c(cancel: &CancellationToken) {
    let c = cancel.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(target: "rts.cli", error = %e, "ctrl-c handler failed");
            return;
        }
        tracing::info!(target: "rts.cli", "Ctrl-C received; cancelling");
        c.cancel();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_seconds() {
        assert_eq!(parse_duration("3").unwrap(), Duration::from_secs(3));
    }

    #[test]
    fn parses_unit_suffixes() {
        assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("3xyz").is_err());
    }
}
