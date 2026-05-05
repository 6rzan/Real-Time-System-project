//! `rts` — CLI entry point.
//!
//! P1 wires the `run-async` subcommand: connect to the live Wikipedia SSE
//! firehose and forward events to stdout as one JSON line per event. Future
//! phases add `run-threaded`, `replay record/play`, and `analyse`.

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
use rts_replay::Rate;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

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
    /// Run the Tokio async pipeline.
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

    /// Capture for this much wall-clock time, then stop. Same suffixes as
    /// `--duration` on `run-async` (`ms`, `s`, `m`, `h`).
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

    /// Replay rate: `1x` (wall-clock), `10x`, `100x`, `2.5`, or `max`.
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

    /// Stop after this much wall-clock time. Accepts plain seconds or a unit
    /// suffix (`ms`, `s`, `m`, `h`). Specify either `--limit` or `--duration`,
    /// or neither (run until Ctrl-C).
    #[arg(long, value_parser = parse_duration)]
    duration: Option<Duration>,
}

fn parse_duration(raw: &str) -> Result<Duration, String> {
    // Split into leading numeric portion and trailing unit suffix.
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

    init_tracing();

    let cli = Cli::parse();
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

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_target(true)
        .with_current_span(false)
        .with_writer(std::io::stderr)
        .init();
}

async fn run_async(args: RunAsyncArgs) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    install_ctrl_c(&cancel);
    if let Some(d) = args.duration {
        let cancel_for_timer = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            tracing::info!(target: "rts.cli", "duration elapsed; cancelling");
            cancel_for_timer.cancel();
        });
    }

    let outcome = ingest::run_sse(&args.url, args.limit, cancel.clone(), |data| {
        // P1: forward each event's data payload (which is one JSON object per
        // event) verbatim to stdout, one line per event. Downstream phases
        // replace this sink with the parser stage.
        println!("{data}");
    })
    .await
    .context("SSE ingest")?;

    tracing::info!(target: "rts.cli", outcome = ?outcome, "ingest finished");
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
    let cancel = cancel.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(target: "rts.cli", error = %e, "ctrl-c handler failed");
            return;
        }
        tracing::info!(target: "rts.cli", "Ctrl-C received; cancelling");
        cancel.cancel();
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
