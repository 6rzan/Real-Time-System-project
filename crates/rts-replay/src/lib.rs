//! `rts-replay` — SSE record / playback for the RTS2601 pipeline.
//!
//! `record` captures live SSE traffic into NDJSON for offline replay.
//! `play` serves an `axum`-backed local HTTP+SSE endpoint that re-emits a
//! recorded fixture at a configurable rate multiplier; both runtimes can point
//! at it for deterministic benchmarks and a network-resilient demo.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
