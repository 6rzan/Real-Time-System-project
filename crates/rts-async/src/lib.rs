//! `rts-async` — Tokio implementation of the RTS2601 pipeline.
//!
//! Composes the runtime-agnostic primitives from [`rts_core`] into an
//! ingest → parse → priority-lane → biased-select worker pool → leaderboard-actor
//! pipeline driven by Tokio.
//!
//! P1 ships the ingestion stage only ([`ingest::run_sse`]); subsequent phases
//! layer parsing, priority dispatch, the leaderboard actor, the watchdog, and
//! the fail-safe controller on top.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

pub mod ingest;
