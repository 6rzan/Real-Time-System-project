//! `rts-async` — Tokio implementation of the RTS2601 pipeline.
//!
//! Composes the runtime-agnostic primitives from [`rts_core`] into an
//! ingest → parse → priority-lane → biased-select worker pool → leaderboard-actor
//! pipeline driven by Tokio.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
