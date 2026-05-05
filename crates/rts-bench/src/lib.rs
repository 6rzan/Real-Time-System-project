//! `rts-bench` — Criterion benchmark harnesses. The actual benches live under
//! `benches/`; this lib crate exists so harness sources can reference internal
//! helpers if needed in later phases.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
