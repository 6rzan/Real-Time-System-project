//! `rts-core` — runtime-agnostic primitives shared by the async and threaded
//! pipelines: event types, priorities, the drop-oldest channel, the metrics
//! recorder, the watchdog, and the fail-safe controller.
//!
//! This crate must not depend on Tokio or spawn any threads itself; it provides
//! the contract that both runtimes implement.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

pub mod channel;
pub mod error;
pub mod event;
pub mod failsafe;
pub mod metrics;
pub mod priority;
pub mod task;
pub mod time;
pub mod watchdog;

pub use error::RtsError;
pub use event::{cow_stats, parse_one, Event, OwnedEvent};
pub use failsafe::FailSafeController;
pub use metrics::Metrics;
pub use priority::Priority;
pub use task::Task;
pub use watchdog::WatchdogState;
