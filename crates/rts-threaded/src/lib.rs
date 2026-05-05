//! `rts-threaded` — `std::thread` implementation of the RTS2601 pipeline.
//!
//! Mirrors `rts-async` but uses OS threads, a `Mutex<BinaryHeap>` priority
//! queue, and `crossbeam-channel` for IPC. Shares all event/metric/fail-safe
//! types with the async pipeline through [`rts_core`].

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]
