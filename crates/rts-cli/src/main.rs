//! `rts` — CLI entry point. Subcommands are wired in P1+.

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc
)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    // Subcommands land here in P1+: run-async, run-threaded, replay, bench, analyse.
    // The return type becomes `anyhow::Result<()>` once handlers can fail.
    println!("rts-cli — bootstrap build. Subcommands wired in subsequent phases.");
}
