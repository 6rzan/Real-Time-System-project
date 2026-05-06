//! P11 — dhat Zero-Copy Proof
//!
//! Runs the `rts-core` parser over a synthetic 10 000-event batch under dhat
//! heap instrumentation.  Two build profiles expose the two paths:
//!
//! | Build command                                              | Parser path      |
//! |------------------------------------------------------------|------------------|
//! | `cargo run -p rts-bench --bin dhat_zero_copy --features dhat-heap` | **zero-copy** (Cow borrowing) |
//! | `cargo run -p rts-bench --bin dhat_zero_copy --features dhat-heap,owned-baseline` | **owned** (all-allocating) |
//!
//! dhat writes `dhat-heap.json` in the current directory when the process
//! exits. The binary also prints a plain-text summary to stdout so CI can
//! capture it without needing the dhat viewer.
//!
//! # Feature gates
//! - `dhat-heap`      — swaps in dhat's allocator (`#[global_allocator]`)
//! - `owned-baseline` — re-exported from `rts-core/owned-baseline`; makes
//!   `parse_one` force-allocate every string field

#![deny(rust_2018_idioms, unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(clippy::missing_panics_doc, clippy::missing_errors_doc)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use rts_core::event::parse_one;

/// 10 000 representative Wikimedia recentchange events.
///
/// Mix of ASCII-only strings (zero-copy path taken by serde) and strings
/// with Unicode escapes (allocating path forced regardless of feature flag).
const ITERATIONS: usize = 10_000;

/// A realistic ASCII-only recentchange event (the common case on Wikipedia).
const ASCII_EVENT: &str =
    r#"{"user":"SomeEditor","bot":false,"server_name":"en.wikipedia.org"}"#;

/// An event with a Unicode escape in the user field — forces serde to
/// allocate even on the zero-copy path.
const ESCAPE_EVENT: &str =
    r#"{"user":"Userédits","bot":false,"server_name":"fr.wikipedia.org"}"#;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let path_label = if cfg!(feature = "owned-baseline") {
        "owned-baseline (all-allocating)"
    } else {
        "zero-copy (Cow borrowing)"
    };

    println!("=== dhat Zero-Copy Proof ===");
    println!("Parser path : {path_label}");
    println!("Iterations  : {ITERATIONS} (80% ASCII, 20% escape)");
    println!();

    let ascii_count = (ITERATIONS * 4) / 5; // 80 %
    let escape_count = ITERATIONS - ascii_count; // 20 %

    // Run the parser — dhat instruments every heap allocation.
    let mut parsed = 0usize;
    for _ in 0..ascii_count {
        if parse_one(ASCII_EVENT).is_ok() {
            parsed += 1;
        }
    }
    for _ in 0..escape_count {
        if parse_one(ESCAPE_EVENT).is_ok() {
            parsed += 1;
        }
    }

    // cow_stats() returns (borrowed, owned) totals since process start.
    let (borrowed, owned) = rts_core::event::cow_stats();
    let total = borrowed + owned;
    let borrow_pct = if total > 0 { (borrowed * 100) / total } else { 0 };

    println!("Events parsed   : {parsed}");
    println!("Cow::Borrowed   : {borrowed}  ({borrow_pct}%)");
    println!("Cow::Owned      : {owned}");
    println!();

    #[cfg(feature = "dhat-heap")]
    println!("dhat profile written to: dhat-heap.json");
    #[cfg(not(feature = "dhat-heap"))]
    println!("(dhat-heap feature not enabled — no profile written)");
    println!();
    println!(
        "To view: open https://nnethercote.github.io/dh_view/dh_view.html and load dhat-heap.json"
    );
}
