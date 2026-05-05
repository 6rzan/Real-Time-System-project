//! Concurrency primitives: bounded drop-oldest ring buffer, refactored for use
//! by both async (Tokio) and threaded (`std::thread`) schedulers in P4+.

// pub mod ring;
