//! Leaderboard actor for the threaded pipeline.
//!
//! A single OS thread owns the `HashMap<wiki → edit_count>`, receiving
//! [`LbCmd`] messages via a `crossbeam_channel`.  Workers send `Update`
//! messages using non-blocking `try_send`; the pipeline sends `Snapshot`
//! before shutdown to retrieve the sorted results.

use std::collections::HashMap;

/// Commands accepted by the leaderboard thread.
pub enum LbCmd {
    /// Increment the edit count for `wiki`.
    Update(String),
    /// Send a sorted snapshot back on the oneshot `reply` channel.
    Snapshot(crossbeam_channel::Sender<Vec<(String, u64)>>),
}

/// The leaderboard actor — runs on a dedicated OS thread.
pub struct Leaderboard {
    rx: crossbeam_channel::Receiver<LbCmd>,
    counts: HashMap<String, u64>,
}

impl Leaderboard {
    #[must_use]
    pub fn new(rx: crossbeam_channel::Receiver<LbCmd>) -> Self {
        Self {
            rx,
            counts: HashMap::new(),
        }
    }

    /// Run the actor loop until all senders are dropped.
    pub fn run(mut self) {
        while let Ok(cmd) = self.rx.recv() {
            match cmd {
                LbCmd::Update(wiki) => {
                    *self.counts.entry(wiki).or_insert(0) += 1;
                }
                LbCmd::Snapshot(reply) => {
                    let mut entries: Vec<(String, u64)> =
                        self.counts.iter().map(|(k, &v)| (k.clone(), v)).collect();
                    entries.sort_unstable_by_key(|b| std::cmp::Reverse(b.1));
                    let _ = reply.send(entries);
                }
            }
        }
    }
}
