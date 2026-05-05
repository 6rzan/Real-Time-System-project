//! Leaderboard actor: counts edits per wiki domain without shared state.
//!
//! All mutations go through a single `mpsc` channel — no Mutex on the hot
//! path. Workers send `LbCmd::Update`; the orchestrator requests a snapshot
//! via `LbCmd::Snapshot` at shutdown.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

/// Commands accepted by the leaderboard actor.
pub enum LbCmd {
    /// Record one edit originating from `server_name`.
    Update(String),
    /// Return all entries sorted by descending edit count.
    Snapshot(oneshot::Sender<Vec<(String, u64)>>),
}

/// Leaderboard actor.
pub struct Leaderboard {
    rx: mpsc::Receiver<LbCmd>,
    counts: HashMap<String, u64>,
}

impl Leaderboard {
    #[must_use]
    pub fn new(rx: mpsc::Receiver<LbCmd>) -> Self {
        Self {
            rx,
            counts: HashMap::new(),
        }
    }

    /// Run the actor loop until all senders are dropped.
    pub async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                LbCmd::Update(name) => {
                    *self.counts.entry(name).or_insert(0) += 1;
                }
                LbCmd::Snapshot(reply) => {
                    let mut entries: Vec<(String, u64)> = self
                        .counts
                        .iter()
                        .map(|(k, v)| (k.clone(), *v))
                        .collect();
                    entries.sort_unstable_by(|a, b| b.1.cmp(&a.1));
                    let _ = reply.send(entries);
                }
            }
        }
    }
}
