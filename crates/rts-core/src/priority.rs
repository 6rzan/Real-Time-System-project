//! Priority levels: Human edits preempt bot activity.

use std::cmp::Ordering;

/// Edit priority. `Human < Bot` in the `Ord` sense, so a min-heap or
/// `biased;` select statement naturally prioritises human edits.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Priority {
    Human = 0,
    Bot = 1,
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Priority {
    #[must_use]
    pub fn from_bot_flag(is_bot: bool) -> Self {
        if is_bot {
            Priority::Bot
        } else {
            Priority::Human
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_less_than_bot() {
        assert!(Priority::Human < Priority::Bot);
    }
}
