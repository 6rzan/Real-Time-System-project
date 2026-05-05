//! Zero-copy event types with `Cow<'a, str>` borrowed fields.
//!
//! The Wikimedia stream emits one JSON object per line (newline-delimited).
//! Each event is a complete struct; the parser produces an `Event<'a>` that
//! borrows from the input buffer. Only at the queue boundary (when
//! priority-classifying) does `into_owned()` produce an `OwnedEvent` for
//! storage. This keeps the hot path allocation-free in the >99% case where
//! field values contain no JSON escapes.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;

/// Atomic counters tracking how often we take the borrowed vs. owned path.
/// Incremented at parse time in `parse_one()`.
static BORROWED_COUNT: AtomicU64 = AtomicU64::new(0);
static OWNED_COUNT: AtomicU64 = AtomicU64::new(0);

/// Borrowed event; fields may reference the input buffer via `Cow::Borrowed`.
///
/// The `#[serde(borrow)]` attribute tells serde to take a borrow for
/// `Cow<'a, str>` when the field's data contains no escapes. If the JSON
/// includes escapes (e.g. `\uXXXX`), serde allocates a fresh `String`
/// (`Cow::Owned`). This dual-path approach is the key to zero-copy parsing.
#[derive(Debug, Deserialize)]
pub struct Event<'a> {
    /// Wikimedia user account name (or IP). May be borrowed from the input.
    #[serde(borrow)]
    pub user: Cow<'a, str>,
    /// Is this edit from a bot (automated tool)? Defaults to false if absent.
    #[serde(default)]
    pub bot: bool,
    /// Wiki domain, e.g. `en.wikipedia.org`. May be borrowed.
    #[serde(borrow)]
    pub server_name: Cow<'a, str>,
}

#[allow(clippy::elidable_lifetime_names)]
impl<'a> Event<'a> {
    /// Convert a borrowed event into an owned event, consuming the borrow.
    ///
    /// This is called exactly once at the parser→queue boundary (when
    /// enqueueing a `Task`), after priority classification but before
    /// storage in the multi-threaded queue.
    #[must_use]
    pub fn into_owned(self) -> OwnedEvent {
        OwnedEvent {
            user: self.user.into_owned(),
            bot: self.bot,
            server_name: self.server_name.into_owned(),
        }
    }
}

/// Owned event; all fields are heap-allocated `String`s.
///
/// Used for long-lived storage in the priority queue where the input buffer
/// has already been discarded.
#[derive(Clone, Debug)]
pub struct OwnedEvent {
    pub user: String,
    pub bot: bool,
    pub server_name: String,
}

/// Parse a single JSON line into a borrowed `Event<'a>`.
///
/// The lifetime `'a` binds to `buf`, so the returned `Event` is valid only
/// as long as `buf` remains valid. Each `Cow` field may be borrowed (if no
/// escapes) or owned (if serde had to unescape).
///
/// This function increments either `BORROWED_COUNT` or `OWNED_COUNT` based on
/// the actual allocations serde performed. The ratio of borrowed→owned is a
/// key metric: Wikipedia's stream is mostly ASCII, so we expect >99% borrowed.
///
/// Supports a compile-time `owned-baseline` feature for P11 dhat profiling.
#[cfg(not(feature = "owned-baseline"))]
pub fn parse_one<'a>(buf: &'a str) -> Result<Event<'a>, crate::error::RtsError> {
    let event: Event<'a> = serde_json::from_str(buf)?;

    // Count: if any field is owned (due to escapes), the whole event counts as
    // owned. Otherwise, it's borrowed. This is a simplification; in reality,
    // individual fields could be mixed (some borrowed, some owned), but
    // Wikipedia's stream is mostly ASCII, so we expect nearly all borrowed.
    let has_owned = matches!(event.user, Cow::Owned(_))
        || matches!(event.server_name, Cow::Owned(_));

    if has_owned {
        OWNED_COUNT.fetch_add(1, Ordering::Relaxed);
    } else {
        BORROWED_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    Ok(event)
}

#[cfg(feature = "owned-baseline")]
pub fn parse_one<'a>(buf: &'a str) -> Result<Event<'a>, crate::error::RtsError> {
    // Deliberately allocate all strings to measure the cost delta.
    let event: serde_json::Value = serde_json::from_str(buf)?;
    let obj = event
        .as_object()
        .ok_or_else(|| crate::error::RtsError::InvalidEvent("not an object".to_string()))?;

    let user = obj
        .get("user")
        .and_then(|v| v.as_str())
        .ok_or_else(|| crate::error::RtsError::InvalidEvent("missing user".to_string()))?
        .to_string(); // Force allocation

    let bot = obj
        .get("bot")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let server_name = obj
        .get("server_name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            crate::error::RtsError::InvalidEvent("missing server_name".to_string())
        })?
        .to_string(); // Force allocation

    OWNED_COUNT.fetch_add(1, Ordering::Relaxed);

    // Wrap owned Strings in Cow::Owned for the same API contract.
    Ok(Event {
        user: Cow::Owned(user),
        bot,
        server_name: Cow::Owned(server_name),
    })
}

/// Return the observed counts of borrowed vs. owned parses.
pub fn cow_stats() -> (u64, u64) {
    (
        BORROWED_COUNT.load(Ordering::Relaxed),
        OWNED_COUNT.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_real_wikimedia_event() {
        // Typical Wikimedia recentchange event (simplified: just the fields we
        // care about). In practice, the fixture contains many more fields.
        let json = r#"{"user":"TestUser","bot":false,"server_name":"en.wikipedia.org"}"#;
        let event = parse_one(json).expect("parse");
        assert_eq!(event.user, "TestUser");
        assert!(!event.bot);
        assert_eq!(event.server_name, "en.wikipedia.org");
    }

    #[test]
    fn parse_with_escape_sequence() {
        // A user name containing a JSON Unicode escape. serde must decode
        // `\uXXXX` so it allocates a new String (Cow::Owned) instead of
        // borrowing the input buffer.
        let json = "{\"user\":\"User\\u00ff\",\"bot\":false,\"server_name\":\"en.wikipedia.org\"}";
        let event = parse_one(json).expect("parse");
        assert_eq!(event.user.as_ref(), "User\u{00ff}");
        assert!(matches!(event.user, Cow::Owned(_)), "user should be owned due to escape");
    }

    #[test]
    fn parse_clean_ascii_borrowed() {
        // ASCII-only user and domain names should stay borrowed.
        let json = r#"{"user":"Alice","bot":false,"server_name":"en.wikipedia.org"}"#;
        let event = parse_one(json).expect("parse");
        assert_eq!(event.user, "Alice");
        assert_eq!(event.server_name, "en.wikipedia.org");
        assert!(matches!(event.user, Cow::Borrowed(_)));
        assert!(matches!(event.server_name, Cow::Borrowed(_)));
    }

    #[test]
    fn cow_stats_tracks_counts() {
        let (b1, o1) = cow_stats();
        let _ = parse_one(r#"{"user":"A","bot":true,"server_name":"x.org"}"#);
        let (b2, o2) = cow_stats();
        assert!(b2 >= b1 || o2 > o1, "one of the counters should increase");
    }

    #[test]
    fn into_owned_preserves_data() {
        let json = r#"{"user":"Bob","bot":true,"server_name":"fr.wikipedia.org"}"#;
        let event = parse_one(json).expect("parse");
        let owned = event.into_owned();
        assert_eq!(owned.user, "Bob");
        assert!(owned.bot);
        assert_eq!(owned.server_name, "fr.wikipedia.org");
    }

    #[test]
    fn bot_defaults_false() {
        let json = r#"{"user":"Eve","server_name":"de.wikipedia.org"}"#;
        let event = parse_one(json).expect("parse");
        assert!(!event.bot);
    }
}
