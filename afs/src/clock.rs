//! Wall-clock helpers shared by Meta and Node.
//!
//! Both roles stamp the same idempotency keys and compare the same Unix
//! millisecond deadlines, so the helpers live below both of them.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch; 0 before the epoch, saturating at
/// `u64::MAX`.
#[must_use]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Constructs the stable identity reused by filesystem mutation retries.
#[must_use]
pub fn timed_request_id(id: impl std::fmt::Display) -> String {
    format!("afs1-{}-{id}", now_unix_ms())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_request_id_keeps_the_wire_shape() {
        let before = now_unix_ms();
        let id = timed_request_id("node-7");
        let after = now_unix_ms();
        let stamp = id
            .strip_prefix("afs1-")
            .and_then(|rest| rest.strip_suffix("-node-7"))
            .and_then(|stamp| stamp.parse::<u64>().ok())
            .expect("afs1-<ms>-<id>");
        assert!((before..=after).contains(&stamp));
    }
}
