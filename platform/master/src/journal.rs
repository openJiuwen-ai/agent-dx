//! A bounded sequence of changed node IDs. Consumers refresh absolute ledger
//! values, so replay and duplicate reports never subtract reservations twice.
use std::collections::{BTreeSet, VecDeque};
pub(crate) struct MutationJournal {
    sequence: u64,
    capacity: usize,
    entries: VecDeque<(u64, String)>,
}
impl MutationJournal {
    pub fn new(capacity: usize) -> Self {
        Self {
            sequence: 0,
            capacity,
            entries: VecDeque::new(),
        }
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn record(&mut self, node: &str) {
        self.sequence += 1;
        self.entries.push_back((self.sequence, node.into()));
        if self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }
    pub fn since(&self, sequence: u64) -> Option<BTreeSet<String>> {
        if sequence == self.sequence {
            return Some(BTreeSet::new());
        }
        if sequence > self.sequence
            || self
                .entries
                .front()
                .is_none_or(|(first, _)| sequence < first - 1)
        {
            return None;
        }
        Some(
            self.entries
                .range(self.entries.len() - (self.sequence - sequence) as usize..)
                .map(|(_, node)| node.clone())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_boundaries_wrapping_and_duplicate_nodes() {
        let mut journal = MutationJournal::new(3);
        assert_eq!(journal.since(0), Some(BTreeSet::new()));
        for id in ["a", "b", "b", "c", "d"] {
            journal.record(id);
        }
        assert!(journal.since(1).is_none());
        assert_eq!(
            journal.since(2),
            Some(["b".into(), "c".into(), "d".into()].into())
        );
        assert_eq!(journal.since(4), Some(["d".into()].into()));
        assert_eq!(journal.since(5), Some(BTreeSet::new()));
        assert!(journal.since(6).is_none());
    }
}
