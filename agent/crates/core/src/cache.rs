//! Bounded FIFO for immutable templates and confirmed affinity snapshots. No expiry semantics.
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

pub struct BoundedCache<K, V> {
    entries: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}
impl<K: Eq + Hash + Clone, V> BoundedCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key)
    }
    pub fn insert(&mut self, key: K, value: V) {
        if !self.entries.contains_key(&key) {
            if self.entries.len() == self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                }
            }
            self.order.push_back(key.clone());
        }
        self.entries.insert(key, value);
    }
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn insertion_evicts_one_entry_and_updates_do_not_consume_capacity() {
        let mut cache = BoundedCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("a", 3);
        assert_eq!(cache.get(&"b"), Some(&2));
        cache.insert("c", 4);
        assert!(cache.get(&"a").is_none());
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&4));
        cache.clear();
        cache.insert("d", 5);
        assert!(cache.get(&"c").is_none());
        assert_eq!(cache.get(&"d"), Some(&5));
    }
}
