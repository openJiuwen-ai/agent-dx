//! Bounded process-local derived state. Eviction never changes authoritative records.
use std::collections::{BTreeMap, VecDeque};

/// A FIFO cache. Replacing an entry preserves its insertion order; capacity zero disables it.
pub struct BoundedCache<K, V> {
    entries: BTreeMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}
impl<K: Ord + Clone, V> BoundedCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key)
    }
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries.get_mut(key)
    }
    pub fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_and_eviction_keep_cache_bounded() {
        let mut cache = BoundedCache::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("a", 3);
        cache.insert("c", 4);
        assert_eq!(cache.get(&"a"), None);
        assert_eq!(cache.get(&"b"), Some(&2));
        assert_eq!(cache.get(&"c"), Some(&4));
        for value in 0..100 {
            cache.insert("c", value);
        }
        assert_eq!(cache.order.len(), 2);
        let mut disabled = BoundedCache::new(0);
        disabled.insert("a", 1);
        assert!(disabled.entries.is_empty());
    }
}
