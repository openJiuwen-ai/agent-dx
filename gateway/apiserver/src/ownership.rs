use lru_slab::LruSlab;
use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

struct Entry<K, V> {
    key: K,
    value: V,
    expiry: Instant,
}
/// Bounded LRU cache. Expiry never extends when an entry is read.
pub struct Cache<K, V> {
    entries: HashMap<K, u32>,
    slots: LruSlab<Entry<K, V>>,
    limit: usize,
}
impl<K: Clone + Eq + Hash, V: Clone> Cache<K, V> {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            slots: LruSlab::new(),
            limit: limit.max(1),
        }
    }
    pub fn get(&mut self, key: &K) -> Option<V> {
        let now = Instant::now();
        let slot = *self.entries.get(key)?;
        if now >= self.slots.peek(slot).expiry {
            self.remove(key);
            None
        } else {
            Some(self.slots.get_mut(slot).value.clone())
        }
    }
    pub fn insert(&mut self, key: K, value: V, ttl: Duration) {
        self.remove(&key);
        if self.entries.len() >= self.limit {
            if let Some(slot) = self.slots.lru() {
                let evicted = self.slots.remove(slot);
                self.entries.remove(&evicted.key);
            }
        }
        let now = Instant::now();
        let slot = self.slots.insert(Entry {
            key: key.clone(),
            value,
            expiry: now + ttl,
        });
        self.entries.insert(key, slot);
    }
    pub fn remove(&mut self, key: &K) {
        if let Some(slot) = self.entries.remove(key) {
            self.slots.remove(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expired_entries_are_not_served_and_reads_do_not_extend_validity() {
        let mut cache = Cache::new(2);
        cache.insert(1, "one", Duration::ZERO);
        assert_eq!(cache.get(&1), None);
        cache.insert(1, "one", Duration::from_secs(30));
        cache.insert(2, "two", Duration::from_secs(30));
        assert_eq!(cache.get(&1), Some("one"));
        cache.insert(3, "three", Duration::from_secs(30));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&1), Some("one"));
    }

    #[test]
    fn replacement_and_removal_preserve_lru_order_and_capacity() {
        let mut cache = Cache::new(2);
        let ttl = Duration::from_secs(600);
        cache.insert(1, "old", ttl);
        cache.insert(2, "two", ttl);
        let expiry = cache.slots.peek(cache.entries[&1]).expiry;
        assert_eq!(cache.get(&1), Some("old"));
        assert_eq!(cache.slots.peek(cache.entries[&1]).expiry, expiry);
        cache.insert(1, "new", ttl);
        cache.insert(3, "three", ttl);
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&1), Some("new"));
        cache.remove(&1);
        cache.insert(4, "four", ttl);
        assert_eq!(cache.get(&3), Some("three"));
        assert_eq!(cache.slots.len(), 2);
    }
}
