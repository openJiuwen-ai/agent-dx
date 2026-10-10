use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};
/// Bounded LRU cache. Expiry never extends when an entry is read.
pub struct Cache<K, V> {
    entries: HashMap<K, (V, Instant, Instant)>,
    limit: usize,
}
impl<K: Clone + Eq + Hash, V: Clone> Cache<K, V> {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            limit,
        }
    }
    pub fn get(&mut self, key: &K) -> Option<V> {
        let now = Instant::now();
        let (v, expiry, used) = self.entries.get_mut(key)?;
        if now >= *expiry {
            self.entries.remove(key);
            None
        } else {
            *used = now;
            Some(v.clone())
        }
    }
    pub fn insert(&mut self, key: K, value: V, ttl: Duration) {
        if !self.entries.contains_key(&key) && self.entries.len() >= self.limit {
            if let Some(old) = self
                .entries
                .iter()
                .min_by_key(|(_, v)| v.2)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&old);
            }
        }
        let now = Instant::now();
        self.entries.insert(key, (value, now + ttl, now));
    }
    pub fn remove(&mut self, key: &K) {
        self.entries.remove(key);
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
}
