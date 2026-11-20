//! Bounded process-local derived state. Eviction never changes authoritative records.
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache dimension reported by `adx_agent_cache_access_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheKind {
    Binding,
    Template,
}
impl CacheKind {
    pub const ALL: [Self; 2] = [Self::Binding, Self::Template];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binding => "binding",
            Self::Template => "template",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Binding => 0,
            Self::Template => 1,
        }
    }
}

/// Cache outcome reported by `adx_agent_cache_access_total`.
///
/// `Bypass` (forced refresh or explicit inconsistency) and `Error` are kept
/// separate from `Miss`: only `hit/(hit+miss)` is a hit ratio, and a forced
/// refresh must not look like cache degradation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheResult {
    Hit,
    Miss,
    Bypass,
    Error,
}
impl CacheResult {
    pub const ALL: [Self; 4] = [Self::Hit, Self::Miss, Self::Bypass, Self::Error];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass => "bypass",
            Self::Error => "error",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Hit => 0,
            Self::Miss => 1,
            Self::Bypass => 2,
            Self::Error => 3,
        }
    }
}

/// Process-local cache counters. Recording never changes cache behavior.
#[derive(Default)]
pub struct CacheMetrics {
    counts: [[AtomicU64; 4]; 2],
}
impl CacheMetrics {
    pub fn record(&self, cache: CacheKind, result: CacheResult) {
        self.counts[cache.index()][result.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn count(&self, cache: CacheKind, result: CacheResult) -> u64 {
        self.counts[cache.index()][result.index()].load(Ordering::Relaxed)
    }
}

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
    fn cache_metrics_keep_each_kind_and_result_independent() {
        let metrics = CacheMetrics::default();
        assert_eq!(metrics.count(CacheKind::Binding, CacheResult::Hit), 0);
        metrics.record(CacheKind::Binding, CacheResult::Hit);
        metrics.record(CacheKind::Binding, CacheResult::Bypass);
        metrics.record(CacheKind::Template, CacheResult::Miss);
        metrics.record(CacheKind::Template, CacheResult::Miss);
        metrics.record(CacheKind::Template, CacheResult::Error);
        assert_eq!(metrics.count(CacheKind::Binding, CacheResult::Hit), 1);
        assert_eq!(metrics.count(CacheKind::Binding, CacheResult::Bypass), 1);
        assert_eq!(metrics.count(CacheKind::Binding, CacheResult::Miss), 0);
        assert_eq!(metrics.count(CacheKind::Template, CacheResult::Miss), 2);
        assert_eq!(metrics.count(CacheKind::Template, CacheResult::Error), 1);
        assert_eq!(metrics.count(CacheKind::Template, CacheResult::Hit), 0);
        let names: Vec<_> = CacheKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .chain(CacheResult::ALL.iter().map(|result| result.as_str()))
            .collect();
        assert_eq!(
            names,
            vec!["binding", "template", "hit", "miss", "bypass", "error"]
        );
    }

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
