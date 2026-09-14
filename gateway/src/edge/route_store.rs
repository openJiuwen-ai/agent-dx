use crate::common::route::{RouteCache, RouteInfo};
use arc_swap::ArcSwap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct RouteChange {
    pub instance_id: String,
    pub previous: Option<RouteInfo>,
    pub current: Option<RouteInfo>,
}

/// Shared, atomically replaceable route state used by both the etcd watcher
/// and every serving adapter in the Edge process.
pub struct RouteStore {
    cache: ArcSwap<RouteCache>,
    ready: AtomicBool,
    changes: broadcast::Sender<RouteChange>,
    watch_revision: AtomicI64,
    last_watch_update_ms: AtomicU64,
    point_get_total: AtomicU64,
}

impl Default for RouteStore {
    fn default() -> Self {
        let (changes, _) = broadcast::channel(1024);
        Self {
            cache: ArcSwap::from_pointee(RouteCache::default()),
            ready: AtomicBool::new(false),
            changes,
            watch_revision: AtomicI64::new(0),
            last_watch_update_ms: AtomicU64::new(0),
            point_get_total: AtomicU64::new(0),
        }
    }
}

impl RouteStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn replace(&self, cache: RouteCache) {
        let previous = self.cache.load_full();
        let previous_entries = previous.entries();
        let current_entries = cache.entries();
        self.cache.store(Arc::new(cache));
        self.ready.store(true, Ordering::Release);
        let instance_ids = previous_entries
            .keys()
            .chain(current_entries.keys())
            .cloned()
            .collect::<HashSet<_>>();
        for instance_id in instance_ids {
            let old = previous_entries.get(&instance_id).cloned();
            let new = current_entries.get(&instance_id).cloned();
            if old != new {
                self.publish(instance_id, old, new);
            }
        }
    }

    pub fn put(&self, route: RouteInfo) {
        let previous = self.get(&route.instance_id);
        if previous.as_ref() == Some(&route) {
            return;
        }
        let instance_id = route.instance_id.clone();
        self.cache.load().put(route);
        let current = self.get(&instance_id);
        self.publish(instance_id, previous, current);
    }

    pub fn delete(&self, instance_id: &str) {
        let previous = self.get(instance_id);
        if previous.is_none() {
            return;
        }
        self.cache.load().delete(instance_id);
        self.publish(instance_id.to_owned(), previous, None);
    }

    pub fn get(&self, instance_id: &str) -> Option<RouteInfo> {
        self.cache.load().get(instance_id)
    }

    pub fn len(&self) -> usize {
        self.cache.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RouteChange> {
        self.changes.subscribe()
    }

    pub fn record_watch_revision(&self, revision: i64) {
        self.watch_revision.store(revision, Ordering::Relaxed);
        self.last_watch_update_ms
            .store(unix_timestamp_ms(), Ordering::Relaxed);
    }

    pub fn record_point_get(&self) {
        self.point_get_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn watch_revision(&self) -> i64 {
        self.watch_revision.load(Ordering::Relaxed)
    }

    pub fn watch_lag_seconds(&self) -> u64 {
        let last = self.last_watch_update_ms.load(Ordering::Relaxed);
        if last == 0 {
            return 0;
        }
        unix_timestamp_ms().saturating_sub(last) / 1000
    }

    pub fn point_get_total(&self) -> u64 {
        self.point_get_total.load(Ordering::Relaxed)
    }

    fn publish(
        &self,
        instance_id: String,
        previous: Option<RouteInfo>,
        current: Option<RouteInfo>,
    ) {
        let _ = self.changes.send(RouteChange {
            instance_id,
            previous,
            current,
        });
    }

    /// Test and embedded-mode hook. Production readiness is set by the initial
    /// revisioned etcd list through `replace`.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
