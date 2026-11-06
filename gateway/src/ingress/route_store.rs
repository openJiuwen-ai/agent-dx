use crate::common::route::{RouteCache, RouteInfo};
use arc_swap::ArcSwap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct RouteChange {
    /// Local publication sequence, independent of the backend watch cursor.
    pub revision: u64,
    pub instance_id: String,
    pub previous: Option<RouteInfo>,
    pub current: Option<RouteInfo>,
}

#[derive(Default)]
struct RouteRevisions {
    latest: u64,
    instances: HashMap<String, u64>,
}

/// Shared, atomically replaceable route state used by both the etcd watcher
/// and every serving adapter in the Ingress process.
pub struct RouteStore {
    cache: ArcSwap<RouteCache>,
    // Serialize cache publication and its revision with session admission snapshots.
    revisions: Mutex<RouteRevisions>,
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
            revisions: Mutex::new(RouteRevisions::default()),
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

    /// # Panics
    /// Panics if the local publication sequence is exhausted.
    pub fn replace(&self, cache: RouteCache) {
        let mut revisions = self.revisions.lock().unwrap_or_else(|e| e.into_inner());
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
                self.publish(&mut revisions, instance_id, old, new);
            }
        }
    }

    /// # Panics
    /// Panics if the local publication sequence is exhausted.
    pub fn put(&self, route: RouteInfo) {
        let mut revisions = self.revisions.lock().unwrap_or_else(|e| e.into_inner());
        let previous = self.get(&route.instance_id);
        if previous.as_ref() == Some(&route) {
            return;
        }
        let instance_id = route.instance_id.clone();
        self.cache.load().put(route);
        let current = self.get(&instance_id);
        self.publish(&mut revisions, instance_id, previous, current);
    }

    /// # Panics
    /// Panics if the local publication sequence is exhausted.
    pub fn delete(&self, instance_id: &str) {
        let mut revisions = self.revisions.lock().unwrap_or_else(|e| e.into_inner());
        let Some(previous) = self.get(instance_id) else {
            return;
        };
        let instance_id = previous.instance_id.clone();
        self.cache.load().delete(&instance_id);
        self.publish(&mut revisions, instance_id, Some(previous), None);
    }

    pub fn get(&self, instance_id: &str) -> Option<RouteInfo> {
        self.cache.load().get(instance_id)
    }

    /// Capture a route and the last published change as one admission snapshot.
    pub(super) fn get_with_revision(&self, instance_id: &str) -> Option<(RouteInfo, u64)> {
        let revisions = self.revisions.lock().unwrap_or_else(|e| e.into_inner());
        let route = self.get(instance_id)?;
        let revision = *revisions.instances.get(&route.instance_id)?;
        Some((route, revision))
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
        revisions: &mut RouteRevisions,
        instance_id: String,
        previous: Option<RouteInfo>,
        current: Option<RouteInfo>,
    ) {
        // Exhaustion must fail closed rather than wrap and stop fencing revocations.
        revisions.latest = revisions
            .latest
            .checked_add(1)
            .expect("route revision exhausted");
        if current.is_some() {
            revisions
                .instances
                .insert(instance_id.clone(), revisions.latest);
        } else {
            revisions.instances.remove(&instance_id);
        }
        let _ = self.changes.send(RouteChange {
            revision: revisions.latest,
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
