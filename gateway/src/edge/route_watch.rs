#![cfg(feature = "etcd-watch")]

use super::resolver::{PointGetError, RoutePointGetter};
use super::route_store::RouteStore;
use crate::common::route::{is_full_route_key, route_key_id, RouteCache, RouteInfo, ROUTE_PREFIX};
use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, EventType, GetOptions, WatchOptions};
use std::collections::HashMap;
use std::sync::Arc;

struct PendingPointGet {
    result: tokio::sync::watch::Sender<Option<Result<Option<RouteInfo>, String>>>,
}

/// RouteWatcher performs the initial list and then watches the exact
/// /adx/route/business/adxk prefix. A compaction or stream failure causes a
/// bounded reconnect and a fresh atomic cache replacement.
pub struct RouteWatcher {
    store: Arc<RouteStore>,
    endpoints: Vec<String>,
    connect_options: ConnectOptions,
    point_gets: tokio::sync::Mutex<HashMap<String, Arc<PendingPointGet>>>,
}

impl RouteWatcher {
    pub fn new(
        store: Arc<RouteStore>,
        endpoints: Vec<String>,
        connect_options: ConnectOptions,
    ) -> Self {
        Self {
            store,
            endpoints,
            connect_options,
            point_gets: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Cache-miss fallback used by the Edge hot path. Concurrent misses for
    /// one instance share one etcd point-get and are released after the
    /// result is applied to the cache.
    async fn resolve_with_point_get(
        &self,
        instance_id: &str,
    ) -> Result<Option<RouteInfo>, PointGetError> {
        if let Some(route) = self.store.get(instance_id) {
            return Ok(Some(route));
        }
        let key = format!("{ROUTE_PREFIX}/{instance_id}");
        let (leader, pending) = {
            let mut pending = self.point_gets.lock().await;
            if let Some(existing) = pending.get(&key) {
                (false, existing.clone())
            } else {
                let (result, _) = tokio::sync::watch::channel(None);
                let created = Arc::new(PendingPointGet { result });
                pending.insert(key.clone(), created.clone());
                (true, created)
            }
        };
        if !leader {
            let mut result = pending.result.subscribe();
            loop {
                if let Some(value) = result.borrow().clone() {
                    return value.map_err(|_| PointGetError::CoalescedFailure);
                }
                if result.changed().await.is_err() {
                    return Err(PointGetError::CoalescedFailure);
                }
            }
        }
        self.store.record_point_get();
        let result: Result<Option<RouteInfo>, PointGetError> = async {
            let mut client = Client::connect(&self.endpoints, Some(self.connect_options.clone()))
                .await
                .map_err(|error| PointGetError::Backend(error.to_string()))?;
            let response = client
                .get(key.as_str(), None)
                .await
                .map_err(|error| PointGetError::Backend(error.to_string()))?;
            let route = match response.kvs().first() {
                Some(kv) => Some(serde_json::from_slice::<RouteInfo>(kv.value()).map_err(
                    |error| PointGetError::Backend(format!("invalid RouteInfo at {key}: {error}")),
                )?),
                None => None,
            };
            if let Some(route) = route.clone() {
                self.store.put(route);
            }
            Ok(route)
        }
        .await;
        let published = result
            .as_ref()
            .map(|route| route.clone())
            .map_err(|error| error.to_string());
        let _ = pending.result.send(Some(published));
        self.point_gets.lock().await.remove(&key);
        result
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.store.mark_not_ready();
            if let Err(error) = self.watch_once().await {
                tracing::warn!(%error, "route watcher stopped; reconnecting");
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    async fn watch_once(&self) -> Result<(), etcd_client::Error> {
        let mut client =
            Client::connect(&self.endpoints, Some(self.connect_options.clone())).await?;
        let response = client
            .get(ROUTE_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?;
        let start_revision = response.header().map(|h| h.revision()).unwrap_or_default() + 1;
        let fresh = RouteCache::default();
        for kv in response.kvs() {
            self.apply_put(&fresh, kv.key_str().unwrap_or_default(), kv.value());
        }
        self.store.replace(fresh);
        self.store.record_watch_revision(start_revision - 1);
        let (_watcher, mut stream) = client
            .watch(
                ROUTE_PREFIX,
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_start_revision(start_revision),
                ),
            )
            .await?;
        while let Some(response) = stream.message().await? {
            if response.compact_revision() != 0 {
                return Ok(());
            }
            if let Some(header) = response.header() {
                self.store.record_watch_revision(header.revision());
            }
            for event in response.events() {
                let Some(kv) = event.kv() else { continue };
                let key = kv.key_str().unwrap_or_default();
                if !is_full_route_key(key) {
                    continue;
                }
                match event.event_type() {
                    EventType::Put => self.apply_put_to_store(key, kv.value()),
                    EventType::Delete => self.store.delete(route_key_id(key)),
                }
            }
        }
        Ok(())
    }

    fn apply_put(&self, cache: &RouteCache, key: &str, value: &[u8]) {
        if !is_full_route_key(key) {
            return;
        }
        match serde_json::from_slice::<RouteInfo>(value) {
            Ok(route) if !route.instance_id.is_empty() => cache.put(route),
            _ => cache.delete(route_key_id(key)),
        }
    }

    fn apply_put_to_store(&self, key: &str, value: &[u8]) {
        if !is_full_route_key(key) {
            return;
        }
        match serde_json::from_slice::<RouteInfo>(value) {
            Ok(route) if !route.instance_id.is_empty() => self.store.put(route),
            _ => self.store.delete(route_key_id(key)),
        }
    }
}

#[async_trait]
impl RoutePointGetter for RouteWatcher {
    async fn point_get(&self, instance_id: &str) -> Result<Option<RouteInfo>, PointGetError> {
        self.resolve_with_point_get(instance_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_key_filter() {
        assert!(is_full_route_key("/adx/route/business/adxk/i"));
        assert!(!is_full_route_key("/adx/route/business/adxk"));
        assert!(!is_full_route_key("/adx/route/business/adxk/i/extra"));
    }
}
