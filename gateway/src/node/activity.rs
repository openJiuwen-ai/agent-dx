use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "activity-client")]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

const ACTIVITY_SHARD_COUNT: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivitySnapshot {
    pub instance_id: String,
    pub active_stream_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityBatch {
    pub gateway_epoch: String,
    pub timestamp_ms: u64,
    pub activities: Vec<ActivitySnapshot>,
}

impl ActivitySnapshot {
    pub fn new(instance_id: impl Into<String>, active_stream_count: u64) -> Self {
        Self {
            instance_id: instance_id.into(),
            active_stream_count,
        }
    }
}

#[derive(Clone)]
pub struct ActivityTracker {
    gateway_epoch: String,
    count_shards: Arc<Vec<Mutex<HashMap<String, u64>>>>,
    changed: Arc<Notify>,
    last_timestamp_ms: Arc<AtomicU64>,
}

impl ActivityTracker {
    pub fn new(gateway_epoch: impl Into<String>) -> Self {
        Self {
            gateway_epoch: gateway_epoch.into(),
            count_shards: Arc::new(
                (0..ACTIVITY_SHARD_COUNT)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            ),
            changed: Arc::new(Notify::new()),
            last_timestamp_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn activate(&self, instance_id: &str) {
        self.change(instance_id, 1);
    }

    pub fn deactivate(&self, instance_id: &str) {
        self.change(instance_id, -1);
    }

    pub fn snapshot(&self) -> ActivityBatch {
        // Writers hold exactly one shard. Taking every shard in stable order
        // preserves the complete-snapshot contract without serializing the
        // normal activate/deactivate path behind one global mutex.
        let shards = self
            .count_shards
            .iter()
            .map(|shard| shard.lock().expect("activity tracker mutex poisoned"))
            .collect::<Vec<_>>();
        let mut activities = Vec::new();
        for counts in &shards {
            activities.extend(
                counts
                    .iter()
                    .filter(|(_, count)| **count > 0)
                    .map(|(instance_id, count)| ActivitySnapshot::new(instance_id, *count)),
            );
        }
        activities.sort_by(|left, right| left.instance_id.cmp(&right.instance_id));
        ActivityBatch {
            gateway_epoch: self.gateway_epoch.clone(),
            timestamp_ms: self.next_timestamp_ms(),
            activities,
        }
    }

    fn change(&self, instance_id: &str, delta: i64) {
        if instance_id.is_empty() {
            return;
        }
        let mut counts = self
            .count_shard(instance_id)
            .lock()
            .expect("activity tracker mutex poisoned");
        let entry = counts.entry(instance_id.to_owned()).or_insert(0);
        if delta > 0 {
            *entry = entry.saturating_add(delta as u64);
        } else {
            *entry = entry.saturating_sub((-delta) as u64);
        }
        if *entry == 0 {
            counts.remove(instance_id);
        }
        drop(counts);
        self.changed.notify_one();
    }

    fn count_shard(&self, instance_id: &str) -> &Mutex<HashMap<String, u64>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        instance_id.hash(&mut hasher);
        &self.count_shards[hasher.finish() as usize % self.count_shards.len()]
    }

    fn next_timestamp_ms(&self) -> u64 {
        let wall_clock = now_ms();
        self.last_timestamp_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                Some(wall_clock.max(previous.saturating_add(1)))
            })
            .unwrap_or_default()
            .saturating_add(1)
            .max(wall_clock)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Publishes a complete gateway snapshot. The tracker remains authoritative
/// while the UDS is unavailable; the publisher retries later and sends the
/// latest batch, so activity reporting cannot affect the TCP data plane.
#[cfg(feature = "activity-client")]
mod client {
    use super::{ActivityTracker, Duration};
    use hyper_util::rt::TokioIo;
    use std::sync::Arc;
    use tokio::net::UnixStream;
    use tonic::transport::{Channel, Endpoint};
    use tonic::Request;
    use tower::service_fn;

    pub mod proto {
        tonic::include_proto!("data_plane_gateway_activity");
    }
    use proto::data_plane_gateway_activity_service_client::DataPlaneGatewayActivityServiceClient;

    const EVENT_COALESCE_DELAY: Duration = Duration::from_millis(10);
    const DISCONNECTED_RETRY_DELAY: Duration = Duration::from_secs(1);
    const ACTIVITY_RPC_TIMEOUT: Duration = Duration::from_secs(2);

    pub async fn run_activity_publisher(
        tracker: Arc<ActivityTracker>,
        uds_path: String,
        interval: Duration,
    ) {
        let mut client: Option<DataPlaneGatewayActivityServiceClient<Channel>> = None;
        let mut next_report = tokio::time::Instant::now();
        loop {
            let sleep = tokio::time::sleep_until(next_report);
            tokio::pin!(sleep);
            tokio::select! {
                _ = tracker.changed.notified() => {
                    // Event-triggered, with a short bounded buffer that turns
                    // a burst of stream opens/closes into one complete batch.
                    tokio::time::sleep(EVENT_COALESCE_DELAY).await;
                }
                _ = &mut sleep => {}
            }
            let batch = tracker.snapshot();
            if client.is_none() {
                client = connect(&uds_path).await.ok();
            }
            if let Some(current) = client.as_mut() {
                let request = proto::DataPlaneGatewayActivityRequest {
                    gateway_epoch: batch.gateway_epoch,
                    timestamp_ms: batch.timestamp_ms,
                    activities: batch
                        .activities
                        .into_iter()
                        .map(|entry| proto::DataPlaneGatewayActivityEntry {
                            instance_id: entry.instance_id,
                            active_stream_count: entry.active_stream_count,
                        })
                        .collect(),
                };
                if current
                    .report_activity(Request::new(request))
                    .await
                    .is_err()
                {
                    client = None;
                }
            }
            next_report = tokio::time::Instant::now()
                + if client.is_some() {
                    interval
                } else {
                    // Startup reports may be rejected until FunctionSystem has
                    // completed Sync/Recover. Retry promptly without touching
                    // the TCP data plane or discarding the local snapshot.
                    DISCONNECTED_RETRY_DELAY.min(interval)
                };
        }
    }

    async fn connect(
        uds_path: &str,
    ) -> Result<DataPlaneGatewayActivityServiceClient<Channel>, tonic::transport::Error> {
        let path = uds_path.to_owned();
        let endpoint = Endpoint::from_static("http://data-plane-gateway-activity")
            .connect_timeout(ACTIVITY_RPC_TIMEOUT)
            .timeout(ACTIVITY_RPC_TIMEOUT);
        let channel = endpoint
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await?;
        Ok(DataPlaneGatewayActivityServiceClient::new(channel))
    }
}

#[cfg(feature = "activity-client")]
pub use client::run_activity_publisher;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_batched_and_zero_entries_are_omitted() {
        let tracker = ActivityTracker::new("epoch");
        tracker.activate("b");
        tracker.activate("a");
        tracker.deactivate("a");
        let batch = tracker.snapshot();
        assert_eq!(batch.gateway_epoch, "epoch");
        assert_eq!(batch.activities.len(), 1);
        assert_eq!(batch.activities[0], ActivitySnapshot::new("b", 1));
    }

    #[test]
    fn snapshot_timestamp_is_strictly_increasing() {
        let tracker = ActivityTracker::new("epoch");
        let first = tracker.snapshot().timestamp_ms;
        let second = tracker.snapshot().timestamp_ms;
        assert!(second > first);
    }

    #[tokio::test]
    async fn stream_change_emits_publisher_event() {
        let tracker = ActivityTracker::new("epoch");
        let changed = tracker.changed.notified();
        tracker.activate("instance");
        tokio::time::timeout(std::time::Duration::from_millis(50), changed)
            .await
            .expect("stream change must wake the batch publisher");
    }
}
