//! Background observations and the latest values used by node reports.
use adx_core::{volumes::StorageMount, Result};
use adx_protocol::control as pb;
use adxlet::resources::Observation;
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};

#[derive(Clone)]
pub struct Snapshot {
    pub capacity: Observation,
    pub valid_until: Instant,
    pub storage_mounts: BTreeMap<String, StorageMount>,
    pub runtime_classes: Vec<String>,
}
impl Snapshot {
    pub fn new(capacity: Observation, valid: Duration) -> Self {
        Self {
            capacity,
            valid_until: Instant::now() + valid,
            storage_mounts: BTreeMap::new(),
            runtime_classes: vec![],
        }
    }
    pub fn apply(&self, report: &mut pb::RegisterNodeRequest) {
        report.capacity = Some(self.capacity.capacity.into());
        report.devices = self
            .capacity
            .devices
            .iter()
            .cloned()
            .map(Into::into)
            .collect();
        report.storage_mounts = self
            .storage_mounts
            .iter()
            .map(|(id, mount)| (id.clone(), mount.clone().into()))
            .collect();
        report.runtime_classes.clone_from(&self.runtime_classes);
    }
}

/// Keep one sample in flight, publishing each completed round before the next.
pub async fn observe<T, F, S, P>(period: Duration, mut sample: S, mut publish: P) -> Result<()>
where
    F: Future<Output = T>,
    S: FnMut() -> F,
    P: FnMut(T) -> Result<()>,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        publish(sample().await)?;
    }
}

/// Poll observations alongside services, including throughout Relay drain.
pub async fn serve<E>(
    services: impl Future<Output = std::result::Result<(), E>>,
    observations: impl Future<Output = std::result::Result<(), E>>,
) -> std::result::Result<(), E> {
    tokio::select! {
        result = services => result,
        result = observations => result,
    }
}
