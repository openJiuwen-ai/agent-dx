//! Background observations and the latest values used by node reports.
use adx_core::{volumes::StorageMount, Error, Result};
use adx_protocol::control as pb;
use adxlet::{resources::Observation, Adxlet};
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

#[derive(Clone)]
pub struct Snapshot {
    pub capacity: Observation,
    pub storage_mounts: BTreeMap<String, StorageMount>,
    pub runtime_classes: Vec<String>,
}
impl Snapshot {
    pub fn new(capacity: Observation) -> Self {
        Self {
            capacity,
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

/// Publish the runtime inventory and verified storage roots used by admission.
pub fn publish_capabilities(
    manager: &Adxlet,
    observed: &watch::Sender<Snapshot>,
    mounts: BTreeMap<String, StorageMount>,
    classes: Result<Vec<String>>,
) -> Result<()> {
    let classes = match classes {
        Ok(classes) => {
            manager.update_runtime_classes(classes.clone())?;
            Some(classes)
        }
        Err(error) => {
            adx_observability::warn!(%error, "runtime capability observation failed; keeping the last successful inventory");
            None
        }
    };
    observed.send_modify(|snapshot| {
        snapshot.storage_mounts = mounts;
        if let Some(classes) = classes {
            snapshot.runtime_classes = classes;
        }
    });
    Ok(())
}

/// Close sandboxd admission after both the failure count and duration are met.
pub struct SandboxdHealth {
    failure_threshold: u32,
    failure_duration: Duration,
    consecutive_failures: u32,
    failing_since: Option<Instant>,
}

impl SandboxdHealth {
    pub fn new(failure_threshold: u32, failure_duration: Duration) -> Result<Self> {
        if failure_threshold == 0 || failure_duration.is_zero() {
            return Err(Error::Invalid(
                "sandboxd health failure threshold and duration must be positive".into(),
            ));
        }
        Ok(Self {
            failure_threshold,
            failure_duration,
            consecutive_failures: 0,
            failing_since: None,
        })
    }

    pub fn update(&mut self, manager: &Adxlet, healthy: bool) -> Result<()> {
        if healthy {
            self.consecutive_failures = 0;
            self.failing_since = None;
            manager.set_sandboxd_healthy(true)
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            let since = self.failing_since.get_or_insert_with(Instant::now);
            manager.set_sandboxd_healthy(
                self.consecutive_failures < self.failure_threshold
                    || since.elapsed() < self.failure_duration,
            )
        }
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
