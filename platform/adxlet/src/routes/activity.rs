//! One proxy subscription per node; only changed executions wake their controllers.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

type Stamp = (String, u64, u64);
type Key = (String, String);
#[derive(Default)]
pub(super) struct ActivityHub {
    slots: std::sync::Mutex<BTreeMap<Key, watch::Sender<Option<Stamp>>>>,
    started: AtomicBool,
}
impl ActivityHub {
    fn subscribe(&self, record: &EnvironmentRecord) -> watch::Receiver<Option<Stamp>> {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        slots
            .entry((record.spec.id.clone(), record.runtime.id.clone()))
            .or_insert_with(|| watch::channel(None).0)
            .subscribe()
    }
    fn disconnect(&self) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        slots.retain(|_, slot| {
            slot.send_replace(None);
            slot.receiver_count() > 0
        });
    }
    fn apply(&self, snapshot: pb::ProxyActivitySnapshot) {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        if snapshot.reset {
            for slot in slots.values() {
                slot.send_replace(None);
            }
        }
        for entry in snapshot.environments {
            let key = (entry.environment_id, entry.runtime_id);
            if entry.removed {
                if let Some(slot) = slots.remove(&key) {
                    slot.send_replace(None);
                }
            } else {
                let value = Some((
                    snapshot.proxy_session_id.clone(),
                    entry.revision,
                    entry.active_streams,
                ));
                let slot = slots.entry(key).or_insert_with(|| watch::channel(None).0);
                slot.send_if_modified(|old| {
                    if *old == value {
                        false
                    } else {
                        *old = value;
                        true
                    }
                });
            }
        }
    }
}
impl UdsRoutes {
    pub(super) fn activity_receiver(
        &self,
        record: &EnvironmentRecord,
    ) -> watch::Receiver<Option<Stamp>> {
        let receiver = self.activity.subscribe(record);
        if !self.activity.started.swap(true, Ordering::AcqRel) {
            let path = self.path.clone();
            let timeout = self.timeout;
            let observations = self.activity.clone();
            let lifetime = Arc::downgrade(&self.local);
            tokio::spawn(async move {
                while lifetime.strong_count() > 0 {
                    let session = async {
                        let mut client = Self::connect_at(path.clone(), timeout).await?;
                        let response = tokio::time::timeout(
                            timeout,
                            client.watch_activity(pb::GetBindingStateRequest {}),
                        )
                        .await
                        .map_err(|_| unavailable("activity subscription timed out"))?
                        .map_err(rpc_error)?;
                        let mut stream = response.into_inner();
                        while let Some(snapshot) = stream.message().await.map_err(rpc_error)? {
                            if lifetime.strong_count() == 0 {
                                return Ok::<(), Error>(());
                            }
                            observations.apply(snapshot);
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = session {
                        adx_observability::warn!(%error, "proxy activity subscription interrupted");
                    }
                    observations.disconnect();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        }
        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deltas_wake_only_the_changed_execution_and_reset_clears_old_observations() {
        let hub = ActivityHub::default();
        let row = |id: &str, revision| pb::ProxyActivity {
            environment_id: id.into(),
            runtime_id: format!("{id}-1"),
            revision,
            active_streams: 0,
            removed: false,
        };
        hub.apply(pb::ProxyActivitySnapshot {
            proxy_session_id: "proxy".into(),
            reset: true,
            environments: vec![row("a", 1), row("b", 1)],
        });
        let slots = hub.slots.lock().unwrap();
        let mut a = slots[&("a".into(), "a-1".into())].subscribe();
        let b = slots[&("b".into(), "b-1".into())].subscribe();
        drop(slots);
        hub.apply(pb::ProxyActivitySnapshot {
            proxy_session_id: "proxy".into(),
            reset: false,
            environments: vec![row("a", 3)],
        });
        assert!(a.has_changed().unwrap());
        assert!(!b.has_changed().unwrap());
        assert_eq!(a.borrow_and_update().as_ref().unwrap().1, 3);
        hub.disconnect();
        assert!(a.borrow().is_none());
        assert!(b.borrow().is_none());
    }
}
