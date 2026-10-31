//! Commit-backed publication state. Redis is read only at startup or to resolve an
//! ambiguous write; list/watch recovery never waits for a globally quiet writer.
use super::*;
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublicationValue {
    Node(String, Option<Arc<StoredNode>>),
    Environment(String, Option<Arc<StoredEnvironment>>),
}
impl PublicationValue {
    pub(super) fn decode(field: &str, raw: Option<&str>) -> Result<Option<Self>> {
        if let Some(id) = field.strip_prefix("environment:") {
            Ok(Some(Self::Environment(
                id.into(),
                raw.map(decode).transpose()?.map(Arc::new),
            )))
        } else if let Some(id) = field.strip_prefix("node:") {
            Ok(Some(Self::Node(
                id.into(),
                raw.map(decode).transpose()?.map(Arc::new),
            )))
        } else {
            Ok(None)
        }
    }
}
#[derive(Default)]
pub(super) struct Catalog {
    pub(super) epoch: u64,
    revision: u64,
    pub(super) nodes: BTreeMap<String, Arc<StoredNode>>,
    environments: BTreeMap<String, Arc<StoredEnvironment>>,
    dirty: BTreeSet<String>,
    applied_revision: u64,
    catching_up: bool,
    resets: [u64; 5],
    skipped: u64,
}
#[derive(Clone)]
pub(crate) struct PublicationSnapshot {
    pub revision: u64,
    pub nodes: BTreeMap<String, Arc<StoredNode>>,
    pub environments: BTreeMap<String, Arc<StoredEnvironment>>,
}

/// Per-object serialization covers the durable write and its in-memory commit.
/// Different objects continue to write concurrently. Cancellation leaves a bounded
/// repair set instead of pretending a possibly applied write never happened.
pub(super) struct PublicationWrite {
    store: RedisStore,
    epoch: u64,
    fields: Vec<String>,
    _guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    completed: bool,
}
impl PublicationWrite {
    pub(super) fn finish(self, applied: bool, values: Vec<PublicationValue>) {
        self.finish_with_fields(applied, values, vec![]);
    }
    pub(super) fn finish_with_fields(
        mut self,
        applied: bool,
        values: Vec<PublicationValue>,
        fields: Vec<String>,
    ) {
        if applied {
            self.store
                .publish_values(self.epoch, values, &self.fields, fields);
        }
        self.completed = true;
    }
}
impl Drop for PublicationWrite {
    fn drop(&mut self) {
        if !self.completed && !self.fields.is_empty() {
            let mut catalog = self
                .store
                .connection
                .catalog
                .lock()
                .expect("publication catalog poisoned");
            if catalog.epoch == self.epoch {
                catalog.dirty.extend(self.fields.iter().cloned());
                self.store
                    .send_publication(&mut catalog, vec![], true, vec![]);
            }
        }
        self._guards.clear();
        let mut registry = self
            .store
            .connection
            .publication_locks
            .lock()
            .expect("publication lock registry poisoned");
        for field in &self.fields {
            if registry
                .get(field)
                .is_some_and(|lock| lock.strong_count() == 0)
            {
                registry.remove(field);
            }
        }
    }
}
impl RedisStore {
    pub(super) async fn publication_write(
        &self,
        epoch: u64,
        fields: Vec<String>,
    ) -> PublicationWrite {
        let fields: Vec<_> = fields
            .into_iter()
            .filter(|f| f.starts_with("node:") || f.starts_with("environment:"))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let locks: Vec<_> = {
            let mut registry = self
                .connection
                .publication_locks
                .lock()
                .expect("publication lock registry poisoned");
            fields
                .iter()
                .map(|field| {
                    let lock = registry
                        .get(field)
                        .and_then(std::sync::Weak::upgrade)
                        .unwrap_or_else(|| Arc::new(Mutex::new(())));
                    registry.insert(field.clone(), Arc::downgrade(&lock));
                    lock
                })
                .collect()
        };
        let mut write = PublicationWrite {
            store: self.clone(),
            epoch,
            fields,
            _guards: Vec::with_capacity(locks.len()),
            completed: true,
        };
        for lock in locks {
            write._guards.push(lock.lock_owned().await);
        }
        write.completed = false;
        write
    }
    pub(super) fn send_publication(
        &self,
        catalog: &mut Catalog,
        values: Vec<PublicationValue>,
        dirty: bool,
        fields: Vec<String>,
    ) {
        catalog.revision += 1;
        self.connection
            .publication_sequence
            .store(catalog.revision, Ordering::Release);
        // Updating the catalog, allocating its cursor and enqueueing the event
        // share one short lock, including on a multi-thread runtime.
        let _ = self.connection.committed_changes.send(ControlChange {
            revision: catalog.revision,
            values,
            dirty,
            fields,
            epoch: catalog.epoch,
        });
    }
    pub(super) fn publish_values(
        &self,
        epoch: u64,
        values: Vec<PublicationValue>,
        repaired: &[String],
        mut fields: Vec<String>,
    ) {
        let mut catalog = self
            .connection
            .catalog
            .lock()
            .expect("publication catalog poisoned");
        if catalog.epoch != epoch {
            return;
        }
        for value in &values {
            match value {
                PublicationValue::Node(id, Some(node)) => {
                    catalog.nodes.insert(id.clone(), node.clone());
                    self.connection
                        .live_nodes
                        .write()
                        .expect("node overlay invariant")
                        .insert(id.clone(), node.as_ref().clone());
                }
                PublicationValue::Node(id, None) => {
                    catalog.nodes.remove(id);
                    self.connection
                        .live_nodes
                        .write()
                        .expect("node overlay invariant")
                        .remove(id);
                }
                PublicationValue::Environment(id, Some(environment)) => {
                    catalog.environments.insert(id.clone(), environment.clone());
                }
                PublicationValue::Environment(id, None) => {
                    catalog.environments.remove(id);
                }
            }
        }
        for field in repaired {
            catalog.dirty.remove(field);
        }
        if !values.is_empty() || !fields.is_empty() {
            fields.extend(values.iter().map(|v| match v {
                PublicationValue::Node(id, _) => format!("node:{id}"),
                PublicationValue::Environment(id, _) => format!("environment:{id}"),
            }));
            self.send_publication(&mut catalog, values, false, fields);
        }
    }
    pub(super) fn publication_values<'a>(
        fields: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    ) -> Result<Vec<PublicationValue>> {
        fields
            .into_iter()
            .filter_map(|(field, raw)| PublicationValue::decode(field, raw).transpose())
            .collect()
    }
    pub(super) fn seed_publication(&self, epoch: u64, saved: StoredSnapshot) {
        let mut catalog = self
            .connection
            .catalog
            .lock()
            .expect("publication catalog poisoned");
        let revision = self
            .connection
            .publication_sequence
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        *catalog = Catalog {
            epoch,
            revision,
            nodes: saved
                .nodes
                .into_iter()
                .map(|(id, n)| (id, Arc::new(n)))
                .collect(),
            environments: saved
                .environments
                .into_iter()
                .map(|(id, e)| (id, Arc::new(e)))
                .collect(),
            dirty: BTreeSet::new(),
            applied_revision: 0,
            catching_up: true,
            resets: [0; 5],
            skipped: 0,
        };
    }
}
/// Fixed labels only; no Environment, request or runtime IDs enter these metrics.
#[derive(Clone, Copy)]
pub(crate) enum PublicationReset {
    Startup,
    ReceiveLag,
    BatchLag,
    Discontinuity,
    AmbiguousWrite,
}
impl PublicationReset {
    fn index(self) -> usize {
        self as usize
    }
}
impl Session {
    pub(crate) fn publication_reset(&self, reason: PublicationReset, skipped: u64) {
        let mut c = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog invariant");
        c.catching_up = true;
        c.resets[reason.index()] = c.resets[reason.index()].saturating_add(1);
        c.skipped = c.skipped.saturating_add(skipped);
    }
    pub(crate) fn publication_applied(&self, revision: u64) {
        let mut c = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog invariant");
        if c.epoch == self.epoch {
            c.applied_revision = revision;
            c.catching_up = false;
        }
    }
    pub(crate) fn publication_metrics(&self) -> Result<String> {
        let c = self
            .store
            .connection
            .catalog
            .lock()
            .map_err(|_| Error::Unavailable("publication catalog unavailable".into()))?;
        let mut text = adx_observability::metrics::Text::default();
        text.gauge(
            "adx_coordinator_publication_catching_up",
            &[],
            u64::from(c.catching_up),
        );
        text.gauge(
            "adx_coordinator_publication_lag_events",
            &[],
            c.revision.saturating_sub(c.applied_revision),
        );
        text.gauge(
            "adx_coordinator_publication_ambiguous_objects",
            &[],
            c.dirty.len() as u64,
        );
        let mut output = text.finish();
        output.push_str("# TYPE adx_coordinator_publication_resets_total counter\n");
        for (reason, count) in [
            "startup",
            "receive_lag",
            "batch_lag",
            "discontinuity",
            "ambiguous_write",
        ]
        .into_iter()
        .zip(c.resets)
        {
            output.push_str(&format!(
                "adx_coordinator_publication_resets_total{{reason=\"{reason}\"}} {count}\n"
            ));
        }
        output.push_str(&format!("# TYPE adx_coordinator_publication_skipped_events_total counter\nadx_coordinator_publication_skipped_events_total {}\n", c.skipped));
        Ok(output)
    }

    pub(crate) fn publication_ready(&self) -> bool {
        let catalog = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog invariant");
        catalog.epoch == self.epoch && catalog.dirty.is_empty()
    }
    pub(crate) fn publication_snapshot(
        &self,
    ) -> Result<(PublicationSnapshot, broadcast::Receiver<ControlChange>)> {
        let catalog = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog poisoned");
        if catalog.epoch != self.epoch || !catalog.dirty.is_empty() {
            return Err(Error::Unavailable(
                "publication awaiting ambiguous write resolution".into(),
            ));
        }
        let receiver = self.store.connection.committed_changes.subscribe();
        Ok((
            PublicationSnapshot {
                revision: catalog.revision,
                nodes: catalog.nodes.clone(),
                environments: catalog.environments.clone(),
            },
            receiver,
        ))
    }
    pub(crate) async fn repair_publication(&self) -> Result<()> {
        // Repair only ambiguous objects, in bounded batches. Successful normal
        // commits never issue these reads and continuous unrelated writes cannot
        // invalidate this object-locked repair.
        let fields: Vec<_> = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog poisoned")
            .dirty
            .iter()
            .take(128)
            .cloned()
            .collect();
        if fields.is_empty() {
            return Ok(());
        }
        let write = self
            .store
            .publication_write(self.epoch, fields.clone())
            .await;
        let mut requested = vec![HEADER.to_owned()];
        requested.extend(fields.clone());
        let raw = self.store.fields_vec(&requested).await?;
        self.header(&raw[0])?;
        let values = RedisStore::publication_values(
            fields
                .iter()
                .zip(raw[1..].iter())
                .map(|(f, v)| (f.as_str(), v.as_deref())),
        )?;
        write.finish(true, values);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn session() -> Session {
        let (changes, _) = broadcast::channel(1024);
        let store = RedisStore {
            key: "publication-unit".into(),
            connection: Arc::new(Connection {
                client: redis::Client::open("redis://127.0.0.1/").unwrap(),
                current: Mutex::new(None),
                timeout: Duration::from_secs(1),
                committed_changes: changes,
                publication_sequence: AtomicU64::new(0),
                active_epoch: AtomicU64::new(1),
                live_nodes: Default::default(),
                artifact_locks: Default::default(),
                publication_locks: Default::default(),
                catalog: Default::default(),
            }),
        };
        store.seed_publication(
            1,
            StoredSnapshot {
                shard_count: 1,
                generation: 0,
                revision: 0,
                nodes: BTreeMap::new(),
                environments: BTreeMap::new(),
            },
        );
        Session {
            store,
            epoch: 1,
            inspection: Arc::new(Mutex::new(None)),
            generations: Arc::new(AtomicU64::new(1_u64 << 32)),
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_commits_snapshot_and_events_have_one_order_and_release_key_locks() {
        let session = session();
        let (snapshot, mut receiver) = session.publication_snapshot().unwrap();
        let mut tasks = Vec::new();
        for task in 0..16 {
            let store = session.store.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..32 {
                    let id = format!("node-{}", task % 4);
                    let write = store.publication_write(1, vec![format!("node:{id}")]).await;
                    write.finish(true, vec![PublicationValue::Node(id, None)]);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let mut revision = snapshot.revision;
        for _ in 0..512 {
            let event = receiver.recv().await.unwrap();
            revision += 1;
            assert_eq!(event.revision, revision);
            assert_eq!(event.epoch, 1);
            assert!(!event.dirty);
        }
        let (final_snapshot, mut next_receiver) = session.publication_snapshot().unwrap();
        assert_eq!(final_snapshot.revision, revision);
        assert!(matches!(
            next_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert!(session
            .store
            .connection
            .publication_locks
            .lock()
            .unwrap()
            .is_empty());
        // A snapshot has captured every event at/below its watermark. Every later
        // commit must be delivered by the receiver captured in the same lock.
        let write = session
            .store
            .publication_write(1, vec!["node:next".into()])
            .await;
        write.finish(true, vec![PublicationValue::Node("next".into(), None)]);
        assert_eq!(next_receiver.recv().await.unwrap().revision, revision + 1);
    }
    #[tokio::test]
    async fn cancellation_marks_only_touched_objects_and_old_epoch_cannot_publish() {
        let session = session();
        let write = session
            .store
            .publication_write(1, vec!["environment:unknown".into()])
            .await;
        drop(write);
        assert!(!session.publication_ready());
        assert_eq!(
            session.store.connection.catalog.lock().unwrap().dirty.len(),
            1
        );
        assert!(session
            .store
            .connection
            .publication_locks
            .lock()
            .unwrap()
            .is_empty());
        session.store.seed_publication(
            2,
            StoredSnapshot {
                shard_count: 1,
                generation: 0,
                revision: 0,
                nodes: BTreeMap::new(),
                environments: BTreeMap::new(),
            },
        );
        let before = session
            .store
            .connection
            .publication_sequence
            .load(Ordering::Acquire);
        session.store.publish_values(
            1,
            vec![PublicationValue::Environment("unknown".into(), None)],
            &[],
            vec![],
        );
        assert_eq!(
            session
                .store
                .connection
                .publication_sequence
                .load(Ordering::Acquire),
            before
        );
        assert!(session.publication_snapshot().is_err());
    }
    #[tokio::test]
    async fn directory_delta_and_volume_wakeup_share_one_atomic_cursor() {
        let session = session();
        let (before, mut receiver) = session.publication_snapshot().unwrap();
        let write = session
            .store
            .publication_write(1, vec!["environment:removed".into()])
            .await;
        write.finish_with_fields(
            true,
            vec![PublicationValue::Environment("removed".into(), None)],
            vec!["volume:shared".into()],
        );
        let event = receiver.recv().await.unwrap();
        assert_eq!(event.revision, before.revision + 1);
        assert!(event.fields.iter().any(|f| f == "volume:shared"));
        assert!(event.fields.iter().any(|f| f == "environment:removed"));
        assert_eq!(event.values.len(), 1);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
}
