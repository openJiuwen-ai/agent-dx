//! SQLite contains only results awaiting cluster publication. It is never a
//! replacement for the authoritative node catalog or the Ingress routing stream.
use crate::{Durability, StateSink};
use adx_core::{CheckpointArtifact, EnvironmentRecord, Error, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection, TransactionBehavior};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Pending {
    #[serde(flatten)]
    record: EnvironmentRecord,
    #[serde(default)]
    stopped_runtime_ids: Vec<String>,
    #[serde(default)]
    cleanup_artifacts: Vec<CheckpointArtifact>,
}
impl std::ops::Deref for Pending {
    type Target = EnvironmentRecord;
    fn deref(&self) -> &Self::Target {
        &self.record
    }
}
type Entry = (i64, Pending);

pub struct JournalSink {
    path: PathBuf,
    upstream: Arc<dyn StateSink>,
    ready: RwLock<bool>,
    environments: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    database_access: Arc<Mutex<()>>,
}

fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("degradation journal: {error}"))
}

impl JournalSink {
    /// The path is opened lazily after an unavailable cluster commit. A healthy
    /// node with no pending journal does not require a writable local disk.
    pub fn new(path: PathBuf, upstream: Arc<dyn StateSink>) -> Self {
        let ready = !path.exists();
        Self {
            path,
            upstream,
            ready: RwLock::new(ready),
            environments: Mutex::default(),
            database_access: Arc::default(),
        }
    }

    async fn database<T: Send + 'static>(
        &self,
        create: bool,
        work: impl FnOnce(Option<Connection>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        // SQLite has one writer. Queue local work asynchronously instead of
        // consuming its busy timeout with this process's other connections.
        // The blocking task owns the guard so caller cancellation cannot let
        // a second operation overlap SQL that is still executing.
        let access = self.database_access.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _access = access;
            if !create && !path.exists() {
                return work(None);
            }
            if create {
                let parent = path.parent().ok_or_else(|| unavailable("invalid path"))?;
                std::fs::create_dir_all(parent).map_err(unavailable)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .mode(0o600)
                        .open(&path)
                        .map_err(unavailable)?;
                }
            }
            let connection = Connection::open(&path).map_err(unavailable)?;
            connection
                .busy_timeout(std::time::Duration::from_secs(2))
                .map_err(unavailable)?;
            connection
                .execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS pending (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    environment TEXT NOT NULL, payload TEXT NOT NULL);
                 CREATE INDEX IF NOT EXISTS pending_environment ON pending(environment,sequence);",
                )
                .map_err(unavailable)?;
            work(Some(connection))
        })
        .await
        .map_err(unavailable)?
    }

    async fn entries(&self, environment: Option<String>) -> Result<Vec<Entry>> {
        self.database(false, move |connection| {
            let Some(connection) = connection else { return Ok(vec![]); };
            let mut query = connection.prepare(
                "SELECT sequence,payload FROM pending WHERE (?1 IS NULL OR environment=?1) ORDER BY sequence"
            ).map_err(unavailable)?;
            let rows = query.query_map([environment], |row| Ok((row.get::<_,i64>(0)?, row.get::<_,String>(1)?)))
                .map_err(unavailable)?;
            rows.map(|row| {
                let (sequence,payload) = row.map_err(unavailable)?;
                Ok((sequence, serde_json::from_str(&payload).map_err(unavailable)?))
            }).collect()
        }).await
    }

    async fn acknowledge(&self, sequence: i64) -> Result<()> {
        self.database(false, move |connection| {
            let connection = connection
                .ok_or_else(|| unavailable("journal disappeared before acknowledgement"))?;
            connection
                .execute("DELETE FROM pending WHERE sequence=?1", [sequence])
                .map_err(unavailable)?;
            Ok(())
        })
        .await
    }

    async fn append(
        &self,
        record: &EnvironmentRecord,
        stopped_runtime_ids: &[String],
        cleanup_artifacts: &[CheckpointArtifact],
    ) -> Result<Durability> {
        let record = Pending {
            record: record.clone(),
            stopped_runtime_ids: stopped_runtime_ids.to_vec(),
            cleanup_artifacts: cleanup_artifacts.to_vec(),
        };
        self.database(true, move |connection| {
            let mut connection = connection.ok_or_else(|| unavailable("missing database"))?;
            // Reserve the WAL writer before reading the prior record. A deferred
            // read transaction cannot wait its way out of a stale-snapshot
            // write upgrade when another environment has committed meanwhile.
            let tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            let previous = {
                let mut query = tx.prepare("SELECT payload FROM pending WHERE environment=?1 ORDER BY sequence DESC LIMIT 1").map_err(unavailable)?;
                let mut rows = query.query([&record.spec.id]).map_err(unavailable)?;
                rows.next().map_err(unavailable)?.map(|row| row.get::<_,String>(0)).transpose().map_err(unavailable)?
            };
            if let Some(previous) = previous {
                let old: Pending = serde_json::from_str(&previous).map_err(unavailable)?;
                if old == record { return Ok(Durability::Journaled); }
                if old.record != record.record && (old.assignment != record.assignment || old.spec != record.spec || old.revision >= record.revision) {
                    return Err(Error::Conflict);
                }
            }
            let payload = serde_json::to_string(&record).map_err(unavailable)?;
            tx.execute("INSERT INTO pending(environment,payload) VALUES (?1,?2)", params![record.spec.id, payload]).map_err(unavailable)?;
            tx.commit().map_err(unavailable)?;
            // SQLite FULL syncs the WAL; also persist initial directory entries.
            Ok(Durability::Journaled)
        }).await?;
        let parent = self
            .path
            .parent()
            .ok_or_else(|| unavailable("invalid path"))?
            .to_owned();
        tokio::task::spawn_blocking(move || std::fs::File::open(parent).and_then(|f| f.sync_all()))
            .await
            .map_err(unavailable)?
            .map_err(unavailable)?;
        Ok(Durability::Journaled)
    }

    async fn publish_entry(&self, pending: &Pending) -> Result<Durability> {
        if pending.cleanup_artifacts.is_empty() {
            self.upstream
                .commit_releasing(&pending.record, &pending.stopped_runtime_ids)
                .await
        } else {
            self.upstream
                .commit_deleted(
                    &pending.record,
                    &pending.cleanup_artifacts,
                    &pending.stopped_runtime_ids,
                )
                .await
        }
    }

    async fn publish(&self, entries: Vec<Entry>) -> Result<()> {
        for (sequence, record) in entries {
            match self.publish_entry(&record).await {
                Ok(Durability::Published) => (),
                Err(error @ (Error::NotFound | Error::Conflict)) => {
                    // A bounded receipt may expire or the name may be recreated.
                    // Only a fresh authoritative catalog can revoke this entry;
                    // discarding it does not assert that its commit succeeded.
                    if self.upstream.has_current_assignment(&record.record).await? {
                        return Err(error);
                    }
                }
                Ok(Durability::Journaled) => {
                    return Err(unavailable("upstream did not publish result"));
                }
                Err(error) => return Err(error),
            }
            self.acknowledge(sequence).await?;
        }
        Ok(())
    }

    pub async fn pending(&self) -> Result<usize> {
        Ok(self.entries(None).await?.len())
    }

    /// Register the current process with Coordinator, then supply its complete node
    /// catalog. Discard obsolete ownership; replay each remaining result in order.
    /// The caller must fetch a fresh catalog after this before reconciling runtime.
    pub async fn recover(&self, catalog: &[EnvironmentRecord]) -> Result<()> {
        let mut ready = self.ready.write().await;
        *ready = false;
        for (sequence, record) in self.entries(None).await? {
            match catalog.iter().find(|r| r.spec.id == record.spec.id) {
                Some(current)
                    if current.assignment == record.assignment
                        && current.spec == record.spec
                        && (!matches!(
                            current.state,
                            adx_core::EnvironmentState::Failed
                                | adx_core::EnvironmentState::Deleted
                        ) || current.resources_held
                            || current.restart_pending
                            || !record.stopped_runtime_ids.is_empty()) =>
                {
                    if current.revision == record.revision && current != &record.record {
                        return Err(Error::Conflict);
                    }
                    if (current.revision < record.revision
                        || current.revision == record.revision
                            && !record.stopped_runtime_ids.is_empty())
                        && self.publish_entry(&record).await? != Durability::Published
                    {
                        return Err(unavailable("recovery result is not published"));
                    }
                }
                _ => (), // The authoritative catalog no longer grants this ownership.
            }
            self.acknowledge(sequence).await?;
        }
        *ready = true;
        Ok(())
    }

    /// A live, already reconciled node may retry publication after a heartbeat.
    pub async fn flush(&self) -> Result<()> {
        let ready = self.ready.write().await;
        if !*ready {
            return Err(unavailable("authoritative recovery required"));
        }
        self.publish(self.entries(None).await?).await
    }
}

#[async_trait]
impl StateSink for JournalSink {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        self.commit_releasing(record, &[]).await
    }

    async fn commit_deleted(
        &self,
        record: &EnvironmentRecord,
        cleanup_artifacts: &[CheckpointArtifact],
        stopped_runtime_ids: &[String],
    ) -> Result<Durability> {
        self.commit_with_details(record, stopped_runtime_ids, cleanup_artifacts)
            .await
    }

    async fn prepare_execution(
        &self,
        record: &EnvironmentRecord,
        previous_runtime_id: &str,
    ) -> Result<adx_core::EnvironmentSpec> {
        if !record
            .spec
            .execution
            .mounts
            .iter()
            .any(|mount| matches!(mount.source, adx_core::execution::MountSource::Volume(_)))
        {
            return Ok(record.spec.clone());
        }
        let ready = self.ready.read().await;
        if !*ready {
            return Err(unavailable("authoritative recovery required"));
        }
        let serial = self
            .environments
            .lock()
            .await
            .entry(record.spec.id.clone())
            .or_default()
            .clone();
        let _serial = serial.lock().await;
        self.publish(self.entries(Some(record.spec.id.clone())).await?)
            .await?;
        self.upstream
            .prepare_execution(record, previous_runtime_id)
            .await
    }

    async fn commit_releasing(
        &self,
        record: &EnvironmentRecord,
        stopped_runtime_ids: &[String],
    ) -> Result<Durability> {
        self.commit_with_details(record, stopped_runtime_ids, &[])
            .await
    }
}

impl JournalSink {
    async fn commit_with_details(
        &self,
        record: &EnvironmentRecord,
        stopped_runtime_ids: &[String],
        cleanup_artifacts: &[CheckpointArtifact],
    ) -> Result<Durability> {
        let ready = self.ready.read().await;
        if !*ready {
            return Err(unavailable("authoritative recovery required"));
        }
        let serial = self
            .environments
            .lock()
            .await
            .entry(record.spec.id.clone())
            .or_default()
            .clone();
        let _serial = serial.lock().await;
        let pending = self.entries(Some(record.spec.id.clone())).await?;
        if let Some((_, latest)) = pending.last() {
            if (&latest.record != record
                || latest.stopped_runtime_ids != stopped_runtime_ids
                || latest.cleanup_artifacts != cleanup_artifacts)
                && (latest.assignment != record.assignment
                    || latest.spec != record.spec
                    || latest.revision >= record.revision)
            {
                return Err(Error::Conflict);
            }
        }
        let result = self.publish(pending).await;
        match result {
            Ok(()) => match self
                .publish_entry(&Pending {
                    record: record.clone(),
                    stopped_runtime_ids: stopped_runtime_ids.to_vec(),
                    cleanup_artifacts: cleanup_artifacts.to_vec(),
                })
                .await
            {
                Ok(Durability::Published) => Ok(Durability::Published),
                Err(Error::Unavailable(_)) | Ok(Durability::Journaled) => {
                    self.append(record, stopped_runtime_ids, cleanup_artifacts)
                        .await
                }
                Err(error) => Err(error),
            },
            Err(Error::Unavailable(_)) => {
                self.append(record, stopped_runtime_ids, cleanup_artifacts)
                    .await
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Published;

    #[async_trait]
    impl StateSink for Published {
        async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
            Ok(Durability::Published)
        }
    }

    #[tokio::test]
    async fn cancelled_database_caller_does_not_release_running_blocking_work() {
        let directory = tempfile::tempdir().unwrap();
        let sink = Arc::new(JournalSink::new(
            directory.path().join("journal.sqlite"),
            Arc::new(Published),
        ));
        let (started, running) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let first_sink = sink.clone();
        let first = tokio::spawn(async move {
            first_sink
                .database(true, move |_connection| {
                    started.send(()).unwrap();
                    blocked.recv().unwrap();
                    Ok(())
                })
                .await
        });
        running.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let mut second = tokio::spawn(async move { sink.database(false, |_| Ok(())).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut second)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        second.await.unwrap().unwrap();
    }
}
