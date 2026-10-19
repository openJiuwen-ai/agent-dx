//! SQLite contains only results awaiting cluster publication. It is never a
//! replacement for the authoritative node catalog or the Ingress routing stream.
use crate::{Durability, StateSink};
use adx_core::{CheckpointArtifact, EnvironmentRecord, Error, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};

type Entry = (i64, PendingEntry);

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct PendingEntry {
    record: EnvironmentRecord,
    #[serde(default)]
    cleanup_artifacts: Vec<CheckpointArtifact>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum StoredEntry {
    Current(PendingEntry),
    Legacy(EnvironmentRecord),
}

impl StoredEntry {
    fn into_pending(self) -> PendingEntry {
        match self {
            Self::Current(entry) => entry,
            Self::Legacy(record) => PendingEntry {
                record,
                cleanup_artifacts: Vec::new(),
            },
        }
    }
}

pub struct JournalSink {
    path: PathBuf,
    upstream: Arc<dyn StateSink>,
    ready: RwLock<bool>,
    environments: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
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
        }
    }

    async fn database<T: Send + 'static>(
        &self,
        create: bool,
        work: impl FnOnce(Option<Connection>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
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
                let entry = serde_json::from_str::<StoredEntry>(&payload)
                    .map_err(unavailable)?
                    .into_pending();
                Ok((sequence, entry))
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
        cleanup_artifacts: &[CheckpointArtifact],
    ) -> Result<Durability> {
        let entry = PendingEntry {
            record: record.clone(),
            cleanup_artifacts: cleanup_artifacts.to_vec(),
        };
        self.database(true, move |connection| {
            let mut connection = connection.ok_or_else(|| unavailable("missing database"))?;
            let tx = connection.transaction().map_err(unavailable)?;
            let previous = {
                let mut query = tx.prepare("SELECT payload FROM pending WHERE environment=?1 ORDER BY sequence DESC LIMIT 1").map_err(unavailable)?;
                let mut rows = query.query([&entry.record.spec.id]).map_err(unavailable)?;
                rows.next().map_err(unavailable)?.map(|row| row.get::<_,String>(0)).transpose().map_err(unavailable)?
            };
            if let Some(previous) = previous {
                let old = serde_json::from_str::<StoredEntry>(&previous)
                    .map_err(unavailable)?
                    .into_pending();
                if old == entry { return Ok(Durability::Journaled); }
                if old.record.assignment != entry.record.assignment
                    || old.record.spec != entry.record.spec
                    || old.record.revision >= entry.record.revision
                {
                    return Err(Error::Conflict);
                }
            }
            let payload = serde_json::to_string(&entry).map_err(unavailable)?;
            tx.execute("INSERT INTO pending(environment,payload) VALUES (?1,?2)", params![entry.record.spec.id, payload]).map_err(unavailable)?;
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

    async fn publish(&self, entries: Vec<Entry>) -> Result<()> {
        for (sequence, entry) in entries {
            if self.publish_entry(&entry).await? != Durability::Published {
                return Err(unavailable("upstream did not publish result"));
            }
            self.acknowledge(sequence).await?;
        }
        Ok(())
    }

    async fn publish_entry(&self, entry: &PendingEntry) -> Result<Durability> {
        if entry.cleanup_artifacts.is_empty() {
            self.upstream.commit(&entry.record).await
        } else {
            self.upstream
                .commit_deleted(&entry.record, &entry.cleanup_artifacts)
                .await
        }
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
        for (sequence, entry) in self.entries(None).await? {
            let record = &entry.record;
            if !entry.cleanup_artifacts.is_empty() {
                if let Some(current) = catalog.iter().find(|r| r.spec.id == record.spec.id) {
                    if current.assignment != record.assignment {
                        // The old deleted generation was durably handed to
                        // Coordinator before this name could be reassigned.
                        self.acknowledge(sequence).await?;
                        continue;
                    }
                    if current.revision > record.revision {
                        return Err(Error::Conflict);
                    }
                }
                // The Coordinator owns the cleanup task after publication. A
                // crash before the local acknowledgement must retry the same
                // durable envelope, even if its catalog has already advanced
                // to a terminal state or compacted the record.
                if self.publish_entry(&entry).await? != Durability::Published {
                    return Err(unavailable("recovery cleanup result is not published"));
                }
                self.acknowledge(sequence).await?;
                continue;
            }
            match catalog.iter().find(|r| r.spec.id == record.spec.id) {
                Some(current)
                    if current.assignment == record.assignment
                        && current.spec == record.spec
                        && !(matches!(
                            current.state,
                            adx_core::EnvironmentState::Failed
                                | adx_core::EnvironmentState::Deleted
                        ) && !current.resources_held
                            && !current.restart_pending) =>
                {
                    if current.revision == record.revision && current != record {
                        return Err(Error::Conflict);
                    }
                    if current.revision < record.revision
                        && self.publish_entry(&entry).await? != Durability::Published
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
        self.commit_with_cleanup(record, &[]).await
    }

    async fn commit_deleted(
        &self,
        record: &EnvironmentRecord,
        cleanup_artifacts: &[CheckpointArtifact],
    ) -> Result<Durability> {
        self.commit_with_cleanup(record, cleanup_artifacts).await
    }
}

impl JournalSink {
    async fn commit_with_cleanup(
        &self,
        record: &EnvironmentRecord,
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
            if (latest.record != *record || latest.cleanup_artifacts != cleanup_artifacts)
                && (latest.record.assignment != record.assignment
                    || latest.record.spec != record.spec
                    || latest.record.revision >= record.revision)
            {
                return Err(Error::Conflict);
            }
        }
        let result = self.publish(pending).await;
        match result {
            Ok(()) => match self
                .publish_entry(&PendingEntry {
                    record: record.clone(),
                    cleanup_artifacts: cleanup_artifacts.to_vec(),
                })
                .await
            {
                Ok(Durability::Published) => Ok(Durability::Published),
                Err(Error::Unavailable(_)) | Ok(Durability::Journaled) => {
                    self.append(record, cleanup_artifacts).await
                }
                Err(error) => Err(error),
            },
            Err(Error::Unavailable(_)) => self.append(record, cleanup_artifacts).await,
            Err(error) => Err(error),
        }
    }
}
