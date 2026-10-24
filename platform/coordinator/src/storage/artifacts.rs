//! Durable inventory and execution holds; deletion uses scoped in-memory exclusion.
use super::*;
use adx_core::snapshots::Snapshot;
use adx_core::CheckpointArtifact;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHold {
    pub assignment: Assignment,
    pub artifacts: Vec<CheckpointArtifact>,
}

/// Serializes artifact references with deletion in the single active Coordinator.
/// Dropping the guard (including on cancellation or failure) permits a fresh retry.
#[must_use = "keep the guard until storage and inventory deletion have completed"]
pub struct ArtifactDeletionGuard {
    _lock: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl RedisStore {
    fn artifact_lock(&self, key: String) -> Result<Arc<tokio::sync::RwLock<()>>> {
        let mut locks = self
            .connection
            .artifact_locks
            .lock()
            .map_err(|_| Error::Unavailable("artifact lock registry poisoned".into()))?;
        // Only active operations retain entries; completed deletions leave no tombstones.
        locks.retain(|_, lock| lock.strong_count() != 0);
        Ok(locks
            .get(&key)
            .and_then(std::sync::Weak::upgrade)
            .unwrap_or_else(|| {
                let lock = Arc::new(tokio::sync::RwLock::new(()));
                locks.insert(key, Arc::downgrade(&lock));
                lock
            }))
    }

    pub(super) fn lock_artifacts(
        &self,
        artifacts: &[CheckpointArtifact],
    ) -> Result<Vec<tokio::sync::OwnedRwLockReadGuard<()>>> {
        artifacts
            .iter()
            .map(artifact_key)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|key| {
                self.artifact_lock(key)?
                    .try_read_owned()
                    .map_err(|_| Error::Conflict)
            })
            .collect()
    }
}

impl Session {
    pub async fn execution_holds(&self) -> Result<Vec<ExecutionHold>> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut cmd = redis::cmd("HVALS");
        cmd.arg(format!("{}:input-holds", self.store.key));
        let values: Vec<String> = self.store.query(cmd).await?;
        values.into_iter().map(|v| decode(&v)).collect()
    }
    /// The node has reconciled and stopped every execution outside its current assignments.
    pub async fn release_execution_holds(&self, node: &str, session: &str) -> Result<()> {
        let [header, raw_node] = self
            .store
            .fields([HEADER.into(), format!("node:{node}")])
            .await?;
        self.header(&header)?;
        let stored: StoredNode = decode(raw_node.as_deref().ok_or(Error::NotFound)?)?;
        if stored
            .session
            .as_ref()
            .is_none_or(|s| s.id != session || !s.routable)
        {
            return Err(Error::Conflict);
        }
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(r#"
            if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] or redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
            local values = redis.call('HGETALL',KEYS[2])
            for i=1,#values,2 do
                if cjson.decode(values[i+1]).assignment.node_id == ARGV[4] then redis.call('HDEL',KEYS[2],values[i]) end
            end
            return 1
        "#).arg(2).arg(&self.store.key).arg(format!("{}:input-holds",self.store.key))
            .arg(header.as_deref().ok_or(Error::Conflict)?).arg(format!("node:{node}"))
            .arg(raw_node.as_deref().ok_or(Error::Conflict)?).arg(node);
        let mut inventory = self.inspection.lock().await;
        let mut committed = inventory.take();
        match self.store.query::<u64>(cmd).await {
            Ok(1) => {
                if let Some(inventory) = committed.as_mut() {
                    inventory
                        .holds
                        .retain(|_, hold| hold.assignment.node_id != node);
                }
                *inventory = committed;
                Ok(())
            }
            Ok(_) => {
                *inventory = committed;
                Err(Error::Conflict)
            }
            Err(error) => {
                *inventory = None;
                Err(error)
            }
        }
    }
    pub async fn shared_artifacts(&self) -> Result<Vec<CheckpointArtifact>> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut cmd = redis::cmd("HVALS");
        cmd.arg(format!("{}:artifacts", self.store.key));
        let values: Vec<String> = self.store.query(cmd).await?;
        values.into_iter().map(|v| decode(&v)).collect()
    }
    /// Check references and exclude concurrent publication until the guard is dropped.
    /// No deletion marker is persisted: interrupted deletion is retried from the inventory.
    pub async fn claim_artifact_deletion(
        &self,
        artifact: &CheckpointArtifact,
        deleting_snapshot: Option<&str>,
    ) -> Result<ArtifactDeletionGuard> {
        let guard = ArtifactDeletionGuard {
            _lock: self
                .store
                .artifact_lock(artifact_key(artifact))?
                .try_write_owned()
                .map_err(|_| Error::Conflict)?,
        };
        let raw = self.store.raw().await?;
        let header = raw.get(HEADER).ok_or(Error::Conflict)?;
        self.header(&Some(header.clone()))?;
        let saved = snapshot(&raw)?;
        if saved.environments.values().any(|e| {
            e.result.as_ref().is_some_and(|r| {
                record_artifacts(r)
                    .iter()
                    .any(|a| same_artifact(a, artifact))
            })
        }) {
            return Err(Error::Conflict);
        }
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(self.snapshots_key());
        let snapshots: BTreeMap<String, String> = self.store.query(cmd).await?;
        for (id, value) in &snapshots {
            let s: Snapshot = decode(value)?;
            if same_artifact(&s.artifact, artifact)
                && s.state != adx_core::snapshots::SnapshotState::Deleted
                && !(deleting_snapshot == Some(id.as_str()) && s.collectable())
            {
                return Err(Error::Conflict);
            }
        }
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(format!("{}:input-holds", self.store.key));
        let holds: BTreeMap<String, String> = self.store.query(cmd).await?;
        for value in holds.values() {
            if decode::<ExecutionHold>(value)?
                .artifacts
                .iter()
                .any(|a| same_artifact(a, artifact))
            {
                return Err(Error::Conflict);
            }
        }
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(
            r#"
            if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
            for k=2,3 do
                local expected = cjson.decode(ARGV[k])
                local count=0
                for field,value in pairs(expected) do
                    count=count+1
                    if redis.call('HGET',KEYS[k],field) ~= value then return 0 end
                end
                if redis.call('HLEN',KEYS[k]) ~= count then return 0 end
            end
            return 1
        "#,
        )
        .arg(3)
        .arg(&self.store.key)
        .arg(self.snapshots_key())
        .arg(format!("{}:input-holds", self.store.key))
        .arg(header)
        .arg(encode(&snapshots)?)
        .arg(encode(&holds)?);
        if self.store.query::<u64>(cmd).await? != 1 {
            return Err(Error::Conflict);
        }
        Ok(guard)
    }
    pub async fn forget_artifact(&self, artifact: &CheckpointArtifact) -> Result<()> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end; redis.call('HDEL',KEYS[2],ARGV[2]); return 1")
            .arg(2).arg(&self.store.key).arg(format!("{}:artifacts",self.store.key))
            .arg(header.as_deref().ok_or(Error::Conflict)?).arg(artifact_key(artifact));
        if self.store.query::<u64>(cmd).await? != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
pub(super) fn artifact_key(a: &CheckpointArtifact) -> String {
    format!("{}:{}", a.storage, a.location)
}
pub(super) fn record_artifacts(r: &EnvironmentRecord) -> Vec<CheckpointArtifact> {
    r.checkpoint
        .iter()
        .chain(r.snapshot_source.iter())
        .map(|p| p.artifact.clone())
        .chain(r.execution_artifacts.iter().cloned())
        .filter(|a| a.storage != "local")
        .collect()
}
pub(super) fn hold_key(a: &Assignment) -> String {
    format!("{}:{}:{}", a.node_id, a.environment_id, a.generation)
}

pub(crate) fn same_artifact(a: &CheckpointArtifact, b: &CheckpointArtifact) -> bool {
    a.storage == b.storage && a.location == b.location
}
