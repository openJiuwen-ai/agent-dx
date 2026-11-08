//! DistributedFs namespace and FileVersion authority on the shared MetaStore.

mod gc;

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use afs_error::{Error, Result};

use crate::clock::now_unix_ms;
use crate::dfs::{
    CallerContext, ChunkObject, ChunkSources, ClaimReplicationTask, CommitFileVersion,
    CommitMetadataMode, CopyId, CopyLocation, CopyRecord, CopyRole, CopyState, Dentry, DentryKey,
    DfsChunkSourcesReply, DfsChunkSourcesRequest, DfsDurabilityPolicy, DfsReadGrant,
    DirEntryRecord, DirPage, EffectiveLocalCopy, FileVersion, FileVersionId, GetXattrRequest,
    InodeAttributes, InodeId, InodeKind, InodeRecord, LayoutRoot, LayoutRootId, LinkRequest,
    ListXattrRequest, LocalCopyPolicy, MkdirRequest, MknodRequest, NamespaceId, OperationId,
    PlacementHealth, PlacementRecord, PlacementSnapshot, ReadLinkRequest, RemoveXattrRequest,
    RenameMode, RenameOutcome, RenameRequest, ReplicaAck, ReplicaGroup, ReplicaGroupId,
    ReplicaTarget, ReplicaWriteGrant, ReplicationClaim, ReplicationConfig, ReplicationTask,
    ReplicationTaskId, ReplicationTaskState, ReportChunkCorruption, ReportReplicationTask,
    RmdirRequest, SetInodeAttrRequest, SetXattrRequest, SourceCandidate, SpecialNodeKind,
    SymlinkRequest, SyncInodeMetadata, UnlinkRequest, ValidateReplicaWriteRequest, WriteLease,
    XattrSetMode,
};

use super::store::{
    MetaEntity, MetaKey, MetaRead, MetaReadView, MetaStore, MetaTxn, NodeSession, OperationResult,
    RequestKey, RequestOutcome, StoreOperation, TxnCondition, TxnMutation, TxnOutcome,
};

mod access;
mod commit;
mod namespace;
mod placement;
mod read_auth;
mod repair;
mod replay;
mod write_lease;
use access::*;
use placement::*;
pub(super) use read_auth::serving_read_copy;
pub(super) use repair::external_copy_matches;
use repair::*;
use replay::*;

/// Most directory entries one `ReadDir` page returns. A slim entry is at
/// most a 255-byte name, an inode id and a kind, so a page stays far below the
/// gRPC message limit; the Node asks for pages as FUSE consumes them.
pub const READ_DIR_PAGE_MAX: usize = 1024;

#[derive(Clone)]
pub struct DfsService {
    store: Arc<dyn MetaStore>,
    replication: ReplicationConfig,
    durability_policy: DfsDurabilityPolicy,
    read_grant_key: Arc<Option<[u8; 32]>>,
    gc_grace_ms: u64,
    gc_metrics: gc::GcMetrics,
    gc_reference_updates: Arc<[tokio::sync::Mutex<()>; GC_REFERENCE_LOCK_SHARDS]>,
    repair_patrol: Arc<std::sync::Mutex<repair::RepairPatrol>>,
}

// Store batches at most 64 submitted transactions in one durable commit pass.
// A create can therefore observe up to one full batch of same-parent namespace
// timestamp churn before the next fair turn should either succeed or report
// contention instead of spinning without a boundary.
const CREATE_PARENT_DRIFT_RETRY_LIMIT: usize = 64;
const FILE_COMMIT_RETRY_LIMIT: usize = 64;
const NAMESPACE_REMOVE_RETRY_LIMIT: usize = 64;
const GC_REFERENCE_LOCK_SHARDS: usize = 64;

enum NamespaceRemoveAttempt {
    Removed(InodeRecord),
    Retry,
}

enum FileCommitAttempt {
    Committed(InodeRecord),
    Retry,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CreateFileRequest {
    pub caller_id: String,
    pub owner_session_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub attributes: InodeAttributes,
    pub lease_seconds: u64,
}

impl DfsService {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self::with_replication_config(store, ReplicationConfig::local_single_copy())
    }

    pub fn with_replication_config(
        store: Arc<dyn MetaStore>,
        replication: ReplicationConfig,
    ) -> Self {
        Self::with_durability_policy(store, replication, DfsDurabilityPolicy::NodeReplicated)
    }

    pub fn with_durability_policy(
        store: Arc<dyn MetaStore>,
        replication: ReplicationConfig,
        durability_policy: DfsDurabilityPolicy,
    ) -> Self {
        use std::io::Read;
        let mut key = [0; 32];
        let read_grant_key = std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut key))
            .ok()
            .map(|()| key);
        Self {
            store,
            replication,
            durability_policy,
            read_grant_key: Arc::new(read_grant_key),
            gc_grace_ms: 1_000,
            gc_metrics: gc::GcMetrics::new().expect("fixed GC metric descriptors are valid"),
            gc_reference_updates: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
            repair_patrol: Arc::default(),
        }
    }

    /// Creates the single filesystem replication contract or verifies the
    /// exact value persisted by an earlier start. A different value is a
    /// reformat boundary, not a live configuration update.
    pub async fn initialize_replication_config(&self) -> Result<()> {
        if self.read_grant_key.is_none() {
            return Err(permission_denied(
                "Meta cannot initialize read authority without OS entropy",
            ));
        }
        if !self.replication.is_valid() {
            return Err(invalid("DFS replication config is invalid"));
        }
        self.initialize_durability_policy().await?;
        let existing = self.store.read(MetaRead::DfsReplicationConfig).await?;
        if let Some(MetaEntity::DfsReplicationConfig(config)) = existing.entity {
            return if config == self.replication {
                Ok(())
            } else {
                Err(conflict(
                    "DFS replication config is immutable; reinitialize the filesystem to change it",
                ))
            };
        }

        let request = RequestKey::new("afs-meta", "dfs-replication-config-v1");
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsInitializeReplicationConfig,
            result: OperationResult::Empty,
        };
        let mut txn = MetaTxn::new(
            request.clone(),
            StoreOperation::DfsInitializeReplicationConfig,
        );
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsReplicationConfig),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsReplicationConfig(self.replication.clone())),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        let _ = self.store.compare_and_commit(txn).await?;
        let persisted = self.store.read(MetaRead::DfsReplicationConfig).await?;
        match persisted.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) if config == self.replication => Ok(()),
            Some(MetaEntity::DfsReplicationConfig(_)) => Err(conflict(
                "DFS replication config changed during initialization",
            )),
            _ => Err(invalid("DFS replication config was not persisted")),
        }
    }

    async fn initialize_durability_policy(&self) -> Result<()> {
        if let DfsDurabilityPolicy::ExternalBackend {
            store_id,
            generation,
        } = &self.durability_policy
        {
            crate::dfs::validate_external_store_identity(store_id, generation)?;
        }
        if self.is_external_backend() && self.replication != ReplicationConfig::local_single_copy()
        {
            return Err(invalid(
                "external durability requires the fixed single-copy configuration",
            ));
        }
        let existing = self.store.read(MetaRead::DfsDurabilityPolicy).await?;
        if let Some(MetaEntity::DfsDurabilityPolicy(policy)) = existing.entity {
            return if policy == self.durability_policy {
                Ok(())
            } else {
                Err(conflict(
                    "DFS durability policy is immutable; reinitialize the filesystem to change it",
                ))
            };
        }
        // A pre-policy filesystem already containing replication authority is
        // NodeReplicated. Its chunks must never be advertised as external data.
        if self.is_external_backend()
            && self
                .store
                .read(MetaRead::DfsReplicationConfig)
                .await?
                .entity
                .is_some()
        {
            return Err(conflict(
                "existing Node-replicated filesystem cannot become an external filesystem",
            ));
        }
        let request = RequestKey::new("afs-meta", "dfs-durability-policy-v1");
        let mut txn = MetaTxn::new(
            request.clone(),
            StoreOperation::DfsInitializeDurabilityPolicy,
        );
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::Missing(MetaKey::DfsDurabilityPolicy),
        ]);
        if self.is_external_backend() {
            txn.conditions
                .push(TxnCondition::Missing(MetaKey::DfsReplicationConfig));
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDurabilityPolicy(
                self.durability_policy.clone(),
            )),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsInitializeDurabilityPolicy,
                result: OperationResult::Empty,
            }),
        ]);
        let _ = self.store.compare_and_commit(txn).await?;
        match self.store.read(MetaRead::DfsDurabilityPolicy).await?.entity {
            Some(MetaEntity::DfsDurabilityPolicy(policy)) if policy == self.durability_policy => {
                Ok(())
            }
            Some(MetaEntity::DfsDurabilityPolicy(_)) => Err(conflict(
                "DFS durability policy changed during initialization",
            )),
            _ => Err(invalid("DFS durability policy was not persisted")),
        }
    }

    fn is_external_backend(&self) -> bool {
        matches!(
            self.durability_policy,
            DfsDurabilityPolicy::ExternalBackend { .. }
        )
    }

    async fn current_durability_policy(&self) -> Result<DfsDurabilityPolicy> {
        match self.store.read(MetaRead::DfsDurabilityPolicy).await?.entity {
            Some(MetaEntity::DfsDurabilityPolicy(policy)) if policy == self.durability_policy => {
                Ok(policy)
            }
            Some(MetaEntity::DfsDurabilityPolicy(_)) => Err(conflict(
                "DFS durability policy differs from the initialized filesystem",
            )),
            None if !self.is_external_backend() => Ok(DfsDurabilityPolicy::NodeReplicated),
            _ => Err(invalid("DFS durability policy is not initialized")),
        }
    }

    pub async fn get_inode(&self, inode_id: InodeId) -> Result<InodeRecord> {
        require_id(&inode_id.0, "inode_id")?;
        let root = inode_id.0 == "1";
        let snapshot = self.store.read(MetaRead::DfsInode(inode_id)).await?;
        match snapshot.entity {
            Some(MetaEntity::DfsInode(inode)) => Ok(inode),
            None if root => Ok(root_inode(NamespaceId::new("default"))),
            _ => Err(Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "DFS inode was not found",
            )),
        }
    }

    pub async fn get_file_version(
        &self,
        version_id: FileVersionId,
    ) -> Result<(FileVersion, LayoutRoot)> {
        require_id(&version_id.0, "version_id")?;
        let version = match self
            .store
            .read(MetaRead::DfsFileVersion(version_id))
            .await?
            .entity
        {
            Some(MetaEntity::DfsFileVersion(version)) => version,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS FileVersion was not found",
                ));
            }
        };
        let layout = match self
            .store
            .read(MetaRead::DfsLayoutRoot(version.layout_root.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsLayoutRoot(layout)) => layout,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS LayoutRoot was not found",
                ));
            }
        };
        Ok((version, layout))
    }

    async fn current_replication_config(&self) -> Result<ReplicationConfig> {
        let snapshot = self.store.read(MetaRead::DfsReplicationConfig).await?;
        match snapshot.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) if config == self.replication => {
                Ok(config)
            }
            Some(MetaEntity::DfsReplicationConfig(_)) => Err(conflict(
                "DFS replication config differs from the initialized filesystem",
            )),
            _ => {
                self.initialize_replication_config().await?;
                Ok(self.replication.clone())
            }
        }
    }

    async fn current_session(
        &self,
        node_id: &str,
        epoch: u64,
        session_id: Option<&str>,
        now: u64,
    ) -> Result<NodeSession> {
        let session = self.current_live_session(node_id, now).await?;
        if session.lease_epoch == epoch && session_id.is_none_or(|id| session.session_id == id) {
            Ok(session)
        } else {
            Err(conflict("Node session is not current"))
        }
    }

    async fn current_live_session(&self, node_id: &str, now: u64) -> Result<NodeSession> {
        let snapshot = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: node_id.to_owned(),
            })
            .await?;
        match snapshot.entity {
            Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now) => {
                Ok(session)
            }
            _ => Err(conflict("Node session is not current")),
        }
    }

    async fn chunk(&self, chunk_id: &crate::dfs::ChunkId) -> Result<ChunkObject> {
        match self
            .store
            .read(MetaRead::DfsChunk(chunk_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsChunk(chunk)) => Ok(chunk),
            _ => Err(not_found("DFS chunk is missing")),
        }
    }

    async fn copy(&self, copy_id: &CopyId) -> Result<CopyRecord> {
        match self
            .store
            .read(MetaRead::DfsCopy(copy_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsCopy(copy)) => Ok(copy),
            _ => Err(not_found("DFS copy is missing")),
        }
    }

    async fn placement(&self, chunk_id: &crate::dfs::ChunkId) -> Result<PlacementRecord> {
        match self
            .store
            .read(MetaRead::DfsPlacement(chunk_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsPlacement(placement)) => Ok(placement),
            _ => Err(not_found("DFS placement is missing")),
        }
    }

    async fn replication_task(&self, task_id: &ReplicationTaskId) -> Result<ReplicationTask> {
        match self
            .store
            .read(MetaRead::DfsReplicationTask(task_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(task)) => Ok(task),
            _ => Err(not_found("DFS replication task is missing")),
        }
    }
}

fn root_inode(namespace_id: NamespaceId) -> InodeRecord {
    InodeRecord {
        namespace_id,
        inode_id: InodeId::new("1"),
        kind: InodeKind::Directory,
        attributes: InodeAttributes {
            mode: 0o755,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime_unix_ms: 0,
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        },
        head_version: None,
        symlink_target: None,
        xattrs: Default::default(),
        revision: 1,
    }
}

fn lease_expiry(lease_seconds: u64) -> Result<u64> {
    if lease_seconds == 0 {
        return Err(invalid("lease_seconds must be greater than zero"));
    }
    Ok(now_unix_ms().saturating_add(lease_seconds.saturating_mul(1000)))
}

fn not_found(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_FOUND, message)
}

fn not_directory(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_DIRECTORY, message)
}

fn is_directory(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_IS_DIRECTORY, message)
}

fn directory_not_empty(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_DIRECTORY_NOT_EMPTY, message)
}

fn already_exists(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_ALREADY_EXISTS, message)
}

fn name_too_long(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NAME_TOO_LONG, message)
}

fn no_data(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NO_DATA, message)
}

fn not_supported(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_SUPPORTED, message)
}

fn operation_not_permitted(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_OPERATION_NOT_PERMITTED, message)
}

fn permission_denied(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

fn invalid_name_error() -> Error {
    invalid("DFS name must be one non-empty path component")
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_CATALOG_INVALID_REQUEST, message)
}

fn require_id(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        Err(invalid(format!("{field} is required")))
    } else {
        Ok(())
    }
}

fn conflict(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_DFS_CONFLICT, message)
}

#[cfg(all(test, feature = "dfs"))]
mod read_recovery_tests;

#[cfg(all(test, feature = "dfs"))]
mod external_policy_tests;

#[cfg(all(test, feature = "dfs"))]
mod gc_fence_tests;

#[cfg(all(test, feature = "dfs"))]
mod placement_hash_tests;

#[cfg(all(test, feature = "dfs"))]
mod effective_local_copy_tests;

#[cfg(all(test, feature = "dfs"))]
mod txn_snapshot_tests;

#[cfg(all(test, feature = "dfs"))]
mod request_digest_golden_tests;
