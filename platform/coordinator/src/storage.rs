//! Single-Coordinator Redis persistence. Lua only compares opaque bytes and performs
//! atomic hash writes; JSON and u64 counters are validated in Rust, never Lua doubles.
mod catalog;
pub(crate) use catalog::{PublicationReset, PublicationValue};
mod checkpoint_cleanup;
mod claims;
mod publication;
mod transport;
pub use claims::{ClaimOutcome, LocalClaim};
mod artifacts;
pub use artifacts::ArtifactDeletionGuard;
mod collection;
mod credentials;
mod failure;
mod inspection;
mod node_retirement;
mod observability;
use observability::CounterUpdate;
mod recovery;
pub use observability::{
    BusinessCounters, CreationResult, CreationSummary, InterruptionSummary, RecoveryState,
};
mod resume;
mod snapshot_preparation;
mod snapshots;
mod volumes;
use crate::Node;
use adx_core::{
    scheduling::validate_device_assignment, Assignment, EnvironmentRecord, EnvironmentSpec,
    EnvironmentState, Error, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, Mutex},
    time::timeout,
};
pub(crate) use volumes::has_volumes;
pub use volumes::{
    AuthorizedExecution, Initialization, InitializationAction, InitializationOwner, StoredVolume,
};

const HEADER: &str = "header";
const CAS: &str = r#"
local old = redis.call('HGET', KEYS[1], 'header')
if ARGV[1] == '' then
  if redis.call('HLEN', KEYS[1]) ~= 0 then return 0 end
  redis.call('DEL',KEYS[3])
elseif old ~= ARGV[1] then return 0 end
if #ARGV == 3 then
  redis.call('HSET', KEYS[1], 'header', ARGV[2], 'epoch', ARGV[#ARGV])
else
  redis.call('HSET', KEYS[1], 'header', ARGV[2], ARGV[3], ARGV[4])
end
for i = 5, #ARGV, 2 do redis.call('HSET', KEYS[2], ARGV[i], ARGV[i+1]) end
return 1
"#;
const RETIRE_FIELDS: &str = r#"
if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
for i = 4, #ARGV do
  redis.call('HDEL', KEYS[1], ARGV[i])
end
redis.call('HSET', KEYS[1], 'header', ARGV[2], 'epoch', ARGV[3])
return 1
"#;
const RETIRE_ENVIRONMENT: &str = r#"
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], ARGV[3]) ~= ARGV[4] then return 0 end
if ARGV[8] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[8] then return 0 end
local facts = cjson.decode(ARGV[7])
if (redis.call('HGET',KEYS[3],facts[1]) or '') ~= facts[2] then return 0 end
if (redis.call('HGET',KEYS[3],facts[4]) or '') ~= facts[5] then return 0 end
local cleanup_cursor=13 + 2 * tonumber(ARGV[12])
if ARGV[cleanup_cursor] ~= '' and redis.call('HEXISTS',KEYS[6],ARGV[cleanup_cursor]) ~= 0 then return 2 end
redis.call('SET', KEYS[2], ARGV[5], 'PX', ARGV[6])
redis.call('HSET',KEYS[3],facts[4],facts[6])
redis.call('HDEL',KEYS[3],facts[1])
redis.call('HDEL', KEYS[1], ARGV[3])
if ARGV[8] ~= '' then
  redis.call('HSET',KEYS[1],'header',ARGV[9])
  if ARGV[10] ~= '' then redis.call('HSET',KEYS[5],ARGV[10],ARGV[11]) end
end
local cursor=13
for _=1,tonumber(ARGV[12]) do
  local key=ARGV[cursor]; local value=ARGV[cursor+1]
  if value == '' then redis.call('HDEL',KEYS[4],key) else redis.call('HSET',KEYS[4],key,value) end
  cursor=cursor+2
end
if ARGV[cursor] ~= '' then
  redis.call('HSET',KEYS[6],ARGV[cursor],ARGV[cursor+1])
end
return 1
"#;
const ATTEMPTS: usize = 32;
// Match the API operation replay window without retaining a deleted Environment.
const DELETED_RECEIPT_TTL: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Header {
    #[serde(default)]
    observability: BusinessCounters,
    schema: u32,
    shards: usize,
    epoch: u64,
    generation: u64,
    revision: u64,
    next_node_shard: usize,
}
impl Header {
    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(Error::Conflict)?;
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.shards == 0
            || self.next_node_shard >= self.shards
            || self.epoch == 0
        {
            return Err(Error::Unavailable(
                "unsupported or corrupt control metadata".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSession {
    pub id: String,
    pub sequence: u64,
    pub routable: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRetirement {
    pub since_unix_seconds: u64,
    pub unregistered: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredNode {
    #[serde(default)]
    pub retirement: Option<NodeRetirement>,
    /// Incident for the current unavailable period; cleared when routable again.
    #[serde(default)]
    pub last_incident_id: Option<String>,
    pub node: Node,
    pub shard_id: usize,
    pub address: String,
    pub proxy_address: String,
    #[serde(default)]
    pub session: Option<NodeSession>,
    /// Administrative admission override. Heartbeats cannot clear it.
    #[serde(default)]
    pub scheduling_paused: bool,
    /// Shared checkpoint backends declared by the current authenticated session.
    #[serde(default)]
    pub checkpoint_scopes: BTreeSet<String>,
    /// Set by durable invalidation; an unfenced routable session is required
    /// before a node can perform checkpoint collection again.
    #[serde(default)]
    pub checkpoint_gc_fenced: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub source: Assignment,
    pub pending: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeIntent {
    /// Initial duration of this wake, retained across placement and background retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_seconds: Option<u64>,
    pub operation_id: String,
    /// A definitive local admission rejection permits a different placement.
    pub local_rejected: bool,
    /// Admission failures are excluded for one bounded placement round.
    #[serde(default)]
    pub rejected_nodes: std::collections::BTreeSet<String>,
    #[serde(default)]
    pub retry_after_unix_seconds: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEnvironment {
    /// Initial ownership generation identifies this lifetime across recovery.
    /// Deleting and recreating the same name starts a different lifetime.
    pub creation_generation: u64,
    #[serde(default)]
    pub creation: Option<CreationSummary>,
    #[serde(default)]
    pub last_interruption: Option<InterruptionSummary>,
    #[serde(default)]
    pub volume_execution: Option<volumes::AuthorizedExecution>,
    #[serde(default)]
    pub resume: Option<ResumeIntent>,
    /// Durable age of a terminal failure; retries and Coordinator restarts do not reset it.
    #[serde(default)]
    pub failed_since_unix_seconds: Option<u64>,
    #[serde(default)]
    pub recovery: Option<Recovery>,
    #[serde(default)]
    pub invalidated: bool,
    pub spec: EnvironmentSpec,
    pub assignment: Assignment,
    pub result: Option<EnvironmentRecord>,
}

/// The accepted result and the complete environment confirmed by the same CAS/read.
/// Deleted environments have no retained record, including receipt replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEnvironment {
    pub result: EnvironmentRecord,
    pub stored: Option<StoredEnvironment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DeletedReceipt {
    environment_id: String,
    generation: u64,
    result_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cleanup_digest: Option<String>,
}

impl DeletedReceipt {
    fn from_result(
        result: &EnvironmentRecord,
        cleanup: &[adx_core::CheckpointArtifact],
    ) -> Result<Self> {
        Ok(Self {
            environment_id: result.spec.id.clone(),
            generation: result.assignment.generation,
            result_digest: format!("{:x}", Sha256::digest(encode(result)?.as_bytes())),
            cleanup_digest: Some(format!(
                "{:x}",
                Sha256::digest(
                    encode(&checkpoint_cleanup::normalized_artifacts(cleanup))?.as_bytes()
                )
            )),
        })
    }

    fn matches(
        &self,
        result: &EnvironmentRecord,
        cleanup: &[adx_core::CheckpointArtifact],
    ) -> Result<bool> {
        let expected = Self::from_result(result, cleanup)?;
        Ok(self.environment_id == expected.environment_id
            && self.generation == expected.generation
            && self.result_digest == expected.result_digest
            && match self.cleanup_digest.as_ref() {
                Some(digest) => Some(digest) == expected.cleanup_digest.as_ref(),
                // Master receipts did not authorize a caller-supplied cleanup payload.
                None => cleanup.is_empty(),
            })
    }
}
impl StoredEnvironment {
    pub fn failed_collectable(&self, now: u64, retention_seconds: u64) -> bool {
        self.result
            .as_ref()
            .is_some_and(|r| r.state == EnvironmentState::Failed && !r.restart_pending)
            && !self.recovery.as_ref().is_some_and(|r| r.pending)
            && self.recovery_point(now).is_none()
            && self
                .failed_since_unix_seconds
                .is_some_and(|since| now.saturating_sub(since) >= retention_seconds)
    }
    pub(crate) fn track_failure(&mut self, now: u64) {
        if self
            .result
            .as_ref()
            .is_some_and(|r| r.state == EnvironmentState::Failed && !r.restart_pending)
        {
            self.failed_since_unix_seconds.get_or_insert(now);
        } else {
            self.failed_since_unix_seconds = None;
        }
    }
    pub fn resources_held(&self) -> bool {
        if !self.invalidated
            && self.volume_execution.as_ref().is_some_and(|execution| {
                !execution.released
                    && self
                        .result
                        .as_ref()
                        .is_none_or(|result| result.runtime.id != execution.runtime_id)
            })
        {
            return true;
        }
        (!self.invalidated && self.recovery.as_ref().is_some_and(|r| r.pending))
            || self.result.as_ref().is_none_or(|r| r.resources_held)
    }
    pub(crate) fn effective_record(&self) -> EnvironmentRecord {
        let mut record = self.result.clone().unwrap_or_else(|| EnvironmentRecord {
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
            runtime: adx_core::Runtime {
                id: format!("{}-{}", self.spec.id, self.assignment.generation),
                ip: None,
            },
            spec: self.spec.clone(),
            assignment: self.assignment.clone(),
            state: EnvironmentState::Pending,
            revision: 0,
            resources_held: true,
            checkpoint: None,
            last_operation: None,
        });
        if let Some(execution) = &self.volume_execution {
            if !self.invalidated && !execution.released && record.runtime.id != execution.runtime_id
            {
                record.runtime.id = execution.runtime_id.clone();
                record.runtime.ip = None;
                record.state = EnvironmentState::Pending;
                record.revision = execution.revision.saturating_sub(1);
                record.resources_held = true;
                record.last_operation = None;
            }
        }
        record
    }
    fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if let Some(seconds) = self
            .resume
            .as_ref()
            .and_then(|intent| intent.run_timeout_seconds)
        {
            let mut policy = self.spec.run_policy.clone().ok_or(Error::Conflict)?;
            policy.timeout_seconds = seconds;
            policy.validate()?;
        }
        if self.spec.id != self.assignment.environment_id
            || self.creation_generation == 0
            || self.creation_generation > self.assignment.generation
        {
            return Err(Error::Conflict);
        }
        validate_device_assignment(&self.spec.scheduling.devices, &self.assignment.devices)?;
        if let Some(r) = &self.result {
            validate_result(r)?;
            if r.spec != self.spec || r.assignment != self.assignment {
                return Err(Error::Conflict);
            }
        }
        if self.volume_execution.as_ref().is_some_and(|execution| {
            execution.revision == 0
                || execution.node_session_id.is_empty()
                || !adx_core::valid_runtime_id(
                    &self.spec.id,
                    self.assignment.generation,
                    &execution.runtime_id,
                )
        }) {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub environment_id: String,
    pub node_id: String,
    pub proxy_address: String,
    pub generation: u64,
    pub environment_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub shard_count: usize,
    pub generation: u64,
    /// Cluster publication cursor, including changes that do not add a route.
    pub revision: u64,
    pub nodes: BTreeMap<String, StoredNode>,
    pub environments: BTreeMap<String, StoredEnvironment>,
}
impl StoredSnapshot {
    pub fn validate(&self) -> Result<()> {
        if self.shard_count == 0 {
            return Err(Error::Conflict);
        }
        for (id, n) in &self.nodes {
            n.node.validate()?;
            if id != &n.node.id
                || n.shard_id >= self.shard_count
                || n.address.trim().is_empty()
                || n.proxy_address.trim().is_empty()
            {
                return Err(Error::Conflict);
            }
        }
        for (id, i) in &self.environments {
            i.validate()?;
            let n = self
                .nodes
                .get(&i.assignment.node_id)
                .ok_or(Error::Conflict)?;
            if id != &i.spec.id
                || i.assignment.shard_id != n.shard_id
                || i.assignment.generation > self.generation
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    /// Only committed Running results are published. Pending reservations,
    /// Failed and Deleted results have no route. Relay still checks binding.
    pub fn routes(&self) -> Result<Vec<Route>> {
        self.validate()?;
        Ok(self
            .environments
            .values()
            .filter_map(|i| {
                i.result
                    .as_ref()
                    .filter(|r| {
                        r.state == EnvironmentState::Running
                            && i.volume_execution.as_ref().is_none_or(|execution| {
                                !execution.released && execution.runtime_id == r.runtime.id
                            })
                            && self.nodes[&r.assignment.node_id]
                                .session
                                .as_ref()
                                .is_none_or(|s| s.routable)
                    })
                    .map(|r| Route {
                        environment_id: r.spec.id.clone(),
                        node_id: r.assignment.node_id.clone(),
                        proxy_address: self.nodes[&r.assignment.node_id].proxy_address.clone(),
                        generation: r.assignment.generation,
                        environment_revision: r.revision,
                    })
            })
            .collect())
    }
}

fn encode<T: Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v)
        .map_err(|_| Error::Invalid("control record serialization failed".into()))
}
fn decode<T: for<'a> Deserialize<'a>>(v: &str) -> Result<T> {
    serde_json::from_str(v).map_err(|_| Error::Unavailable("corrupt control record".into()))
}
fn validate_result(r: &EnvironmentRecord) -> Result<()> {
    if r.restart_attempts
        > r.spec
            .lifecycle
            .restart
            .as_ref()
            .map_or(0, |p| p.max_attempts)
        || (r.restart_pending
            && (r.state != EnvironmentState::Failed
                || r.spec
                    .lifecycle
                    .restart
                    .as_ref()
                    .is_none_or(|p| r.restart_attempts >= p.max_attempts)))
    {
        return Err(Error::Conflict);
    }
    if r.revision == 0
        || !adx_core::valid_runtime_id(&r.spec.id, r.assignment.generation, &r.runtime.id)
    {
        return Err(Error::Conflict);
    }
    if let Some(cp) = r.checkpoint.as_ref().or(r.snapshot_source.as_ref()) {
        if cp.id.is_empty()
            || cp.artifact.storage.is_empty()
            || cp.artifact.location.is_empty()
            || cp.artifact.size_bytes == 0
            || cp.expires_at_unix_seconds == Some(0)
            || match &cp.origin {
                None => !adx_core::valid_runtime_id(
                    &r.spec.id,
                    r.assignment.generation,
                    &cp.source_runtime_id,
                ),
                Some(origin) => {
                    (if origin.environment_id == r.spec.id {
                        origin.ownership_generation >= r.assignment.generation
                    } else {
                        r.spec.snapshot_id.is_none()
                    }) || origin.runtime_id != cp.source_runtime_id
                        || !adx_core::valid_runtime_id(
                            &origin.environment_id,
                            origin.ownership_generation,
                            &origin.runtime_id,
                        )
                }
            }
        {
            return Err(Error::Conflict);
        }
    }
    if r.last_operation.as_ref().is_some_and(|op| {
        op.id.is_empty() || op.expected_revision == 0 || op.expected_revision >= r.revision
    }) {
        return Err(Error::Conflict);
    }
    match r.state {
        EnvironmentState::Running if r.resources_held && r.runtime.ip.is_some() => Ok(()),
        EnvironmentState::Paused
            if !r.resources_held
                && r.runtime.ip.is_none()
                && (r.checkpoint.is_some() || r.snapshot_source.is_some()) =>
        {
            Ok(())
        }
        EnvironmentState::Deleted if !r.resources_held => Ok(()),
        EnvironmentState::Failed => Ok(()),
        _ => Err(Error::Invalid(
            "expected a completed node result with consistent resource ownership".into(),
        )),
    }
}
fn next_result(old: &StoredEnvironment, r: &EnvironmentRecord) -> Result<bool> {
    let mut expected_spec = old.spec.clone();
    expected_spec.execution.network = r.spec.execution.network.clone();
    if expected_spec != r.spec || old.assignment != r.assignment {
        return Err(Error::Conflict);
    }
    validate_result(r)?;
    if old.invalidated
        && (!matches!(
            r.state,
            EnvironmentState::Failed | EnvironmentState::Deleted | EnvironmentState::Paused
        ) || r.resources_held
            || r.restart_pending
            || r.runtime.ip.is_some()
            || (r.state == EnvironmentState::Paused
                && old
                    .result
                    .as_ref()
                    .is_none_or(|previous| previous.state != EnvironmentState::Paused))
            || (r.state != EnvironmentState::Deleted
                && old
                    .result
                    .as_ref()
                    .is_none_or(|previous| previous.checkpoint != r.checkpoint)))
    {
        return Err(Error::Conflict);
    }
    if let Some(previous) = &old.result {
        if previous == r {
            return Ok(false);
        }
        let restarting = previous.state == EnvironmentState::Failed
            && previous.restart_pending
            && previous.restart_attempts.checked_add(1) == Some(r.restart_attempts)
            && r.runtime.id != previous.runtime.id
            && r.checkpoint == previous.checkpoint
            && matches!(
                r.state,
                EnvironmentState::Running | EnvironmentState::Failed
            );
        let prepared = previous
            .checkpoint
            .as_ref()
            .or(previous.snapshot_source.as_ref())
            .is_some_and(|point| previous.execution_artifacts.contains(&point.artifact));
        let resume_completed = previous.state == EnvironmentState::Paused
            && r.checkpoint == previous.checkpoint
            && r.snapshot_source == previous.snapshot_source
            && r.runtime.id != previous.runtime.id
            && (r.state == EnvironmentState::Failed
                || r.last_operation.as_ref().is_some_and(|op| {
                    // Input reservation adds one Paused revision before the backend starts.
                    let expected = op.expected_revision == previous.revision
                        || (op.expected_revision.checked_add(1) == Some(previous.revision)
                            && prepared
                            && (old
                                .resume
                                .as_ref()
                                .is_none_or(|intent| intent.operation_id == op.id)
                                || old
                                    .recovery
                                    .as_ref()
                                    .is_some_and(|recovery| recovery.pending)));
                    op.kind == adx_core::LifecycleKind::Resume && expected
                }));
        if r.restart_attempts < previous.restart_attempts
            || (r.restart_attempts != previous.restart_attempts && !restarting)
            || r.revision <= previous.revision
            || previous.state == EnvironmentState::Deleted
            || (!previous.resources_held && r.resources_held && !restarting && !resume_completed)
        {
            return Err(Error::Conflict);
        }
    }
    Ok(true)
}

struct Connection {
    client: redis::Client,
    current: Mutex<Option<Arc<transport::Lease>>>,
    timeout: Duration,
    committed_changes: broadcast::Sender<ControlChange>,
    publication_sequence: AtomicU64,
    active_epoch: AtomicU64,
    live_nodes: std::sync::RwLock<BTreeMap<String, StoredNode>>,
    catalog: std::sync::Mutex<catalog::Catalog>,
    publication_locks:
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Weak<Mutex<()>>>>,
    artifact_locks: std::sync::Mutex<BTreeMap<String, std::sync::Weak<tokio::sync::RwLock<()>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ControlChange {
    pub(crate) revision: u64,
    pub(crate) values: Vec<PublicationValue>,
    pub(crate) dirty: bool,
    pub(crate) fields: Vec<String>,
    pub(crate) epoch: u64,
}

#[derive(Clone)]
pub struct RedisStore {
    connection: Arc<Connection>,
    key: String,
}
impl RedisStore {
    fn notify_committed_change(&self, _revision: u64, fields: Vec<String>) {
        // Volume waiters share this bus. Their events carry an empty directory
        // delta, preserving cursor continuity without a full routing scan.
        debug_assert!(fields
            .iter()
            .all(|f| !f.starts_with("environment:") && !f.starts_with("node:")));
        let mut catalog = self
            .connection
            .catalog
            .lock()
            .expect("publication catalog invariant");
        self.send_publication(&mut catalog, vec![], false, fields);
    }

    pub async fn connect(url: &str, namespace: &str, duration: Duration) -> Result<Self> {
        if duration.is_zero()
            || namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err(Error::Invalid("invalid Redis namespace or timeout".into()));
        }
        // Do not include connection strings or payloads in errors (may contain secrets).
        let client = redis::Client::open(url)
            .map_err(|_| Error::Invalid("invalid Redis endpoint".into()))?;
        let (committed_changes, _) = broadcast::channel(1024);
        let store = Self {
            connection: Arc::new(Connection {
                client,
                current: Mutex::new(None),
                timeout: duration,
                committed_changes,
                artifact_locks: Default::default(),
                catalog: Default::default(),
                publication_locks: Default::default(),
                publication_sequence: AtomicU64::new(0),
                active_epoch: AtomicU64::new(0),
                live_nodes: Default::default(),
            }),
            key: format!("adx:{{{namespace}}}:control:v1"),
        };
        let _: String = store.query(redis::cmd("PING")).await?;
        Ok(store)
    }
    async fn fields<const N: usize>(&self, fields: [String; N]) -> Result<[Option<String>; N]> {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.key).arg(&fields);
        let values: Vec<Option<String>> = self.query(cmd).await?;
        values.try_into().map_err(|_| {
            Error::Unavailable("Redis returned an unexpected control field count".into())
        })
    }
    async fn fields_vec(&self, fields: &[String]) -> Result<Vec<Option<String>>> {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.key).arg(fields);
        let values: Vec<Option<String>> = self.query(cmd).await?;
        if values.len() != fields.len() {
            return Err(Error::Unavailable(
                "Redis returned an unexpected control field count".into(),
            ));
        }
        Ok(values)
    }
    async fn raw(&self) -> Result<BTreeMap<String, String>> {
        let mut cmd = redis::cmd("HGETALL");
        cmd.arg(&self.key);
        self.query(cmd).await
    }
    async fn cas(
        &self,
        expected: &str,
        header: &Header,
        field: Option<(&str, String)>,
    ) -> Result<bool> {
        let header_only = field.is_none();
        let changed = field
            .as_ref()
            .map(|(name, _)| vec![(*name).to_owned()])
            .unwrap_or_default();
        let values = Self::publication_values(field.iter().map(|(f, v)| (*f, Some(v.as_str()))))?;
        let artifacts = match &field {
            Some((name, value)) if name.starts_with("environment:") => {
                decode::<StoredEnvironment>(value)?
                    .result
                    .as_ref()
                    .map(artifacts::record_artifacts)
                    .unwrap_or_default()
            }
            _ => vec![],
        };
        let _access = self.lock_artifacts(&artifacts)?;
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(CAS)
            .arg(3)
            .arg(&self.key)
            .arg(format!("{}:artifacts", self.key))
            .arg(format!("{}:business", self.key))
            .arg(expected)
            .arg(encode(header)?);
        if let Some((name, value)) = field {
            cmd.arg(name).arg(value);
            for artifact in artifacts {
                cmd.arg(artifacts::artifact_key(&artifact))
                    .arg(encode(&artifact)?);
            }
        }
        if header_only {
            cmd.arg(header.epoch.to_string());
        }
        let write = self.publication_write(header.epoch, changed).await;
        let applied: u8 = self.query(cmd).await?;
        if applied == 2 {
            return Err(Error::Conflict);
        }
        write.finish(applied == 1, values);
        Ok(applied == 1)
    }
    /// Compare only the touched record under a stable process epoch.
    async fn object_cas(
        &self,
        _header_bytes: &str,
        header: &Header,
        field: Option<(&str, Option<&str>, String)>,
    ) -> Result<bool> {
        self.object_cas_with_counters(header, field, None).await
    }
    async fn object_cas_with_counters(
        &self,
        header: &Header,
        field: Option<(&str, Option<&str>, String)>,
        counters: Option<&CounterUpdate>,
    ) -> Result<bool> {
        let (name, old, next) = field.ok_or(Error::Conflict)?;
        let values = Self::publication_values([(name, Some(next.as_str()))])?;
        let artifacts = if name.starts_with("environment:") {
            decode::<StoredEnvironment>(&next)?
                .result
                .as_ref()
                .map(artifacts::record_artifacts)
                .unwrap_or_default()
        } else {
            vec![]
        };
        let attached_artifacts = if name.starts_with("environment:") {
            let next: StoredEnvironment = decode(&next)?;
            let previous = old.map(decode::<StoredEnvironment>).transpose()?;
            let owned = |stored: &StoredEnvironment| {
                stored
                    .result
                    .as_ref()
                    .map(|record| {
                        record
                            .checkpoint
                            .iter()
                            .chain(record.snapshot_source.iter())
                            .map(|point| point.artifact.clone())
                            .chain(record.execution_artifacts.iter().cloned())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            let previous = previous.as_ref().map(owned).unwrap_or_default();
            owned(&next)
                .into_iter()
                .filter(|artifact| !previous.contains(artifact))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let guarded_header = if attached_artifacts.is_empty() {
            None
        } else {
            let mut next = header.clone();
            next.advance()?;
            Some((encode(header)?, encode(&next)?))
        };
        let _access = self.lock_artifacts(&artifacts)?;
        let membership = if name.starts_with("environment:") {
            let environment: StoredEnvironment = decode(&next)?;
            format!("node-ownership:{}", environment.assignment.node_id)
        } else {
            String::new()
        };
        let node_session = if name.starts_with("node:") {
            let node: StoredNode = decode(&next)?;
            let previous = old.map(decode::<StoredNode>).transpose()?;
            if previous
                .as_ref()
                .and_then(|n| n.session.as_ref())
                .map(|s| &s.id)
                != node.session.as_ref().map(|s| &s.id)
            {
                node.session
                    .as_ref()
                    .map(|s| s.id.as_str())
                    .unwrap_or("")
                    .to_owned()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            local old = redis.call('HGET',KEYS[1],ARGV[2])
            if ARGV[3] == '' then if old then return 0 end
            elseif old ~= ARGV[3] then return 0 end
            if not old and string.sub(ARGV[2],1,5) == 'node:' and redis.call('HEXISTS',KEYS[1],'retired-node:' .. string.sub(ARGV[2],6)) ~= 0 then return 0 end
            local facts = cjson.decode(ARGV[7])
            if facts[1] ~= '' and (redis.call('HGET',KEYS[3],facts[1]) or '') ~= facts[2] then return 0 end
            if ARGV[8] ~= '' and redis.call('HGET',KEYS[1],'header') ~= ARGV[8] then return 0 end
            redis.call('HSET',KEYS[1],ARGV[2],ARGV[4])
            if ARGV[8] ~= '' then redis.call('HSET',KEYS[1],'header',ARGV[9]) end
            if facts[1] ~= '' then redis.call('HSET',KEYS[3],facts[1],facts[3]) end
            if ARGV[5] ~= '' then redis.call('HSET',KEYS[1],'node-session:' .. string.sub(ARGV[2],6),ARGV[5]) end
            if ARGV[6] ~= '' then redis.call('HINCRBY',KEYS[1],ARGV[6],1) end
            for i=10,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end
            return 1
        "#).arg(3).arg(&self.key).arg(format!("{}:artifacts", self.key)).arg(format!("{}:business", self.key))
            .arg(header.epoch.to_string()).arg(name).arg(old.unwrap_or("")).arg(next).arg(node_session).arg(membership)
            .arg(counters.map(CounterUpdate::argument).transpose()?.unwrap_or_else(|| r#"["","",""]"#.into()))
            .arg(guarded_header.as_ref().map(|pair| pair.0.as_str()).unwrap_or(""))
            .arg(guarded_header.as_ref().map(|pair| pair.1.as_str()).unwrap_or(""));
        for artifact in artifacts {
            cmd.arg(artifacts::artifact_key(&artifact))
                .arg(encode(&artifact)?);
        }
        let write = self
            .publication_write(header.epoch, vec![name.into()])
            .await;
        let applied = self.query::<u8>(cmd).await? == 1;
        write.finish(applied, values);
        Ok(applied)
    }
    fn deleted_receipt_key(&self, id: &str) -> String {
        format!("{}:deleted:{id}", self.key)
    }
    async fn deleted_receipt(&self, id: &str) -> Result<Option<DeletedReceipt>> {
        let mut cmd = redis::cmd("GET");
        cmd.arg(self.deleted_receipt_key(id));
        self.query::<Option<String>>(cmd)
            .await?
            .map(|value| decode(&value))
            .transpose()
    }
    async fn retire_fields(
        &self,
        expected: &str,
        header: &Header,
        retired: &[String],
    ) -> Result<bool> {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(RETIRE_FIELDS)
            .arg(1)
            .arg(&self.key)
            .arg(expected)
            .arg(encode(header)?)
            .arg(header.epoch.to_string());
        for field in retired {
            cmd.arg(field);
        }
        let applied: u8 = self.query(cmd).await?;
        Ok(applied == 1)
    }
    async fn retire_environment_with_cleanup(
        &self,
        expected: (&str, &str),
        header: &Header,
        receipt: &DeletedReceipt,
        counters: &CounterUpdate,
        catalog: Option<&volumes::Catalog>,
        cleanup: Option<&adx_core::CheckpointCleanup>,
    ) -> Result<bool> {
        let (expected_header, expected_environment) = expected;
        let field = format!("environment:{}", receipt.environment_id);
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(RETIRE_ENVIRONMENT)
            .arg(6)
            .arg(&self.key)
            .arg(self.deleted_receipt_key(&receipt.environment_id))
            .arg(format!("{}:business", self.key))
            .arg(format!("{}:volumes", self.key))
            .arg(format!("{}:volume-references", self.key))
            .arg(format!("{}:checkpoint-cleanups", self.key))
            .arg(header.epoch.to_string())
            .arg(encode(header)?)
            .arg(&field)
            .arg(expected_environment)
            .arg(encode(receipt)?)
            .arg(DELETED_RECEIPT_TTL.as_millis() as u64)
            .arg(counters.argument()?);
        if let Some(catalog) = catalog {
            cmd.arg(&catalog.header_value)
                .arg(encode(&catalog.advanced_header()?)?)
                .arg(&receipt.environment_id)
                .arg(catalog.reference_value(&receipt.environment_id)?);
            catalog.append_writes(&mut cmd)?;
        } else {
            cmd.arg(expected_header)
                .arg(encode(header)?)
                .arg("")
                .arg("")
                .arg(0);
        }
        if let Some(task) = cleanup {
            cmd.arg(task.id()).arg(encode(task)?);
        } else {
            cmd.arg("").arg("");
        }
        let values = Self::publication_values([(field.as_str(), None)])?;
        let write = self
            .publication_write(header.epoch, vec![field.clone()])
            .await;
        let applied: u8 = self.query(cmd).await?;
        if applied == 2 {
            return Err(Error::Conflict);
        }
        write.finish_with_fields(
            applied == 1,
            values,
            catalog
                .into_iter()
                .flat_map(volumes::Catalog::change_fields)
                .collect(),
        );
        Ok(applied == 1)
    }
    /// Called once per Coordinator startup, never on a Redis reconnect. A new epoch
    /// rejects old Coordinator writes; this is fencing, not leader election or HA.
    pub async fn begin(&self, shards: usize) -> Result<Session> {
        if shards == 0 {
            return Err(Error::Invalid("shard count must be positive".into()));
        }
        for _ in 0..ATTEMPTS {
            let raw = self.raw().await?;
            let old = raw.get(HEADER).map(String::as_str).unwrap_or("");
            let mut retired = Vec::new();
            let mut h = if raw.is_empty() {
                Header {
                    observability: BusinessCounters {
                        started_at_unix_millis: adx_core::observation::now_millis()?,
                        ..Default::default()
                    },
                    schema: 1,
                    shards,
                    epoch: 1,
                    generation: 0,
                    revision: 0,
                    next_node_shard: 0,
                }
            } else {
                let mut h: Header = decode(old)?;
                h.validate()?;
                if h.shards != shards {
                    return Err(Error::Conflict);
                }
                let mut normalized = raw.clone();
                for (field, value) in raw.iter().filter(|(key, _)| key.starts_with("capsule:")) {
                    let id = field.trim_start_matches("capsule:");
                    legacy_deleted_capsule(id, value)?;
                    let new_field = format!("environment:{id}");
                    if normalized.contains_key(&new_field) {
                        return Err(Error::Unavailable(
                            "conflicting legacy control identity".into(),
                        ));
                    }
                    normalized.remove(field);
                    retired.push(field.clone());
                }
                for (field, value) in raw
                    .iter()
                    .filter(|(key, _)| key.starts_with("environment:"))
                {
                    let record: StoredEnvironment = decode(value)?;
                    if record.result.as_ref().is_some_and(|result| {
                        result.state == EnvironmentState::Deleted && !result.resources_held
                    }) {
                        normalized.remove(field);
                        retired.push(field.clone());
                    }
                }
                let saved = snapshot(&normalized)
                    .map_err(|_| Error::Unavailable("control snapshot incompatible".into()))?;
                h.epoch = h.epoch.checked_add(1).ok_or(Error::Conflict)?.max(
                    (saved.generation >> 32)
                        .checked_add(1)
                        .ok_or(Error::Conflict)?,
                );
                h
            };
            h.epoch.checked_mul(1_u64 << 32).ok_or(Error::Conflict)?;
            h.advance()?;
            let applied = if retired.is_empty() {
                self.cas(old, &h, None).await?
            } else {
                self.retire_fields(old, &h, &retired).await?
            };
            if applied {
                self.connection
                    .active_epoch
                    .store(h.epoch, Ordering::Release);
                self.connection
                    .live_nodes
                    .write()
                    .expect("node overlay lock poisoned")
                    .clear();
                let session = Session {
                    store: self.clone(),
                    epoch: h.epoch,
                    inspection: Arc::new(Mutex::new(None)),
                    generations: Arc::new(AtomicU64::new(
                        h.epoch.checked_mul(1_u64 << 32).ok_or(Error::Conflict)?,
                    )),
                };
                // Object CAS does not advance the shared header. An old writer
                // can therefore commit after the pre-fence scan but before the
                // epoch change. Read once after fencing; no new Session is yet
                // exposed and all old-epoch Redis writes are now rejected.
                let active = self.raw().await?;
                session.header(&active.get(HEADER).cloned())?;
                self.seed_publication(h.epoch, snapshot(&active)?);
                session.compact_retired_counters(&active).await?;
                return Ok(session);
            }
        }
        Err(Error::Unavailable(
            "concurrent control writes; retry request".into(),
        ))
    }
}

fn legacy_deleted_capsule(id: &str, encoded: &str) -> Result<StoredEnvironment> {
    let mut value: serde_json::Value = decode(encoded)?;
    let result = value
        .get("result")
        .ok_or_else(|| Error::Unavailable("unsupported legacy control record".into()))?;
    if result.get("state").and_then(serde_json::Value::as_str) != Some("Deleted")
        || result
            .get("resources_held")
            .and_then(serde_json::Value::as_bool)
            != Some(false)
        || result
            .get("restart_pending")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        || result.get("checkpoint").is_some_and(|v| !v.is_null())
        || value.get("recovery").is_some_and(|v| !v.is_null())
    {
        return Err(Error::Unavailable(
            "legacy control record requires explicit recovery".into(),
        ));
    }
    fn rename_spec(value: &mut serde_json::Value) -> Result<()> {
        let fields = value
            .as_object_mut()
            .ok_or_else(|| Error::Unavailable("corrupt legacy control record".into()))?;
        if fields.contains_key("runtime_profile") {
            return Err(Error::Unavailable(
                "ambiguous legacy runtime profile".into(),
            ));
        }
        if let Some(profile) = fields.remove("environment") {
            fields.insert("runtime_profile".into(), profile);
        }
        Ok(())
    }
    fn rename_assignment(value: &mut serde_json::Value) -> Result<()> {
        let fields = value
            .as_object_mut()
            .ok_or_else(|| Error::Unavailable("corrupt legacy assignment".into()))?;
        if fields.contains_key("environment_id") {
            return Err(Error::Unavailable("ambiguous legacy assignment".into()));
        }
        let old_id = fields
            .remove("capsule_id")
            .ok_or_else(|| Error::Unavailable("corrupt legacy assignment".into()))?;
        fields.insert("environment_id".into(), old_id);
        Ok(())
    }
    rename_spec(&mut value["spec"])?;
    rename_assignment(&mut value["assignment"])?;
    rename_spec(&mut value["result"]["spec"])?;
    rename_assignment(&mut value["result"]["assignment"])?;
    let record: StoredEnvironment = serde_json::from_value(value)
        .map_err(|_| Error::Unavailable("corrupt legacy control record".into()))?;
    record
        .validate()
        .map_err(|_| Error::Unavailable("corrupt legacy control record".into()))?;
    if record.spec.id != id {
        return Err(Error::Unavailable(
            "legacy control identity mismatch".into(),
        ));
    }
    Ok(record)
}
fn snapshot(raw: &BTreeMap<String, String>) -> Result<StoredSnapshot> {
    let h: Header = decode(
        raw.get(HEADER)
            .ok_or_else(|| Error::Unavailable("control metadata missing".into()))?,
    )?;
    h.validate()?;
    let mut out = StoredSnapshot {
        shard_count: h.shards,
        generation: h.generation,
        revision: h.revision,
        nodes: BTreeMap::new(),
        environments: BTreeMap::new(),
    };
    for (key, value) in raw {
        if let Some(id) = key.strip_prefix("node:") {
            out.nodes.insert(id.into(), decode(value)?);
        } else if let Some(id) = key.strip_prefix("environment:") {
            out.environments.insert(id.into(), decode(value)?);
        } else if key != HEADER
            && key != "epoch"
            && key != "claim-fence"
            && !key.starts_with("node-session:")
            && !key.starts_with("node-ownership:")
            && !key.starts_with("retired-node:")
            && !key.starts_with("node-departed:")
        {
            return Err(Error::Unavailable("unknown control metadata field".into()));
        }
    }
    out.generation = out
        .environments
        .values()
        .map(|e| e.assignment.generation)
        .max()
        .unwrap_or(0)
        .max(h.generation);
    out.validate()?;
    Ok(out)
}
#[derive(Clone)]
pub struct Session {
    store: RedisStore,
    epoch: u64,
    inspection: Arc<Mutex<Option<inspection::InspectionInventory>>>,
    generations: Arc<AtomicU64>,
}
impl Session {
    pub(crate) fn committed_changes(&self) -> broadcast::Receiver<ControlChange> {
        self.store.connection.committed_changes.subscribe()
    }

    pub async fn revision(&self) -> Result<u64> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        Ok(self
            .store
            .connection
            .publication_sequence
            .load(Ordering::Acquire))
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub async fn advertise(&self, namespace: &str, address: &str, ttl: Duration) -> Result<()> {
        let (key, control) = adx_discovery::keys(namespace)?;
        if control != self.store.key || ttl.as_millis() == 0 || ttl.as_millis() > u64::MAX as u128 {
            return Err(Error::Invalid("invalid discovery publication".into()));
        }
        let record = adx_discovery::CoordinatorEndpoint {
            schema: 1,
            epoch: self.epoch,
            address: address.into(),
        };
        record.validate()?;
        for _ in 0..ATTEMPTS {
            let [header_value] = self.store.fields([HEADER.into()]).await?;
            self.header(&header_value)?;
            let expected_header = header_value.as_deref().ok_or(Error::Conflict)?;
            let mut cmd = redis::cmd("EVAL");
            cmd.arg("if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end; redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]); return 1")
                .arg(2)
                .arg(&self.store.key)
                .arg(&key)
                .arg(expected_header)
                .arg(encode(&record)?)
                .arg(ttl.as_millis() as u64);
            let accepted: u64 = self.store.query(cmd).await?;
            if accepted == 1 {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "discovery publication contention".into(),
        ))
    }

    pub async fn bootstrap_credential(
        &self,
        key: &str,
        credential: &crate::auth::Credential,
    ) -> Result<()> {
        credential.validate()?;
        let digest = crate::auth::digest(key)?;
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let expected_header = header_value.as_deref().ok_or(Error::Conflict)?;
        let encoded = serde_json::to_string(credential)
            .map_err(|_| Error::Invalid("credential encoding failed".into()))?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
            if redis.call('HEXISTS', KEYS[3], ARGV[2]) == 1 then return 1 end
            local old=redis.call('HGET', KEYS[2], ARGV[2])
            if old and old ~= ARGV[3] then return 0 end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
            return 1
        "#,
            )
            .arg(3)
            .arg(&self.store.key)
            .arg(format!("{}:credentials", self.store.key))
            .arg(format!("{}:revoked-credentials", self.store.key))
            .arg(expected_header)
            .arg(digest)
            .arg(encoded);
        let accepted: u64 = self.store.query(command).await?;
        if accepted != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub async fn credential(&self, digest: &str) -> Result<crate::auth::Credential> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        self.header(&header_value)?;
        let mut command = redis::cmd("HGET");
        command
            .arg(format!("{}:credentials", self.store.key))
            .arg(digest);
        let value: Option<String> = self.store.query(command).await?;
        let value: crate::auth::Credential = decode(value.as_deref().ok_or(Error::NotFound)?)?;
        value.validate()?;
        Ok(value)
    }

    pub async fn get(&self, id: &str) -> Result<StoredEnvironment> {
        let [header_value, environment_value] = self
            .store
            .fields([HEADER.into(), format!("environment:{id}")])
            .await?;
        self.header(&header_value)?;
        let record: StoredEnvironment =
            decode(environment_value.as_deref().ok_or(Error::NotFound)?)?;
        record.validate()?;
        Ok(record)
    }

    pub(crate) async fn get_node(&self, id: &str) -> Result<StoredNode> {
        if let Some(node) = self
            .store
            .connection
            .live_nodes
            .read()
            .expect("node overlay lock poisoned")
            .get(id)
            .cloned()
        {
            node.node.validate()?;
            return Ok(node);
        }
        let [header, node] = self
            .store
            .fields([HEADER.into(), format!("node:{id}")])
            .await?;
        self.header(&header)?;
        let node: StoredNode = decode(node.as_deref().ok_or(Error::NotFound)?)?;
        node.node.validate()?;
        Ok(node)
    }
    fn header(&self, value: &Option<String>) -> Result<Header> {
        let header: Header = decode(value.as_deref().ok_or(Error::Conflict)?)?;
        header.validate()?;
        if header.epoch != self.epoch {
            return Err(Error::Conflict);
        }
        Ok(header)
    }
    pub async fn snapshot(&self) -> Result<StoredSnapshot> {
        // Never label an older snapshot with a later event sequence: consumers
        // would otherwise skip the event needed to repair that snapshot.
        for _ in 0..ATTEMPTS {
            let before = self
                .store
                .connection
                .publication_sequence
                .load(Ordering::Acquire);
            let raw = self.store.raw().await?;
            self.header(&raw.get(HEADER).cloned())?;
            let mut saved = snapshot(&raw)?;
            saved.revision = before;
            saved.generation = saved
                .generation
                .max(self.generations.load(Ordering::Acquire));
            for (id, node) in self
                .store
                .connection
                .live_nodes
                .read()
                .expect("node overlay lock poisoned")
                .iter()
            {
                saved.nodes.insert(id.clone(), node.clone());
            }
            if before
                == self
                    .store
                    .connection
                    .publication_sequence
                    .load(Ordering::Acquire)
            {
                return Ok(saved);
            }
        }
        Err(Error::Unavailable(
            "publication snapshot changed; retry".into(),
        ))
    }
    pub(crate) fn observe_node(&self, mut node: StoredNode) {
        // The Coordinator validates heartbeat sessions before this call. The
        // catalog lock also fences a delayed memory-only observation against a
        // concurrent persisted registration, retirement or operator override.
        let mut catalog = self
            .store
            .connection
            .catalog
            .lock()
            .expect("publication catalog invariant");
        if catalog.epoch != self.epoch
            || self.store.connection.active_epoch.load(Ordering::Acquire) != self.epoch
        {
            return;
        }
        let id = node.node.id.clone();
        let Some(current) = catalog.nodes.get(&id) else {
            return;
        };
        if current.session.as_ref().map(|s| &s.id) != node.session.as_ref().map(|s| &s.id)
            || current
                .session
                .as_ref()
                .zip(node.session.as_ref())
                .is_some_and(|(old, new)| old.sequence > new.sequence)
            || current.retirement.as_ref().is_some_and(|r| r.unregistered)
        {
            return;
        }
        node.scheduling_paused = current.scheduling_paused;
        node.node.available &= !node.scheduling_paused;
        if **current == node {
            return;
        }
        let value = PublicationValue::Node(id.clone(), Some(Arc::new(node.clone())));
        catalog.nodes.insert(id.clone(), Arc::new(node.clone()));
        self.store
            .connection
            .live_nodes
            .write()
            .expect("node overlay invariant")
            .insert(id, node);
        self.store
            .send_publication(&mut catalog, vec![value], false, vec![]);
    }
    pub(crate) fn next_generation(&self) -> Result<u64> {
        let base = self.epoch.checked_mul(1_u64 << 32).ok_or(Error::Conflict)?;
        self.generations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value
                    .checked_add(1)
                    .filter(|next| *next - base < (1_u64 << 32))
            })
            .map(|value| value + 1)
            .map_err(|_| Error::Unavailable("execution identity space exhausted".into()))
    }
    /// Node identity keeps its assigned ShardScheduler. Only first registration scans
    /// ShardScheduler counts; capacity refreshes read/write a single node field.
    pub async fn register(
        &self,
        node: Node,
        address: String,
        proxy_address: String,
    ) -> Result<StoredNode> {
        self.register_session(node, address, proxy_address, None)
            .await
    }
    /// Re-enable successful publication after a live process completes recovery.
    /// Ordinary heartbeats never enter this transition or touch Redis.
    async fn restore_node_publication(&self, id: &str, session_id: &str) -> Result<()> {
        let field = format!("node:{id}");
        let token = format!("node-session:{id}");
        for _ in 0..ATTEMPTS {
            let [header, raw, active] = self
                .store
                .fields([HEADER.into(), field.clone(), token.clone()])
                .await?;
            self.header(&header)?;
            let raw = raw.ok_or(Error::NotFound)?;
            let mut registered: StoredNode = decode(&raw)?;
            if registered
                .session
                .as_ref()
                .is_none_or(|session| session.id != session_id)
            {
                return Err(Error::Conflict);
            }
            if active.as_deref() == Some(session_id)
                && registered.last_incident_id.is_none()
                && registered.retirement.is_none()
            {
                return Ok(());
            }
            if active.as_deref().is_some_and(|s| s != session_id) {
                return Err(Error::Conflict);
            }
            registered.last_incident_id = None;
            registered.retirement = None;
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
                if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
                if (redis.call('HGET',KEYS[1],ARGV[4]) or '') ~= ARGV[7] then return 0 end
                redis.call('HSET',KEYS[1],ARGV[2],ARGV[6],ARGV[4],ARGV[5]); return 1
            "#,
                )
                .arg(1)
                .arg(&self.store.key)
                .arg(self.epoch.to_string())
                .arg(&field)
                .arg(raw)
                .arg(&token)
                .arg(session_id)
                .arg(encode(&registered)?)
                .arg(active.as_deref().unwrap_or(""));
            let write = self
                .store
                .publication_write(self.epoch, vec![field.clone()])
                .await;
            let applied = self.store.query::<u8>(command).await? == 1;
            write.finish(
                applied,
                vec![PublicationValue::Node(
                    id.into(),
                    Some(Arc::new(registered)),
                )],
            );
            if applied {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "node recovery registration changed; retry request".into(),
        ))
    }

    pub async fn register_session(
        &self,
        node: Node,
        address: String,
        proxy_address: String,
        session: Option<NodeSession>,
    ) -> Result<StoredNode> {
        self.register_session_with_scopes(node, address, proxy_address, session, BTreeSet::new())
            .await
    }

    pub async fn register_session_with_scopes(
        &self,
        mut node: Node,
        address: String,
        proxy_address: String,
        session: Option<NodeSession>,
        checkpoint_scopes: BTreeSet<String>,
    ) -> Result<StoredNode> {
        node.validate()?;
        if address.trim().is_empty() || proxy_address.trim().is_empty() {
            return Err(Error::Invalid("node addresses required".into()));
        }
        let cached = self
            .store
            .connection
            .live_nodes
            .read()
            .expect("node overlay lock poisoned")
            .get(&node.id)
            .cloned();
        if let Some(previous) = cached {
            if previous.retirement.as_ref().is_some_and(|r| r.unregistered)
                && previous.session.as_ref().map(|s| &s.id) == session.as_ref().map(|s| &s.id)
            {
                return Err(Error::Conflict);
            }
            if self.store.connection.active_epoch.load(Ordering::Acquire) == self.epoch
                && previous.address == address
                && previous.proxy_address == proxy_address
                && previous.session.as_ref().map(|s| &s.id) == session.as_ref().map(|s| &s.id)
            {
                if let Some(current) = session.as_ref().filter(|current| {
                    current.routable && previous.session.as_ref().is_some_and(|old| !old.routable)
                }) {
                    self.restore_node_publication(&node.id, &current.id).await?;
                }
                node.available &= !previous.scheduling_paused;
                let checkpoint_gc_fenced =
                    previous.checkpoint_gc_fenced && !session.as_ref().is_some_and(|s| s.routable);
                let record = StoredNode {
                    retirement: if session.as_ref().is_some_and(|s| s.routable) {
                        None
                    } else {
                        previous.retirement.clone()
                    },
                    last_incident_id: if session.as_ref().is_some_and(|s| s.routable) {
                        None
                    } else {
                        previous.last_incident_id.clone()
                    },
                    node,
                    address,
                    proxy_address,
                    session,
                    shard_id: previous.shard_id,
                    scheduling_paused: previous.scheduling_paused,
                    checkpoint_scopes: checkpoint_scopes.clone(),
                    checkpoint_gc_fenced,
                };
                self.observe_node(record.clone());
                return Ok(record);
            }
        }
        let field = format!("node:{}", node.id);
        for _ in 0..ATTEMPTS {
            let retired_field = format!("retired-node:{}", node.id);
            let departed_field = node_retirement::departure_field(
                &node.id,
                session.as_ref().map_or("", |s| s.id.as_str()),
            );
            let [header_value, node_value, retired_value, departed] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    retired_field.clone(),
                    departed_field,
                ])
                .await?;
            if departed.is_some() {
                return Err(Error::Conflict);
            }
            let header = self.header(&header_value)?;
            if node_value.is_none() {
                if let Some(raw) = retired_value.as_deref() {
                    let record = self
                        .register_retired_node(
                            &node,
                            &address,
                            &proxy_address,
                            session.as_ref(),
                            &checkpoint_scopes,
                            raw,
                        )
                        .await?;
                    if let Some(record) = record {
                        return Ok(record);
                    }
                    continue;
                }
            }
            let previous = node_value
                .as_deref()
                .map(decode::<StoredNode>)
                .transpose()?;
            if let Some(previous) = &previous {
                if previous.retirement.as_ref().is_some_and(|r| r.unregistered)
                    && previous.session.as_ref().map(|s| &s.id) == session.as_ref().map(|s| &s.id)
                {
                    return Err(Error::Conflict);
                }
                if previous.address == address
                    && previous.proxy_address == proxy_address
                    && previous.session.as_ref().map(|s| &s.id) == session.as_ref().map(|s| &s.id)
                {
                    if let Some(current) = session.as_ref().filter(|current| {
                        current.routable
                            && previous.session.as_ref().is_some_and(|old| !old.routable)
                    }) {
                        self.restore_node_publication(&node.id, &current.id).await?;
                    }
                    node.available &= !previous.scheduling_paused;
                    let checkpoint_gc_fenced = previous.checkpoint_gc_fenced
                        && !session.as_ref().is_some_and(|s| s.routable);
                    let record = StoredNode {
                        retirement: if session.as_ref().is_some_and(|s| s.routable) {
                            None
                        } else {
                            previous.retirement.clone()
                        },
                        last_incident_id: if session.as_ref().is_some_and(|s| s.routable) {
                            None
                        } else {
                            previous.last_incident_id.clone()
                        },
                        node,
                        address,
                        proxy_address,
                        session,
                        shard_id: previous.shard_id,
                        scheduling_paused: previous.scheduling_paused,
                        checkpoint_scopes: checkpoint_scopes.clone(),
                        checkpoint_gc_fenced,
                    };
                    self.observe_node(record.clone());
                    return Ok(record);
                }
            }
            let (shard, scheduling_paused, checkpoint_gc_fenced) = if let Some(previous) = &previous
            {
                (
                    previous.shard_id,
                    previous.scheduling_paused,
                    previous.checkpoint_gc_fenced && !session.as_ref().is_some_and(|s| s.routable),
                )
            } else {
                let current = self.snapshot().await?;
                let mut counts = vec![0usize; header.shards];
                for node in current.nodes.values() {
                    counts[node.shard_id] += 1;
                }
                let shard = (0..header.shards)
                    .map(|offset| (header.next_node_shard + offset) % header.shards)
                    .min_by_key(|shard| counts[*shard])
                    .expect("nonempty shards");
                (shard, false, false)
            };
            if scheduling_paused {
                node.available = false;
            }
            let record = StoredNode {
                retirement: previous
                    .as_ref()
                    .filter(|n| {
                        n.session.as_ref().map(|s| &s.id) == session.as_ref().map(|s| &s.id)
                    })
                    .and_then(|n| n.retirement.clone()),
                // Reconciliation heartbeats keep the active outage. A confirmed
                // routable registration closes it, even within the same process.
                last_incident_id: if session.as_ref().is_some_and(|s| s.routable) {
                    None
                } else {
                    previous
                        .as_ref()
                        .and_then(|node| node.last_incident_id.clone())
                },
                node: node.clone(),
                shard_id: shard,
                address: address.clone(),
                proxy_address: proxy_address.clone(),
                session: session.clone(),
                scheduling_paused,
                checkpoint_scopes: checkpoint_scopes.clone(),
                checkpoint_gc_fenced,
            };
            let encoded = encode(&record)?;
            if node_value.as_ref() == Some(&encoded) {
                self.observe_node(record.clone());
                return Ok(record);
            }
            let mut fields = vec![(field.clone(), encoded.clone())];
            let catalog = if let Some((previous, next)) = previous
                .as_ref()
                .and_then(|node| node.session.as_ref())
                .zip(record.session.as_ref())
                .filter(|(previous, next)| previous.id != next.id)
            {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                let environments = snapshot(&catalog.control)?.environments;
                if catalog.control.get(&field) != node_value.as_ref() {
                    continue;
                }
                for mut environment in environments.into_values().filter(|environment| {
                    environment.assignment.node_id == record.node.id
                        && !environment.invalidated
                        && volumes::has_volumes(&environment.spec)
                }) {
                    catalog.rebind_execution_session(&mut environment, &previous.id, &next.id)?;
                    fields.push((
                        format!("environment:{}", environment.spec.id),
                        encode(&environment)?,
                    ));
                }
                Some(catalog)
            } else {
                None
            };
            let expected = header_value.as_deref().ok_or(Error::Conflict)?;
            let applied = match catalog {
                Some(catalog) => {
                    self.control_volume_cas(
                        expected,
                        &catalog.advanced_header()?,
                        &fields,
                        &catalog,
                        None,
                    )
                    .await?
                }
                None => {
                    self.store
                        .object_cas(
                            expected,
                            &header,
                            Some((&field, node_value.as_deref(), encoded)),
                        )
                        .await?
                }
            };
            if applied {
                return Ok(record);
            }
        }
        Err(Error::Unavailable(
            "concurrent node registration; retry request".into(),
        ))
    }

    /// Persist an operator admission override without changing node ownership or session.
    pub async fn set_node_scheduling(
        &self,
        id: &str,
        paused: bool,
        available: bool,
    ) -> Result<StoredNode> {
        if id.trim().is_empty() {
            return Err(Error::Invalid("node id required".into()));
        }
        let field = format!("node:{id}");
        for _ in 0..ATTEMPTS {
            let [header_value, node_value] =
                self.store.fields([HEADER.into(), field.clone()]).await?;
            let header = self.header(&header_value)?;
            node_value.as_deref().ok_or(Error::NotFound)?;
            let mut record = self.get_node(id).await?;
            let available = available && !paused;
            if record.scheduling_paused == paused && record.node.available == available {
                return Ok(record);
            }
            record.scheduling_paused = paused;
            record.node.available = available;
            if self
                .store
                .object_cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, node_value.as_deref(), encode(&record)?)),
                )
                .await?
            {
                return Ok(record);
            }
        }
        Err(Error::Unavailable(
            "concurrent node scheduling update; retry request".into(),
        ))
    }
    /// Persist a scheduler-selected assignment before sending it to the node.
    /// Ordinary node lifecycle operations do not write an intent here.
    pub async fn reserve(
        &self,
        spec: EnvironmentSpec,
        assignment: Assignment,
    ) -> Result<StoredEnvironment> {
        self.reserve_with_request(spec, assignment, "").await
    }
    pub async fn reserve_with_request(
        &self,
        spec: EnvironmentSpec,
        assignment: Assignment,
        request_id: &str,
    ) -> Result<StoredEnvironment> {
        self.pin_volumes(&spec).await?;
        if let Some(id) = &spec.snapshot_id {
            let snapshot = self.get_snapshot(id).await?;
            if snapshot.template.tenant_id != spec.tenant_id
                || !snapshot
                    .references
                    .contains(&adx_core::snapshots::Reference::Restore {
                        environment_id: spec.id.clone(),
                    })
            {
                return Err(Error::Conflict);
            }
        }
        let record = StoredEnvironment {
            creation_generation: assignment.generation,
            creation: Some(CreationSummary::new(request_id)?),
            last_interruption: None,
            volume_execution: None,
            resume: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec,
            assignment,
            result: None,
        };
        record.validate()?;
        if record.assignment.generation >> 32 == self.epoch {
            self.generations
                .fetch_max(record.assignment.generation, Ordering::AcqRel);
        }
        let field = format!("environment:{}", record.spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", record.assignment.node_id),
                ])
                .await?;
            let header = self.header(&header_value)?;
            if let Some(environment_value) = &environment_value {
                let existing: StoredEnvironment = decode(environment_value)?;
                return if existing.spec == record.spec && existing.assignment == record.assignment {
                    Ok(existing)
                } else {
                    Err(Error::Conflict)
                };
            }
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != record.assignment.shard_id {
                return Err(Error::Conflict);
            }
            let catalog = if volumes::has_volumes(&record.spec) {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                catalog.reserve(&record, &node)?;
                Some(catalog)
            } else {
                None
            };

            if self
                .assignment_cas(&header, None, &record, catalog.as_ref(), None)
                .await?
            {
                return Ok(record);
            }
        }
        Err(Error::Unavailable(
            "concurrent allocation; retry request".into(),
        ))
    }
    /// Replace an unexecuted reservation after its node explicitly rejected it
    /// or confirmed cleanup. This is not failover of a running Environment.
    pub async fn replace_rejected(
        &self,
        previous: &Assignment,
        replacement: Assignment,
    ) -> Result<StoredEnvironment> {
        if previous.environment_id != replacement.environment_id
            || replacement.generation <= previous.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("environment:{}", previous.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let header = self.header(&header_value)?;
            let mut stored_environment: StoredEnvironment =
                decode(environment_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_environment.validate()?;
            if stored_environment.assignment == replacement {
                return Ok(stored_environment);
            }
            if stored_environment.invalidated
                || &stored_environment.assignment != previous
                || stored_environment
                    .result
                    .as_ref()
                    .is_some_and(|r| r.state != EnvironmentState::Failed || r.resources_held)
            {
                return Err(Error::Conflict);
            }
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != replacement.shard_id {
                return Err(Error::Conflict);
            }
            let mut catalog = if volumes::has_volumes(&stored_environment.spec) {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                let source = self.get_node(&previous.node_id).await?;
                catalog.retire_unstarted(&stored_environment, &source)?;
                Some(catalog)
            } else {
                None
            };
            stored_environment.assignment = replacement.clone();
            stored_environment.volume_execution = None;
            stored_environment.result = None;
            stored_environment.validate()?;
            if let Some(catalog) = &mut catalog {
                catalog.reserve(&stored_environment, &node)?;
            }

            if self
                .assignment_cas(
                    &header,
                    environment_value.as_deref(),
                    &stored_environment,
                    catalog.as_ref(),
                    None,
                )
                .await?
            {
                return Ok(stored_environment);
            }
        }
        Err(Error::Unavailable(
            "concurrent reassignment; retry request".into(),
        ))
    }
    /// One result write: state and the route publication cursor change atomically.
    /// Identical replay is idempotent; older versions never roll state backward.
    pub async fn commit(&self, result: EnvironmentRecord) -> Result<EnvironmentRecord> {
        Ok(self.commit_with_state(result).await?.result)
    }

    /// Retire a completed execution and its object cleanup authority atomically.
    /// Submitted cleanup artifacts are valid only on a Deleted result.
    pub async fn commit_with_cleanup(
        &self,
        result: EnvironmentRecord,
        cleanup_artifacts: &[adx_core::CheckpointArtifact],
    ) -> Result<EnvironmentRecord> {
        Ok(self
            .commit_releasing_with_cleanup_state(result, "", &[], cleanup_artifacts)
            .await?
            .result)
    }

    /// Commit a result and return its authoritative summaries without a follow-up read.
    /// Replay returns the stored record; deletion returns only the accepted result.
    pub async fn commit_with_state(
        &self,
        result: EnvironmentRecord,
    ) -> Result<CommittedEnvironment> {
        self.commit_releasing_with_state(result, "", &[]).await
    }

    /// Publish the result and explicitly confirmed stopped attempts in one CAS.
    /// Releasing compute capacity alone does not release a Volume.
    pub async fn commit_releasing(
        &self,
        result: EnvironmentRecord,
        session: &str,
        stopped: &[String],
    ) -> Result<EnvironmentRecord> {
        Ok(self
            .commit_releasing_with_state(result, session, stopped)
            .await?
            .result)
    }

    pub(crate) async fn commit_releasing_with_state(
        &self,
        result: EnvironmentRecord,
        session: &str,
        stopped: &[String],
    ) -> Result<CommittedEnvironment> {
        self.commit_releasing_with_cleanup_state(result, session, stopped, &[])
            .await
    }

    pub(crate) async fn commit_releasing_with_cleanup_state(
        &self,
        result: EnvironmentRecord,
        session: &str,
        stopped: &[String],
        cleanup_artifacts: &[adx_core::CheckpointArtifact],
    ) -> Result<CommittedEnvironment> {
        if !cleanup_artifacts.is_empty() && result.state != EnvironmentState::Deleted {
            return Err(Error::Invalid("cleanup artifacts require Deleted".into()));
        }
        if !cleanup_artifacts.is_empty() {
            adx_core::CheckpointCleanup {
                assignment: result.assignment.clone(),
                revision: result.revision,
                artifacts: checkpoint_cleanup::normalized_artifacts(cleanup_artifacts),
            }
            .validate()?;
        }
        if let Some(checkpoint) = &result.checkpoint {
            if let Some(origin) = checkpoint
                .origin
                .as_ref()
                .filter(|origin| origin.environment_id != result.spec.id)
            {
                let snapshot = self
                    .get_snapshot(result.spec.snapshot_id.as_deref().ok_or(Error::Conflict)?)
                    .await?;
                if snapshot.template.tenant_id != result.spec.tenant_id
                    || snapshot.origin()? != *origin
                    || checkpoint.artifact == snapshot.artifact
                {
                    return Err(Error::Conflict);
                }
            }
        }
        if !stopped.is_empty()
            && (session.is_empty()
                || stopped.len() > 1024
                || stopped.iter().any(|runtime| {
                    !adx_core::valid_runtime_id(
                        &result.spec.id,
                        result.assignment.generation,
                        runtime,
                    )
                }))
        {
            return Err(Error::Invalid(
                "invalid stopped execution identities".into(),
            ));
        }
        if let Some(source) = &result.snapshot_source {
            let snapshot = self
                .get_snapshot(result.spec.snapshot_id.as_deref().ok_or(Error::Conflict)?)
                .await?;
            if snapshot.template.tenant_id != result.spec.tenant_id
                || snapshot.artifact != source.artifact
                || source.origin.as_ref() != Some(&snapshot.origin()?)
                || !snapshot
                    .references
                    .contains(&adx_core::snapshots::Reference::Restore {
                        environment_id: result.spec.id.clone(),
                    })
            {
                return Err(Error::Conflict);
            }
        }
        let field = format!("environment:{}", result.spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, environment_value] =
                self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut counters = CounterUpdate::load(&self.store, field.clone()).await?;
            let mut header = self.header(&header_value)?;
            let Some(environment_value) = environment_value.as_deref() else {
                if result.state == EnvironmentState::Deleted {
                    return match self.store.deleted_receipt(&result.spec.id).await? {
                        Some(receipt) if receipt.matches(&result, cleanup_artifacts)? => {
                            Ok(CommittedEnvironment {
                                result,
                                stored: None,
                            })
                        }
                        Some(_) => Err(Error::Conflict),
                        None => Err(Error::NotFound),
                    };
                }
                return Err(Error::NotFound);
            };
            let mut stored_environment: StoredEnvironment = decode(environment_value)?;
            if !(next_result(&stored_environment, &result)?
                || result.state == EnvironmentState::Running && stored_environment.resume.is_some())
                && stopped.is_empty()
            {
                // A replay is not a new commit and must not republish an older value.
                return Ok(CommittedEnvironment {
                    result,
                    stored: Some(stored_environment),
                });
            }
            let catalog = if volumes::has_volumes(&result.spec) {
                let mut catalog = self.volume_catalog().await?;
                if Some(&catalog.header_value) != header_value.as_ref() {
                    continue;
                }
                catalog.apply_result(&result, session, stopped)?;
                if let Some(execution) = &mut stored_environment.volume_execution {
                    if !result.resources_held
                        && execution.node_session_id == session
                        && stopped.contains(&execution.runtime_id)
                    {
                        execution.released = true;
                    }
                }
                Some(catalog)
            } else {
                None
            };
            let cleanup = if result.state == EnvironmentState::Deleted {
                self.prepare_checkpoint_cleanup(&stored_environment, &result, cleanup_artifacts)
                    .await?
            } else {
                None
            };
            if cleanup.is_some() && catalog.is_none() {
                header.advance()?;
            }
            if result.state != EnvironmentState::Paused {
                stored_environment.resume = None;
                if let Some(recovery) = &mut stored_environment.recovery {
                    recovery.pending = false;
                }
            }
            // Runtime network policy is the only mutable part of an Environment
            // specification. Persist it with the result so later lifecycle
            // commits compare against the version already enforced by the
            // runtime, while every placement and resource field remains
            // fenced by `next_result` above.
            let previous_observation = stored_environment.last_interruption.clone();
            stored_environment.observe_result(&result, &mut counters.value)?;
            stored_environment.spec = result.spec.clone();
            stored_environment.result = Some(result.clone());
            stored_environment.track_failure(collection::now()?);
            stored_environment.validate()?;
            let applied = if result.state == EnvironmentState::Deleted {
                counters.prepare_retirement(&self.store).await?;
                self.store
                    .retire_environment_with_cleanup(
                        (
                            header_value.as_deref().ok_or(Error::Conflict)?,
                            environment_value,
                        ),
                        &header,
                        &DeletedReceipt::from_result(&result, cleanup_artifacts)?,
                        &counters,
                        catalog.as_ref(),
                        cleanup.as_ref(),
                    )
                    .await?
            } else {
                self.assignment_cas(
                    &header,
                    Some(environment_value),
                    &stored_environment,
                    catalog.as_ref(),
                    Some(&counters),
                )
                .await?
            };
            if applied {
                stored_environment.log_observation_change(previous_observation.as_ref());
                let stored =
                    (result.state != EnvironmentState::Deleted).then_some(stored_environment);
                return Ok(CommittedEnvironment { result, stored });
            }
        }
        Err(Error::Unavailable(
            "concurrent environment commit; retry request".into(),
        ))
    }
}

#[cfg(test)]
use crate::rpc::cancellation_test_redis as transport_test_redis;

#[cfg(test)]
mod transport_tests {
    use super::*;

    async fn client_id(store: &RedisStore) -> u64 {
        let mut command = redis::cmd("CLIENT");
        command.arg("ID");
        store.query(command).await.unwrap()
    }

    #[tokio::test]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn redis_command_error_does_not_discard_ordered_transport() {
        let redis = transport_test_redis::Redis::new().await;
        let store = redis.store().await;
        let before = client_id(&store).await;
        let mut command = redis::cmd("EVAL");
        command
            .arg("return redis.error_reply('validation rejected')")
            .arg(0);
        assert!(store.query::<u8>(command).await.is_err());
        assert_eq!(
            client_id(&store).await,
            before,
            "a command rejection must preserve the transport ordering of other pending writes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires isolated Redis; set ADX_TEST_REDIS_SERVER"]
    async fn delayed_failure_of_old_transport_preserves_reconnected_client() {
        let redis = transport_test_redis::Redis::new().await;
        let store = redis.store().await;
        let before = client_id(&store).await;
        let old = store.connection.current.lock().await.clone().unwrap();
        let control = RedisStore::connect(&redis.url, "control", Duration::from_secs(2))
            .await
            .unwrap();
        let mut kill = redis::cmd("CLIENT");
        kill.arg("KILL").arg("ID").arg(before);
        assert_eq!(control.query::<u8>(kill).await.unwrap(), 1);
        let _: String = store.query(redis::cmd("PING")).await.unwrap();
        let replacement = client_id(&store).await;
        assert_ne!(replacement, before);
        // Resume the failure callback of a second request on the old transport
        // only after a real disconnect/reconnect has installed its replacement.
        store.discard_connection(&old).await;
        assert_eq!(client_id(&store).await, replacement);
    }
}
