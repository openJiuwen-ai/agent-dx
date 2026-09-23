//! Single-Master Redis persistence. Lua only compares opaque bytes and performs
//! atomic hash writes; JSON and u64 counters are validated in Rust, never Lua doubles.
mod claims;
pub use claims::{ClaimOutcome, LocalClaim};
mod credentials;
mod failure;
mod recovery;
mod snapshots;
use crate::Node;
use adx_core::{
    scheduling::validate_device_assignment, Assignment, CapsuleRecord, CapsuleSpec, CapsuleState,
    Error, Result,
};
use redis::{aio::MultiplexedConnection, FromRedisValue};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::timeout};

const HEADER: &str = "header";
const CAS: &str = r#"
local old = redis.call('HGET', KEYS[1], 'header')
if ARGV[1] == '' then
  if redis.call('HLEN', KEYS[1]) ~= 0 then return 0 end
elseif old ~= ARGV[1] then return 0 end
if #ARGV == 2 then
  redis.call('HSET', KEYS[1], 'header', ARGV[2])
else
  redis.call('HSET', KEYS[1], 'header', ARGV[2], ARGV[3], ARGV[4])
end
return 1
"#;
const ATTEMPTS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Header {
    schema: u32,
    #[serde(alias = "domains")]
    shards: usize,
    epoch: u64,
    generation: u64,
    revision: u64,
    #[serde(alias = "next_node_domain")]
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
pub struct StoredNode {
    pub node: Node,
    #[serde(alias = "domain_id")]
    pub shard_id: usize,
    pub address: String,
    pub proxy_address: String,
    #[serde(default)]
    pub session: Option<NodeSession>,
    /// Administrative admission override. Heartbeats cannot clear it.
    #[serde(default)]
    pub scheduling_paused: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub source: Assignment,
    pub pending: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCapsule {
    #[serde(default)]
    pub recovery: Option<Recovery>,
    #[serde(default)]
    pub invalidated: bool,
    pub spec: CapsuleSpec,
    pub assignment: Assignment,
    pub result: Option<CapsuleRecord>,
}
impl StoredCapsule {
    pub fn resources_held(&self) -> bool {
        (!self.invalidated && self.recovery.as_ref().is_some_and(|r| r.pending))
            || self.result.as_ref().is_none_or(|r| r.resources_held)
    }
    pub(crate) fn effective_record(&self) -> CapsuleRecord {
        self.result.clone().unwrap_or_else(|| CapsuleRecord {
            restart_attempts: 0,
            restart_pending: false,
            runtime: adx_core::Runtime {
                id: format!("{}-{}", self.spec.id, self.assignment.generation),
                ip: None,
            },
            spec: self.spec.clone(),
            assignment: self.assignment.clone(),
            state: CapsuleState::Pending,
            revision: 0,
            resources_held: true,
            checkpoint: None,
            last_operation: None,
        })
    }
    fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if self.spec.id != self.assignment.capsule_id || self.assignment.generation == 0 {
            return Err(Error::Conflict);
        }
        validate_device_assignment(&self.spec.scheduling.devices, &self.assignment.devices)?;
        if let Some(r) = &self.result {
            validate_result(r)?;
            if r.spec != self.spec || r.assignment != self.assignment {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub capsule_id: String,
    pub node_id: String,
    pub proxy_address: String,
    pub generation: u64,
    pub capsule_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub shard_count: usize,
    pub generation: u64,
    /// Cluster publication cursor, including changes that do not add a route.
    pub revision: u64,
    pub nodes: BTreeMap<String, StoredNode>,
    pub capsules: BTreeMap<String, StoredCapsule>,
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
        for (id, i) in &self.capsules {
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
    /// Failed and Deleted results have no route. Node Proxy still checks binding.
    pub fn routes(&self) -> Result<Vec<Route>> {
        self.validate()?;
        Ok(self
            .capsules
            .values()
            .filter_map(|i| {
                i.result
                    .as_ref()
                    .filter(|r| {
                        r.state == CapsuleState::Running
                            && self.nodes[&r.assignment.node_id]
                                .session
                                .as_ref()
                                .is_none_or(|s| s.routable)
                    })
                    .map(|r| Route {
                        capsule_id: r.spec.id.clone(),
                        node_id: r.assignment.node_id.clone(),
                        proxy_address: self.nodes[&r.assignment.node_id].proxy_address.clone(),
                        generation: r.assignment.generation,
                        capsule_revision: r.revision,
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
fn validate_result(r: &CapsuleRecord) -> Result<()> {
    if r.restart_attempts
        > r.spec
            .lifecycle
            .restart
            .as_ref()
            .map_or(0, |p| p.max_attempts)
        || (r.restart_pending
            && (r.state != CapsuleState::Failed
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
    if let Some(cp) = &r.checkpoint {
        if cp.id.is_empty()
            || cp.artifact.storage.is_empty()
            || cp.artifact.location.is_empty()
            || cp.artifact.size_bytes == 0
            || cp.expires_at_unix_seconds == 0
            || match &cp.origin {
                None => !adx_core::valid_runtime_id(
                    &r.spec.id,
                    r.assignment.generation,
                    &cp.source_runtime_id,
                ),
                Some(origin) => {
                    (if origin.capsule_id == r.spec.id {
                        origin.ownership_generation >= r.assignment.generation
                    } else {
                        r.spec.snapshot_id.is_none()
                    }) || origin.runtime_id != cp.source_runtime_id
                        || !adx_core::valid_runtime_id(
                            &origin.capsule_id,
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
        CapsuleState::Running if r.resources_held && r.runtime.ip.is_some() => Ok(()),
        CapsuleState::Paused
            if !r.resources_held && r.runtime.ip.is_none() && r.checkpoint.is_some() =>
        {
            Ok(())
        }
        CapsuleState::Deleted if !r.resources_held => Ok(()),
        CapsuleState::Failed => Ok(()),
        _ => Err(Error::Invalid(
            "expected a completed node result with consistent resource ownership".into(),
        )),
    }
}
fn next_result(old: &StoredCapsule, r: &CapsuleRecord) -> Result<bool> {
    let mut expected_spec = old.spec.clone();
    expected_spec.sandbox.network = r.spec.sandbox.network.clone();
    if expected_spec != r.spec || old.assignment != r.assignment {
        return Err(Error::Conflict);
    }
    validate_result(r)?;
    if old.invalidated
        && (!matches!(r.state, CapsuleState::Failed | CapsuleState::Deleted)
            || r.resources_held
            || r.restart_pending
            || r.runtime.ip.is_some()
            || (r.state != CapsuleState::Deleted
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
        let restarting = previous.state == CapsuleState::Failed
            && previous.restart_pending
            && previous.restart_attempts.checked_add(1) == Some(r.restart_attempts)
            && r.runtime.id != previous.runtime.id
            && r.checkpoint == previous.checkpoint
            && matches!(r.state, CapsuleState::Running | CapsuleState::Failed);
        if r.restart_attempts < previous.restart_attempts
            || (r.restart_attempts != previous.restart_attempts && !restarting)
            || r.revision <= previous.revision
            || previous.state == CapsuleState::Deleted
            || (!previous.resources_held
                && r.resources_held
                && !restarting
                && !(previous.state == CapsuleState::Paused
                    && r.checkpoint == previous.checkpoint
                    && r.runtime.id != previous.runtime.id
                    && (r.state == CapsuleState::Failed
                        || r.last_operation.as_ref().is_some_and(|op| {
                            op.kind == adx_core::LifecycleKind::Resume
                                && op.expected_revision == previous.revision
                        }))))
        {
            return Err(Error::Conflict);
        }
    }
    Ok(true)
}

struct Connection {
    client: redis::Client,
    current: Mutex<Option<MultiplexedConnection>>,
    timeout: Duration,
}
#[derive(Clone)]
pub struct RedisStore {
    connection: Arc<Connection>,
    key: String,
}
impl RedisStore {
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
        let store = Self {
            connection: Arc::new(Connection {
                client,
                current: Mutex::new(None),
                timeout: duration,
            }),
            key: format!("adx:{{{namespace}}}:control:v1"),
        };
        let _: String = store.query(redis::cmd("PING")).await?;
        Ok(store)
    }
    async fn query<T: FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        let operation = async {
            let mut guard = self.connection.current.lock().await;
            if guard.is_none() {
                *guard = Some(
                    self.connection
                        .client
                        .get_multiplexed_async_connection()
                        .await?,
                );
            }
            let mut conn = guard.as_ref().expect("connection initialized").clone();
            drop(guard);
            cmd.query_async(&mut conn).await
        };
        match timeout(self.connection.timeout, operation).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                *self.connection.current.lock().await = None;
                // A timeout may follow an applied write. Never replay it blindly:
                // the caller retries the same identity/result through CAS.
                Err(Error::Unavailable(
                    "Redis operation unavailable or timed out".into(),
                ))
            }
        }
    }
    async fn fields<const N: usize>(&self, fields: [String; N]) -> Result<[Option<String>; N]> {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.key).arg(&fields);
        let values: Vec<Option<String>> = self.query(cmd).await?;
        values.try_into().map_err(|_| {
            Error::Unavailable("Redis returned an unexpected control field count".into())
        })
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
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(CAS)
            .arg(1)
            .arg(&self.key)
            .arg(expected)
            .arg(encode(header)?);
        if let Some((name, value)) = field {
            cmd.arg(name).arg(value);
        }
        let applied: u8 = self.query(cmd).await?;
        Ok(applied == 1)
    }
    /// Called once per Master startup, never on a Redis reconnect. A new epoch
    /// rejects old Master writes; this is fencing, not leader election or HA.
    pub async fn begin(&self, shards: usize) -> Result<Session> {
        if shards == 0 {
            return Err(Error::Invalid("shard count must be positive".into()));
        }
        for _ in 0..ATTEMPTS {
            let raw = self.raw().await?;
            let old = raw.get(HEADER).map(String::as_str).unwrap_or("");
            let mut h = if raw.is_empty() {
                Header {
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
                snapshot(&raw)?.validate()?;
                h.epoch = h.epoch.checked_add(1).ok_or(Error::Conflict)?;
                h
            };
            h.advance()?;
            if self.cas(old, &h, None).await? {
                return Ok(Session {
                    store: self.clone(),
                    epoch: h.epoch,
                });
            }
        }
        Err(Error::Unavailable(
            "concurrent control writes; retry request".into(),
        ))
    }
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
        capsules: BTreeMap::new(),
    };
    for (key, value) in raw {
        if let Some(id) = key.strip_prefix("node:") {
            out.nodes.insert(id.into(), decode(value)?);
        } else if let Some(id) = key.strip_prefix("capsule:") {
            out.capsules.insert(id.into(), decode(value)?);
        } else if key != HEADER {
            return Err(Error::Unavailable("unknown control metadata field".into()));
        }
    }
    out.validate()?;
    Ok(out)
}
#[derive(Clone)]
pub struct Session {
    store: RedisStore,
    epoch: u64,
}
impl Session {
    pub async fn revision(&self) -> Result<u64> {
        let [header_value] = self.store.fields([HEADER.into()]).await?;
        Ok(self.header(&header_value)?.revision)
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub async fn advertise(&self, namespace: &str, address: &str, ttl: Duration) -> Result<()> {
        let (key, control) = adx_discovery::keys(namespace)?;
        if control != self.store.key || ttl.as_millis() == 0 || ttl.as_millis() > u64::MAX as u128 {
            return Err(Error::Invalid("invalid discovery publication".into()));
        }
        let record = adx_discovery::MasterEndpoint {
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

    pub async fn get(&self, id: &str) -> Result<StoredCapsule> {
        let [header_value, capsule_value] = self
            .store
            .fields([HEADER.into(), format!("capsule:{id}")])
            .await?;
        self.header(&header_value)?;
        let record: StoredCapsule = decode(capsule_value.as_deref().ok_or(Error::NotFound)?)?;
        record.validate()?;
        Ok(record)
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
        let raw = self.store.raw().await?;
        self.header(&raw.get(HEADER).cloned())?;
        snapshot(&raw)
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
    pub async fn register_session(
        &self,
        mut node: Node,
        address: String,
        proxy_address: String,
        session: Option<NodeSession>,
    ) -> Result<StoredNode> {
        node.validate()?;
        if address.trim().is_empty() || proxy_address.trim().is_empty() {
            return Err(Error::Invalid("node addresses required".into()));
        }
        let field = format!("node:{}", node.id);
        for _ in 0..ATTEMPTS {
            let [header_value, node_value] =
                self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&header_value)?;
            let previous = node_value
                .as_deref()
                .map(decode::<StoredNode>)
                .transpose()?;
            let (shard, scheduling_paused) = if let Some(previous) = &previous {
                (previous.shard_id, previous.scheduling_paused)
            } else {
                let current = self.snapshot().await?;
                if current.revision != header.revision {
                    continue;
                }
                let mut counts = vec![0usize; header.shards];
                for node in current.nodes.values() {
                    counts[node.shard_id] += 1;
                }
                let shard = (0..header.shards)
                    .map(|offset| (header.next_node_shard + offset) % header.shards)
                    .min_by_key(|shard| counts[*shard])
                    .expect("nonempty shards");
                header.next_node_shard = (shard + 1) % header.shards;
                (shard, false)
            };
            if scheduling_paused {
                node.available = false;
            }
            let record = StoredNode {
                node: node.clone(),
                shard_id: shard,
                address: address.clone(),
                proxy_address: proxy_address.clone(),
                session: session.clone(),
                scheduling_paused,
            };
            let encoded = encode(&record)?;
            if node_value.as_ref() == Some(&encoded) {
                return Ok(record);
            }
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encoded)),
                )
                .await?
            {
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
            let mut header = self.header(&header_value)?;
            let mut record: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            let available = available && !paused;
            if record.scheduling_paused == paused && record.node.available == available {
                return Ok(record);
            }
            record.scheduling_paused = paused;
            record.node.available = available;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&record)?)),
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
        spec: CapsuleSpec,
        assignment: Assignment,
    ) -> Result<StoredCapsule> {
        if let Some(id) = &spec.snapshot_id {
            let snapshot = self.get_snapshot(id).await?;
            if snapshot.template.tenant_id != spec.tenant_id
                || !snapshot
                    .references
                    .contains(&adx_core::snapshots::Reference::Restore {
                        capsule_id: spec.id.clone(),
                    })
            {
                return Err(Error::Conflict);
            }
        }
        let record = StoredCapsule {
            recovery: None,
            invalidated: false,
            spec,
            assignment,
            result: None,
        };
        record.validate()?;
        let field = format!("capsule:{}", record.spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, capsule_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", record.assignment.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            if let Some(capsule_value) = &capsule_value {
                let existing: StoredCapsule = decode(capsule_value)?;
                return if existing.spec == record.spec && existing.assignment == record.assignment {
                    Ok(existing)
                } else {
                    Err(Error::Conflict)
                };
            }
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != record.assignment.shard_id
                || record.assignment.generation <= header.generation
            {
                return Err(Error::Conflict);
            }
            header.generation = record.assignment.generation;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&record)?)),
                )
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
    /// or confirmed cleanup. This is not failover of a running Capsule.
    pub async fn replace_rejected(
        &self,
        previous: &Assignment,
        replacement: Assignment,
    ) -> Result<StoredCapsule> {
        if previous.capsule_id != replacement.capsule_id
            || replacement.generation <= previous.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("capsule:{}", previous.capsule_id);
        for _ in 0..ATTEMPTS {
            let [header_value, capsule_value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let mut stored_capsule: StoredCapsule =
                decode(capsule_value.as_deref().ok_or(Error::NotFound)?)?;
            stored_capsule.validate()?;
            if stored_capsule.assignment == replacement {
                return Ok(stored_capsule);
            }
            if stored_capsule.invalidated
                || &stored_capsule.assignment != previous
                || stored_capsule
                    .result
                    .as_ref()
                    .is_some_and(|r| r.state != CapsuleState::Failed || r.resources_held)
                || replacement.generation <= header.generation
            {
                return Err(Error::Conflict);
            }
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != replacement.shard_id {
                return Err(Error::Conflict);
            }
            stored_capsule.assignment = replacement.clone();
            stored_capsule.result = None;
            stored_capsule.validate()?;
            header.generation = replacement.generation;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&stored_capsule)?)),
                )
                .await?
            {
                return Ok(stored_capsule);
            }
        }
        Err(Error::Unavailable(
            "concurrent reassignment; retry request".into(),
        ))
    }
    /// One result write: state and the route publication cursor change atomically.
    /// Identical replay is idempotent; older versions never roll state backward.
    pub async fn commit(&self, result: CapsuleRecord) -> Result<CapsuleRecord> {
        if let Some(cp) = &result.checkpoint {
            if let Some(origin) = cp
                .origin
                .as_ref()
                .filter(|o| o.capsule_id != result.spec.id)
            {
                let snapshot = self
                    .get_snapshot(result.spec.snapshot_id.as_deref().ok_or(Error::Conflict)?)
                    .await?;
                if snapshot.template.tenant_id != result.spec.tenant_id
                    || snapshot.origin()? != *origin
                    || cp.artifact == snapshot.artifact
                {
                    return Err(Error::Conflict);
                }
            }
        }
        let field = format!("capsule:{}", result.spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, capsule_value] =
                self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&header_value)?;
            let mut stored_capsule: StoredCapsule =
                decode(capsule_value.as_deref().ok_or(Error::NotFound)?)?;
            if !next_result(&stored_capsule, &result)? {
                return Ok(result);
            }
            if result.state != CapsuleState::Paused {
                if let Some(recovery) = &mut stored_capsule.recovery {
                    recovery.pending = false;
                }
            }
            // Runtime network policy is the only mutable part of an Capsule
            // specification. Persist it with the result so later lifecycle
            // commits compare against the version already enforced by the
            // runtime, while every placement and resource field remains
            // fenced by `next_result` above.
            stored_capsule.spec = result.spec.clone();
            stored_capsule.result = Some(result.clone());
            stored_capsule.validate()?;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value
                        .as_deref()
                        .expect("validated control header is present"),
                    &header,
                    Some((&field, encode(&stored_capsule)?)),
                )
                .await?
            {
                return Ok(result);
            }
        }
        Err(Error::Unavailable(
            "concurrent capsule commit; retry request".into(),
        ))
    }
}

#[cfg(test)]
mod naming_tests {
    use super::*;
    #[test]
    fn reads_old_header_without_losing_epoch_and_writes_only_shard_fields() {
        let old = r#"{"schema":1,"domains":2,"epoch":7,"generation":11,"revision":23,"next_node_domain":1}"#;
        let h: Header = decode(old).unwrap();
        h.validate().unwrap();
        assert_eq!(
            (
                h.shards,
                h.epoch,
                h.generation,
                h.revision,
                h.next_node_shard
            ),
            (2, 7, 11, 23, 1)
        );
        let v: serde_json::Value = serde_json::from_str(&encode(&h).unwrap()).unwrap();
        assert_eq!(v["shards"], 2);
        assert!(v.get("domains").is_none());
        assert!(v.get("next_node_domain").is_none());
    }
}
