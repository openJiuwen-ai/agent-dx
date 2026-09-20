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
    scheduling::validate_device_assignment, Assignment, Error, InstanceRecord, InstanceSpec,
    InstanceState, Result,
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
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub source: Assignment,
    pub pending: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredInstance {
    #[serde(default)]
    pub recovery: Option<Recovery>,
    #[serde(default)]
    pub invalidated: bool,
    pub spec: InstanceSpec,
    pub assignment: Assignment,
    pub result: Option<InstanceRecord>,
}
impl StoredInstance {
    pub fn resources_held(&self) -> bool {
        (!self.invalidated && self.recovery.as_ref().is_some_and(|r| r.pending))
            || self.result.as_ref().is_none_or(|r| r.resources_held)
    }
    pub(crate) fn effective_record(&self) -> InstanceRecord {
        self.result.clone().unwrap_or_else(|| InstanceRecord {
            restart_attempts: 0,
            restart_pending: false,
            runtime_id: format!("{}-{}", self.spec.id, self.assignment.generation),
            spec: self.spec.clone(),
            assignment: self.assignment.clone(),
            state: InstanceState::Pending,
            revision: 0,
            resources_held: true,
            runtime_ip: None,
            checkpoint: None,
            last_operation: None,
        })
    }
    fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        if self.spec.id != self.assignment.instance_id || self.assignment.generation == 0 {
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
    pub instance_id: String,
    pub node_id: String,
    pub proxy_address: String,
    pub generation: u64,
    pub instance_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub shard_count: usize,
    pub generation: u64,
    /// Cluster publication cursor, including changes that do not add a route.
    pub revision: u64,
    pub nodes: BTreeMap<String, StoredNode>,
    pub instances: BTreeMap<String, StoredInstance>,
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
        for (id, i) in &self.instances {
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
            .instances
            .values()
            .filter_map(|i| {
                i.result
                    .as_ref()
                    .filter(|r| {
                        r.state == InstanceState::Running
                            && self.nodes[&r.assignment.node_id]
                                .session
                                .as_ref()
                                .is_none_or(|s| s.routable)
                    })
                    .map(|r| Route {
                        instance_id: r.spec.id.clone(),
                        node_id: r.assignment.node_id.clone(),
                        proxy_address: self.nodes[&r.assignment.node_id].proxy_address.clone(),
                        generation: r.assignment.generation,
                        instance_revision: r.revision,
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
fn validate_result(r: &InstanceRecord) -> Result<()> {
    if r.restart_attempts
        > r.spec
            .lifecycle
            .restart
            .as_ref()
            .map_or(0, |p| p.max_attempts)
        || (r.restart_pending
            && (r.state != InstanceState::Failed
                || r.spec
                    .lifecycle
                    .restart
                    .as_ref()
                    .is_none_or(|p| r.restart_attempts >= p.max_attempts)))
    {
        return Err(Error::Conflict);
    }
    if r.revision == 0
        || !adx_core::valid_runtime_id(&r.spec.id, r.assignment.generation, &r.runtime_id)
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
                    (if origin.instance_id == r.spec.id {
                        origin.ownership_generation >= r.assignment.generation
                    } else {
                        r.spec.snapshot_id.is_none()
                    }) || origin.runtime_id != cp.source_runtime_id
                        || !adx_core::valid_runtime_id(
                            &origin.instance_id,
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
        InstanceState::Running if r.resources_held && r.runtime_ip.is_some() => Ok(()),
        InstanceState::Paused
            if !r.resources_held && r.runtime_ip.is_none() && r.checkpoint.is_some() =>
        {
            Ok(())
        }
        InstanceState::Deleted if !r.resources_held => Ok(()),
        InstanceState::Failed => Ok(()),
        _ => Err(Error::Invalid(
            "expected a completed node result with consistent resource ownership".into(),
        )),
    }
}
fn next_result(old: &StoredInstance, r: &InstanceRecord) -> Result<bool> {
    if old.spec != r.spec || old.assignment != r.assignment {
        return Err(Error::Conflict);
    }
    validate_result(r)?;
    if old.invalidated
        && (!matches!(r.state, InstanceState::Failed | InstanceState::Deleted)
            || r.resources_held
            || r.restart_pending
            || r.runtime_ip.is_some()
            || (r.state != InstanceState::Deleted
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
        let restarting = previous.state == InstanceState::Failed
            && previous.restart_pending
            && previous.restart_attempts.checked_add(1) == Some(r.restart_attempts)
            && r.runtime_id != previous.runtime_id
            && r.checkpoint == previous.checkpoint
            && matches!(r.state, InstanceState::Running | InstanceState::Failed);
        if r.restart_attempts < previous.restart_attempts
            || (r.restart_attempts != previous.restart_attempts && !restarting)
            || r.revision <= previous.revision
            || previous.state == InstanceState::Deleted
            || (!previous.resources_held
                && r.resources_held
                && !restarting
                && !(previous.state == InstanceState::Paused
                    && r.checkpoint == previous.checkpoint
                    && r.runtime_id != previous.runtime_id
                    && (r.state == InstanceState::Failed
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
    async fn fields(&self, fields: &[String]) -> Result<Vec<Option<String>>> {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.key).arg(fields);
        self.query(cmd).await
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
        instances: BTreeMap::new(),
    };
    for (key, value) in raw {
        if let Some(id) = key.strip_prefix("node:") {
            out.nodes.insert(id.into(), decode(value)?);
        } else if let Some(id) = key.strip_prefix("instance:") {
            out.instances.insert(id.into(), decode(value)?);
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
        Ok(self
            .header(&self.store.fields(&[HEADER.into()]).await?[0])?
            .revision)
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
            let raw = self.store.fields(&[HEADER.into()]).await?;
            self.header(&raw[0])?;
            let mut cmd = redis::cmd("EVAL");
            cmd.arg("if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end; redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]); return 1").arg(2).arg(&self.store.key).arg(&key).arg(raw[0].as_deref().ok_or(Error::Conflict)?).arg(encode(&record)?).arg(ttl.as_millis() as u64);
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
        let header = self.store.fields(&[HEADER.into()]).await?;
        self.header(&header[0])?;
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
            .arg(header[0].as_deref().ok_or(Error::Conflict)?)
            .arg(digest)
            .arg(encoded);
        let accepted: u64 = self.store.query(command).await?;
        if accepted != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub async fn credential(&self, digest: &str) -> Result<crate::auth::Credential> {
        self.header(&self.store.fields(&[HEADER.into()]).await?[0])?;
        let mut command = redis::cmd("HGET");
        command
            .arg(format!("{}:credentials", self.store.key))
            .arg(digest);
        let value: Option<String> = self.store.query(command).await?;
        let value: crate::auth::Credential = decode(value.as_deref().ok_or(Error::NotFound)?)?;
        value.validate()?;
        Ok(value)
    }

    pub async fn get(&self, id: &str) -> Result<StoredInstance> {
        let values = self
            .store
            .fields(&[HEADER.into(), format!("instance:{id}")])
            .await?;
        self.header(&values[0])?;
        let record: StoredInstance = decode(values[1].as_deref().ok_or(Error::NotFound)?)?;
        record.validate()?;
        Ok(record)
    }

    fn header(&self, value: &Option<String>) -> Result<Header> {
        let h: Header = decode(value.as_deref().ok_or(Error::Conflict)?)?;
        h.validate()?;
        if h.epoch != self.epoch {
            return Err(Error::Conflict);
        }
        Ok(h)
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
        node: Node,
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
            let values = self.store.fields(&[HEADER.into(), field.clone()]).await?;
            let mut h = self.header(&values[0])?;
            let shard = if let Some(v) = &values[1] {
                decode::<StoredNode>(v)?.shard_id
            } else {
                let current = self.snapshot().await?;
                if current.revision != h.revision {
                    continue;
                }
                let mut counts = vec![0usize; h.shards];
                for n in current.nodes.values() {
                    counts[n.shard_id] += 1;
                }
                let shard = (0..h.shards)
                    .map(|offset| (h.next_node_shard + offset) % h.shards)
                    .min_by_key(|d| counts[*d])
                    .expect("nonempty shards");
                h.next_node_shard = (shard + 1) % h.shards;
                shard
            };
            let record = StoredNode {
                node: node.clone(),
                shard_id: shard,
                address: address.clone(),
                proxy_address: proxy_address.clone(),
                session: session.clone(),
            };
            let encoded = encode(&record)?;
            if values[1].as_ref() == Some(&encoded) {
                return Ok(record);
            }
            h.advance()?;
            if self
                .store
                .cas(
                    values[0]
                        .as_deref()
                        .expect("validated control header is present"),
                    &h,
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
    /// Persist a scheduler-selected assignment before sending it to the node.
    /// Ordinary node lifecycle operations do not write an intent here.
    pub async fn reserve(
        &self,
        spec: InstanceSpec,
        assignment: Assignment,
    ) -> Result<StoredInstance> {
        if let Some(id) = &spec.snapshot_id {
            let snapshot = self.get_snapshot(id).await?;
            if snapshot.template.tenant_id != spec.tenant_id
                || !snapshot
                    .references
                    .contains(&adx_core::snapshots::Reference::Restore {
                        instance_id: spec.id.clone(),
                    })
            {
                return Err(Error::Conflict);
            }
        }
        let record = StoredInstance {
            recovery: None,
            invalidated: false,
            spec,
            assignment,
            result: None,
        };
        record.validate()?;
        let field = format!("instance:{}", record.spec.id);
        for _ in 0..ATTEMPTS {
            let values = self
                .store
                .fields(&[
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", record.assignment.node_id),
                ])
                .await?;
            let mut h = self.header(&values[0])?;
            if let Some(v) = &values[1] {
                let old: StoredInstance = decode(v)?;
                return if old.spec == record.spec && old.assignment == record.assignment {
                    Ok(old)
                } else {
                    Err(Error::Conflict)
                };
            }
            let node: StoredNode = decode(values[2].as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != record.assignment.shard_id
                || record.assignment.generation <= h.generation
            {
                return Err(Error::Conflict);
            }
            h.generation = record.assignment.generation;
            h.advance()?;
            if self
                .store
                .cas(
                    values[0]
                        .as_deref()
                        .expect("validated control header is present"),
                    &h,
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
    /// or confirmed cleanup. This is not failover of a running Instance.
    pub async fn replace_rejected(
        &self,
        previous: &Assignment,
        replacement: Assignment,
    ) -> Result<StoredInstance> {
        if previous.instance_id != replacement.instance_id
            || replacement.generation <= previous.generation
        {
            return Err(Error::Conflict);
        }
        let field = format!("instance:{}", previous.instance_id);
        for _ in 0..ATTEMPTS {
            let values = self
                .store
                .fields(&[
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", replacement.node_id),
                ])
                .await?;
            let mut h = self.header(&values[0])?;
            let mut old: StoredInstance = decode(values[1].as_deref().ok_or(Error::NotFound)?)?;
            old.validate()?;
            if old.assignment == replacement {
                return Ok(old);
            }
            if old.invalidated
                || &old.assignment != previous
                || old
                    .result
                    .as_ref()
                    .is_some_and(|r| r.state != InstanceState::Failed || r.resources_held)
                || replacement.generation <= h.generation
            {
                return Err(Error::Conflict);
            }
            let node: StoredNode = decode(values[2].as_deref().ok_or(Error::NotFound)?)?;
            if node.shard_id != replacement.shard_id {
                return Err(Error::Conflict);
            }
            old.assignment = replacement.clone();
            old.result = None;
            old.validate()?;
            h.generation = replacement.generation;
            h.advance()?;
            if self
                .store
                .cas(
                    values[0]
                        .as_deref()
                        .expect("validated control header is present"),
                    &h,
                    Some((&field, encode(&old)?)),
                )
                .await?
            {
                return Ok(old);
            }
        }
        Err(Error::Unavailable(
            "concurrent reassignment; retry request".into(),
        ))
    }
    /// One result write: state and the route publication cursor change atomically.
    /// Identical replay is idempotent; older versions never roll state backward.
    pub async fn commit(&self, result: InstanceRecord) -> Result<InstanceRecord> {
        if let Some(cp) = &result.checkpoint {
            if let Some(origin) = cp
                .origin
                .as_ref()
                .filter(|o| o.instance_id != result.spec.id)
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
        let field = format!("instance:{}", result.spec.id);
        for _ in 0..ATTEMPTS {
            let values = self.store.fields(&[HEADER.into(), field.clone()]).await?;
            let mut h = self.header(&values[0])?;
            let mut old: StoredInstance = decode(values[1].as_deref().ok_or(Error::NotFound)?)?;
            if !next_result(&old, &result)? {
                return Ok(result);
            }
            if result.state != InstanceState::Paused {
                if let Some(recovery) = &mut old.recovery {
                    recovery.pending = false;
                }
            }
            old.result = Some(result.clone());
            h.advance()?;
            if self
                .store
                .cas(
                    values[0]
                        .as_deref()
                        .expect("validated control header is present"),
                    &h,
                    Some((&field, encode(&old)?)),
                )
                .await?
            {
                return Ok(result);
            }
        }
        Err(Error::Unavailable(
            "concurrent instance commit; retry request".into(),
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
