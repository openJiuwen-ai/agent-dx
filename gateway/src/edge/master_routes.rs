//! New control-plane route subscription. Cache hits never call discovery or Redis.
use super::{
    auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
    RouteStore,
};
use crate::common::route::{DataPlaneSecurityMode, InstanceStatus, RouteCache, RouteInfo};
use adx_discovery::RedisDiscovery;
use adx_protocol::control as pb;
use adx_protocol::tls::TlsFiles;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub struct RouteConsumer {
    store: Arc<RouteStore>,
    cursor: Option<(u64, u64)>,
    routes: BTreeMap<String, pb::PublishedRoute>,
}
impl RouteConsumer {
    pub fn new(store: Arc<RouteStore>) -> Self {
        Self {
            store,
            cursor: None,
            routes: BTreeMap::new(),
        }
    }
    pub fn apply(&mut self, f: pb::RouteFrame) -> Result<(), String> {
        if f.epoch == 0
            || (!f.reset && self.cursor != Some((f.epoch, f.base_revision)))
            || (!f.reset && f.revision <= f.base_revision)
            || self
                .cursor
                .is_some_and(|(e, r)| f.epoch < e || (f.epoch == e && f.revision < r))
        {
            return Err("route cursor mismatch; full synchronization required".into());
        }
        if f.reset && (!f.deleted.is_empty() || f.base_revision != 0) {
            return Err("invalid full route frame".into());
        }
        let mut ids = BTreeSet::new();
        let mut next = if f.reset {
            BTreeMap::new()
        } else {
            self.routes.clone()
        };
        for r in f.upserts {
            if r.instance_id.is_empty()
                || r.tenant_id.is_empty()
                || r.generation == 0
                || r.instance_revision == 0
                || !adx_protocol::valid_runtime_id(&r.instance_id, r.generation, &r.runtime_id)
                || r.runtime_ip.parse::<std::net::IpAddr>().is_err()
                || r.node_proxy_address.contains(['/', '@', '?', '#'])
                || r.node_proxy_address
                    .parse::<http::uri::Authority>()
                    .ok()
                    .is_none_or(|a| a.port_u16().is_none())
                || !ids.insert(r.instance_id.clone())
            {
                return Err("invalid published route".into());
            }
            if let Some(old) = self.routes.get(&r.instance_id) {
                if (r.generation, r.instance_revision) < (old.generation, old.instance_revision) {
                    return Err("route execution version regressed".into());
                }
            }
            next.insert(r.instance_id.clone(), r);
        }
        for id in f.deleted {
            if id.is_empty() || !ids.insert(id.clone()) {
                return Err("conflicting route delta".into());
            }
            next.remove(&id);
        }
        if self.cursor == Some((f.epoch, f.revision)) && next != self.routes {
            return Err("conflicting route snapshot at the same cursor".into());
        }
        let cache = RouteCache::default();
        for r in next.values() {
            cache.put(RouteInfo {
                instance_id: r.instance_id.clone(),
                tenant_id: r.tenant_id.clone(),
                sandbox_id: r.runtime_id.clone(),
                sandbox_ip: r.runtime_ip.clone(),
                node_proxy_address: r.node_proxy_address.clone(),
                instance_status: InstanceStatus {
                    code: 3,
                    ..Default::default()
                },
                tunnel_security_mode: DataPlaneSecurityMode::TlsToken,
                port_forward_security_mode: DataPlaneSecurityMode::TlsToken,
                port_forward_routes: vec![],
            });
        }
        self.store.replace(cache);
        self.store
            .record_watch_revision(f.revision.min(i64::MAX as u64) as i64);
        self.cursor = Some((f.epoch, f.revision));
        self.routes = next;
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub redis_url: String,
    pub namespace: String,
    pub tls: TlsFiles,
    pub rpc_timeout_seconds: u64,
    pub refresh_seconds: u64,
    pub auth_cache_seconds: u64,
    pub auth_cache_entries: usize,
}
struct Cached {
    identity: AuthenticatedIdentity,
    until: Instant,
}
pub struct MasterConnection {
    discovery: RedisDiscovery,
    tls: ClientTlsConfig,
    channel: RwLock<Option<Channel>>,
    timeout: Duration,
    refresh: Duration,
    cache: Mutex<HashMap<Vec<u8>, Cached>>,
    cache_ttl: Duration,
    cache_entries: usize,
}
impl std::fmt::Debug for MasterConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterConnection")
    }
}
impl MasterConnection {
    pub fn new(c: ControlConfig) -> Result<Self, Box<dyn std::error::Error>> {
        if c.rpc_timeout_seconds == 0 || c.refresh_seconds == 0 || c.auth_cache_entries == 0 {
            return Err("positive control intervals and cache budget required".into());
        }
        let (_, tls, _) = c.tls.load()?;
        let timeout = Duration::from_secs(c.rpc_timeout_seconds);
        Ok(Self {
            discovery: RedisDiscovery::new(&c.redis_url, &c.namespace, timeout)?,
            tls,
            channel: RwLock::new(None),
            timeout,
            refresh: Duration::from_secs(c.refresh_seconds),
            cache: Mutex::default(),
            cache_ttl: Duration::from_secs(c.auth_cache_seconds),
            cache_entries: c.auth_cache_entries,
        })
    }
    pub async fn run(self: Arc<Self>, store: Arc<RouteStore>) {
        let mut consumer = RouteConsumer::new(store);
        loop {
            if self.subscribe(&mut consumer).await.is_err() {
                tracing::warn!(
                    "Master route subscription interrupted; retaining synchronized cache"
                );
            }
            tokio::time::sleep(self.refresh).await;
        }
    }
    async fn subscribe(
        &self,
        consumer: &mut RouteConsumer,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = self.discovery.lookup().await?;
        let channel = Endpoint::from_shared(endpoint.address.clone())?
            .tls_config(self.tls.clone())?
            .connect_timeout(self.timeout)
            .connect()
            .await?;
        *self.channel.write().unwrap() = Some(channel.clone());
        let mut client = pb::route_service_client::RouteServiceClient::new(channel)
            .max_decoding_message_size(64 * 1024 * 1024);
        let mut stream =
            tokio::time::timeout(self.timeout, client.watch_routes(pb::WatchRoutesRequest {}))
                .await??
                .into_inner();
        let mut first = true;
        let mut tick = tokio::time::interval(self.refresh);
        loop {
            tokio::select! {
             message=stream.message()=>{let frame=message?.ok_or("route stream ended")?;
              if frame.epoch!=endpoint.epoch || (first&&!frame.reset){return Err("unexpected route stream epoch or initial delta".into());}
              consumer.apply(frame).map_err(std::io::Error::other)?;first=false;
             }
             _=tick.tick()=>{if let Ok(found)=self.discovery.lookup().await{if found!=endpoint{return Ok(());}}}
            }
        }
    }
}
#[async_trait::async_trait]
impl CredentialVerifier for MasterConnection {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError> {
        if !(32..=512).contains(&key.len()) {
            return Err(AuthError::Invalid("API key length".into()));
        }
        let digest = Sha256::digest(key.as_bytes()).to_vec();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        {
            let mut cache = self.cache.lock().unwrap();
            cache.retain(|_, c| {
                c.until > Instant::now()
                    && c.identity.expires_at_unix.is_none_or(|v| v > now as i64)
            });
            if let Some(c) = cache.get(&digest) {
                return Ok(c.identity.clone());
            }
        }
        let channel = self
            .channel
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| AuthError::IamUnavailable("Master connection not ready".into()))?;
        let result = tokio::time::timeout(
            self.timeout,
            pb::auth_service_client::AuthServiceClient::new(channel).verify_api_key(
                pb::VerifyApiKeyRequest {
                    api_key: key.into(),
                },
            ),
        )
        .await
        .map_err(|_| AuthError::IamUnavailable("Master deadline".into()))?
        .map_err(|s| {
            if s.code() == tonic::Code::Unauthenticated {
                AuthError::Invalid("API key rejected".into())
            } else {
                AuthError::IamUnavailable("Master verification unavailable".into())
            }
        })?
        .into_inner();
        let caller = result
            .caller
            .filter(|c| !c.tenant_id.is_empty())
            .ok_or_else(|| AuthError::Invalid("missing tenant".into()))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let exp = result.expires_at_unix_seconds;
        if exp != 0 && exp <= now {
            return Err(AuthError::Expired);
        }
        let identity = AuthenticatedIdentity {
            tenant_id: caller.tenant_id,
            expires_at_unix: if exp == 0 {
                None
            } else {
                Some(i64::try_from(exp).map_err(|_| AuthError::Invalid("expiry overflow".into()))?)
            },
        };
        let ttl = if exp == 0 {
            self.cache_ttl
        } else {
            self.cache_ttl.min(Duration::from_secs(exp - now))
        };
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= self.cache_entries {
            if let Some(victim) = cache.keys().next().cloned() {
                cache.remove(&victim);
            }
        }
        cache.insert(
            digest,
            Cached {
                identity: identity.clone(),
                until: Instant::now() + ttl,
            },
        );
        Ok(identity)
    }
}
