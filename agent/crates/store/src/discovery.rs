//! Redis leases for Activator membership, separate from Template/Environment records.
use crate::{Error, RedisRepository, Result};
use adx_agent_core::discovery::ActivatorEndpoint;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const REGISTRY: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if ARGV[1] == 'list' then
  local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. now, '+inf')
  local result = {}
  for _, id in ipairs(ids) do
    local record = redis.call('HGET', KEYS[2], id)
    if not record then return redis.error_reply('missing registry member') end
    table.insert(result, record)
  end
  return result
end
local id = ARGV[2]
local current = redis.call('HGET', KEYS[2], id)
if ARGV[1] == 'remove' then
  if current and cjson.decode(current).incarnation == ARGV[3] then
    redis.call('HDEL', KEYS[2], id)
    redis.call('ZREM', KEYS[1], id)
  end
  return 1
end
local expiry = tonumber(redis.call('ZSCORE', KEYS[1], id) or '0')
if expiry > now and current and cjson.decode(current).incarnation ~= ARGV[3] then return 0 end
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
for _, old in ipairs(expired) do redis.call('HDEL', KEYS[2], old) end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if not redis.call('ZSCORE', KEYS[1], id) and redis.call('ZCARD', KEYS[1]) >= 4096 then return -1 end
redis.call('HSET', KEYS[2], id, ARGV[5])
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[4]), id)
return 1
"#;

#[derive(Serialize, Deserialize)]
struct Registration {
    endpoint: ActivatorEndpoint,
    incarnation: String,
}
#[derive(Clone)]
pub struct RedisRegistry {
    store: RedisRepository,
    leases: String,
    records: String,
}
impl RedisRegistry {
    /// Validate settings without connecting. Discovery failures must not block warm traffic.
    pub fn new(url: &str, namespace: &str, timeout: Duration) -> Result<Self> {
        Ok(Self {
            store: RedisRepository::lazy(url, namespace, timeout, 1)?,
            leases: format!("adx:v2:{namespace}:activators:leases"),
            records: format!("adx:v2:{namespace}:activators:records"),
        })
    }
    fn command(&self, operation: &str) -> redis::Cmd {
        let mut command = redis::cmd("EVAL");
        command
            .arg(REGISTRY)
            .arg(2)
            .arg(&self.leases)
            .arg(&self.records)
            .arg(operation);
        command
    }
    /// Server-time leases avoid depending on Gateway/Activator clock agreement.
    /// A live instance with the same id and another incarnation returns Conflict.
    pub async fn renew(
        &self,
        endpoint: &ActivatorEndpoint,
        incarnation: &str,
        lease: Duration,
    ) -> Result<()> {
        endpoint.validate(true).map_err(Error::Invalid)?;
        if incarnation.is_empty()
            || incarnation.len() > 128
            || !(100..=300_000).contains(&lease.as_millis())
        {
            return Err(Error::Invalid(
                "invalid Activator incarnation or lease (100..300000 ms)".into(),
            ));
        }
        let value = serde_json::to_string(&Registration {
            endpoint: endpoint.clone(),
            incarnation: incarnation.into(),
        })
        .map_err(|e| Error::Invalid(e.to_string()))?;
        let mut command = self.command("renew");
        command
            .arg(&endpoint.id)
            .arg(incarnation)
            .arg(
                u64::try_from(lease.as_millis())
                    .map_err(|_| Error::Invalid("lease overflow".into()))?,
            )
            .arg(value);
        match self.store.execute::<i64>(command, true).await? {
            1 => Ok(()),
            0 => Err(Error::Conflict(
                "Activator instance id already registered".into(),
            )),
            _ => Err(Error::Unavailable(
                "Activator registry capacity reached".into(),
            )),
        }
    }
    /// Only this process incarnation may remove its registration.
    pub async fn unregister(&self, id: &str, incarnation: &str) -> Result<()> {
        let mut command = self.command("remove");
        command.arg(id).arg(incarnation);
        let _: i64 = self.store.execute(command, true).await?;
        Ok(())
    }
    /// Return only unexpired members. An empty successful snapshot is authoritative.
    pub async fn members(&self) -> Result<Vec<ActivatorEndpoint>> {
        let values: Vec<String> = self.store.execute(self.command("list"), false).await?;
        values
            .into_iter()
            .map(|value| {
                let registration: Registration = serde_json::from_str(&value)
                    .map_err(|_| Error::Corrupt("invalid Activator registration".into()))?;
                registration
                    .endpoint
                    .validate(true)
                    .map_err(Error::Corrupt)?;
                Ok(registration.endpoint)
            })
            .collect()
    }
}
