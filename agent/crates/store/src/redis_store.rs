use crate::*;
use redis::aio::MultiplexedConnection;
use std::time::Duration;

// Validate all preconditions before mutation. Values remain opaque JSON strings;
// only the revision string is decoded, so application u64 fields never pass through Lua numbers.
const CAS: &str = r#"
local count = tonumber(ARGV[1])
for i = count + 1, #KEYS do
  local kind = redis.call('TYPE', KEYS[i]).ok
  if kind ~= 'none' and kind ~= 'zset' then return redis.error_reply('invalid index type') end
end
for i = 1, count do
  local key = KEYS[i]
  local current = redis.call('GET', key)
  local expected = ARGV[(i-1)*3+2]
  if expected == '' then
    if current then return 0 end
  else
    if not current then return 0 end
    local decoded = cjson.decode(current)
    if decoded.revision ~= expected then return 0 end
  end
end
for i = 1, count do
  local key = KEYS[i]
  local replacement = ARGV[(i-1)*3+3]
  local index = tonumber(ARGV[(i-1)*3+4])
  if replacement == '#delete' then
    redis.call('DEL', key)
    if index > 0 then redis.call('ZREM', KEYS[index], key) end
  elseif replacement ~= '' then
    redis.call('SET', key, replacement)
    if index > 0 then redis.call('ZADD', KEYS[index], 0, key) end
  end
end
return 1
"#;

const PAGE: &str = r#"
local members = redis.call('ZRANGEBYLEX', KEYS[1], ARGV[1], '+', 'LIMIT', 0, ARGV[2])
local result = {}
for _, key in ipairs(members) do
  local value = redis.call('GET', key)
  if not value then return redis.error_reply('missing Environment index member') end
  table.insert(result, key)
  table.insert(result, value)
end
return result
"#;

struct ConnectionState {
    generation: u64,
    connection: Option<MultiplexedConnection>,
}

#[derive(Clone)]
pub struct RedisRepository {
    client: redis::Client,
    connection: std::sync::Arc<tokio::sync::Mutex<ConnectionState>>,
    budget: std::sync::Arc<tokio::sync::Semaphore>,
    prefix: String,
    timeout: Duration,
}
impl RedisRepository {
    pub async fn connect(url: &str, namespace: &str, timeout: Duration) -> Result<Self> {
        Self::connect_with_budget(url, namespace, timeout, 64).await
    }
    pub async fn connect_with_budget(
        url: &str,
        namespace: &str,
        timeout: Duration,
        inflight: usize,
    ) -> Result<Self> {
        if namespace.is_empty()
            || namespace.len() > 128
            || timeout.is_zero()
            || !(1..=adx_agent_core::limits::REDIS_MAX_INFLIGHT).contains(&inflight)
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(Error::Invalid(
                "invalid namespace, timeout or Redis concurrency budget".into(),
            ));
        }
        let client =
            redis::Client::open(url).map_err(|_| Error::Invalid("invalid Redis URL".into()))?;
        let connection = tokio::time::timeout(timeout, client.get_multiplexed_async_connection())
            .await
            .map_err(|_| Error::Unavailable("Redis connection timed out".into()))?
            .map_err(|_| Error::Unavailable("Redis connection failed".into()))?;
        let repository = Self {
            client,
            connection: std::sync::Arc::new(tokio::sync::Mutex::new(ConnectionState {
                generation: 0,
                connection: Some(connection),
            })),
            budget: std::sync::Arc::new(tokio::sync::Semaphore::new(inflight)),
            prefix: format!("adx:v2:{namespace}:"),
            timeout,
        };
        repository.check_schema().await?;
        Ok(repository)
    }
    async fn check_schema(&self) -> Result<()> {
        const SCHEMA: &str = "environment-index-v1";
        let key = format!("{}schema", self.prefix);
        let mut set = redis::cmd("SET");
        set.arg(&key).arg(SCHEMA).arg("NX");
        let _: Option<String> = self.execute(set, true).await?;
        let mut get = redis::cmd("GET");
        get.arg(key);
        let current: Option<String> = self.execute(get, false).await?;
        if current.as_deref() != Some(SCHEMA) {
            return Err(Error::Invalid(
                "Agent state schema initialization conflict".into(),
            ));
        }
        Ok(())
    }
    fn key(&self, key: &Key) -> String {
        format!("{}{}", self.prefix, key.as_str())
    }

    /// Reconnect for a subsequent call, but NEVER replay a command whose result is unknown.
    async fn execute<T: redis::FromRedisValue>(
        &self,
        command: redis::Cmd,
        write: bool,
    ) -> Result<T> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let _permit = tokio::time::timeout_at(deadline, self.budget.acquire())
            .await
            .map_err(|_| Error::Unavailable("Redis admission timed out".into()))?
            .map_err(|_| Error::Unavailable("Redis admission closed".into()))?;
        let (generation, mut connection) = tokio::time::timeout_at(deadline, async {
            let mut state = self.connection.lock().await;
            if state.connection.is_none() {
                let connection = self
                    .client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| Error::Unavailable("Redis reconnect failed".into()))?;
                state.generation = state.generation.wrapping_add(1);
                state.connection = Some(connection);
            }
            Ok::<_, Error>((
                state.generation,
                state.connection.as_ref().expect("connected").clone(),
            ))
        })
        .await
        .map_err(|_| Error::Unavailable("Redis reconnect timed out".into()))??;
        let result =
            tokio::time::timeout_at(deadline, command.query_async::<T>(&mut connection)).await;
        match result {
            Ok(Ok(value)) => Ok(value),
            _ => {
                let mut state = self.connection.lock().await;
                if state.generation == generation {
                    state.connection = None;
                }
                if write {
                    Err(Error::OutcomeUnknown(
                        "Redis write outcome unknown; read original identities before retry".into(),
                    ))
                } else {
                    Err(Error::Unavailable("Redis read failed or timed out".into()))
                }
            }
        }
    }
}

#[async_trait]
impl Repository for RedisRepository {
    async fn get(&self, key: &Key) -> Result<Option<Record>> {
        let mut command = redis::cmd("GET");
        command.arg(self.key(key));
        let result: Option<String> = self.execute(command, false).await?;
        result
            .map(|raw| serde_json::from_str(&raw).map_err(|e| Error::Corrupt(e.to_string())))
            .transpose()
    }
    async fn commit(&self, tx: &Transaction) -> Result<bool> {
        let indexes: Vec<_> = tx
            .checks
            .iter()
            .filter_map(|c| c.key.1.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut command = redis::cmd("EVAL");
        command.arg(CAS).arg(tx.checks.len() + indexes.len());
        for check in &tx.checks {
            command.arg(self.key(&check.key));
        }
        for index in &indexes {
            command.arg(format!("{}{}", self.prefix, index.0));
        }
        command.arg(tx.checks.len());
        for check in &tx.checks {
            command.arg(check.expected.as_ref().map_or("", Revision::as_str));
            match tx.puts.iter().find(|put| put.key == check.key) {
                Some(put) => command.arg(
                    serde_json::to_string(&put.record)
                        .map_err(|e| Error::Invalid(e.to_string()))?,
                ),
                None => command.arg(if tx.deletes.contains(&check.key) {
                    "#delete"
                } else {
                    ""
                }),
            };
            command.arg(
                check
                    .key
                    .1
                    .as_ref()
                    .and_then(|index| indexes.iter().position(|i| i == index))
                    .map_or(0, |i| tx.checks.len() + i + 1),
            );
        }
        let result: i64 = self.execute(command, true).await?;
        match result {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::OutcomeUnknown(
                "unexpected transaction result".into(),
            )),
        }
    }
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Record)>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(PAGE)
            .arg(1)
            .arg(format!("{}{}", self.prefix, index.0))
            .arg(after.map_or_else(|| "-".to_owned(), |v| format!("({}{v}", self.prefix)))
            .arg(limit);
        let values: Vec<String> = self.execute(command, false).await?;
        if !values.len().is_multiple_of(2) {
            return Err(Error::Corrupt("invalid Environment page response".into()));
        }
        values
            .chunks_exact(2)
            .map(|pair| {
                let key = pair[0]
                    .strip_prefix(&self.prefix)
                    .ok_or_else(|| Error::Corrupt("invalid Environment index member".into()))?;
                let record =
                    serde_json::from_str(&pair[1]).map_err(|e| Error::Corrupt(e.to_string()))?;
                Ok((key.to_owned(), record))
            })
            .collect()
    }
}
