//! Redis service for the shared-service Meta log (R16, `kv_log`).
//!
//! Every compare-and-write step is one Lua script, which Redis runs
//! atomically. Durability is the Redis instance's own: an acknowledged write
//! survives a Redis crash only with `appendonly yes` and `appendfsync always`,
//! so `connect` refuses any other setting, an eviction policy other than
//! `noeviction`, and keys under the prefix that carry a TTL. Replication to a
//! Redis replica is asynchronous and gives no additional guarantee.

use afs_error::{Error, Result};
use redis::{AsyncCommands, Value, aio::MultiplexedConnection};

use super::{
    journal::{invalid_log, unavailable},
    kv_log::{
        Head, KvFuture, KvService, Swap, chunk_id_of, chunk_key, frame_key, part_key, part_owner_of,
    },
};

/// Framed bytes of one append script. Redis accepts much larger requests;
/// this keeps one script's execution, which blocks the server, short.
const APPEND_BOUND: usize = 256 * 1024 * 1024;
/// One checkpoint chunk (one `SET`).
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
/// Keys per `MGET`/`UNLINK`/`SCAN` round trip.
const KEY_PAGE: usize = 512;

const OPEN_SCRIPT: &str = r#"
local layout = redis.call('GET', KEYS[1])
if not layout then
  if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
    return redis.error_reply('AFS Meta log prefix has a term or tip but no layout key')
  end
  redis.call('SET', KEYS[1], ARGV[1])
  layout = ARGV[1]
end
local term = redis.call('INCR', KEYS[2])
return {layout, term}
"#;

const APPEND_SCRIPT: &str = r#"
local term = redis.call('GET', KEYS[1])
if term ~= ARGV[1] then
  return {'fenced', term or '0'}
end
local tip = redis.call('GET', KEYS[2])
if ARGV[2] == '' then
  if tip then return {'changed'} end
elseif tip ~= ARGV[2] then
  return {'changed'}
end
for i = 3, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i + 1])
end
redis.call('SET', KEYS[2], ARGV[3])
return {'done'}
"#;

const SWITCH_SCRIPT: &str = r#"
local term = redis.call('GET', KEYS[1])
if term ~= ARGV[1] then
  return {'fenced', term or '0'}
end
local pointer = redis.call('GET', KEYS[2])
if ARGV[2] == '' then
  if pointer then return {'changed'} end
elseif pointer ~= ARGV[2] then
  return {'changed'}
end
redis.call('SET', KEYS[2], ARGV[3])
return {'done'}
"#;

/// A Redis instance holding one Meta log under `prefix`.
pub struct RedisService {
    client: redis::Client,
    prefix: String,
    chunk_bytes: usize,
    connection: tokio::sync::Mutex<Option<MultiplexedConnection>>,
}

fn redis_error(error: redis::RedisError) -> Error {
    unavailable(format!("redis Meta log: {error}"))
}

impl RedisService {
    /// Connect and refuse an instance whose settings would not keep an
    /// acknowledged write durable.
    pub async fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|error| {
            Error::coded(
                afs_error::CONFIG_INVALID,
                format!("meta_redis_url is not a valid Redis URL: {error}"),
            )
        })?;
        let service = Self {
            client,
            prefix: prefix.to_owned(),
            chunk_bytes: CHUNK_BYTES,
            connection: tokio::sync::Mutex::new(None),
        };
        service.probe().await?;
        Ok(service)
    }

    /// Test-only: smaller checkpoint chunks so a small image spans several.
    #[cfg(test)]
    pub(super) fn with_chunk_bytes(mut self, bytes: usize) -> Self {
        self.chunk_bytes = bytes;
        self
    }

    async fn connection(&self) -> Result<MultiplexedConnection> {
        let mut slot = self.connection.lock().await;
        if let Some(connection) = slot.as_ref() {
            return Ok(connection.clone());
        }
        let connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)?;
        *slot = Some(connection.clone());
        Ok(connection)
    }

    /// Drop a connection that failed so the next request reconnects.
    async fn reset(&self, error: redis::RedisError) -> Error {
        if error.is_io_error() || error.is_connection_dropped() || error.is_timeout() {
            *self.connection.lock().await = None;
        }
        redis_error(error)
    }

    async fn query<T: redis::FromRedisValue>(&self, command: &redis::Cmd) -> Result<T> {
        let mut connection = self.connection().await?;
        match command.query_async(&mut connection).await {
            Ok(value) => Ok(value),
            Err(error) => Err(self.reset(error).await),
        }
    }

    async fn probe(&self) -> Result<()> {
        for (name, expected) in [
            ("appendonly", "yes"),
            ("appendfsync", "always"),
            ("no-appendfsync-on-rewrite", "no"),
            ("maxmemory-policy", "noeviction"),
        ] {
            let reply: Vec<String> = self
                .query(redis::cmd("CONFIG").arg("GET").arg(name))
                .await
                .map_err(|error| {
                    Error::coded(
                        afs_error::CONFIG_INVALID,
                        format!(
                            "redis Meta log requires CONFIG GET to verify {name}={expected}: {error}"
                        ),
                    )
                })?;
            let actual = reply.get(1).map(String::as_str).unwrap_or("");
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(Error::coded(
                    afs_error::CONFIG_INVALID,
                    format!("redis Meta log requires {name}={expected}, found {actual:?}"),
                ));
            }
        }
        for key in [self.key("layout"), self.key("term"), self.key("tip")] {
            let ttl: i64 = self.query(redis::cmd("PTTL").arg(&key)).await?;
            if ttl >= 0 {
                return Err(Error::coded(
                    afs_error::CONFIG_INVALID,
                    format!("redis Meta log key {key} has a TTL; Meta keys must never expire"),
                ));
            }
        }
        Ok(())
    }

    fn key(&self, name: &str) -> String {
        format!("{}/{name}", self.prefix)
    }

    async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
        let mut connection = self.connection().await?;
        let mut cursor = 0u64;
        let mut keys = Vec::new();
        loop {
            let (next, page): (u64, Vec<String>) = match redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(KEY_PAGE)
                .query_async(&mut connection)
                .await
            {
                Ok(reply) => reply,
                Err(error) => return Err(self.reset(error).await),
            };
            keys.extend(page);
            if next == 0 {
                return Ok(keys);
            }
            cursor = next;
        }
    }

    async fn unlink(&self, keys: &[String]) -> Result<()> {
        for page in keys.chunks(KEY_PAGE) {
            let _: i64 = self.query(redis::cmd("UNLINK").arg(page)).await?;
        }
        Ok(())
    }

    /// Escape glob metacharacters of the prefix for `SCAN MATCH`.
    fn pattern(&self, suffix: &str) -> String {
        let mut escaped = String::new();
        for c in self.prefix.chars() {
            if matches!(c, '*' | '?' | '[' | ']' | '\\') {
                escaped.push('\\');
            }
            escaped.push(c);
        }
        format!("{escaped}/{suffix}")
    }
}

fn swap(reply: Vec<Value>) -> Result<Swap> {
    let text = |value: &Value| -> Option<String> {
        match value {
            Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
            Value::SimpleString(text) => Some(text.clone()),
            Value::Int(number) => Some(number.to_string()),
            _ => None,
        }
    };
    match reply.first().and_then(text).as_deref() {
        Some("done") => Ok(Swap::Done),
        Some("changed") => Ok(Swap::Changed),
        Some("fenced") => Ok(Swap::Fenced(
            reply
                .get(1)
                .and_then(text)
                .and_then(|term| term.parse().ok())
                .unwrap_or(0),
        )),
        _ => Err(invalid_log(
            "redis Meta log script returned an unknown reply",
        )),
    }
}

fn bytes_of(value: Value) -> Result<Option<Vec<u8>>> {
    match value {
        Value::Nil => Ok(None),
        Value::BulkString(bytes) => Ok(Some(bytes)),
        Value::SimpleString(text) => Ok(Some(text.into_bytes())),
        _ => Err(invalid_log("redis Meta log value has an unexpected type")),
    }
}

impl KvService for RedisService {
    fn name(&self) -> &'static str {
        "redis"
    }

    fn append_bound_bytes(&self) -> usize {
        APPEND_BOUND
    }

    fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    fn open_writer<'a>(&'a self, layout: &'a [u8]) -> KvFuture<'a, (Vec<u8>, u64)> {
        Box::pin(async move {
            // A prefix without a layout key must be empty: never adopt data.
            let has_layout: bool = self
                .query(redis::cmd("EXISTS").arg(self.key("layout")))
                .await?;
            if !has_layout && !self.scan(&self.pattern("*")).await?.is_empty() {
                return Err(invalid_log(
                    "redis Meta log prefix is not empty but has no layout key; data retained",
                ));
            }
            let (stored, term): (Vec<u8>, u64) = self
                .query(
                    redis::cmd("EVAL")
                        .arg(OPEN_SCRIPT)
                        .arg(3)
                        .arg(self.key("layout"))
                        .arg(self.key("term"))
                        .arg(self.key("tip"))
                        .arg(layout),
                )
                .await?;
            Ok((stored, term))
        })
    }

    fn term(&self) -> KvFuture<'_, u64> {
        Box::pin(async move {
            let term: Option<u64> = self.query(redis::cmd("GET").arg(self.key("term"))).await?;
            Ok(term.unwrap_or(0))
        })
    }

    fn head(&self) -> KvFuture<'_, Head> {
        Box::pin(async move {
            let values: Vec<Value> = self
                .query(
                    redis::cmd("MGET")
                        .arg(self.key("term"))
                        .arg(self.key("tip"))
                        .arg(self.key("checkpoint")),
                )
                .await?;
            let mut values = values.into_iter();
            let mut next = || values.next().map_or(Ok(None), bytes_of);
            let term = next()?
                .map(|bytes| {
                    String::from_utf8(bytes)
                        .ok()
                        .and_then(|text| text.parse::<u64>().ok())
                        .ok_or_else(|| invalid_log("redis Meta log term is not a number"))
                })
                .transpose()?
                .unwrap_or(0);
            Ok(Head {
                term,
                tip: next()?,
                checkpoint: next()?,
            })
        })
    }

    fn append<'a>(
        &'a self,
        term: u64,
        expected_tip: Option<&'a [u8]>,
        tip: &'a [u8],
        first_sequence: u64,
        frames: &'a [Vec<u8>],
    ) -> KvFuture<'a, Swap> {
        Box::pin(async move {
            let mut command = redis::cmd("EVAL");
            command
                .arg(APPEND_SCRIPT)
                .arg(2 + frames.len())
                .arg(self.key("term"))
                .arg(self.key("tip"));
            for offset in 0..frames.len() {
                command.arg(frame_key(&self.prefix, first_sequence + offset as u64));
            }
            command
                .arg(term.to_string())
                .arg(expected_tip.unwrap_or(&[]))
                .arg(tip);
            for frame in frames {
                command.arg(frame.as_slice());
            }
            swap(self.query(&command).await?)
        })
    }

    fn frames(&self, from: u64, through: u64) -> KvFuture<'_, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let keys: Vec<String> = (from..=through)
                .map(|sequence| frame_key(&self.prefix, sequence))
                .collect();
            let mut frames = Vec::with_capacity(keys.len());
            for page in keys.chunks(KEY_PAGE) {
                let values: Vec<Value> = self.query(redis::cmd("MGET").arg(page)).await?;
                for value in values {
                    frames.push(bytes_of(value)?);
                }
            }
            Ok(frames)
        })
    }

    fn put_chunk<'a>(&'a self, id: &'a str, index: u32, bytes: &'a [u8]) -> KvFuture<'a, ()> {
        Box::pin(async move {
            let mut connection = self.connection().await?;
            match connection
                .set::<_, _, ()>(chunk_key(&self.prefix, id, index), bytes)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => Err(self.reset(error).await),
            }
        })
    }

    fn chunk<'a>(&'a self, id: &'a str, index: u32) -> KvFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let value: Value = self
                .query(redis::cmd("GET").arg(chunk_key(&self.prefix, id, index)))
                .await?;
            bytes_of(value)
        })
    }

    fn switch_checkpoint<'a>(
        &'a self,
        term: u64,
        expected: Option<&'a [u8]>,
        pointer: &'a [u8],
    ) -> KvFuture<'a, Swap> {
        Box::pin(async move {
            swap(
                self.query(
                    redis::cmd("EVAL")
                        .arg(SWITCH_SCRIPT)
                        .arg(2)
                        .arg(self.key("term"))
                        .arg(self.key("checkpoint"))
                        .arg(term.to_string())
                        .arg(expected.unwrap_or(&[]))
                        .arg(pointer),
                )
                .await?,
            )
        })
    }

    fn put_part<'a>(
        &'a self,
        term: u64,
        sequence: u64,
        index: u32,
        bytes: &'a [u8],
    ) -> KvFuture<'a, ()> {
        Box::pin(async move {
            let _: () = self
                .query(
                    redis::cmd("SET")
                        .arg(part_key(&self.prefix, term, sequence, index))
                        .arg(bytes),
                )
                .await?;
            Ok(())
        })
    }

    fn part(&self, term: u64, sequence: u64, index: u32) -> KvFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            let value: Value = self
                .query(redis::cmd("GET").arg(part_key(&self.prefix, term, sequence, index)))
                .await?;
            bytes_of(value)
        })
    }

    fn part_owners(&self) -> KvFuture<'_, Vec<(u64, u64)>> {
        Box::pin(async move {
            let mut owners: Vec<(u64, u64)> = self
                .scan(&self.pattern("part/*"))
                .await?
                .iter()
                .filter_map(|key| part_owner_of(&self.prefix, key))
                .collect();
            owners.sort_unstable();
            owners.dedup();
            Ok(owners)
        })
    }

    fn delete_parts(&self, term: u64, sequence: u64) -> KvFuture<'_, ()> {
        Box::pin(async move {
            let keys = self
                .scan(&self.pattern(&format!("part/{term:020}/{sequence:020}/*")))
                .await?;
            self.unlink(&keys).await
        })
    }

    fn delete_frames_through(&self, through: u64) -> KvFuture<'_, ()> {
        Box::pin(async move {
            let floor = format!("{}/wal/", self.prefix);
            let keys: Vec<String> = self
                .scan(&self.pattern("wal/*"))
                .await?
                .into_iter()
                .filter(|key| {
                    key.strip_prefix(&floor)
                        .and_then(|sequence| sequence.parse::<u64>().ok())
                        .is_some_and(|sequence| sequence <= through)
                })
                .collect();
            self.unlink(&keys).await
        })
    }

    fn chunk_ids(&self) -> KvFuture<'_, Vec<String>> {
        Box::pin(async move {
            let mut ids: Vec<String> = self
                .scan(&self.pattern("chunk/*"))
                .await?
                .iter()
                .filter_map(|key| chunk_id_of(&self.prefix, key))
                .collect();
            ids.sort();
            ids.dedup();
            Ok(ids)
        })
    }

    fn delete_chunks<'a>(&'a self, id: &'a str) -> KvFuture<'a, ()> {
        Box::pin(async move {
            let keys = self.scan(&self.pattern(&format!("chunk/{id}/*"))).await?;
            self.unlink(&keys).await
        })
    }

    #[cfg(test)]
    fn dump(&self) -> KvFuture<'_, Vec<(String, Vec<u8>)>> {
        Box::pin(async move {
            let mut keys = self.scan(&self.pattern("*")).await?;
            keys.sort();
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                let value: Value = self.query(redis::cmd("GET").arg(&key)).await?;
                out.push((key, bytes_of(value)?.unwrap_or_default()));
            }
            Ok(out)
        })
    }
}
