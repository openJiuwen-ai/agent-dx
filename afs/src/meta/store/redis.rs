//! Redis backend for the unified Meta Store.
//!
//! Redis stores one opaque, CAS-protected full snapshot. It is a durable Meta
//! backend here, not a cache layer.

use afs_error::{Error, Result};

use super::{BackendPersistence, MetaFuture, StoreBackend};

const SNAPSHOT_KEY: &str = "afs:meta:snapshot";
const VERSION_FIELD: &str = "version";
const PAYLOAD_FIELD: &str = "payload";

const COMMIT_SCRIPT: &str = r#"
local current = redis.call('HGET', KEYS[1], 'version')
if not current then
  current = '0'
end
if current ~= ARGV[1] then
  return {'mismatch', current}
end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'payload', ARGV[3])
redis.call('PERSIST', KEYS[1])
return {'ok', ARGV[2]}
"#;

/// Redis storage for an opaque, full Store snapshot.
#[derive(Clone)]
pub struct RedisBackend {
    client: redis::Client,
    snapshot_key: String,
}

impl RedisBackend {
    /// Connects to the configured Redis instance and rejects unsafe durability
    /// settings before Meta can serve filesystem authority from it.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let client = redis::Client::open(endpoint.as_str()).map_err(redis_error)?;
        let backend = Self {
            client,
            snapshot_key: SNAPSHOT_KEY.to_owned(),
        };
        let mut connection = backend.connection().await?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        backend.probe_config(&mut connection).await?;
        Ok(backend)
    }

    /// Loads the current full snapshot and its CAS version.
    pub async fn load(&self) -> Result<Option<(u64, Vec<u8>)>> {
        let mut connection = self.connection().await?;
        self.ensure_no_ttl(&mut connection).await?;
        let (version, payload): (Option<String>, Option<Vec<u8>>) = redis::cmd("HMGET")
            .arg(&self.snapshot_key)
            .arg(VERSION_FIELD)
            .arg(PAYLOAD_FIELD)
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        match (version, payload) {
            (None, None) => Ok(None),
            (Some(version), Some(payload)) => {
                let version = parse_version(&version)?;
                if version == 0 {
                    return Err(invalid_contract("redis snapshot version must be non-zero"));
                }
                Ok(Some((version, payload)))
            }
            _ => Err(invalid_contract(
                "redis snapshot hash has version/payload mismatch",
            )),
        }
    }

    /// Atomically replaces the full snapshot when the observed version matches.
    ///
    /// `expected_version == 0` creates the snapshot only if it is absent.
    pub async fn commit(&self, expected_version: u64, bytes: &[u8]) -> Result<u64> {
        let new_version = expected_version
            .checked_add(1)
            .ok_or_else(|| invalid_contract("redis snapshot version overflow"))?;
        let mut connection = self.connection().await?;
        let response: Vec<String> = redis::cmd("EVAL")
            .arg(COMMIT_SCRIPT)
            .arg(1)
            .arg(&self.snapshot_key)
            .arg(expected_version.to_string())
            .arg(new_version.to_string())
            .arg(bytes)
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        match response.as_slice() {
            [status, version] if status == "ok" => {
                let committed = parse_version(version)?;
                if committed != new_version {
                    return Err(invalid_contract("redis commit returned wrong version"));
                }
                Ok(committed)
            }
            [status, current] if status == "mismatch" => Err(cas_mismatch(current)),
            _ => Err(invalid_contract(
                "redis commit script returned invalid shape",
            )),
        }
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(redis_error)
    }

    async fn probe_config(&self, connection: &mut redis::aio::MultiplexedConnection) -> Result<()> {
        for (name, expected) in [
            ("appendonly", "yes"),
            ("appendfsync", "always"),
            ("no-appendfsync-on-rewrite", "no"),
            ("maxmemory-policy", "noeviction"),
        ] {
            let actual = config_get(connection, name).await?;
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(Error::coded(
                    afs_error::CONFIG_INVALID,
                    format!("redis meta store requires {name}={expected}, found {actual}"),
                ));
            }
        }
        Ok(())
    }

    async fn ensure_no_ttl(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
    ) -> Result<()> {
        let ttl: i64 = redis::cmd("TTL")
            .arg(&self.snapshot_key)
            .query_async(connection)
            .await
            .map_err(redis_error)?;
        if ttl >= 0 {
            return Err(invalid_contract(
                "redis snapshot key must not have an expiration",
            ));
        }
        Ok(())
    }
}

impl StoreBackend for RedisBackend {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        Box::pin(async move { RedisBackend::load(self).await })
    }

    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
        Box::pin(async move { RedisBackend::commit(self, expected_version, &bytes).await })
    }
}

async fn config_get(
    connection: &mut redis::aio::MultiplexedConnection,
    name: &str,
) -> Result<String> {
    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg(name)
        .query_async(connection)
        .await
        .map_err(redis_error)?;
    parse_config_get(name, &values)
}

fn parse_config_get(name: &str, values: &[String]) -> Result<String> {
    match values {
        [key, value] if key.eq_ignore_ascii_case(name) => Ok(value.clone()),
        [] => Err(Error::coded(
            afs_error::CONFIG_INVALID,
            format!("redis CONFIG GET {name} returned no value"),
        )),
        _ => Err(Error::coded(
            afs_error::CONFIG_INVALID,
            format!("redis CONFIG GET {name} returned invalid shape"),
        )),
    }
}

fn parse_version(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| invalid_contract("redis snapshot version is not a u64"))
}

fn redis_error(error: redis::RedisError) -> Error {
    Error::coded(
        afs_error::IO_UNAVAILABLE,
        format!("redis snapshot store unavailable: {error}"),
    )
}

fn cas_mismatch(current: &str) -> Error {
    Error::coded(
        afs_error::IO_UNAVAILABLE,
        format!("redis snapshot CAS mismatch at version {current}; reload before retrying"),
    )
}

fn invalid_contract(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_config_get_shape() {
        assert_eq!(
            parse_config_get(
                "appendfsync",
                &["appendfsync".to_owned(), "always".to_owned()]
            )
            .unwrap(),
            "always"
        );
        assert!(parse_config_get("appendfsync", &[]).is_err());
        assert!(
            parse_config_get(
                "appendfsync",
                &[
                    "appendfsync".to_owned(),
                    "always".to_owned(),
                    "extra".to_owned(),
                ],
            )
            .is_err()
        );
    }

    async fn reset_dedicated_database(endpoint: &str) {
        let mut connection = redis::Client::open(endpoint)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: String = redis::cmd("FLUSHDB")
            .query_async(&mut connection)
            .await
            .unwrap();
    }

    /// Run only against a fresh dedicated Redis database configured with:
    /// appendonly yes, appendfsync always, no-appendfsync-on-rewrite no and
    /// maxmemory-policy noeviction. This writes the fixed production key.
    #[tokio::test]
    #[ignore = "requires a fresh dedicated durable Redis; see docs/testing/validation.md"]
    async fn redis_ack_reconnect_and_stale_cas_on_dedicated_instance() {
        let endpoint = std::env::var("AFS_TEST_REDIS_STORE_ENDPOINT")
            .expect("AFS_TEST_REDIS_STORE_ENDPOINT is required; no implicit Redis PASS");
        reset_dedicated_database(&endpoint).await;
        let backend = RedisBackend::connect(endpoint.clone()).await.unwrap();
        assert!(backend.load().await.unwrap().is_none());
        let version = backend.commit(0, b"first").await.unwrap();
        assert!(backend.commit(0, b"stale").await.is_err());
        let reopened = RedisBackend::connect(endpoint).await.unwrap();
        assert_eq!(
            reopened.load().await.unwrap(),
            Some((version, b"first".to_vec()))
        );
        let next = reopened.commit(version, b"second").await.unwrap();
        assert_eq!(next, version + 1);
        assert_eq!(
            backend.load().await.unwrap(),
            Some((next, b"second".to_vec()))
        );
    }
}
