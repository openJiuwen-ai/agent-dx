//! Redis holds a renewable Coordinator endpoint, never environment ownership decisions.
use adx_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::Instant;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorEndpoint {
    pub schema: u32,
    pub epoch: u64,
    pub address: String,
}
impl CoordinatorEndpoint {
    pub fn validate(&self) -> Result<()> {
        let uri: http::Uri = self
            .address
            .parse()
            .map_err(|_| Error::Invalid("invalid Coordinator endpoint".into()))?;
        if self.schema != 1
            || self.epoch == 0
            || !matches!(uri.scheme_str(), Some("https" | "http"))
            || uri
                .authority()
                .is_none_or(|a| a.port_u16().is_none_or(|p| p == 0))
            || uri.authority().is_some_and(|a| a.as_str().contains('@'))
            || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
        {
            return Err(Error::Invalid(
                "versioned HTTP(S) Coordinator endpoint required".into(),
            ));
        }
        Ok(())
    }
}
pub fn keys(namespace: &str) -> Result<(String, String)> {
    validate_namespace(namespace)?;
    Ok((
        format!("adx:{{{namespace}}}:coordinator:v1"),
        format!("adx:{{{namespace}}}:control:v1"),
    ))
}

/// The Coordinator primary lease key in the same validated namespace. Holds
/// the lease record that gates who may take a new epoch; see
/// `adx_coordinator::lease`.
pub fn lease_key(namespace: &str) -> Result<String> {
    validate_namespace(namespace)?;
    Ok(format!("adx:{{{namespace}}}:coordinator:lease"))
}

fn validate_namespace(namespace: &str) -> Result<()> {
    if namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(Error::Invalid("invalid Redis namespace".into()));
    }
    Ok(())
}
#[derive(Clone)]
pub struct RedisDiscovery {
    client: redis::Client,
    connection: Arc<Mutex<Option<Arc<Connection>>>>,
    namespace: String,
    timeout: Duration,
}

struct Connection {
    transport: redis::aio::MultiplexedConnection,
    failed: AtomicBool,
}

struct Read {
    connection: Arc<Connection>,
    completed: bool,
}

impl Drop for Read {
    fn drop(&mut self) {
        if !self.completed {
            // A cancelled or timed-out read can still be queued in the driver.
            // Retire only its connection, without awaiting a mutex after expiry.
            self.connection.failed.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Lock,
    Connect,
    Query,
}

impl RedisDiscovery {
    pub fn new(url: &str, namespace: &str, timeout: Duration) -> Result<Self> {
        keys(namespace)?;
        if timeout.is_zero() {
            return Err(Error::Invalid("discovery timeout must be positive".into()));
        }
        Ok(Self {
            client: redis::Client::open(url)
                .map_err(|_| Error::Invalid("invalid discovery Redis endpoint".into()))?,
            connection: Arc::default(),
            namespace: namespace.into(),
            timeout,
        })
    }
    /// Reads and validates the current endpoint and control epoch.
    ///
    /// One transient connection failure may be recovered by repeating this
    /// read. Lock wait, connection setup and both attempts share one deadline.
    /// Missing, malformed or mismatched records fail closed without retries.
    pub async fn lookup(&self) -> Result<CoordinatorEndpoint> {
        let (key, control) = keys(&self.namespace)?;
        let started = Instant::now();
        let deadline = started
            .checked_add(self.timeout)
            .ok_or_else(|| Error::Invalid("discovery timeout is too large".into()))?;
        let mut phase = Phase::Lock;
        let mut attempt = 0;
        let operation = async {
            for number in 1..=2 {
                // timeout_at may poll once even when the deadline has elapsed.
                // Never initiate a fresh connection or command in that case.
                if Instant::now() >= deadline {
                    break;
                }
                attempt = number;
                match self.read(&mut phase, &key, &control).await {
                    Ok(values) => return Ok(values),
                    Err(error) => {
                        let retry = number == 1 && transient(&error) && Instant::now() < deadline;
                        // RedisError's text may include server-controlled values
                        // or credentials. Only emit its bounded enum category.
                        adx_observability::warn!(
                            ?phase,
                            category = ?error.kind(),
                            attempt,
                            retry,
                            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                            "Coordinator discovery Redis attempt failed"
                        );
                        if !retry {
                            return Err(Error::Unavailable(format!(
                                "Coordinator discovery {phase:?} failed ({:?})",
                                error.kind()
                            )));
                        }
                    }
                }
            }
            Err(Error::Unavailable(
                "Coordinator discovery deadline exceeded".into(),
            ))
        };
        let values = tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| {
                adx_observability::warn!(
                    ?phase,
                    category = "deadline",
                    attempt,
                    retry = false,
                    elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                    "Coordinator discovery deadline exceeded"
                );
                Error::Unavailable("Coordinator discovery deadline exceeded".into())
            })??;
        let result = decode(values);
        if let Err(error) = &result {
            adx_observability::warn!(
                phase = "validation",
                category = if matches!(error, Error::NotFound) {
                    "missing_or_epoch_mismatch"
                } else {
                    "invalid_record"
                },
                attempt,
                retry = false,
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "Coordinator discovery record rejected"
            );
        }
        result
    }

    async fn read(
        &self,
        phase: &mut Phase,
        key: &str,
        control: &str,
    ) -> redis::RedisResult<Vec<Option<String>>> {
        *phase = Phase::Lock;
        let mut guard = self.connection.lock().await;
        let connection = match guard.as_ref() {
            Some(connection) if !connection.failed.load(Ordering::Acquire) => connection.clone(),
            _ => {
                *phase = Phase::Connect;
                // Serialize replacement only; queries remain multiplexed.
                // The outer absolute deadline is the sole timeout authority.
                let config = redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(None)
                    .set_response_timeout(None);
                let transport = self
                    .client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?;
                let connection = Arc::new(Connection {
                    transport,
                    failed: AtomicBool::new(false),
                });
                *guard = Some(connection.clone());
                connection
            }
        };
        drop(guard);
        *phase = Phase::Query;
        let mut read = Read {
            connection,
            completed: false,
        };
        let result = redis::cmd("EVAL")
            .arg("return {redis.call('GET', KEYS[1]), redis.call('HGET', KEYS[2], 'header')}")
            .arg(2)
            .arg(key)
            .arg(control)
            .query_async(&mut read.connection.transport.clone())
            .await;
        read.completed = true;
        if result.as_ref().is_err_and(transient) {
            // A late failure can mark its old generation, never a replacement
            // installed by another caller. No global cache invalidation needed.
            read.connection.failed.store(true, Ordering::Release);
        }
        result
    }
}

fn transient(error: &redis::RedisError) -> bool {
    error.is_connection_dropped() || error.is_timeout()
}

fn decode(values: Vec<Option<String>>) -> Result<CoordinatorEndpoint> {
    if values.len() != 2 {
        return Err(Error::Unavailable("invalid discovery response".into()));
    }
    let value = values[0].as_deref().ok_or(Error::NotFound)?;
    let endpoint: CoordinatorEndpoint = serde_json::from_str(value)
        .map_err(|_| Error::Unavailable("invalid Coordinator discovery record".into()))?;
    endpoint.validate()?;
    #[derive(Deserialize)]
    struct Header {
        epoch: u64,
    }
    let header: Header = serde_json::from_str(values[1].as_deref().ok_or(Error::NotFound)?)
        .map_err(|_| Error::Unavailable("invalid discovery epoch".into()))?;
    if endpoint.epoch != header.epoch {
        return Err(Error::NotFound);
    }
    Ok(endpoint)
}

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use test_support as support;

    #[tokio::test]
    async fn late_old_read_cancellation_cannot_invalidate_a_replacement() {
        let peer = support::RedisPeer::new("http://127.0.0.1:9000").await;
        let reader = RedisDiscovery::new(&peer.url, "test", Duration::from_secs(1)).unwrap();
        reader.lookup().await.unwrap();
        let old = reader.connection.lock().await.clone().unwrap();
        let late = Read {
            connection: old.clone(),
            completed: false,
        };
        old.failed.store(true, Ordering::Release);
        reader.lookup().await.unwrap();
        let replacement = reader.connection.lock().await.clone().unwrap();
        assert!(!Arc::ptr_eq(&old, &replacement));
        drop(late);
        assert!(!replacement.failed.load(Ordering::Acquire));
        reader.lookup().await.unwrap();
        assert_eq!(peer.state.connections.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn only_transient_connection_errors_are_retryable() {
        use std::io::{Error as IoError, ErrorKind};
        for kind in [
            ErrorKind::BrokenPipe,
            ErrorKind::ConnectionReset,
            ErrorKind::UnexpectedEof,
            ErrorKind::ConnectionRefused,
            ErrorKind::TimedOut,
        ] {
            assert!(transient(&IoError::from(kind).into()), "{kind:?}");
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::InvalidInput,
            ErrorKind::Unsupported,
        ] {
            assert!(!transient(&IoError::from(kind).into()), "{kind:?}");
        }
        for kind in [
            redis::ErrorKind::AuthenticationFailed,
            redis::ErrorKind::InvalidClientConfig,
            redis::ErrorKind::UnexpectedReturnType,
            redis::ErrorKind::Parse,
        ] {
            assert!(
                !transient(&redis::RedisError::from((kind, "permanent"))),
                "{kind:?}"
            );
        }
    }
    #[tokio::test]
    async fn deadline_includes_lock_wait_and_never_waits_to_invalidate() {
        let reader =
            RedisDiscovery::new("redis://127.0.0.1:1", "test", Duration::from_millis(20)).unwrap();
        let _guard = reader.connection.lock().await;
        let result = tokio::time::timeout(Duration::from_millis(200), reader.lookup())
            .await
            .expect("discovery waited for cache invalidation beyond its deadline");
        assert!(matches!(result, Err(Error::Unavailable(_))));
    }
    #[test]
    fn validates_versioned_rpc_endpoint_and_namespace() {
        for address in [
            "ftp://host:1",
            "https://user@host:1",
            "https://host:1/path",
            "https://host:1/?x=1",
            "bad",
        ] {
            assert!(CoordinatorEndpoint {
                schema: 1,
                epoch: 1,
                address: address.into()
            }
            .validate()
            .is_err());
        }
        assert!(CoordinatorEndpoint {
            schema: 1,
            epoch: 1,
            address: "https://host:123".into()
        }
        .validate()
        .is_ok());
        assert!(CoordinatorEndpoint {
            schema: 1,
            epoch: 1,
            address: "http://host:123".into()
        }
        .validate()
        .is_ok());
        for ns in ["", "a}b", "a/b", "a:b"] {
            assert!(keys(ns).is_err());
        }
    }
}
