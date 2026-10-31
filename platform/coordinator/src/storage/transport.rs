//! A Redis transport lease has a server-side identity. A replacement must fence
//! that identity before publication repair can read through the new connection.
use super::*;
use redis::{aio::MultiplexedConnection, FromRedisValue};
use std::{num::NonZeroU64, sync::atomic::AtomicBool};

pub(super) struct Lease {
    connection: MultiplexedConnection,
    server_id: String,
    client_id: NonZeroU64,
    failed: AtomicBool,
}

struct Request {
    lease: Arc<Lease>,
    completed: bool,
}
impl Drop for Request {
    fn drop(&mut self) {
        // Cancellation can leave a queued mutation behind just like a timeout.
        // The next caller must fence it before reading an authoritative result.
        if !self.completed {
            self.lease.failed.store(true, Ordering::Release);
        }
    }
}

fn unavailable() -> Error {
    Error::Unavailable("Redis operation unavailable or timed out".into())
}

impl RedisStore {
    async fn open_lease(&self) -> Result<Lease> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(None);
        let mut connection = self
            .connection
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|_| unavailable())?;
        let client_id: u64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut connection)
            .await
            .map_err(|_| unavailable())?;
        // Reject malformed identities before targeting a connection for recovery.
        let client_id = NonZeroU64::new(client_id).ok_or_else(unavailable)?;
        let server: redis::InfoDict = redis::cmd("INFO")
            .arg("SERVER")
            .query_async(&mut connection)
            .await
            .map_err(|_| unavailable())?;
        let server_id = server
            .get::<String>("run_id")
            .filter(|id| !id.is_empty())
            .ok_or_else(unavailable)?;
        Ok(Lease {
            connection,
            server_id,
            client_id,
            failed: AtomicBool::new(false),
        })
    }

    async fn lease(&self) -> Result<Arc<Lease>> {
        // Serialize replacement only. Ordinary commands release this mutex
        // before awaiting Redis and continue to use its multiplexed transport.
        let mut current = self.connection.current.lock().await;
        if let Some(lease) = current
            .as_ref()
            .filter(|lease| !lease.failed.load(Ordering::Acquire))
        {
            return Ok(lease.clone());
        }
        let mut next = self.open_lease().await?;
        if let Some(previous) = current.as_ref() {
            if previous.server_id == next.server_id {
                if previous.client_id == next.client_id {
                    return Err(unavailable());
                }
                // CLIENT KILL executes on the same Redis event loop as EVAL.
                // Once acknowledged, an old command has either already applied
                // or cannot execute later. This also fences commands queued in
                // the old client's driver or delayed before reaching Redis.
                // A zero result means that exact client is already gone.
                let _: u64 = redis::cmd("CLIENT")
                    .arg("KILL")
                    .arg("ID")
                    .arg(previous.client_id.get())
                    .query_async(&mut next.connection)
                    .await
                    .map_err(|_| unavailable())?;
            }
            // A Redis restart changes run_id and destroys its old clients. Never
            // use an ID from that process to kill a client of the new process.
        }
        let next = Arc::new(next);
        *current = Some(next.clone());
        Ok(next)
    }

    pub(super) async fn query<T: FromRedisValue>(&self, cmd: redis::Cmd) -> Result<T> {
        let operation = async {
            let mut retry_read = matches!(
                cmd.args_iter().next(),
                Some(redis::Arg::Simple(
                    b"GET" | b"HGET" | b"HMGET" | b"HGETALL" | b"HKEYS" | b"HVALS" | b"PING"
                ))
            );
            loop {
                let lease = self.lease().await?;
                let mut request = Request {
                    lease: lease.clone(),
                    completed: false,
                };
                let mut connection = lease.connection.clone();
                let result = cmd.query_async(&mut connection).await;
                request.completed = true;
                let dropped = result
                    .as_ref()
                    .is_err_and(|error: &redis::RedisError| error.is_connection_dropped());
                if dropped {
                    self.discard_connection(&lease).await;
                }
                if retry_read && dropped {
                    retry_read = false;
                    continue;
                }
                // Command rejection is a completed response, not transport loss.
                // Mutations are never replayed here, even after a timeout.
                return result.map_err(|_| unavailable());
            }
        };
        timeout(self.connection.timeout, operation)
            .await
            .map_err(|_| unavailable())?
    }

    pub(super) async fn discard_connection(&self, lease: &Arc<Lease>) {
        // Keep the failed lease's identity until replacement has fenced it.
        // A delayed failure on an older lease cannot retire its replacement.
        lease.failed.store(true, Ordering::Release);
    }
}
