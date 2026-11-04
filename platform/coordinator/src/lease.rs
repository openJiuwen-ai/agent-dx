//! Redis lease gating which Coordinator instance may take a new epoch.
//!
//! The lease is coordination only, never fencing: write correctness comes
//! from the storage epoch, so even a pathological Redis failover that
//! resurrects a stale lease cannot make two primaries write — only the
//! higher epoch's writes survive the header CAS. This is why the lease
//! lives beside the control hash in the same namespace but carries no
//! authority of its own.
use adx_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// The lease record. `renewed_at` is Redis server time (`TIME`), so expiry
/// never depends on Coordinator clock agreement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub holder: String,
    /// 0 until the holder bound the epoch it took at `begin` (through the
    /// first renew after it). An acquired-but-unbound lease is the crash
    /// window between acquire and begin; it recovers by expiry, without
    /// manual cleanup.
    pub epoch: u64,
    pub renewed_at: u64,
    pub lease_ms: u64,
}

/// The lease key plus the server time it was read at, for standby
/// observation without clock synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseView {
    pub record: Option<LeaseRecord>,
    pub server_now_ms: u64,
}

impl LeaseView {
    /// A record exists and has not expired by server time.
    pub fn live(&self) -> bool {
        self.record.as_ref().is_some_and(|record| {
            record.renewed_at.saturating_add(record.lease_ms) > self.server_now_ms
        })
    }
}

// All scripts read Redis server TIME, and all checks and writes happen
// inside one atomic script: a renew racing a takeover acquire resolves one
// way, and the loser sees the winner's record on its next read.
const ACQUIRE: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local record = redis.call('GET', KEYS[1])
if record then
  local lease = cjson.decode(record)
  if tonumber(lease.renewed_at) + tonumber(lease.lease_ms) > now then return 0 end
end
redis.call('SET', KEYS[1], cjson.encode({holder = ARGV[1], epoch = 0, renewed_at = now, lease_ms = tonumber(ARGV[2])}))
return 1
"#;
const RENEW: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local record = redis.call('GET', KEYS[1])
if not record then return 0 end
local lease = cjson.decode(record)
if lease.holder ~= ARGV[1] then return 0 end
if tonumber(lease.renewed_at) + tonumber(lease.lease_ms) <= now then return 0 end
if tonumber(lease.epoch) ~= 0 and tonumber(lease.epoch) ~= tonumber(ARGV[2]) then return 0 end
redis.call('SET', KEYS[1], cjson.encode({holder = ARGV[1], epoch = tonumber(ARGV[2]), renewed_at = now, lease_ms = tonumber(ARGV[3])}))
return 1
"#;
const RELEASE: &str = r#"
local record = redis.call('GET', KEYS[1])
if not record then return 1 end
local lease = cjson.decode(record)
if lease.holder ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"#;
const OBSERVE: &str = r#"
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
return {redis.call('GET', KEYS[1]), string.format('%d', now)}
"#;

#[derive(Clone)]
pub struct CoordinatorLease {
    client: redis::Client,
    connection: Arc<Mutex<Option<redis::aio::MultiplexedConnection>>>,
    key: String,
    timeout: Duration,
    /// Observability stamps for the metrics endpoint: the Unix millisecond
    /// of this process's last successful acquire or renew (0 before the
    /// first success) and the epoch bound to the lease. Clones share them.
    renewed_at_unix_ms: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl CoordinatorLease {
    pub fn new(url: &str, namespace: &str, timeout: Duration) -> Result<Self> {
        let key = adx_discovery::lease_key(namespace)?;
        if timeout.is_zero() {
            return Err(Error::Invalid("lease timeout must be positive".into()));
        }
        Ok(Self {
            client: redis::Client::open(url)
                .map_err(|_| Error::Invalid("invalid lease Redis endpoint".into()))?,
            connection: Arc::default(),
            key,
            timeout,
            renewed_at_unix_ms: Arc::default(),
            epoch: Arc::default(),
        })
    }

    /// Take the lease when it is absent or expired. Returns `false` while a
    /// live lease is held by anyone — the caller should run as the standby.
    /// The record starts with epoch 0; the holder binds its real epoch
    /// through the first [`Self::renew`] after `begin`.
    pub async fn acquire(&self, holder: &str, lease: Duration) -> Result<bool> {
        validate_holder(holder)?;
        let lease_ms = validate_lease(lease)?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(ACQUIRE)
            .arg(1)
            .arg(&self.key)
            .arg(holder)
            .arg(lease_ms);
        let applied: i64 = self.query(command).await?;
        if applied == 1 {
            self.renewed_at_unix_ms
                .store(unix_now_ms(), Ordering::Relaxed);
            self.epoch.store(0, Ordering::Relaxed);
        }
        Ok(applied == 1)
    }

    /// Renew the lease. CAS-checks holder and epoch, and fails once the
    /// lease has expired — missing the renewal deadline means losing
    /// primaryship even if no standby has stepped in yet, so a paused
    /// primary can never resurrect its own lease. An epoch of 0 in the
    /// record ("acquired but begin has not bound an epoch") is bound here
    /// by the holder's first renew.
    pub async fn renew(&self, holder: &str, epoch: u64, lease: Duration) -> Result<bool> {
        validate_holder(holder)?;
        let lease_ms = validate_lease(lease)?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(RENEW)
            .arg(1)
            .arg(&self.key)
            .arg(holder)
            .arg(epoch)
            .arg(lease_ms);
        let applied: i64 = self.query(command).await?;
        if applied == 1 {
            self.renewed_at_unix_ms
                .store(unix_now_ms(), Ordering::Relaxed);
            self.epoch.store(epoch, Ordering::Relaxed);
        }
        Ok(applied == 1)
    }

    /// Release the lease; only the holder may. Idempotent when the key is
    /// already gone. Returns `false` when another holder owns the lease
    /// (the caller never held primaryship it could return).
    pub async fn release(&self, holder: &str) -> Result<bool> {
        validate_holder(holder)?;
        let mut command = redis::cmd("EVAL");
        command.arg(RELEASE).arg(1).arg(&self.key).arg(holder);
        let applied: i64 = self.query(command).await?;
        Ok(applied == 1)
    }

    /// Read the lease key together with the server time it was read at.
    /// The standby observes through this to decide when the primary's lease
    /// has expired, without trusting its own clock.
    /// Metrics stamps: the Unix millisecond of the last successful acquire
    /// or renew by this process, and the epoch bound to the lease.
    pub fn stamps(&self) -> (u64, u64) {
        (
            self.renewed_at_unix_ms.load(Ordering::Relaxed),
            self.epoch.load(Ordering::Relaxed),
        )
    }

    pub async fn observe(&self) -> Result<LeaseView> {
        let mut command = redis::cmd("EVAL");
        command.arg(OBSERVE).arg(1).arg(&self.key);
        let values: Vec<Option<String>> = self.query(command).await?;
        if values.len() != 2 {
            return Err(Error::Unavailable("invalid lease observation".into()));
        }
        let server_now_ms = values[1]
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| Error::Unavailable("invalid lease server time".into()))?;
        let record = match values[0].as_deref() {
            None => None,
            Some(encoded) => {
                let record: LeaseRecord = serde_json::from_str(encoded)
                    .map_err(|_| Error::Unavailable("invalid Coordinator lease record".into()))?;
                validate_holder(&record.holder)?;
                Some(record)
            }
        };
        Ok(LeaseView {
            record,
            server_now_ms,
        })
    }

    /// Primaryship supervision: renew the lease every `lease / 3` until
    /// `stop` fires. A definitive rejection ends the supervision
    /// immediately — an epoch whose lease right was lost must not be
    /// exercised any further. An unavailable Redis is retried within the
    /// remaining lease budget, anchored at the renewal tick: past it, the
    /// lease has factually expired server-side, so the supervision ends
    /// rather than let the primary outlive its own authority. Each attempt
    /// is additionally cut at the remaining budget, so a hanging attempt
    /// cannot outlive the lease even when the RPC timeout exceeds it.
    pub async fn supervise(
        &self,
        holder: &str,
        epoch: u64,
        lease: Duration,
        stop: impl std::future::Future<Output = ()> + Send,
    ) -> Result<()> {
        validate_holder(holder)?;
        validate_lease(lease)?;
        let mut stop = Box::pin(stop);
        let mut tick = tokio::time::interval(lease / 3);
        // The interval's first tick completes immediately; the lease was
        // just renewed by the caller, so consume it without renewing again.
        let _ = tick.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = &mut stop => return Ok(()),
                _ = tick.tick() => {}
            }
            // A lease renewed at this tick stays live until now + lease, so
            // this deadline is when retries stop being retries and become a
            // factually expired lease.
            let deadline = tokio::time::Instant::now() + lease;
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(Error::Unavailable(
                        "Coordinator lease renewals exhausted the lease budget".into(),
                    ));
                }
                let attempt = self.renew(holder, epoch, lease);
                match tokio::time::timeout(remaining, attempt).await {
                    Ok(Ok(true)) => break,
                    Ok(Ok(false)) => {
                        return Err(Error::Unavailable("Coordinator primary lease lost".into()))
                    }
                    Ok(Err(Error::Invalid(message))) => return Err(Error::Invalid(message)),
                    Ok(Err(error)) => {
                        adx_observability::warn!("Coordinator lease renewal unavailable: {error}");
                        tokio::select! {
                            biased;
                            _ = &mut stop => return Ok(()),
                            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                        }
                    }
                    Err(_) => {
                        // The attempt hung past the remaining budget: the
                        // lease is factually expired server-side.
                        return Err(Error::Unavailable(
                            "Coordinator lease renewals exhausted the lease budget".into(),
                        ));
                    }
                }
            }
        }
    }

    async fn query<T: redis::FromRedisValue>(&self, command: redis::Cmd) -> Result<T> {
        let operation = async {
            let mut guard = self.connection.lock().await;
            if guard.is_none() {
                *guard = Some(self.client.get_multiplexed_async_connection().await?);
            }
            let mut connection = guard
                .as_ref()
                .expect("connection is initialized above")
                .clone();
            drop(guard);
            command.query_async(&mut connection).await
        };
        match tokio::time::timeout(self.timeout, operation).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                *self.connection.lock().await = None;
                Err(Error::Unavailable("Coordinator lease unavailable".into()))
            }
        }
    }
}

fn validate_holder(holder: &str) -> Result<()> {
    if holder.is_empty()
        || holder.len() > 128
        || !holder
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(Error::Invalid("invalid lease holder identity".into()));
    }
    Ok(())
}

fn validate_lease(lease: Duration) -> Result<u64> {
    let millis = u64::try_from(lease.as_millis())
        .map_err(|_| Error::Invalid("lease duration overflow".into()))?;
    if !(100..=600_000).contains(&millis) {
        return Err(Error::Invalid("lease must be 100..600000 ms".into()));
    }
    Ok(millis)
}
