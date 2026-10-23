//! Finite ownership of Meta watch response construction and streaming queues.
//! This is not authority state or a network-buffer/RSS budget. A fixed registry
//! and one weak reaper release idle response payloads without consumer polling.
use super::store::{
    WATCH_RESPONSE_BASE_BYTES, WATCH_RESPONSE_MAX_BYTES, WATCH_RESPONSE_MAX_EVENTS,
};
use afs_error::{Error, Result};
use afs_protocol::meta::RootCommand;
use afs_tracing::tracing;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;
use tokio_stream::Stream;
use tonic::Status;
fn full(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_CAPACITY, message)
}
pub(super) fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}

// Covers both the bounded domain batch and its moved/converted wire batch
// during construction. Fixed registry metadata has a separate 64KiB reserve.
const RESPONSE_CHARGE: u64 = 2 * WATCH_RESPONSE_MAX_BYTES;
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub slots: usize,
    pub held_bytes: u64,
    pub lifetime: Duration,
    pub reap: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            slots: 16,
            held_bytes: 512 * 1024 * 1024,
            lifetime: Duration::from_secs(30),
            reap: Duration::from_millis(100),
        }
    }
}
struct Slot {
    generation: u64,
    deadline: Instant,
    // None is a builder still owning external intermediates; expiry must not
    // release its charge until that operation has actually exited.
    commands: Option<VecDeque<RootCommand>>,
}
struct Registry {
    slots: Vec<Option<Slot>>,
    next_generation: u64,
    bytes: u64,
}
pub(super) struct Budget {
    limits: Limits,
    registry: Mutex<Registry>,
    reaper_started: AtomicBool,
    failed: AtomicBool,
}
impl Budget {
    #[cfg(test)]
    pub fn new(limits: Limits) -> Result<Arc<Self>> {
        if !(1..=128).contains(&limits.slots)
            || limits.held_bytes < RESPONSE_CHARGE
            || limits.lifetime.is_zero()
            || limits.lifetime > Duration::from_secs(60)
            || limits.reap.is_zero()
            || limits.reap > limits.lifetime
        {
            return Err(unavailable("invalid finite watch ownership limits"));
        }
        Ok(Self::build(limits))
    }
    pub fn standard() -> Arc<Self> {
        // Internal defaults are fixed here; external configuration is not yet
        // accepted. The checked constructor is exercised by the limits tests.
        Self::build(Limits::default())
    }
    fn build(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            registry: Mutex::new(Registry {
                slots: std::iter::repeat_with(|| None).take(limits.slots).collect(),
                next_generation: 1,
                bytes: 0,
            }),
            reaper_started: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        })
    }
    // Constructors may run outside Tokio; start exactly one reaper at first
    // asynchronous admission, not one sleeping task for every short response.
    fn start(self: &Arc<Self>) -> Result<()> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| unavailable("watch admission requires an active runtime"))?;
        if self
            .reaper_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let weak = Arc::downgrade(self);
            let period = self.limits.reap;
            runtime.spawn(async move {
                loop {
                    tokio::time::sleep(period).await;
                    let Some(budget) = weak.upgrade() else {
                        break;
                    };
                    if let Err(error) = budget.reap() {
                        budget.failed.store(true, Ordering::Release);
                        tracing::error!(%error, "Meta watch reaper failed closed");
                        break;
                    }
                }
            });
        }
        Ok(())
    }
    fn reap(&self) -> Result<()> {
        let retired = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| unavailable("watch registry poisoned"))?;
            let now = Instant::now();
            let mut retired = Vec::new();
            for index in 0..registry.slots.len() {
                if registry.slots[index]
                    .as_ref()
                    .is_some_and(|slot| slot.deadline <= now && slot.commands.is_some())
                {
                    retired.push(registry.slots[index].take());
                    registry.bytes = registry
                        .bytes
                        .checked_sub(RESPONSE_CHARGE)
                        .ok_or_else(|| unavailable("watch ownership accounting underflow"))?;
                }
            }
            retired
        };
        // Dropping a potentially large old queue never holds the registry lock.
        drop(retired);
        Ok(())
    }
    pub fn reserve(self: &Arc<Self>) -> Result<Lease> {
        if self.failed.load(Ordering::Acquire) {
            return Err(unavailable("watch budget failed"));
        }
        self.start()?;
        self.reap()?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| unavailable("watch registry poisoned"))?;
        let index = registry
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| full("watch response count exhausted"))?;
        let bytes = registry
            .bytes
            .checked_add(RESPONSE_CHARGE)
            .filter(|n| *n <= self.limits.held_bytes)
            .ok_or_else(|| full("watch response byte capacity exhausted"))?;
        let generation = registry.next_generation;
        let next = generation
            .checked_add(1)
            .ok_or_else(|| unavailable("watch generation exhausted"))?;
        let deadline = Instant::now()
            .checked_add(self.limits.lifetime)
            .ok_or_else(|| unavailable("watch deadline overflow"))?;
        registry.next_generation = next;
        registry.bytes = bytes;
        registry.slots[index] = Some(Slot {
            generation,
            deadline,
            commands: None,
        });
        Ok(Lease {
            owner: self.clone(),
            index,
            generation,
            deadline,
        })
    }
    fn release(&self, index: usize, generation: u64) {
        let retired = match self.registry.lock() {
            Ok(mut registry) => {
                if registry.slots[index]
                    .as_ref()
                    .is_some_and(|slot| slot.generation == generation)
                {
                    let retired = registry.slots[index].take();
                    if let Some(bytes) = registry.bytes.checked_sub(RESPONSE_CHARGE) {
                        registry.bytes = bytes;
                    } else {
                        self.failed.store(true, Ordering::Release);
                    }
                    retired
                } else {
                    None
                }
            }
            Err(_) => {
                self.failed.store(true, Ordering::Release);
                None
            }
        };
        drop(retired);
    }
    #[cfg(test)]
    fn stats(&self) -> (usize, u64, usize) {
        let registry = self.registry.lock().unwrap();
        (
            registry.slots.iter().flatten().count(),
            registry.bytes,
            registry
                .slots
                .iter()
                .flatten()
                .filter_map(|slot| slot.commands.as_ref())
                .map(VecDeque::len)
                .sum(),
        )
    }
}
pub(super) struct Lease {
    owner: Arc<Budget>,
    index: usize,
    generation: u64,
    pub deadline: Instant,
}
impl Lease {
    pub fn check(&self) -> Result<()> {
        if self.owner.failed.load(Ordering::Acquire) || Instant::now() >= self.deadline {
            return Err(unavailable(
                "watch response expired or failed; poll again from last complete revision",
            ));
        }
        Ok(())
    }
    pub fn stream(self, commands: Vec<RootCommand>) -> Result<ResponseStream> {
        self.check()?;
        wire_charge(&commands)?;
        {
            let mut registry = self
                .owner
                .registry
                .lock()
                .map_err(|_| unavailable("watch registry poisoned"))?;
            let slot = registry.slots[self.index]
                .as_mut()
                .filter(|slot| slot.generation == self.generation)
                .ok_or_else(|| unavailable("watch response ownership retired"))?;
            self.check()?;
            if slot.commands.is_some() {
                return Err(unavailable("watch response already installed"));
            }
            slot.commands = Some(VecDeque::from(commands));
        }
        Ok(ResponseStream { lease: Some(self) })
    }
    fn next(&self) -> Result<Option<RootCommand>> {
        self.check()?;
        let command = {
            let mut registry = self
                .owner
                .registry
                .lock()
                .map_err(|_| unavailable("watch registry poisoned"))?;
            let slot = registry.slots[self.index]
                .as_mut()
                .filter(|slot| slot.generation == self.generation)
                .ok_or_else(|| unavailable("watch response retired; poll again"))?;
            slot.commands
                .as_mut()
                .ok_or_else(|| unavailable("watch response not installed"))?
                .pop_front()
        };
        self.check()?;
        Ok(command)
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.owner.release(self.index, self.generation);
    }
}

/// Counts actual Vec/String capacities, not protobuf encoded size. Exhaustive
/// destructuring makes adding a wire field require an explicit ownership audit.
pub(super) fn wire_charge(commands: &Vec<RootCommand>) -> Result<u64> {
    if commands.len() > WATCH_RESPONSE_MAX_EVENTS {
        return Err(full("watch wire count exceeds response bound"));
    }
    let mut bytes = WATCH_RESPONSE_BASE_BYTES
        .checked_add(
            u64::try_from(commands.capacity())
                .ok()
                .and_then(|n| n.checked_mul(std::mem::size_of::<RootCommand>() as u64))
                .ok_or_else(|| full("watch wire capacity overflow"))?,
        )
        .ok_or_else(|| full("watch wire byte overflow"))?;
    for RootCommand {
        command_id,
        command_type: _,
        access,
        revision: _,
    } in commands
    {
        let mut extra = command_id.capacity();
        if let Some(afs_protocol::meta::RootAccess {
            root_id,
            root_epoch: _,
            home_node_id,
            holder_node_id,
            session_id,
            access_generation: _,
            rights,
            fencing_token,
            home_session_id,
        }) = access
        {
            extra = extra
                .checked_add(
                    rights
                        .capacity()
                        .checked_mul(std::mem::size_of::<i32>())
                        .ok_or_else(|| full("watch rights capacity overflow"))?,
                )
                .ok_or_else(|| full("watch wire capacity overflow"))?;
            for value in [
                root_id,
                home_node_id,
                holder_node_id,
                session_id,
                fencing_token,
                home_session_id,
            ] {
                extra = extra
                    .checked_add(value.capacity())
                    .ok_or_else(|| full("watch wire capacity overflow"))?;
            }
        }
        bytes = bytes
            .checked_add(u64::try_from(extra).map_err(|_| full("watch wire capacity conversion"))?)
            .filter(|n| *n <= WATCH_RESPONSE_MAX_BYTES)
            .ok_or_else(|| full("watch wire bytes exceed response bound"))?;
    }
    if bytes > WATCH_RESPONSE_MAX_BYTES {
        return Err(full("watch wire bytes exceed response bound"));
    }
    Ok(bytes)
}
pub(super) struct ResponseStream {
    lease: Option<Lease>,
}
impl Stream for ResponseStream {
    type Item = std::result::Result<RootCommand, Status>;
    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(lease) = self.lease.as_ref() else {
            return Poll::Ready(None);
        };
        match lease.next() {
            Ok(Some(command)) => Poll::Ready(Some(Ok(command))),
            Ok(None) => {
                self.lease.take();
                Poll::Ready(None)
            }
            Err(error) => {
                self.lease.take();
                Poll::Ready(Some(Err(
                    afs_transport::grpc::error_status::error_to_status(error),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    fn limits(slots: usize, responses: u64) -> Limits {
        Limits {
            slots,
            held_bytes: responses * RESPONSE_CHARGE,
            lifetime: Duration::from_millis(80),
            reap: Duration::from_millis(5),
        }
    }
    fn commands() -> Vec<RootCommand> {
        vec![RootCommand {
            command_id: "command".into(),
            revision: 1,
            ..RootCommand::default()
        }]
    }
    #[test]
    fn p3_watch_ownership_invalid_limits_and_owned_wire_spare_refused() {
        assert!(Budget::new(limits(0, 1)).is_err());
        assert!(Budget::new(limits(129, 1)).is_err());
        assert!(Budget::new(limits(1, 0)).is_err());
        assert!(
            Budget::new(Limits {
                lifetime: Duration::ZERO,
                ..limits(1, 1)
            })
            .is_err()
        );
        let mut wire = commands();
        wire[0].command_id = String::with_capacity(17 * 1024 * 1024);
        wire[0].command_id.push('x');
        assert!(
            wire_charge(&wire).is_err(),
            "encoded length is not owned capacity"
        );
        assert!(Budget::new(Limits::default()).is_ok());
    }
    #[tokio::test]
    async fn p3_watch_ownership_count_and_bytes_refuse_before_construction() {
        for policy in [limits(1, 2), limits(2, 1)] {
            let budget = Budget::new(Limits {
                lifetime: Duration::from_secs(5),
                ..policy
            })
            .unwrap();
            let first = budget.reserve().unwrap();
            assert!(budget.reserve().is_err());
            assert_eq!(budget.stats(), (1, RESPONSE_CHARGE, 0));
            drop(first);
            assert_eq!(budget.stats(), (0, 0, 0));
            let next = budget.reserve().unwrap();
            drop(next);
        }
    }
    #[tokio::test]
    async fn p3_watch_ownership_idle_queue_actively_retires_and_stale_drop_is_safe() {
        let budget = Budget::new(limits(1, 1)).unwrap();
        let mut old = budget.reserve().unwrap().stream(commands()).unwrap();
        assert_eq!(budget.stats(), (1, RESPONSE_CHARGE, 1));
        tokio::time::sleep(Duration::from_millis(160)).await;
        assert_eq!(
            budget.stats(),
            (0, 0, 0),
            "unpolled queue must actually detach its payload"
        );
        let replacement = budget.reserve().unwrap();
        assert!(old.next().await.unwrap().is_err());
        assert!(old.next().await.is_none());
        drop(old);
        assert_eq!(
            budget.stats(),
            (1, RESPONSE_CHARGE, 0),
            "expired stream cannot release the new slot generation"
        );
        drop(replacement);
        assert_eq!(budget.stats(), (0, 0, 0));
    }
    #[tokio::test]
    async fn p3_watch_ownership_expired_builder_keeps_charge_until_exit() {
        let budget = Budget::new(limits(1, 1)).unwrap();
        let builder = budget.reserve().unwrap();
        tokio::time::sleep(Duration::from_millis(160)).await;
        assert_eq!(budget.stats(), (1, RESPONSE_CHARGE, 0));
        assert!(
            budget.reserve().is_err(),
            "still-owned intermediates cannot be charged to a replacement"
        );
        assert!(builder.stream(commands()).is_err());
        assert_eq!(budget.stats(), (0, 0, 0));
    }
    #[tokio::test]
    async fn p3_watch_ownership_normal_eof_cancellation_and_fixed_registry() {
        let budget = Budget::new(limits(1, 1)).unwrap();
        for _ in 0..1024 {
            let cancelled = budget.reserve().unwrap();
            drop(cancelled);
        }
        assert_eq!(budget.registry.lock().unwrap().slots.len(), 1);
        let mut stream = budget.reserve().unwrap().stream(commands()).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().command_id, "command");
        assert!(stream.next().await.is_none());
        assert_eq!(budget.stats(), (0, 0, 0));
    }
}
