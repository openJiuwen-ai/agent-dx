//! Read budgets and MetaReadView.

use super::*;

#[derive(Clone, Copy)]
pub(super) struct ReadLimits {
    pub(super) views: usize,
    pub(super) held_bytes: u64,
    pub(super) lifetime: Duration,
    pub(super) reap: Duration,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            views: 128,
            held_bytes: 8 * 1024 * 1024 * 1024,
            lifetime: Duration::from_secs(30),
            reap: Duration::from_millis(100),
        }
    }
}

#[cfg(test)]
pub(super) struct ReadBudgetStats {
    pub(super) views: usize,
    pub(super) bytes: u64,
}

const VIEW_CHARGE_UNIT: u64 = 4096;

impl ReadLimits {
    pub(super) fn validate(self) -> Result<Self> {
        let units = self.held_bytes / VIEW_CHARGE_UNIT;
        if !(1..=4096).contains(&self.views)
            || units == 0
            || units > u64::from(u32::MAX)
            || units > Semaphore::MAX_PERMITS as u64
            || self.lifetime.is_zero()
            || self.lifetime > Duration::from_secs(60)
            || self.reap.is_zero()
            || self.reap > self.lifetime
        {
            return Err(invalid_contract("invalid finite Meta read-view limits"));
        }
        Ok(self)
    }
}

pub(super) struct ReadAllocation {
    pub(super) state: Arc<State>,
    _bytes: OwnedSemaphorePermit,
}

pub(super) struct ReadHold {
    allocation: Arc<ReadAllocation>,
    _slot: OwnedSemaphorePermit,
    // Keeps the single reaper alive while an idle or executing view owns data.
    owner: Arc<ReadBudget>,
}

pub(super) struct ReadPin {
    held: Mutex<Option<Arc<ReadHold>>>,
    deadline: tokio::time::Instant,
    pub(super) visible: Weak<Visible>,
}

impl ReadPin {
    fn check(&self) -> Result<()> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(unavailable("Meta read view expired; acquire a fresh view"));
        }
        let visible = self
            .visible
            .upgrade()
            .ok_or_else(|| unavailable("Meta read authority retired"))?;
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable("Meta read authority failed; restart to reload"));
        }
        Ok(())
    }
    fn capture(&self) -> Result<Arc<ReadHold>> {
        self.check()?;
        let held = self
            .held
            .lock()
            .map_err(|_| unavailable("Meta read pin lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| unavailable("Meta read view was released"))?;
        if held.owner.bytes.is_closed() {
            return Err(unavailable("Meta read budget failed"));
        }
        self.check()?;
        Ok(held)
    }
    fn expire(&self, now: tokio::time::Instant) -> Result<()> {
        if now < self.deadline {
            return Ok(());
        }
        // Destruction may release a large old tree. It happens outside both
        // the admission registry and pin locks; an executing read still owns
        // its Arc<ReadHold> and therefore retains both permits until it ends.
        let retired = {
            self.held
                .lock()
                .map_err(|_| unavailable("Meta read pin lock poisoned"))?
                .take()
        };
        drop(retired);
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ReadRegistry {
    pub(super) views: Vec<Weak<ReadPin>>,
    pub(super) allocations: std::collections::BTreeMap<usize, Weak<ReadAllocation>>,
}

pub(super) struct ReadBudget {
    limits: ReadLimits,
    pub(super) bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    pub(super) registry: Mutex<ReadRegistry>,
}

impl ReadBudget {
    pub(super) fn new(limits: ReadLimits) -> Result<Arc<Self>> {
        let limits = limits.validate()?;
        let units = usize::try_from(limits.held_bytes / VIEW_CHARGE_UNIT)
            .map_err(|_| invalid_contract("Meta read byte units overflow"))?;
        let budget = Arc::new(Self {
            limits,
            bytes: Arc::new(Semaphore::new(units)),
            slots: Arc::new(Semaphore::new(limits.views)),
            registry: Mutex::new(ReadRegistry::default()),
        });
        let weak = Arc::downgrade(&budget);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(limits.reap).await;
                let Some(budget) = weak.upgrade() else {
                    break;
                };
                if let Err(error) = budget.reap() {
                    budget.bytes.close();
                    budget.slots.close();
                    tracing::error!(%error, "Meta read budget reaper failed closed");
                    break;
                }
            }
        });
        Ok(budget)
    }
    pub(super) fn reap(&self) -> Result<()> {
        let pins = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
            registry
                .views
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        let now = tokio::time::Instant::now();
        for pin in pins {
            pin.expire(now)?;
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
        // Expired handles may remain owned by a caller; they no longer retain
        // a State and must not accumulate weak entries in this registry.
        registry
            .views
            .retain(|weak| weak.upgrade().is_some_and(|pin| pin.deadline > now));
        registry
            .allocations
            .retain(|_, allocation| allocation.strong_count() > 0);
        Ok(())
    }
    pub(super) fn pin(
        self: &Arc<Self>,
        state: Arc<State>,
        visible: &Arc<Visible>,
    ) -> Result<MetaReadView> {
        self.reap()?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| journal::full("Meta read view count exhausted"))?;
        let charge = state.pin_charge()?;
        let units = u32::try_from(charge.div_ceil(VIEW_CHARGE_UNIT))
            .map_err(|_| journal::full("Meta pinned state charge overflow"))?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
        // Another thread can finish its reap before a just-created view is
        // registered/dropped. Recheck weak-entry bounds under this same lock;
        // slot permits alone must not allow dead registry entries to grow.
        registry.views.retain(|pin| pin.strong_count() > 0);
        registry
            .allocations
            .retain(|_, allocation| allocation.strong_count() > 0);
        if registry.views.len() >= self.limits.views {
            return Err(journal::full("Meta read registry count exhausted"));
        }
        let key = Arc::as_ptr(&state) as usize;
        let allocation =
            if let Some(allocation) = registry.allocations.get(&key).and_then(Weak::upgrade) {
                if !Arc::ptr_eq(&allocation.state, &state) {
                    return Err(invalid_contract("Meta pinned state identity changed"));
                }
                allocation
            } else {
                if registry.allocations.len() >= self.limits.views {
                    return Err(journal::full("Meta read allocation registry exhausted"));
                }
                let permit = self
                    .bytes
                    .clone()
                    .try_acquire_many_owned(units)
                    .map_err(|_| journal::full("Meta pinned state byte capacity exhausted"))?;
                let allocation = Arc::new(ReadAllocation {
                    state,
                    _bytes: permit,
                });
                registry
                    .allocations
                    .insert(key, Arc::downgrade(&allocation));
                allocation
            };
        let deadline = tokio::time::Instant::now()
            .checked_add(self.limits.lifetime)
            .ok_or_else(|| invalid_contract("Meta read deadline overflow"))?;
        let pin = Arc::new(ReadPin {
            held: Mutex::new(Some(Arc::new(ReadHold {
                allocation,
                _slot: slot,
                owner: self.clone(),
            }))),
            deadline,
            visible: Arc::downgrade(visible),
        });
        registry.views.push(Arc::downgrade(&pin));
        Ok(MetaReadView {
            state: ReadState::Pinned(pin),
        })
    }
    #[cfg(test)]
    pub(super) fn stats(&self) -> ReadBudgetStats {
        ReadBudgetStats {
            views: self.limits.views - self.slots.available_permits(),
            bytes: (self.limits.held_bytes / VIEW_CHARGE_UNIT
                - self.bytes.available_permits() as u64)
                * VIEW_CHARGE_UNIT,
        }
    }
}

pub(super) enum ReadState {
    Pinned(Arc<ReadPin>),
    // Test-only reference state machine; never an unbounded production view.
    #[cfg(test)]
    Unmanaged(Arc<State>),
}

/// Immutable compound read of one acknowledged authority revision.
/// Canonical views have finite admission and lifetime; callers must acquire a
/// fresh view after expiry or a known authority failure. They cannot mutate it.
pub struct MetaReadView {
    pub(super) state: ReadState,
}

impl MetaReadView {
    #[cfg(test)]
    pub(super) fn unmanaged(state: Arc<State>) -> Self {
        Self {
            state: ReadState::Unmanaged(state),
        }
    }
    #[cfg(test)]
    pub(super) fn state_for_test(&self) -> Arc<State> {
        match &self.state {
            ReadState::Unmanaged(state) => state.clone(),
            ReadState::Pinned(pin) => pin
                .held
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .allocation
                .state
                .clone(),
        }
    }
    fn with_state<T>(&self, read: impl FnOnce(&State) -> Result<T>) -> Result<T> {
        match &self.state {
            #[cfg(test)]
            ReadState::Unmanaged(state) => read(state),
            ReadState::Pinned(pin) => {
                let held = pin.capture()?;
                let result = read(&held.allocation.state);
                pin.check()?;
                if held.owner.bytes.is_closed() {
                    return Err(unavailable("Meta read budget failed"));
                }
                result
            }
        }
    }

    /// Reads one key from this pinned revision for related-key CAS.
    pub(crate) fn lookup(&self, key: &MetaKey) -> Result<MetaSnapshot> {
        self.with_state(|state| {
            let value = state.entities.get(key);
            Ok(MetaSnapshot {
                revision: value.map_or(state.revision, |entry| entry.revision),
                entity: value.map(|entry| entry.entity.clone()),
                request_outcome: None,
                entities: Vec::new(),
            })
        })
    }

    pub(crate) fn snapshot(&self) -> Result<(StoreRevision, Vec<MetaEntity>)> {
        self.with_state(|state| {
            Ok((
                state.revision,
                state
                    .entities
                    .values()
                    .map(|entry| entry.entity.clone())
                    .collect(),
            ))
        })
    }

    pub(crate) fn statistics(&self) -> Result<serde_json::Value> {
        self.with_state(|state| {
            let mut kinds = std::collections::BTreeMap::new();
            for entry in state.entities.values() {
                *kinds.entry(entry.entity.name()).or_insert(0_u64) += 1;
            }
            Ok(
                serde_json::json!({"revision": state.revision.0, "entities": kinds,
                "requests": state.requests.len(), "events": state.events.len(),
                "watch_floor": state.compacted_to.0}),
            )
        })
    }

    pub async fn read(&self, read: MetaRead) -> Result<MetaSnapshot> {
        self.with_state(|state| state.read(read))
    }
}
