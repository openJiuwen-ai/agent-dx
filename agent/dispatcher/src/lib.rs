//! Managed Session orchestration. Sandbox calls go only through the Gateway boundary.
pub mod routing;
pub mod server;
pub mod transport;

use adx_agent_core::sandbox::{
    CreateSandbox, Sandbox, SandboxError, SandboxObservation, SandboxPhase,
};
use adx_agent_core::{cache::BoundedCache, limits};
use adx_agent_core::{DesiredState, Instance, InstancePhase, Scope, Session, SessionPhase};
use adx_agent_store::{AgentState, Error as StoreError, Reservation};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum Error {
    Invalid(String),
    NotFound,
    NotReady(String),
    Conflict(String),
    Unavailable(String),
    OutcomeUnknown(String),
    Unsupported(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::Invalid(m) => Self::Invalid(m),
            StoreError::Conflict(m) => Self::Conflict(m),
            StoreError::Corrupt(_) => Self::Unavailable("stored state is invalid".into()),
            StoreError::Unavailable(m) => Self::Unavailable(m),
            StoreError::OutcomeUnknown(m) => Self::OutcomeUnknown(m),
        }
    }
}
impl From<SandboxError> for Error {
    fn from(value: SandboxError) -> Self {
        match value {
            SandboxError::NotFound => Self::NotFound,
            SandboxError::Invalid(m) => Self::Invalid(m),
            SandboxError::Conflict(m) => Self::Conflict(m),
            SandboxError::Unsupported(m) => Self::Unsupported(m),
            SandboxError::Unavailable(m) => Self::Unavailable(m),
            SandboxError::OutcomeUnknown(m) => Self::OutcomeUnknown(m),
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub use adx_agent_core::dispatcher::{ResolveRequest, Target};
#[derive(Debug, Clone)]
pub struct Config {
    pub create_timeout: Duration,
    pub poll_interval: Duration,
    pub max_poll_interval: Duration,
    pub pool_cache_ttl: Duration,
    pub max_cached_sessions: usize,
    pub max_cached_affinities_per_session: usize,
    pub max_sandbox_inflight: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            create_timeout: limits::CREATE_TIMEOUT,
            poll_interval: Duration::from_millis(500),
            max_poll_interval: Duration::from_secs(5),
            pool_cache_ttl: Duration::from_secs(30),
            max_cached_sessions: limits::SESSION_CACHE_ENTRIES,
            max_cached_affinities_per_session: limits::AFFINITY_CACHE_ENTRIES,
            max_sandbox_inflight: 32,
        }
    }
}
#[derive(Clone, Copy)]
struct CreationPoll {
    next: tokio::time::Instant,
    delay: Duration,
}

struct SessionSlot {
    cache: Mutex<LocalSession>,
    // Coalesce cache refreshes only, never allocation, Sandbox calls or backoff waits.
    refresh: AsyncMutex<()>,
}

#[derive(Clone)]
struct SessionSnapshot {
    session: Session,
    pool: Vec<Instance>,
    refreshed: Instant,
    epoch: u64,
}

struct LocalSession {
    epoch: u64,
    session: Option<Session>,
    affinities: BoundedCache<String, Instance>,
    cursor: routing::RoundRobin,
    pool: Vec<Instance>,
    refreshed: Option<Instant>,
}
impl LocalSession {
    fn clear(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        self.session = None;
        self.pool.clear();
        self.affinities.clear();
        self.refreshed = None;
    }
    fn snapshot(&self) -> Option<SessionSnapshot> {
        Some(SessionSnapshot {
            session: self.session.clone()?,
            pool: self.pool.clone(),
            refreshed: self.refreshed?,
            epoch: self.epoch,
        })
    }
}

pub struct Dispatcher {
    state: AgentState,
    sandbox: Arc<dyn Sandbox>,
    boot_id: String,
    config: Config,
    sessions: Mutex<HashMap<Scope, Arc<SessionSlot>>>,
    sandbox_budget: tokio::sync::Semaphore,
    work: Mutex<HashMap<String, std::sync::Weak<AsyncMutex<()>>>>,
    creation_polls: Mutex<BoundedCache<String, CreationPoll>>,
}
impl Dispatcher {
    pub fn new(
        state: AgentState,
        sandbox: Arc<dyn Sandbox>,
        boot_id: String,
        config: Config,
    ) -> Result<Self> {
        if uuid::Uuid::parse_str(&boot_id).is_err()
            || config.create_timeout.as_millis() == 0
            || config.poll_interval.is_zero()
            || config.max_poll_interval < config.poll_interval
            || config.max_cached_sessions == 0
            || config.max_cached_affinities_per_session == 0
            || config.max_sandbox_inflight == 0
        {
            return Err(Error::Invalid("invalid Dispatcher configuration".into()));
        }
        Ok(Self {
            state,
            sandbox,
            boot_id,
            sandbox_budget: tokio::sync::Semaphore::new(config.max_sandbox_inflight),
            creation_polls: Mutex::new(BoundedCache::new(config.max_cached_sessions)),
            config,
            sessions: Mutex::new(HashMap::new()),
            work: Mutex::new(HashMap::new()),
        })
    }
    fn local(&self, scope: &Scope) -> Arc<SessionSlot> {
        let mut sessions = self.sessions.lock().expect("session cache mutex");
        if let Some(local) = sessions.get(scope) {
            return local.clone();
        }
        if sessions.len() >= self.config.max_cached_sessions {
            // Evict a single idle slot; active requests retain their shared lock/cursor.
            let idle = sessions
                .iter()
                .find(|(_, local)| Arc::strong_count(local) == 1)
                .map(|(key, _)| key.clone());
            if let Some(key) = idle {
                sessions.remove(&key);
            }
        }
        // If every slot is busy, this request gets an uncached local cursor rather than unbounded cache growth.
        let local = Arc::new(SessionSlot {
            refresh: AsyncMutex::new(()),
            cache: Mutex::new(LocalSession {
                epoch: 0,
                session: None,
                affinities: BoundedCache::new(self.config.max_cached_affinities_per_session),
                cursor: routing::RoundRobin::new(&self.boot_id, scope),
                pool: Vec::new(),
                refreshed: None,
            }),
        });
        if sessions.len() < self.config.max_cached_sessions {
            sessions.insert(scope.clone(), local.clone());
        }
        local
    }
    fn work_lock(&self, instance: &Instance) -> Arc<AsyncMutex<()>> {
        let key = adx_agent_core::encode_key(&[&instance.tenant, &instance.id]);
        let mut work = self.work.lock().expect("work mutex");
        if let Some(lock) = work.get(&key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        work.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(AsyncMutex::new(()));
        work.insert(key, Arc::downgrade(&lock));
        lock
    }
    // Request waiters and background recovery share the same per-instance backoff.
    // This is a bounded local optimization; replacement nodes may make an initial query.
    fn creation_check_due(&self, instance: &Instance) -> bool {
        let key = adx_agent_core::encode_key(&[&instance.tenant, &instance.id]);
        let now = tokio::time::Instant::now();
        let mut polls = self.creation_polls.lock().expect("creation poll cache");
        let delay = if let Some(previous) = polls.get(&key) {
            if now < previous.next {
                return false;
            }
            previous
                .delay
                .saturating_mul(2)
                .min(self.config.max_poll_interval)
        } else {
            self.config.poll_interval
        };
        polls.insert(
            key,
            CreationPoll {
                next: now + delay,
                delay,
            },
        );
        true
    }

    pub async fn resolve(&self, request: &ResolveRequest) -> Result<Target> {
        self.resolve_with_deadline(request, None).await
    }
    pub async fn resolve_with_deadline(
        &self,
        request: &ResolveRequest,
        caller_deadline_ms: Option<u64>,
    ) -> Result<Target> {
        request.scope.validate().map_err(Error::Invalid)?;
        if request
            .affinity_key
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > limits::IDENTIFIER_BYTES)
        {
            return Err(Error::Invalid("invalid affinity ID".into()));
        }
        // Selection and a newly reserved instance share one deadline, including queue time.
        let started_ms = adx_agent_core::unix_time_millis();
        let deadline_ms = started_ms
            .checked_add(
                u64::try_from(self.config.create_timeout.as_millis())
                    .map_err(|_| Error::Invalid("creation timeout overflow".into()))?,
            )
            .ok_or_else(|| Error::Invalid("creation deadline overflow".into()))?;
        let deadline_ms = caller_deadline_ms.map_or(deadline_ms, |caller| caller.min(deadline_ms));
        let remaining = deadline_ms.saturating_sub(started_ms);
        if remaining == 0 {
            return Err(Error::NotReady(
                "selection deadline expired before execution".into(),
            ));
        }
        match tokio::time::timeout(
            Duration::from_millis(remaining),
            self.resolve_inner(request, deadline_ms),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.invalidate(&request.scope);
                // The request deadline may cancel observation before its expiry branch runs.
                // Persist cleanup intent; the existing recovery loop performs the delete.
                if let Ok(pool) = self.state.pool(&request.scope).await {
                    for instance in pool.into_iter().filter(Instance::creation_expired) {
                        self.state
                            .expire_creation(&instance.tenant, &instance.id)
                            .await?;
                    }
                }
                Err(Error::NotReady(limits::CREATE_TIMEOUT_MESSAGE.into()))
            }
        }
    }
    /// Transport-only allowance for committing timeout cleanup and sending the result.
    pub fn request_timeout(&self) -> Duration {
        self.config
            .create_timeout
            .saturating_add(limits::DISPATCHER_FINISH_TIMEOUT)
    }

    fn invalidate(&self, scope: &Scope) {
        if let Some(local) = self
            .sessions
            .lock()
            .expect("session cache mutex")
            .remove(scope)
        {
            // Fence in-flight cache writes even if a request still holds this detached slot.
            local.cache.lock().expect("local Session cache").clear();
        }
    }
    fn target(instance: &Instance, scope: &Scope, generation: &str) -> Result<Target> {
        if instance.desired != DesiredState::Running || instance.phase != InstancePhase::Ready {
            return Err(Error::NotReady(
                "selected logical instance is unavailable".into(),
            ));
        }
        let target = Target::from_instance(instance);
        if target.scope != *scope || target.session_generation != generation {
            return Err(Error::Unavailable("instance lifecycle mismatch".into()));
        }
        Ok(target)
    }
    async fn refresh_local(
        &self,
        scope: &Scope,
        local: &SessionSlot,
        bypass: bool,
    ) -> Result<SessionSnapshot> {
        let observed_epoch = local.cache.lock().expect("local Session cache").epoch;
        let _refresh = local.refresh.lock().await;
        let epoch = {
            let mut slot = local.cache.lock().expect("local Session cache");
            // Reuse a refresh that completed while this ordinary miss was waiting.
            // An explicit bypass always performs its own authoritative read.
            if !bypass && slot.epoch != observed_epoch {
                if let Some(snapshot) = slot.snapshot() {
                    return Ok(snapshot);
                }
            }
            // Clear before I/O: a failed or cancelled refresh cannot revive old affinities.
            slot.clear();
            slot.epoch
        };
        let session = self.state.session(scope).await?.ok_or(Error::NotFound)?;
        if session.phase != SessionPhase::Active {
            return Err(Error::Conflict("Session is deleting".into()));
        }
        let pool = self.state.pool(scope).await?;
        if pool
            .iter()
            .any(|i| i.scope != *scope || i.session_generation != session.generation)
        {
            return Err(Error::Conflict(
                "Session lifecycle changed during refresh".into(),
            ));
        }
        let mut slot = local.cache.lock().expect("local Session cache");
        if slot.epoch != epoch {
            return Err(Error::Conflict(
                "Session cache invalidated during refresh".into(),
            ));
        }
        slot.epoch = slot.epoch.wrapping_add(1);
        slot.pool = pool;
        slot.session = Some(session);
        slot.refreshed = Some(Instant::now());
        Ok(slot.snapshot().expect("loaded Session snapshot"))
    }
    fn cache_affinity(
        local: &SessionSlot,
        snapshot: &SessionSnapshot,
        key: &str,
        instance: Instance,
    ) -> bool {
        let mut slot = local.cache.lock().expect("local Session cache");
        if slot.epoch != snapshot.epoch {
            return false; // A bypass/invalidation superseded this I/O result.
        }
        slot.affinities.insert(key.to_owned(), instance);
        true
    }
    async fn resolve_inner(&self, request: &ResolveRequest, deadline_ms: u64) -> Result<Target> {
        let scope = &request.scope;
        let cold_id = uuid::Uuid::new_v4().to_string();
        let local = self.local(scope);
        if request.bypass_cache {
            self.refresh_local(scope, &local, true).await?;
        }
        let mut poll_delay = self.config.poll_interval;
        let mut waited_for_creation = false;
        loop {
            // Warm affinity hits only hold the cache lock while reading local state.
            if let Some(key) = &request.affinity_key {
                let cached = {
                    let slot = local.cache.lock().expect("local Session cache");
                    slot.session.as_ref().and_then(|session| {
                        slot.affinities
                            .get(key)
                            .map(|instance| Self::target(instance, scope, &session.generation))
                    })
                };
                if let Some(target) = cached {
                    return target;
                }
            }
            let cached = local.cache.lock().expect("local Session cache").snapshot();
            let mut snapshot = match cached {
                Some(snapshot) => snapshot,
                None => self.refresh_local(scope, &local, false).await?,
            };
            if let Some(key) = &request.affinity_key {
                if let Some(binding) = self
                    .state
                    .affinity_in_session(scope, &snapshot.session.generation, key)
                    .await?
                {
                    let instance = self
                        .state
                        .instance(&scope.tenant, &binding.instance_id)
                        .await?
                        .ok_or_else(|| {
                            Error::Unavailable(
                                "bound logical instance missing; exit not confirmed".into(),
                            )
                        })?;
                    if instance.phase != InstancePhase::Deleted {
                        let target = Self::target(&instance, scope, &snapshot.session.generation)?;
                        if Self::cache_affinity(&local, &snapshot, key, instance) {
                            return Ok(target);
                        }
                        continue;
                    }
                }
            }
            if snapshot.refreshed.elapsed() >= self.config.pool_cache_ttl {
                snapshot = self.refresh_local(scope, &local, false).await?;
            }
            let mut candidates: Vec<_> = snapshot
                .pool
                .iter()
                .filter(|i| i.accepts_binding(scope))
                .map(|i| i.id.clone())
                .collect();
            candidates.sort();
            let chosen = {
                let mut slot = local.cache.lock().expect("local Session cache");
                (slot.epoch == snapshot.epoch)
                    .then(|| slot.cursor.choose(&candidates).map(str::to_owned))
            };
            let Some(candidate) = chosen else {
                continue;
            };
            if let Some(candidate) = candidate {
                let instance = if let Some(key) = &request.affinity_key {
                    let binding = self
                        .state
                        .bind_in_session(scope, &snapshot.session.generation, key, &candidate)
                        .await?;
                    self.state
                        .instance(&scope.tenant, &binding.instance_id)
                        .await?
                        .ok_or_else(|| Error::NotReady("selected instance missing".into()))?
                } else {
                    snapshot
                        .pool
                        .iter()
                        .find(|i| i.id == candidate)
                        .expect("cached candidate")
                        .clone()
                };
                let target = Self::target(&instance, scope, &snapshot.session.generation)?;
                if let Some(key) = &request.affinity_key {
                    if !Self::cache_affinity(&local, &snapshot, key, instance) {
                        continue;
                    }
                }
                return Ok(target);
            }
            if snapshot.pool.is_empty() {
                if waited_for_creation {
                    return Err(Error::NotReady(
                        "instance creation ended without a Ready instance".into(),
                    ));
                }
                waited_for_creation = true;
                match self
                    .state
                    .reserve_cold_start_in_session(
                        scope,
                        &snapshot.session.generation,
                        &cold_id,
                        &format!("adx-{cold_id}"),
                        deadline_ms,
                    )
                    .await?
                {
                    Reservation::Created(id) | Reservation::Existing(id) => {
                        if let Some(instance) = self.state.instance(&scope.tenant, &id).await? {
                            self.reconcile(&instance).await?;
                        }
                    }
                    Reservation::PoolExists(_) => (),
                }
            } else {
                let creating: Vec<_> = snapshot
                    .pool
                    .iter()
                    .filter(|i| i.phase == InstancePhase::Creating)
                    .cloned()
                    .collect();
                if creating.is_empty() {
                    return Err(Error::NotReady(
                        "no Ready or Creating instance; existing logical instances are unavailable"
                            .into(),
                    ));
                }
                waited_for_creation = true;
                for instance in creating {
                    self.reconcile(&instance).await?;
                }
            }
            let snapshot = self.refresh_local(scope, &local, false).await?;
            if waited_for_creation && snapshot.pool.is_empty() {
                return Err(Error::NotReady(
                    "instance creation ended without a Ready instance".into(),
                ));
            }
            if snapshot
                .pool
                .iter()
                .any(|instance| instance.accepts_binding(scope))
            {
                continue;
            }
            let remaining = snapshot
                .pool
                .iter()
                .filter(|instance| instance.phase == InstancePhase::Creating)
                .map(|instance| {
                    Duration::from_millis(
                        instance
                            .create_deadline_ms
                            .saturating_sub(adx_agent_core::unix_time_millis()),
                    )
                })
                .min()
                .unwrap_or(poll_delay);
            tokio::time::sleep(poll_delay.min(remaining)).await;
            poll_delay = poll_delay
                .saturating_mul(2)
                .min(self.config.max_poll_interval);
        }
    }
    pub async fn release(&self, scope: &Scope) -> Result<()> {
        self.invalidate(scope);
        let session = self.state.release_session(scope).await?;
        for id in &session.instances {
            let instance = self.state.begin_delete(&scope.tenant, id).await?;
            self.reconcile(&instance).await?;
        }
        self.state
            .finish_session_delete(scope, &session.generation)
            .await?;
        Ok(())
    }
    pub async fn release_instance(&self, scope: &Scope, id: &str) -> Result<()> {
        self.invalidate(scope);
        let instance = self
            .state
            .instance(&scope.tenant, id)
            .await?
            .ok_or_else(|| Error::Invalid("instance not found".into()))?;
        let session = self
            .state
            .session(scope)
            .await?
            .ok_or_else(|| Error::Invalid("Session not found".into()))?;
        if instance.scope != *scope || instance.session_generation != session.generation {
            return Err(Error::Conflict(
                "instance does not belong to Session".into(),
            ));
        }
        let instance = self.state.begin_delete(&scope.tenant, id).await?;
        self.reconcile(&instance).await
    }
    fn validate_observation(instance: &Instance, observed: &SandboxObservation) -> Result<()> {
        if observed.id != instance.sandbox_id || observed.tenant != instance.tenant {
            return Err(Error::Unavailable(
                "Sandbox response identity mismatch".into(),
            ));
        }
        Ok(())
    }
    async fn observe_creating(&self, instance: &Instance) -> Result<SandboxObservation> {
        let observed = self
            .sandbox
            .get(&instance.tenant, &instance.sandbox_id)
            .await?;
        let observation = match observed {
            Some(observed) => observed,
            None if instance.phase == InstancePhase::Creating => {
                let template = self
                    .state
                    .template(
                        &instance.scope.tenant,
                        &instance.scope.template,
                        &instance.scope.version,
                    )
                    .await?
                    .ok_or_else(|| Error::Invalid("instance template version missing".into()))?;
                match self
                    .sandbox
                    .create(&CreateSandbox {
                        id: instance.sandbox_id.clone(),
                        tenant: instance.tenant.clone(),
                        execution: (&template).into(),
                    })
                    .await
                {
                    Ok(observation) => observation,
                    Err(error @ (SandboxError::Invalid(_) | SandboxError::Unsupported(_))) => {
                        self.state
                            .mark_failed(&instance.tenant, &instance.id, error.to_string())
                            .await?;
                        return Err(error.into());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            None => {
                return Err(Error::NotReady(
                    "Sandbox is absent; only Platform may recover the execution".into(),
                ))
            }
        };
        Ok(observation)
    }

    async fn delete_pending(&self, instance: &Instance) -> Result<()> {
        self.invalidate(&instance.scope);
        let observation = self
            .sandbox
            .delete(&instance.tenant, &instance.sandbox_id)
            .await?;
        Self::validate_observation(instance, &observation)?;
        if observation.phase == SandboxPhase::Deleted {
            self.state
                .confirm_deleted(&instance.tenant, &instance.id)
                .await?;
        }
        Ok(())
    }

    pub async fn reconcile(&self, hint: &Instance) -> Result<()> {
        let lock = self.work_lock(hint);
        let _guard = lock.lock().await;
        let _budget = self
            .sandbox_budget
            .acquire()
            .await
            .map_err(|_| Error::Unavailable("Sandbox budget closed".into()))?;
        let Some(mut instance) = self.state.instance(&hint.tenant, &hint.id).await? else {
            return Ok(());
        };
        let scope = instance.scope.clone();
        let session_generation = instance.session_generation.clone();
        if instance.phase == InstancePhase::Deleted {
            return Ok(());
        }
        if self.state.session(&scope).await?.is_none_or(|session| {
            session.phase != SessionPhase::Active || session.generation != session_generation
        }) {
            instance = self
                .state
                .begin_delete(&instance.tenant, &instance.id)
                .await?;
        }
        if instance.creation_expired() {
            instance = self
                .state
                .expire_creation(&instance.tenant, &instance.id)
                .await?;
        }
        if instance.desired == DesiredState::Deleted {
            return self.delete_pending(&instance).await;
        }
        // Ready/Failed are not health-monitoring work. The Session cleanup
        // above still converts stable instances to pending deletion when necessary.
        if instance.phase != InstancePhase::Creating {
            return Ok(());
        }
        if !self.creation_check_due(&instance) {
            return Ok(());
        }
        let remaining = Duration::from_millis(
            instance
                .create_deadline_ms
                .saturating_sub(adx_agent_core::unix_time_millis()),
        );
        let observed = tokio::time::timeout(remaining, self.observe_creating(&instance)).await;
        if instance.creation_expired() || observed.is_err() {
            let expired = self
                .state
                .expire_creation(&instance.tenant, &instance.id)
                .await?;
            if expired.desired == DesiredState::Deleted {
                return self.delete_pending(&expired).await;
            }
            return Ok(()); // Another worker completed creation before the deadline.
        }
        let observation = observed.expect("checked creation deadline")?;
        Self::validate_observation(&instance, &observation)?;
        // Re-read desired state after a potentially slow create, closing create/kill overlap.
        let latest = self
            .state
            .instance(&instance.tenant, &instance.id)
            .await?
            .ok_or_else(|| Error::Unavailable("instance state disappeared".into()))?;
        let session_active =
            self.state.session(&scope).await?.is_some_and(|c| {
                c.phase == SessionPhase::Active && c.generation == session_generation
            });
        if latest.desired == DesiredState::Deleted || !session_active {
            self.state
                .begin_delete(&instance.tenant, &instance.id)
                .await?;
            let deleted = self
                .sandbox
                .delete(&instance.tenant, &instance.sandbox_id)
                .await?;
            Self::validate_observation(&instance, &deleted)?;
            if deleted.phase == SandboxPhase::Deleted {
                self.state
                    .confirm_deleted(&instance.tenant, &instance.id)
                    .await?;
            }
            return Ok(());
        }
        if latest.phase != InstancePhase::Creating {
            return Ok(());
        }
        match observation.phase {
            SandboxPhase::Running
                if observation.ready && latest.phase == InstancePhase::Creating =>
            {
                self.state
                    .mark_ready(&instance.tenant, &instance.id)
                    .await?;
            }
            SandboxPhase::Deleted => {
                self.state
                    .begin_delete(&instance.tenant, &instance.id)
                    .await?;
                self.state
                    .confirm_deleted(&instance.tenant, &instance.id)
                    .await?;
                self.invalidate(&scope);
            }
            SandboxPhase::Failed => {
                self.state
                    .mark_failed(
                        &instance.tenant,
                        &instance.id,
                        observation
                            .message
                            .unwrap_or_else(|| "Sandbox execution unavailable".into()),
                    )
                    .await?;
                self.invalidate(&scope);
            }
            _ => (),
        }
        Ok(())
    }
    pub async fn recover_sessions_page(&self, cursor: u64) -> Result<u64> {
        let (next, sessions) = self
            .state
            .scan_sessions(cursor, limits::SCAN_PAGE_COUNT)
            .await?;
        for session in sessions
            .into_iter()
            .filter(|s| s.phase == SessionPhase::Deleting)
        {
            self.invalidate(&session.scope);
            for id in &session.instances {
                if let Some(instance) = self.state.instance(&session.scope.tenant, id).await? {
                    if let Err(error) = self.reconcile(&instance).await {
                        eprintln!("session cleanup pending: {error}");
                    }
                }
            }
            self.state
                .finish_session_delete(&session.scope, &session.generation)
                .await?;
        }
        Ok(next)
    }
    pub async fn recover_page(&self, cursor: u64) -> Result<u64> {
        let (next, instances) = self
            .state
            .scan_instances(cursor, limits::SCAN_PAGE_COUNT)
            .await?;
        // One failed backend must not prevent unrelated current states from being reconciled.
        for instance in instances.into_iter().filter(|i| i.needs_reconciliation()) {
            if let Err(error) = self.reconcile(&instance).await {
                eprintln!(
                    "Dispatcher recovery tenant={} instance={} error={error}",
                    instance.tenant, instance.id
                );
            }
        }
        Ok(next)
    }
}
