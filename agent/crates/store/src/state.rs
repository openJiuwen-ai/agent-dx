//! Product transactions on top of Repository, shared by Redis and test fixtures.
use crate::*;
use adx_agent_core::{
    AffinityBinding, DesiredState, Instance, InstancePhase, Scope, Session, SessionPhase,
    TemplateVersion,
};
use std::sync::Arc;

use adx_agent_core::limits;
const CONFLICT_BUDGET: usize = limits::CAS_ATTEMPTS;

#[derive(Clone)]
pub struct AgentState {
    store: Arc<dyn Repository>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reservation {
    Created(String),
    /// The requested stable identity was already reserved by an earlier attempt.
    Existing(String),
    PoolExists(Vec<String>),
}

fn session_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new(
        "session",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.session_id,
        ],
    )
}
fn instance_key(tenant: &str, id: &str) -> Result<Key> {
    Key::new("instance", &[tenant, id])
}
fn affinity_key(scope: &Scope, generation: &str, affinity: &str) -> Result<Key> {
    Key::new(
        "affinity",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.session_id,
            generation,
            affinity,
        ],
    )
}
fn check(key: &Key, record: &Record) -> Check {
    Check {
        key: key.clone(),
        expected: Some(record.revision.clone()),
    }
}
fn put<T: Serialize>(key: &Key, value: &T) -> Result<Put> {
    Ok(Put {
        key: key.clone(),
        record: Record::new(value)?,
    })
}
fn conflict(message: &str) -> Error {
    Error::Conflict(message.into())
}

impl AgentState {
    pub fn new(store: Arc<dyn Repository>) -> Self {
        Self { store }
    }

    pub async fn affinity(&self, scope: &Scope, affinity: &str) -> Result<Option<AffinityBinding>> {
        let Some(session) = self.session(scope).await? else {
            return Ok(None);
        };
        self.affinity_in_session(scope, &session.generation, affinity)
            .await
    }
    pub async fn affinity_in_session(
        &self,
        scope: &Scope,
        generation: &str,
        affinity: &str,
    ) -> Result<Option<AffinityBinding>> {
        scope.validate().map_err(Error::Invalid)?;
        self.store
            .get(&affinity_key(scope, generation, affinity)?)
            .await?
            .map(|record| record.decode())
            .transpose()
    }
    pub async fn scan_sessions(&self, cursor: u64, count: u32) -> Result<(u64, Vec<Session>)> {
        let page = self.store.scan("session", cursor, count).await?;
        let mut sessions = Vec::new();
        for key in page.keys {
            if let Some(record) = self.store.get(&key).await? {
                sessions.push(record.decode()?);
            }
        }
        Ok((page.cursor, sessions))
    }

    pub async fn scan_instances(&self, cursor: u64, count: u32) -> Result<(u64, Vec<Instance>)> {
        let page = self.store.scan("instance", cursor, count).await?;
        let mut instances = Vec::new();
        for key in page.keys {
            if let Some(record) = self.store.get(&key).await? {
                instances.push(record.decode()?);
            }
        }
        Ok((page.cursor, instances))
    }

    pub async fn pool(&self, scope: &Scope) -> Result<Vec<Instance>> {
        let session = self
            .session(scope)
            .await?
            .ok_or_else(|| conflict("session missing"))?;
        if session.phase != SessionPhase::Active {
            return Err(conflict("session is releasing"));
        }
        let mut instances = Vec::new();
        for id in session.instances {
            if let Some(instance) = self.instance(&scope.tenant, &id).await? {
                instances.push(instance);
            }
        }
        Ok(instances)
    }

    pub async fn mark_failed(&self, tenant: &str, id: &str, message: String) -> Result<()> {
        let key = instance_key(tenant, id)?;
        for _ in 0..CONFLICT_BUDGET {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| conflict("instance missing"))?;
            let mut instance: Instance = record.decode()?;
            if instance.desired != DesiredState::Running {
                return Err(conflict("instance is deleting"));
            }
            // Only unfinished creation can fail here. Runtime health after Ready is
            // owned by Substrate; concurrent startup observations cannot regress it.
            if instance.phase != InstancePhase::Creating {
                return Ok(());
            }
            instance.phase = InstancePhase::Failed;
            instance.status_message = Some(message.clone());
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &instance)?],
                )?)
                .await?
            {
                return Ok(());
            }
        }
        Err(conflict("failure update contention"))
    }

    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        template.validate().map_err(Error::Invalid)?;
        let key = Key::new("template", &[tenant, &template.name, &template.version])?;
        let tx = Transaction::new(
            vec![Check {
                key: key.clone(),
                expected: None,
            }],
            vec![put(&key, template)?],
        )?;
        if self.store.commit(&tx).await? {
            return Ok(());
        }
        let existing = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| conflict("template changed"))?;
        if existing.decode::<TemplateVersion>()? == *template {
            Ok(())
        } else {
            Err(conflict("template versions are immutable"))
        }
    }

    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<TemplateVersion>> {
        self.store
            .get(&Key::new("template", &[tenant, name, version])?)
            .await?
            .map(|record| record.decode())
            .transpose()
    }

    pub async fn create_session(&self, scope: Scope) -> Result<()> {
        let key = session_key(&scope)?;
        if self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?
            .is_none()
        {
            return Err(Error::Invalid("template version does not exist".into()));
        }
        let value = Session {
            scope,
            generation: uuid::Uuid::new_v4().to_string(),
            phase: SessionPhase::Active,
            instances: BTreeSet::new(),
        };
        let tx = Transaction::new(
            vec![Check {
                key: key.clone(),
                expected: None,
            }],
            vec![put(&key, &value)?],
        )?;
        if self.store.commit(&tx).await? {
            return Ok(());
        }
        let existing: Session = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| conflict("session missing"))?
            .decode()?;
        if existing.phase == SessionPhase::Active {
            Ok(())
        } else {
            Err(conflict("session is deleting"))
        }
    }

    pub async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Instance>> {
        self.store
            .get(&instance_key(tenant, id)?)
            .await?
            .map(|record| record.decode())
            .transpose()
    }

    pub async fn session(&self, scope: &Scope) -> Result<Option<Session>> {
        self.store
            .get(&session_key(scope)?)
            .await?
            .map(|record| record.decode())
            .transpose()
    }

    /// Fences new reservations and bindings before workers start deleting individual sandboxes.
    /// The current Session/Instance records are sufficient to resume interrupted release.
    pub async fn release_session(&self, scope: &Scope) -> Result<Session> {
        let key = session_key(scope)?;
        let generation = self
            .session(scope)
            .await?
            .ok_or_else(|| conflict("session missing"))?
            .generation;
        for _ in 0..CONFLICT_BUDGET {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| conflict("session missing"))?;
            let mut session: Session = record.decode()?;
            if session.generation != generation {
                return Err(conflict("session lifecycle changed during deletion"));
            }
            if session.phase != SessionPhase::Active {
                return Ok(session);
            }
            session.phase = SessionPhase::Deleting;
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &session)?],
                )?)
                .await?
            {
                return Ok(session);
            }
        }
        Err(conflict("session release contention"))
    }

    pub async fn begin_delete(&self, tenant: &str, id: &str) -> Result<Instance> {
        self.begin_delete_inner(tenant, id, false).await
    }
    /// Atomically expire only unfinished creation; a concurrent Ready must not be deleted.
    pub async fn expire_creation(&self, tenant: &str, id: &str) -> Result<Instance> {
        self.begin_delete_inner(tenant, id, true).await
    }
    async fn begin_delete_inner(
        &self,
        tenant: &str,
        id: &str,
        only_expired: bool,
    ) -> Result<Instance> {
        let key = instance_key(tenant, id)?;
        for _ in 0..CONFLICT_BUDGET {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| conflict("instance missing"))?;
            let mut instance: Instance = record.decode()?;
            if instance.desired == DesiredState::Deleted {
                return Ok(instance);
            }
            if only_expired {
                if !instance.creation_expired() {
                    return Ok(instance);
                }
                instance.status_message = Some(limits::CREATE_TIMEOUT_MESSAGE.into());
            }
            instance.desired = DesiredState::Deleted;
            instance.phase = InstancePhase::Deleting;
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &instance)?],
                )?)
                .await?
            {
                return Ok(instance);
            }
        }
        Err(conflict("instance delete contention"))
    }

    /// Only call after Platform confirms deletion. Tombstones are deliberately retained.
    pub async fn confirm_deleted(&self, tenant: &str, id: &str) -> Result<()> {
        let key = instance_key(tenant, id)?;
        for _ in 0..CONFLICT_BUDGET {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| conflict("instance missing"))?;
            let mut instance: Instance = record.decode()?;
            if instance.desired != DesiredState::Deleted {
                return Err(conflict("deletion was not requested"));
            }
            if instance.phase == InstancePhase::Deleted {
                return Ok(());
            }
            let mut checks = vec![check(&key, &record)];
            let mut puts = Vec::new();
            {
                let scope = &instance.scope;
                let session_generation = &instance.session_generation;
                let ck = session_key(scope)?;
                if let Some(cr) = self.store.get(&ck).await? {
                    let mut session: Session = cr.decode()?;
                    if session.generation == *session_generation {
                        session.instances.remove(id);
                        checks.push(check(&ck, &cr));
                        puts.push(put(&ck, &session)?);
                    }
                }
            }
            instance.phase = InstancePhase::Deleted;
            puts.push(put(&key, &instance)?);
            if self.store.commit(&Transaction::new(checks, puts)?).await? {
                return Ok(());
            }
        }
        Err(conflict("deletion confirmation contention"))
    }

    /// Atomically claim an empty pool for request-triggered cold start.
    /// Reuse an existing pool; this is not a general instance-count or preallocation policy.
    /// Caller supplies stable IDs once, including across timeouts.
    pub async fn reserve_cold_start(
        &self,
        scope: &Scope,
        id: &str,
        sandbox_id: &str,
    ) -> Result<Reservation> {
        let session = self
            .session(scope)
            .await?
            .ok_or_else(|| conflict("session missing"))?;
        self.reserve_cold_start_in_session(
            scope,
            &session.generation,
            id,
            sandbox_id,
            adx_agent_core::unix_time_millis() + limits::CREATE_TIMEOUT.as_millis() as u64,
        )
        .await
    }
    pub async fn reserve_cold_start_in_session(
        &self,
        scope: &Scope,
        generation: &str,
        id: &str,
        sandbox_id: &str,
        create_deadline_ms: u64,
    ) -> Result<Reservation> {
        if create_deadline_ms == 0 {
            return Err(Error::Invalid("invalid creation deadline".into()));
        }
        let ck = session_key(scope)?;
        let ik = instance_key(&scope.tenant, id)?;
        let instance = Instance {
            id: id.into(),
            tenant: scope.tenant.clone(),
            scope: scope.clone(),
            session_generation: generation.into(),
            sandbox_id: sandbox_id.into(),
            desired: DesiredState::Running,
            phase: InstancePhase::Creating,
            status_message: None,
            create_deadline_ms,
        };
        instance.validate().map_err(Error::Invalid)?;
        for _ in 0..CONFLICT_BUDGET {
            let cr = self
                .store
                .get(&ck)
                .await?
                .ok_or_else(|| conflict("session missing"))?;
            let mut session: Session = cr.decode()?;
            if session.phase != SessionPhase::Active || session.generation != generation {
                return Err(conflict("session is releasing"));
            }
            if let Some(existing) = self.store.get(&ik).await? {
                let old: Instance = existing.decode()?;
                if old.scope != instance.scope
                    || old.session_generation != instance.session_generation
                    || old.sandbox_id != instance.sandbox_id
                    || !session.instances.contains(id)
                    || old.desired != DesiredState::Running
                {
                    return Err(conflict("instance identity already used or released"));
                }
                return Ok(Reservation::Existing(id.into()));
            }
            if !session.instances.is_empty() {
                return Ok(Reservation::PoolExists(
                    session.instances.into_iter().collect(),
                ));
            }
            session.instances.insert(id.into());
            let tx = Transaction::new(
                vec![
                    check(&ck, &cr),
                    Check {
                        key: ik.clone(),
                        expected: None,
                    },
                ],
                vec![put(&ck, &session)?, put(&ik, &instance)?],
            )?;
            if self.store.commit(&tx).await? {
                return Ok(Reservation::Created(id.into()));
            }
        }
        Err(conflict("cold-start reservation contention"))
    }

    /// Caller confirms readiness via Sandbox/RRT before committing this transition.
    pub async fn mark_ready(&self, tenant: &str, id: &str) -> Result<()> {
        let key = instance_key(tenant, id)?;
        for _ in 0..CONFLICT_BUDGET {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| conflict("instance missing"))?;
            let mut instance: Instance = record.decode()?;
            if instance.desired != DesiredState::Running {
                return Err(conflict("instance is deleting"));
            }
            if instance.phase == InstancePhase::Ready {
                return Ok(());
            }
            if instance.phase != InstancePhase::Creating {
                return Err(conflict("invalid instance phase transition"));
            }
            let mut checks = vec![check(&key, &record)];
            {
                let scope = &instance.scope;
                let session_generation = &instance.session_generation;
                let ck = session_key(scope)?;
                let cr = self
                    .store
                    .get(&ck)
                    .await?
                    .ok_or_else(|| conflict("session missing"))?;
                let session: Session = cr.decode()?;
                if session.phase != SessionPhase::Active
                    || session.generation != *session_generation
                {
                    return Err(conflict("session is releasing"));
                }
                checks.push(check(&ck, &cr));
            }
            let expired = instance.creation_expired();
            if expired {
                instance.desired = DesiredState::Deleted;
                instance.phase = InstancePhase::Deleting;
                instance.status_message = Some(limits::CREATE_TIMEOUT_MESSAGE.into());
            } else {
                instance.phase = InstancePhase::Ready;
                instance.status_message = None;
            }
            if self
                .store
                .commit(&Transaction::new(checks, vec![put(&key, &instance)?])?)
                .await?
            {
                return if expired {
                    Err(conflict(limits::CREATE_TIMEOUT_MESSAGE))
                } else {
                    Ok(())
                };
            }
        }
        Err(conflict("instance transition contention"))
    }

    /// Bind once per logical instance lifetime. Failed/restarting instances retain their binding.
    pub async fn bind(
        &self,
        scope: &Scope,
        affinity: &str,
        candidate: &str,
    ) -> Result<AffinityBinding> {
        let session = self
            .session(scope)
            .await?
            .ok_or_else(|| conflict("session missing"))?;
        self.bind_in_session(scope, &session.generation, affinity, candidate)
            .await
    }
    pub async fn bind_in_session(
        &self,
        scope: &Scope,
        generation: &str,
        affinity: &str,
        candidate: &str,
    ) -> Result<AffinityBinding> {
        let ck = session_key(scope)?;
        let ak = affinity_key(scope, generation, affinity)?;
        for _ in 0..CONFLICT_BUDGET {
            let cr = self
                .store
                .get(&ck)
                .await?
                .ok_or_else(|| conflict("session missing"))?;
            let session: Session = cr.decode()?;
            if session.phase != SessionPhase::Active || session.generation != generation {
                return Err(conflict("session deleted or lifecycle changed"));
            }
            let existing = self.store.get(&ak).await?;
            let mut checks = vec![
                check(&ck, &cr),
                Check {
                    key: ak.clone(),
                    expected: existing.as_ref().map(|r| r.revision.clone()),
                },
            ];
            if let Some(record) = &existing {
                let binding: AffinityBinding = record.decode()?;
                if binding.scope != *scope
                    || binding.session_generation != generation
                    || binding.affinity_key != affinity
                {
                    return Err(Error::Corrupt("affinity identity mismatch".into()));
                }
                let old_key = instance_key(&scope.tenant, &binding.instance_id)?;
                let old_record = self.store.get(&old_key).await?;
                let old = old_record
                    .as_ref()
                    .map(|r| r.decode::<Instance>())
                    .transpose()?;
                match old {
                    Some(ref instance) if instance.phase != InstancePhase::Deleted => {
                        if !session.instances.contains(&instance.id)
                            || (instance.scope != *scope
                                || instance.session_generation != generation)
                        {
                            return Err(Error::Corrupt("bound instance ownership mismatch".into()));
                        }
                        if instance.desired != DesiredState::Running
                            || instance.phase != InstancePhase::Ready
                        {
                            return Err(conflict(
                                "bound logical instance is temporarily unavailable",
                            ));
                        }
                        return Ok(binding);
                    }
                    Some(_) => {
                        checks.push(check(&old_key, old_record.as_ref().expect("present")));
                    }
                    None => {
                        return Err(Error::Unavailable(
                            "bound instance state missing; termination is not confirmed".into(),
                        ))
                    }
                }
            }
            if !session.instances.contains(candidate) {
                return Err(conflict("session does not admit candidate"));
            }
            let ik = instance_key(&scope.tenant, candidate)?;
            let ir = self
                .store
                .get(&ik)
                .await?
                .ok_or_else(|| conflict("candidate missing"))?;
            let instance: Instance = ir.decode()?;
            if !instance.accepts_binding(scope) || instance.session_generation != generation {
                return Err(conflict("candidate is not ready in this lifecycle"));
            }
            checks.push(check(&ik, &ir));
            let binding = AffinityBinding {
                scope: scope.clone(),
                session_generation: generation.into(),
                affinity_key: affinity.into(),
                instance_id: candidate.into(),
            };
            if self
                .store
                .commit(&Transaction::new(checks, vec![put(&ak, &binding)?])?)
                .await?
            {
                return Ok(binding);
            }
        }
        Err(conflict("affinity binding contention"))
    }

    /// Deletion remains recoverable until all children are gone. No permanent public-ID tombstone.
    pub async fn finish_session_delete(&self, scope: &Scope, generation: &str) -> Result<bool> {
        let ck = session_key(scope)?;
        let Some(cr) = self.store.get(&ck).await? else {
            return Ok(true);
        };
        let session: Session = cr.decode()?;
        if session.generation != generation {
            return Ok(true);
        }
        if session.phase != SessionPhase::Deleting || !session.instances.is_empty() {
            return Ok(false);
        }
        // SCAN can duplicate keys. Deletes are conditional and idempotent. Rescan after mutation,
        // also supporting offset-based test repositories without skipping shifted entries.
        loop {
            let mut cursor = 0;
            let mut removed = false;
            loop {
                let page = self.store.scan("affinity", cursor, 100).await?;
                for key in page.keys {
                    let Some(record) = self.store.get(&key).await? else {
                        continue;
                    };
                    let binding: AffinityBinding = record.decode()?;
                    if binding.scope == *scope && binding.session_generation == generation {
                        if self
                            .store
                            .commit(&Transaction::with_deletes(
                                vec![check(&ck, &cr), check(&key, &record)],
                                vec![],
                                vec![key],
                            )?)
                            .await?
                        {
                            removed = true;
                        } else {
                            return Ok(false);
                        }
                    }
                }
                cursor = page.cursor;
                if cursor == 0 {
                    break;
                }
            }
            if !removed {
                break;
            }
        }
        self.store
            .commit(&Transaction::with_deletes(
                vec![check(&ck, &cr)],
                vec![],
                vec![ck],
            )?)
            .await
    }
}
