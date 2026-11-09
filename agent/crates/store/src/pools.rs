//! Prewarmed pool records. Every ownership transfer is decided by one revision CAS;
//! an unclaimed slot never returns to Ready after a claim was granted.
use crate::{
    state::{binding_key, check, put},
    *,
};
use adx_agent_core::{
    limits::CAS_ATTEMPTS, pool::*, AgentBinding, BindingPhase, EnvironmentRef, Scope,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const SLOT_ATTEMPTS: usize = 32;

fn pool_key(tenant: &str, pool_id: &str) -> Result<Key> {
    for value in [tenant, pool_id] {
        adx_agent_core::identifier(value, "pool scope").map_err(Error::Invalid)?;
    }
    Key::new("pool", &[tenant, pool_id])
}
fn slot_key(tenant: &str, pool_id: &str, slot_id: &str) -> Result<Key> {
    for value in [tenant, pool_id, slot_id] {
        adx_agent_core::identifier(value, "pool slot").map_err(Error::Invalid)?;
    }
    Key::new("pool-slot", &[tenant, pool_id, slot_id])
}
fn claim_key(tenant: &str, pool_id: &str, claim_id: &str) -> Result<Key> {
    for value in [tenant, pool_id, claim_id] {
        adx_agent_core::identifier(value, "pool claim").map_err(Error::Invalid)?;
    }
    Ok(Key(
        format!(
            "pool-claim:{}",
            adx_agent_core::encode_key(&[tenant, claim_id])
        ),
        vec![Index::pool_claims(tenant, pool_id)?],
    ))
}
fn retired_key(tenant: &str, pool_id: &str, slot_id: &str) -> Result<Key> {
    for value in [tenant, pool_id, slot_id] {
        adx_agent_core::identifier(value, "retired pool slot").map_err(Error::Invalid)?;
    }
    Key::new("pool-retired", &[tenant, pool_id, slot_id])
}
fn leader_key() -> Key {
    Key::new("pool-leader", &["v1"]).expect("fixed leader key")
}

#[derive(Serialize, Deserialize)]
struct Leader {
    holder: String,
    expires_at_unix_ms: u64,
}

fn new_slot_key() -> String {
    let bytes = uuid::Uuid::new_v4();
    format!("sk-{}", URL_SAFE_NO_PAD.encode(bytes.as_bytes()))
}
fn digest(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}
fn slot_id() -> String {
    let bytes = uuid::Uuid::new_v4();
    format!("s-{bytes}")
}
fn deadline(seconds: u64, now: u64) -> Result<u64> {
    now.checked_add(seconds.saturating_mul(1000))
        .ok_or_else(|| Error::Invalid("pool deadline overflow".into()))
}

#[derive(Default)]
struct Counts {
    key_pending: u64,
    provisioning: u64,
    ready_running: u64,
    ready_paused: u64,
    claiming: u64,
    quarantined: u64,
    retiring: u64,
}
impl Counts {
    fn add(&mut self, slot: &PoolSlot) {
        match slot.state {
            SlotState::KeyPending => self.key_pending += 1,
            SlotState::Provisioning => self.provisioning += 1,
            SlotState::Ready => match slot.phase {
                Some(SlotPhase::Paused) => self.ready_paused += 1,
                _ => self.ready_running += 1,
            },
            SlotState::Claiming => self.claiming += 1,
            SlotState::Quarantined => self.quarantined += 1,
            SlotState::Retiring => self.retiring += 1,
        }
    }
    fn active(&self) -> u64 {
        self.key_pending
            + self.provisioning
            + self.ready_running
            + self.ready_paused
            + self.claiming
    }
}

impl AgentState {
    async fn commit_pool_transaction(
        &self,
        tenant: &str,
        pool_id: &str,
        transaction: &Transaction,
    ) -> Result<bool> {
        let key = pool_key(tenant, pool_id)?;
        let record = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("pool disappeared".into()))?;
        let mut checks = transaction.checks.clone();
        if let Some(expected) = checks.iter().find(|check| check.key == key) {
            if expected.expected.as_ref() != Some(&record.revision) {
                return Ok(false);
            }
        } else {
            checks.push(check(&key, &record));
        }
        let mut puts = transaction.puts.clone();
        puts.push(put(&key, &record.decode::<PoolConfig>()?)?);
        self.store
            .commit(&Transaction::with_deletes(
                checks,
                puts,
                transaction.deletes.clone(),
            )?)
            .await
    }

    pub async fn put_pool(
        &self,
        config: &PoolConfig,
        expected: Option<&Revision>,
    ) -> Result<Revision> {
        config.validate().map_err(Error::Invalid)?;
        let key = pool_key(&config.tenant, &config.pool_id)?;
        let value = put(&key, config)?;
        let revision = value.record.revision.clone();
        if !self
            .store
            .commit(&Transaction::new(
                vec![Check {
                    key,
                    expected: expected.cloned(),
                }],
                vec![value],
            )?)
            .await?
        {
            return Err(Error::Conflict(
                "pool configuration revision changed".into(),
            ));
        }
        Ok(revision)
    }

    pub async fn pool(
        &self,
        tenant: &str,
        pool_id: &str,
    ) -> Result<Option<(Revision, PoolConfig)>> {
        let Some(record) = self.store.get(&pool_key(tenant, pool_id)?).await? else {
            return Ok(None);
        };
        let config: PoolConfig = record.decode()?;
        config.validate().map_err(Error::Corrupt)?;
        if config.tenant != tenant || config.pool_id != pool_id {
            return Err(Error::Corrupt("pool identity mismatch".into()));
        }
        Ok(Some((record.revision, config)))
    }

    pub async fn pools(&self) -> Result<Vec<(Revision, PoolConfig)>> {
        let mut result = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .store
                .page(&Index::pools(), after.as_deref(), 128)
                .await?;
            if page.is_empty() {
                return Ok(result);
            }
            for (key, record) in page {
                after = Some(key);
                let config: PoolConfig = record.decode()?;
                config.validate().map_err(Error::Corrupt)?;
                result.push((record.revision, config));
            }
        }
    }

    async fn slots(&self, tenant: &str, pool_id: &str) -> Result<Vec<(Key, Record, PoolSlot)>> {
        let mut result = Vec::new();
        let mut after = None;
        let index = Index::pool_slots(tenant, pool_id)?;
        loop {
            let page = self.store.page(&index, after.as_deref(), 128).await?;
            if page.is_empty() {
                return Ok(result);
            }
            for (key, record) in page {
                let slot: PoolSlot = record.decode()?;
                if slot_key(tenant, pool_id, &slot.slot_id)?.as_str() != key {
                    return Err(Error::Corrupt("pool slot index identity mismatch".into()));
                }
                after = Some(key);
                result.push((slot_key(tenant, pool_id, &slot.slot_id)?, record, slot));
            }
        }
    }

    pub async fn pool_slots(&self, tenant: &str, pool_id: &str) -> Result<Vec<PoolSlot>> {
        Ok(self
            .slots(tenant, pool_id)
            .await?
            .into_iter()
            .map(|(_, _, slot)| slot)
            .collect())
    }

    pub async fn pool_slot(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
    ) -> Result<Option<(Revision, PoolSlot)>> {
        let Some(record) = self.store.get(&slot_key(tenant, pool_id, slot_id)?).await? else {
            return Ok(None);
        };
        let slot: PoolSlot = record.decode()?;
        if slot.slot_id != slot_id {
            return Err(Error::Corrupt("pool slot identity mismatch".into()));
        }
        Ok(Some((record.revision, slot)))
    }

    /// Create one KeyPending slot inside the caller's deficit. The key is persisted with
    /// the slot before any LiteLLM request can observe it.
    pub async fn reserve_pool_slot(
        &self,
        tenant: &str,
        pool_id: &str,
        model_api_base: &str,
        now: u64,
    ) -> Result<Option<PoolSlot>> {
        adx_agent_core::identifier(model_api_base, "model API base").map_err(Error::Invalid)?;
        for _ in 0..SLOT_ATTEMPTS {
            let Some((pool_revision, config)) = self.pool(tenant, pool_id).await? else {
                return Err(Error::Invalid("pool does not exist".into()));
            };
            if !config.enabled {
                return Ok(None);
            }
            let slots = self.slots(tenant, pool_id).await?;
            let mut counts = Counts::default();
            for (_, _, slot) in &slots {
                counts.add(slot);
            }
            let ready = counts.ready_running + counts.ready_paused;
            let inflight = counts.key_pending + counts.provisioning;
            let deficit = ready + inflight < config.min_ready
                || counts.ready_running + inflight < config.min_running;
            let physical = counts.active() + counts.quarantined + counts.retiring;
            if !deficit
                || physical >= config.max_slots
                || counts.key_pending + counts.provisioning >= config.max_provisioning
            {
                return Ok(None);
            }
            let id = slot_id();
            let environment_id = format!("adx-{}", uuid::Uuid::new_v4());
            let model_api_key = new_slot_key();
            let slot = PoolSlot {
                creation_attempted: false,
                slot_id: id.clone(),
                state: SlotState::KeyPending,
                phase: None,
                version: config.version.clone(),
                environment_id,
                workspace: format!("pool-{id}"),
                model_owner: format!("pool:{id}"),
                key_digest: digest(&model_api_key),
                parameters: BTreeMap::from([
                    ("workspace".into(), format!("pool-{id}")),
                    ("model_api_key".into(), model_api_key),
                    ("model_api_base".into(), model_api_base.into()),
                ]),
                volumes: BTreeMap::new(),
                created_at_unix_ms: now,
                deadline_unix_ms: Some(deadline(config.key_confirm_timeout_seconds, now)?),
                claim_id: None,
                quarantine_reason: None,
            };
            let key = slot_key(tenant, pool_id, &id)?;
            let workspace_key = Key::new("pool-workspace", &[&id])?;
            let Some(pool_record) = self.store.get(&pool_key(tenant, pool_id)?).await? else {
                return Ok(None);
            };
            if pool_record.revision != pool_revision {
                continue;
            }
            if self
                .commit_pool_transaction(
                    tenant,
                    pool_id,
                    &Transaction::new(
                        vec![
                            check(&pool_key(tenant, pool_id)?, &pool_record),
                            Check {
                                key: workspace_key.clone(),
                                expected: None,
                            },
                            Check {
                                key: key.clone(),
                                expected: None,
                            },
                        ],
                        vec![put(&key, &slot)?, put(&workspace_key, &slot.workspace)?],
                    )?,
                )
                .await?
            {
                return Ok(Some(slot));
            }
        }
        Err(Error::Conflict("pool reservation contention".into()))
    }

    pub async fn pending_pool_keys(
        &self,
        tenant: &str,
        pool_id: &str,
        limit: usize,
    ) -> Result<Vec<PendingKey>> {
        let mut result = Vec::new();
        for (_, _, slot) in self.slots(tenant, pool_id).await? {
            if slot.state == SlotState::KeyPending {
                let key = slot
                    .parameters
                    .get("model_api_key")
                    .ok_or_else(|| Error::Corrupt("pending slot has no key".into()))?;
                result.push(PendingKey {
                    slot_id: slot.slot_id,
                    model_owner: slot.model_owner,
                    model_api_key: key.clone(),
                    key_digest: slot.key_digest,
                    deadline_unix_ms: slot.deadline_unix_ms.unwrap_or(u64::MAX),
                });
                if result.len() >= limit {
                    break;
                }
            }
        }
        Ok(result)
    }

    pub async fn confirm_pool_key(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
        key_digest: &str,
        now: u64,
    ) -> Result<SlotView> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
        let (_, config) = self
            .pool(tenant, pool_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool does not exist".into()))?;
        if slot.state != SlotState::KeyPending || slot.key_digest != key_digest {
            return Err(Error::Conflict("pool key is not pending".into()));
        }
        slot.state = SlotState::Provisioning;
        slot.deadline_unix_ms = Some(deadline(config.provision_timeout_seconds, now)?);
        let key = slot_key(tenant, pool_id, slot_id)?;
        if !self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?
        {
            return Err(Error::Conflict(
                "pool slot changed during key confirmation".into(),
            ));
        }
        Ok(SlotView {
            slot_id: slot.slot_id,
            state: slot.state,
            phase: slot.phase,
        })
    }

    pub async fn begin_pool_provisioning(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
    ) -> Result<()> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot disappeared".into()))?;
        if slot.state != SlotState::Provisioning {
            return Err(Error::Conflict(
                "pool slot is no longer provisioning".into(),
            ));
        }
        if slot.creation_attempted {
            return Ok(());
        }
        slot.creation_attempted = true;
        let key = slot_key(tenant, pool_id, slot_id)?;
        if self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict("pool provisioning changed".into()))
        }
    }

    pub async fn fix_pool_slot_volumes(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
        references: &BTreeMap<String, String>,
    ) -> Result<PoolSlot> {
        for _ in 0..CAS_ATTEMPTS {
            let (revision, mut slot) = self
                .pool_slot(tenant, pool_id, slot_id)
                .await?
                .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
            if slot.state != SlotState::Provisioning {
                return Err(Error::Conflict(
                    "pool slot is no longer provisioning".into(),
                ));
            }
            let mut changed = false;
            for (id, uid) in references {
                if slot.volumes.get(id).is_some_and(|fixed| fixed != uid) {
                    return Err(Error::Conflict("fixed pool Volume UID changed".into()));
                }
                if slot.volumes.get(id) != Some(uid) {
                    slot.volumes.insert(id.clone(), uid.clone());
                    changed = true;
                }
            }
            if !changed {
                return Ok(slot);
            }
            let key = slot_key(tenant, pool_id, slot_id)?;
            if self
                .commit_pool_transaction(
                    tenant,
                    pool_id,
                    &Transaction::new(
                        vec![Check {
                            key: key.clone(),
                            expected: Some(revision),
                        }],
                        vec![put(&key, &slot)?],
                    )?,
                )
                .await?
            {
                return Ok(slot);
            }
        }
        Err(Error::Conflict("pool Volume reference contention".into()))
    }

    pub async fn mark_pool_slot_ready(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
        phase: SlotPhase,
    ) -> Result<()> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
        if slot.state != SlotState::Provisioning {
            return Err(Error::Conflict("pool slot is not provisioning".into()));
        }
        slot.state = SlotState::Ready;
        slot.phase = Some(phase);
        slot.deadline_unix_ms = None;
        let key = slot_key(tenant, pool_id, slot_id)?;
        if !self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?
        {
            return Err(Error::Conflict(
                "pool slot changed while becoming ready".into(),
            ));
        }
        Ok(())
    }

    pub async fn observe_pool_slot_phase(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
        phase: SlotPhase,
    ) -> Result<()> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
        if slot.state != SlotState::Ready || slot.phase == Some(phase) {
            return Ok(());
        }
        slot.phase = Some(phase);
        let key = slot_key(tenant, pool_id, slot_id)?;
        let _ = self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?;
        Ok(())
    }

    pub async fn quarantine_pool_slot(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
        reason: &str,
    ) -> Result<()> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
        if slot.state == SlotState::Claiming {
            return Err(Error::Conflict(
                "a Claiming slot must be aborted through its claim".into(),
            ));
        }
        if slot.state == SlotState::Quarantined || slot.state == SlotState::Retiring {
            return Ok(());
        }
        slot.state = SlotState::Quarantined;
        slot.phase = None;
        slot.quarantine_reason = Some(reason.into());
        slot.deadline_unix_ms = None;
        let key = slot_key(tenant, pool_id, slot_id)?;
        let _ = self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?;
        Ok(())
    }

    pub async fn begin_pool_slot_retirement(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
    ) -> Result<()> {
        let (revision, mut slot) = self
            .pool_slot(tenant, pool_id, slot_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool slot does not exist".into()))?;
        if slot.state == SlotState::Claiming {
            return Err(Error::Conflict(
                "a Claiming slot must be aborted before retirement".into(),
            ));
        }
        if slot.state == SlotState::Retiring {
            return Ok(());
        }
        slot.state = SlotState::Retiring;
        slot.phase = None;
        slot.deadline_unix_ms = None;
        let key = slot_key(tenant, pool_id, slot_id)?;
        let _ = self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: Some(revision),
                    }],
                    vec![put(&key, &slot)?],
                )?,
            )
            .await?;
        Ok(())
    }

    pub async fn claim_pool_slot(
        &self,
        tenant: &str,
        pool_id: &str,
        scope: &Scope,
        claim_id: &str,
        now: u64,
    ) -> Result<Option<ClaimOutcome>> {
        scope.validate().map_err(Error::Invalid)?;
        adx_agent_core::identifier(claim_id, "claim_id").map_err(Error::Invalid)?;
        if let Some((_, claim)) = self.pool_claim(tenant, pool_id, claim_id).await? {
            if claim.scope != *scope {
                return Err(Error::Conflict(
                    "claim identity belongs to another request".into(),
                ));
            }
            return self.claim_outcome(tenant, claim).await.map(Some);
        }
        let (pool_revision, config) = self
            .pool(tenant, pool_id)
            .await?
            .ok_or_else(|| Error::Conflict("pool does not exist".into()))?;
        if !config.enabled {
            return Ok(None);
        }
        let (version, alias_check) = self.desired_version(scope).await?;
        if version != config.version {
            return Ok(None);
        }
        if scope.tenant != tenant || scope.template != config.template {
            return Err(Error::Invalid("claim scope does not match pool".into()));
        }
        let binding = binding_key(scope)?;
        if self.store.get(&binding).await?.is_some() {
            return Ok(None);
        }
        let mut candidates: Vec<_> = self
            .slots(tenant, pool_id)
            .await?
            .into_iter()
            .filter(|(_, _, slot)| slot.state == SlotState::Ready && slot.version == config.version)
            .collect();
        candidates.sort_by_key(|(_, _, slot)| {
            (
                slot.phase != Some(SlotPhase::Running),
                slot.created_at_unix_ms,
                slot.slot_id.clone(),
            )
        });
        for (key, record, mut slot) in candidates {
            let mut checks = vec![
                Check {
                    key: binding.clone(),
                    expected: None,
                },
                check(&key, &record),
                Check {
                    key: pool_key(tenant, pool_id)?,
                    expected: Some(pool_revision.clone()),
                },
            ];
            checks.extend(alias_check.clone());
            let claim = PoolClaim {
                claim_id: claim_id.into(),
                pool_id: pool_id.into(),
                state: ClaimState::Granted,
                slot_id: slot.slot_id.clone(),
                environment_id: slot.environment_id.clone(),
                scope: scope.clone(),
                deadline_unix_ms: deadline(config.claim_timeout_seconds, now)?,
                released: false,
                reason: None,
            };
            let claim_record = claim_key(tenant, pool_id, claim_id)?;
            checks.push(Check {
                key: claim_record.clone(),
                expected: None,
            });
            slot.state = SlotState::Claiming;
            slot.claim_id = Some(claim_id.into());
            if self
                .commit_pool_transaction(
                    tenant,
                    pool_id,
                    &Transaction::new(
                        checks,
                        vec![put(&key, &slot)?, put(&claim_record, &claim)?],
                    )?,
                )
                .await?
            {
                return Ok(Some(ClaimOutcome::Granted {
                    claim_id: claim_id.into(),
                    slot_id: slot.slot_id,
                    environment_id: slot.environment_id,
                    version: slot.version,
                    phase: slot.phase.unwrap_or(SlotPhase::Running),
                    parameters: slot.parameters,
                    model_owner: slot.model_owner,
                    claim_deadline_unix_ms: claim.deadline_unix_ms,
                }));
            }
            if let Some((_, claim)) = self.pool_claim(tenant, pool_id, claim_id).await? {
                if claim.scope != *scope {
                    return Err(Error::Conflict(
                        "claim identity belongs to another request".into(),
                    ));
                }
                return self.claim_outcome(tenant, claim).await.map(Some);
            }
            if self.store.get(&binding).await?.is_some() {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub async fn pool_claim(
        &self,
        tenant: &str,
        pool_id: &str,
        claim_id: &str,
    ) -> Result<Option<(Revision, PoolClaim)>> {
        let Some(record) = self
            .store
            .get(&claim_key(tenant, pool_id, claim_id)?)
            .await?
        else {
            return Ok(None);
        };
        let claim: PoolClaim = record.decode()?;
        if claim.claim_id != claim_id || claim.scope.tenant != tenant {
            return Err(Error::Corrupt("pool claim identity mismatch".into()));
        }
        Ok(Some((record.revision, claim)))
    }

    pub async fn lookup_pool_claim(
        &self,
        tenant: &str,
        claim_id: &str,
    ) -> Result<Option<PoolClaim>> {
        Ok(self
            .pool_claim(tenant, "lookup", claim_id)
            .await?
            .map(|(_, claim)| claim))
    }

    pub async fn delete_pool_claim(
        &self,
        tenant: &str,
        pool_id: &str,
        claim_id: &str,
    ) -> Result<()> {
        let key = claim_key(tenant, pool_id, claim_id)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(());
        };
        let claim: PoolClaim = record.decode()?;
        if claim.pool_id != pool_id || claim.state != ClaimState::Committed {
            return Err(Error::Conflict(
                "only the original committed claim may be removed".into(),
            ));
        }
        if self
            .store
            .commit(&Transaction::with_deletes(
                vec![check(&key, &record)],
                vec![],
                vec![key],
            )?)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict("pool claim changed during cleanup".into()))
        }
    }

    async fn claim_outcome(&self, tenant: &str, claim: PoolClaim) -> Result<ClaimOutcome> {
        match claim.state {
            ClaimState::Granted => {
                let slot = self
                    .pool_slot(tenant, &claim.pool_id, &claim.slot_id)
                    .await?
                    .map(|(_, slot)| slot)
                    .ok_or_else(|| Error::Conflict("granted claim has no slot".into()))?;
                Ok(ClaimOutcome::Granted {
                    claim_id: claim.claim_id,
                    slot_id: slot.slot_id,
                    environment_id: slot.environment_id,
                    version: slot.version,
                    phase: slot.phase.unwrap_or(SlotPhase::Running),
                    parameters: slot.parameters,
                    model_owner: slot.model_owner,
                    claim_deadline_unix_ms: claim.deadline_unix_ms,
                })
            }
            ClaimState::Committed => {
                let binding = self
                    .binding(&claim.scope)
                    .await?
                    .ok_or_else(|| Error::Conflict("committed claim has no binding".into()))?;
                Ok(ClaimOutcome::Committed {
                    claim_id: claim.claim_id,
                    binding,
                })
            }
            ClaimState::Aborted => Ok(ClaimOutcome::Aborted {
                claim_id: claim.claim_id,
                released: claim.released,
            }),
        }
    }

    pub async fn commit_pool_claim(
        &self,
        tenant: &str,
        pool_id: &str,
        claim_id: &str,
    ) -> Result<ClaimOutcome> {
        for _ in 0..SLOT_ATTEMPTS {
            let (claim_revision, claim) = self
                .pool_claim(tenant, pool_id, claim_id)
                .await?
                .ok_or_else(|| Error::Conflict("claim does not exist".into()))?;
            if claim.pool_id != pool_id {
                return Err(Error::Conflict("claim belongs to another pool".into()));
            }
            match claim.state {
                ClaimState::Committed => return self.claim_outcome(tenant, claim).await,
                ClaimState::Aborted => return self.claim_outcome(tenant, claim).await,
                ClaimState::Granted => {}
            }
            let (slot_revision, slot) = self
                .pool_slot(tenant, pool_id, &claim.slot_id)
                .await?
                .ok_or_else(|| Error::Conflict("claim slot does not exist".into()))?;
            if slot.state != SlotState::Claiming || slot.claim_id.as_deref() != Some(claim_id) {
                return Err(Error::Conflict(
                    "claim slot is not owned by this claim".into(),
                ));
            }
            let (version, alias_check) = self.desired_version(&claim.scope).await?;
            if version != slot.version {
                return Err(Error::Conflict("desired template version changed".into()));
            }
            let binding = AgentBinding {
                scope: claim.scope.clone(),
                generation: uuid::Uuid::new_v4().to_string(),
                current: EnvironmentRef::new(slot.version.clone(), slot.environment_id.clone()),
                secondary: None,
                phase: BindingPhase::Active,
                parameters: slot.parameters.clone(),
                volumes: slot.volumes.clone(),
                creation_attempts: Default::default(),
            };
            let binding_record = binding_key(&claim.scope)?;
            let slot_record = slot_key(tenant, pool_id, &claim.slot_id)?;
            let claim_record = claim_key(tenant, pool_id, claim_id)?;
            let mut checks = vec![
                Check {
                    key: claim_record.clone(),
                    expected: Some(claim_revision),
                },
                Check {
                    key: slot_record.clone(),
                    expected: Some(slot_revision),
                },
                Check {
                    key: binding_record.clone(),
                    expected: None,
                },
            ];
            checks.extend(alias_check);
            let mut committed = claim.clone();
            committed.state = ClaimState::Committed;
            if self
                .commit_pool_transaction(
                    tenant,
                    pool_id,
                    &Transaction::with_deletes(
                        checks,
                        vec![
                            put(&binding_record, &binding)?,
                            put(&claim_record, &committed)?,
                        ],
                        vec![slot_record],
                    )?,
                )
                .await?
            {
                return Ok(ClaimOutcome::Committed {
                    claim_id: claim_id.into(),
                    binding,
                });
            }
        }
        Err(Error::Conflict("claim commit contention".into()))
    }

    pub async fn abort_pool_claim(
        &self,
        tenant: &str,
        pool_id: &str,
        claim_id: &str,
        reason: Option<&str>,
    ) -> Result<ClaimOutcome> {
        for _ in 0..SLOT_ATTEMPTS {
            let (claim_revision, mut claim) = self
                .pool_claim(tenant, pool_id, claim_id)
                .await?
                .ok_or_else(|| Error::Conflict("claim does not exist".into()))?;
            if claim.pool_id != pool_id {
                return Err(Error::Conflict("claim belongs to another pool".into()));
            }
            if claim.state != ClaimState::Granted {
                return self.claim_outcome(tenant, claim).await;
            }
            claim.state = ClaimState::Aborted;
            claim.reason = reason.map(str::to_owned);
            let claim_record = claim_key(tenant, pool_id, claim_id)?;
            let mut checks = vec![Check {
                key: claim_record.clone(),
                expected: Some(claim_revision),
            }];
            let mut puts = vec![put(&claim_record, &claim)?];
            if let Some((slot_revision, mut slot)) =
                self.pool_slot(tenant, pool_id, &claim.slot_id).await?
            {
                if slot.state == SlotState::Claiming {
                    slot.state = SlotState::Quarantined;
                    slot.phase = None;
                    slot.quarantine_reason = reason.map(str::to_owned);
                    slot.deadline_unix_ms = None;
                    let slot_record = slot_key(tenant, pool_id, &claim.slot_id)?;
                    checks.push(Check {
                        key: slot_record.clone(),
                        expected: Some(slot_revision),
                    });
                    puts.push(put(&slot_record, &slot)?);
                }
            }
            if self
                .commit_pool_transaction(tenant, pool_id, &Transaction::new(checks, puts)?)
                .await?
            {
                return Ok(ClaimOutcome::Aborted {
                    claim_id: claim_id.into(),
                    released: false,
                });
            }
        }
        Err(Error::Conflict("claim abort contention".into()))
    }

    /// Abort every Granted claim whose deadline elapsed. The slot and claim are changed
    /// in one transaction so a late commit cannot win after an expiry.
    pub async fn expire_pool_claims(&self, tenant: &str, pool_id: &str, now: u64) -> Result<u64> {
        let mut expired = 0;
        for (_, _, claim) in self.pool_claim_records(tenant, pool_id).await? {
            if claim.state == ClaimState::Granted && claim.deadline_unix_ms <= now {
                self.abort_pool_claim(tenant, pool_id, &claim.claim_id, Some("claim_timeout"))
                    .await?;
                expired += 1;
            }
        }
        Ok(expired)
    }

    pub async fn pool_claim_records(
        &self,
        tenant: &str,
        pool_id: &str,
    ) -> Result<Vec<(String, Record, PoolClaim)>> {
        let mut result = Vec::new();
        let mut after = None;
        let index = Index::pool_claims(tenant, pool_id)?;
        loop {
            let page = self.store.page(&index, after.as_deref(), 128).await?;
            if page.is_empty() {
                return Ok(result);
            }
            for (key, record) in page {
                after = Some(key.clone());
                let claim: PoolClaim = record.decode()?;
                if claim.pool_id != pool_id {
                    return Err(Error::Corrupt("pool claim index identity mismatch".into()));
                }
                result.push((key, record, claim));
            }
        }
    }

    pub async fn retire_pool_slot(&self, tenant: &str, pool_id: &str, slot_id: &str) -> Result<()> {
        let Some((slot_revision, slot)) = self.pool_slot(tenant, pool_id, slot_id).await? else {
            return Ok(());
        };
        if slot.state != SlotState::Retiring && slot.state != SlotState::Quarantined {
            return Err(Error::Conflict(
                "only a quarantined or retiring slot can be released".into(),
            ));
        }
        let slot_record = slot_key(tenant, pool_id, slot_id)?;
        let retired_record = retired_key(tenant, pool_id, slot_id)?;
        let existing = self.store.get(&retired_record).await?;
        let retired = PoolRetired {
            slot_id: slot.slot_id.clone(),
            model_owner: slot.model_owner.clone(),
            key_digest: slot.key_digest.clone(),
            preserve_key: slot.claim_id.is_some(),
            model_api_key: slot
                .parameters
                .get("model_api_key")
                .cloned()
                .ok_or_else(|| Error::Corrupt("retiring slot has no key".into()))?,
        };
        if let Some(record) = &existing {
            let current: PoolRetired = record.decode()?;
            if current.model_owner != retired.model_owner
                || current.key_digest != retired.key_digest
            {
                return Err(Error::Conflict("retired slot identity changed".into()));
            }
        }
        let mut checks = vec![
            Check {
                key: slot_record.clone(),
                expected: Some(slot_revision),
            },
            Check {
                key: retired_record.clone(),
                expected: existing.as_ref().map(|record| record.revision.clone()),
            },
        ];
        let mut puts = Vec::new();
        if existing.is_none() {
            puts.push(put(&retired_record, &retired)?);
        }
        if let Some(claim_id) = &slot.claim_id {
            if let Some((claim_revision, mut claim)) =
                self.pool_claim(tenant, pool_id, claim_id).await?
            {
                if claim.state == ClaimState::Aborted && !claim.released {
                    claim.released = true;
                    let claim_record = claim_key(tenant, pool_id, claim_id)?;
                    checks.push(Check {
                        key: claim_record.clone(),
                        expected: Some(claim_revision),
                    });
                    puts.push(put(&claim_record, &claim)?);
                }
            }
        }
        if !self
            .commit_pool_transaction(
                tenant,
                pool_id,
                &Transaction::with_deletes(checks, puts, vec![slot_record])?,
            )
            .await?
        {
            return Err(Error::Conflict(
                "pool slot changed during retirement".into(),
            ));
        }
        Ok(())
    }

    pub async fn retired_pool_keys(
        &self,
        tenant: &str,
        pool_id: &str,
        limit: usize,
    ) -> Result<Vec<RetiredSlot>> {
        let mut result = Vec::new();
        let mut after = None;
        let index = Index::pool_retired(tenant, pool_id)?;
        loop {
            let page = self.store.page(&index, after.as_deref(), 128).await?;
            if page.is_empty() {
                return Ok(result);
            }
            for (key, record) in page {
                after = Some(key);
                let retired: PoolRetired = record.decode()?;
                result.push(RetiredSlot {
                    slot_id: retired.slot_id,
                    model_owner: retired.model_owner,
                    key_digest: retired.key_digest,
                    preserve_key: retired.preserve_key,
                    model_api_key: retired.model_api_key,
                });
                if result.len() >= limit {
                    return Ok(result);
                }
            }
        }
    }

    pub async fn ack_retired_pool_key(
        &self,
        tenant: &str,
        pool_id: &str,
        slot_id: &str,
    ) -> Result<()> {
        let key = retired_key(tenant, pool_id, slot_id)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(());
        };
        if self
            .store
            .commit(&Transaction::with_deletes(
                vec![check(&key, &record)],
                vec![],
                vec![key],
            )?)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict("retired pool key changed".into()))
        }
    }

    pub async fn pool_status(&self, tenant: &str, pool_id: &str) -> Result<PoolStatus> {
        let (_, config) = self
            .pool(tenant, pool_id)
            .await?
            .ok_or_else(|| Error::Invalid("pool does not exist".into()))?;
        let mut counts = Counts::default();
        for (_, _, slot) in self.slots(tenant, pool_id).await? {
            counts.add(&slot);
        }
        let mut claims_granted = 0;
        let mut claims_committed = 0;
        let mut claims_aborted = 0;
        for (_, _, claim) in self.pool_claim_records(tenant, pool_id).await? {
            match claim.state {
                ClaimState::Granted => claims_granted += 1,
                ClaimState::Committed => claims_committed += 1,
                ClaimState::Aborted => claims_aborted += 1,
            }
        }
        Ok(PoolStatus {
            pool_id: config.pool_id,
            version: config.version,
            enabled: config.enabled,
            counts: PoolStatusCounts {
                key_pending: counts.key_pending,
                provisioning: counts.provisioning,
                ready_running: counts.ready_running,
                ready_paused: counts.ready_paused,
                claiming: counts.claiming,
                quarantined: counts.quarantined,
                retiring: counts.retiring,
                claims_granted,
                claims_committed,
                claims_aborted,
            },
        })
    }

    /// One holder renews the pool controller lease. Expired or absent leases are taken over.
    pub async fn renew_pool_lease(&self, holder: &str, now: u64, ttl_ms: u64) -> Result<bool> {
        let key = leader_key();
        let record = self.store.get(&key).await?;
        if let Some(current) = &record {
            let leader: Leader = current.decode()?;
            if leader.holder != holder && leader.expires_at_unix_ms > now {
                return Ok(false);
            }
        }
        let value = Leader {
            holder: holder.into(),
            expires_at_unix_ms: now
                .checked_add(ttl_ms)
                .ok_or_else(|| Error::Invalid("pool lease overflow".into()))?,
        };
        self.store
            .commit(&Transaction::new(
                vec![Check {
                    key: key.clone(),
                    expected: record.map(|record| record.revision),
                }],
                vec![put(&key, &value)?],
            )?)
            .await
    }
}
