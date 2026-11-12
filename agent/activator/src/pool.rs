//! Prewarmed slot API and single-holder reconciliation.
//! Platform remains authoritative for Environment readiness, pause/resume and deletion.
use crate::{creation, Activator, Error, Result};
use adx_agent_core::{
    environment::{
        CreateEnvironment, EnvironmentError, EnvironmentObservation, EnvironmentPhase,
        ExecutionSpec,
    },
    pool::*,
    TemplateVersion,
};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

impl Activator {
    pub async fn lookup_pool_claim(&self, request: &ClaimLookupRequest) -> Result<PoolClaim> {
        self.state
            .lookup_pool_claim(&request.tenant, &request.claim_id)
            .await?
            .ok_or(Error::NotFound)
    }
    pub async fn put_pool(&self, request: &PoolPutRequest) -> Result<PoolSnapshot> {
        request.config.validate().map_err(Error::Invalid)?;
        let template = self
            .template(
                &request.config.tenant,
                &request.config.template,
                &request.config.version,
            )
            .await?;
        if template.name != request.config.template {
            return Err(Error::Invalid("pool template identity mismatch".into()));
        }
        let expected = request
            .expected_revision
            .as_deref()
            .map(adx_agent_store::Revision::parse)
            .transpose()?;
        let revision = self
            .state
            .put_pool(&request.config, expected.as_ref())
            .await?;
        Ok(PoolSnapshot {
            config: request.config.clone(),
            revision: revision.as_str().into(),
        })
    }

    pub async fn pool(&self, tenant: &str, pool_id: &str) -> Result<PoolSnapshot> {
        let (revision, config) = self
            .state
            .pool(tenant, pool_id)
            .await?
            .ok_or(Error::NotFound)?;
        Ok(PoolSnapshot {
            config,
            revision: revision.as_str().into(),
        })
    }

    pub async fn pool_status(&self, tenant: &str, pool_id: &str) -> Result<PoolStatus> {
        Ok(self.state.pool_status(tenant, pool_id).await?)
    }

    pub async fn drain_pool(&self, tenant: &str, pool_id: &str) -> Result<PoolSnapshot> {
        let (revision, mut config) = self
            .state
            .pool(tenant, pool_id)
            .await?
            .ok_or(Error::NotFound)?;
        config.enabled = false;
        let next = self.state.put_pool(&config, Some(&revision)).await?;
        Ok(PoolSnapshot {
            config,
            revision: next.as_str().into(),
        })
    }

    pub async fn reserve_pool_slot(
        &self,
        request: &ReserveRequest,
    ) -> Result<Option<ReservedSlot>> {
        adx_agent_core::identifier(&request.model_api_base, "model API base")
            .map_err(Error::Invalid)?;
        let (_, config) = self
            .state
            .pool(&request.tenant, &request.pool_id)
            .await?
            .ok_or(Error::NotFound)?;
        self.template(&config.tenant, &config.template, &config.version)
            .await?;
        Ok(self
            .state
            .reserve_pool_slot(
                &request.tenant,
                &request.pool_id,
                &request.model_api_base,
                adx_agent_core::unix_time_millis(),
            )
            .await?
            .map(|slot| ReservedSlot {
                slot_id: slot.slot_id,
                state: slot.state,
                environment_id: slot.environment_id,
                version: slot.version,
                workspace: slot.workspace,
                model_owner: slot.model_owner,
                key_digest: slot.key_digest,
            }))
    }

    pub async fn pending_pool_keys(&self, request: &PendingKeysRequest) -> Result<PendingKeyPage> {
        validate_limit(request.limit)?;
        Ok(PendingKeyPage {
            slots: self
                .state
                .pending_pool_keys(&request.tenant, &request.pool_id, request.limit)
                .await?,
        })
    }

    pub async fn confirm_pool_key(&self, request: &KeyConfirmedRequest) -> Result<SlotView> {
        Ok(self
            .state
            .confirm_pool_key(
                &request.tenant,
                &request.pool_id,
                &request.slot_id,
                &request.key_digest,
                adx_agent_core::unix_time_millis(),
            )
            .await?)
    }

    pub async fn claim_pool(&self, request: &ClaimRequest) -> Result<ClaimOutcome> {
        self.state
            .claim_pool_slot(
                &request.scope.tenant,
                &request.pool_id,
                &request.scope,
                &request.claim_id,
                adx_agent_core::unix_time_millis(),
            )
            .await?
            .ok_or(Error::NotFound)
    }

    pub async fn commit_pool(&self, request: &CommitRequest) -> Result<ClaimOutcome> {
        Ok(self
            .state
            .commit_pool_claim(&request.tenant, &request.pool_id, &request.claim_id)
            .await?)
    }

    pub async fn abort_pool(&self, request: &AbortRequest) -> Result<ClaimOutcome> {
        let outcome = self
            .state
            .abort_pool_claim(
                &request.tenant,
                &request.pool_id,
                &request.claim_id,
                request.reason.as_deref(),
            )
            .await?;
        match outcome {
            ClaimOutcome::Committed { .. } => Err(Error::Conflict(
                "claim already committed; it cannot be aborted".into(),
            )),
            other => Ok(other),
        }
    }

    pub async fn retired_pool_keys(&self, request: &RetiredRequest) -> Result<RetiredPage> {
        validate_limit(request.limit)?;
        Ok(RetiredPage {
            slots: self
                .state
                .retired_pool_keys(&request.tenant, &request.pool_id, request.limit)
                .await?,
        })
    }

    pub async fn ack_retired_pool_key(&self, request: &AckRetiredRequest) -> Result<()> {
        Ok(self
            .state
            .ack_retired_pool_key(&request.tenant, &request.pool_id, &request.slot_id)
            .await?)
    }

    pub async fn pool_configs(&self) -> Result<Vec<PoolConfig>> {
        Ok(self
            .state
            .pools()
            .await?
            .into_iter()
            .map(|(_, config)| config)
            .collect())
    }

    pub async fn renew_pool_lease(&self, holder: &str, ttl: Duration) -> Result<bool> {
        let ttl_ms = u64::try_from(ttl.as_millis())
            .map_err(|_| Error::Invalid("pool lease is too long".into()))?;
        Ok(self
            .state
            .renew_pool_lease(holder, adx_agent_core::unix_time_millis(), ttl_ms)
            .await?)
    }

    /// One bounded reconciliation pass. Every mutation is a CAS, so a restarted holder
    /// continues from durable slot state.
    pub async fn reconcile_pool(
        &self,
        tenant: &str,
        pool_id: &str,
        deadline_unix_ms: u64,
    ) -> Result<()> {
        let (_, config) = self
            .state
            .pool(tenant, pool_id)
            .await?
            .ok_or(Error::NotFound)?;
        let now = adx_agent_core::unix_time_millis();
        self.state.expire_pool_claims(tenant, pool_id, now).await?;
        let template = self
            .template(&config.tenant, &config.template, &config.version)
            .await?;
        let mut slots = self.state.pool_slots(tenant, pool_id).await?;
        let active = slots.iter().filter(|slot| slot.state.active()).count() as u64;
        let excess = active.saturating_sub(config.max_slots);
        slots.sort_by_key(|slot| {
            (
                slot.phase != Some(SlotPhase::Paused),
                slot.created_at_unix_ms,
                slot.slot_id.clone(),
            )
        });
        let victims: Vec<_> = slots
            .iter()
            .filter(|slot| slot.state.active() && slot.state != SlotState::Claiming)
            .take(usize::try_from(excess).unwrap_or(usize::MAX))
            .map(|slot| slot.slot_id.clone())
            .collect();
        for id in victims {
            self.state
                .begin_pool_slot_retirement(tenant, pool_id, &id)
                .await?;
        }
        slots = self.state.pool_slots(tenant, pool_id).await?;
        let running_ready = slots
            .iter()
            .filter(|slot| slot.state == SlotState::Ready && slot.phase == Some(SlotPhase::Running))
            .count() as u64;
        let inflight = slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::KeyPending | SlotState::Provisioning))
            .count() as u64;
        let active = slots.iter().filter(|slot| slot.state.active()).count() as u64;
        let replacements = config
            .min_running
            .saturating_sub(running_ready + inflight)
            .saturating_sub(config.max_slots.saturating_sub(active));
        if replacements > 0 {
            let mut paused: Vec<_> = slots
                .iter()
                .filter(|slot| {
                    slot.state == SlotState::Ready && slot.phase == Some(SlotPhase::Paused)
                })
                .collect();
            paused.sort_by_key(|slot| (slot.created_at_unix_ms, slot.slot_id.clone()));
            for slot in paused
                .into_iter()
                .take(usize::try_from(replacements).unwrap_or(usize::MAX))
            {
                self.state
                    .begin_pool_slot_retirement(tenant, pool_id, &slot.slot_id)
                    .await?;
            }
        }
        for slot in self.state.pool_slots(tenant, pool_id).await? {
            match slot.state {
                SlotState::KeyPending => {
                    if !config.enabled {
                        self.state
                            .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "pool_disabled")
                            .await?;
                    } else if slot.version != config.version {
                        self.state
                            .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "version_changed")
                            .await?;
                    } else if slot
                        .deadline_unix_ms
                        .is_some_and(|deadline| deadline <= now)
                    {
                        self.state
                            .quarantine_pool_slot(
                                tenant,
                                pool_id,
                                &slot.slot_id,
                                "key_confirmation_timeout",
                            )
                            .await?;
                    }
                }
                SlotState::Provisioning => {
                    if !config.enabled {
                        self.state
                            .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "pool_disabled")
                            .await?;
                        continue;
                    }
                    if slot.version != config.version {
                        self.state
                            .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "version_changed")
                            .await?;
                        continue;
                    }
                    if slot
                        .deadline_unix_ms
                        .is_some_and(|deadline| deadline <= now)
                    {
                        self.state
                            .quarantine_pool_slot(
                                tenant,
                                pool_id,
                                &slot.slot_id,
                                "provision_timeout",
                            )
                            .await?;
                        continue;
                    }
                    self.provision_pool_slot(tenant, pool_id, &template, &slot, deadline_unix_ms)
                        .await?;
                }
                SlotState::Ready => {
                    let expired = config.max_ready_age_seconds.saturating_mul(1000);
                    if !config.enabled
                        || slot.version != config.version
                        || now.saturating_sub(slot.created_at_unix_ms) >= expired
                    {
                        self.state
                            .begin_pool_slot_retirement(tenant, pool_id, &slot.slot_id)
                            .await?;
                        continue;
                    }
                    match self.environment.get(tenant, &slot.environment_id).await {
                        Ok(Some(observed))
                            if observed.id == slot.environment_id && observed.tenant == tenant =>
                        {
                            match observed.phase {
                                EnvironmentPhase::Running if observed.ready => {
                                    self.state
                                        .observe_pool_slot_phase(
                                            tenant,
                                            pool_id,
                                            &slot.slot_id,
                                            SlotPhase::Running,
                                        )
                                        .await?
                                }
                                EnvironmentPhase::Paused => {
                                    self.state
                                        .observe_pool_slot_phase(
                                            tenant,
                                            pool_id,
                                            &slot.slot_id,
                                            SlotPhase::Paused,
                                        )
                                        .await?
                                }
                                EnvironmentPhase::Failed | EnvironmentPhase::Deleted => {
                                    self.state
                                        .quarantine_pool_slot(
                                            tenant,
                                            pool_id,
                                            &slot.slot_id,
                                            "platform_failed",
                                        )
                                        .await?
                                }
                                _ => {}
                            }
                        }
                        Ok(Some(_)) => {
                            self.state
                                .quarantine_pool_slot(
                                    tenant,
                                    pool_id,
                                    &slot.slot_id,
                                    "environment_identity_mismatch",
                                )
                                .await?
                        }
                        Ok(None) => {
                            self.state
                                .quarantine_pool_slot(
                                    tenant,
                                    pool_id,
                                    &slot.slot_id,
                                    "environment_missing",
                                )
                                .await?
                        }
                        Err(
                            EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_),
                        ) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                SlotState::Quarantined | SlotState::Retiring => {
                    self.cleanup_pool_slot(tenant, pool_id, &slot).await?;
                }
                SlotState::Claiming => {}
            }
        }
        Ok(())
    }

    async fn provision_pool_slot(
        &self,
        tenant: &str,
        pool_id: &str,
        template: &TemplateVersion,
        slot: &PoolSlot,
        deadline_unix_ms: u64,
    ) -> Result<()> {
        self.state
            .begin_pool_provisioning(tenant, pool_id, &slot.slot_id)
            .await?;
        let mut execution: ExecutionSpec = template.into();
        execution.mounts = template
            .render_mounts(&slot.parameters)
            .map_err(Error::Invalid)?;
        execution.env = template
            .render_env(&slot.parameters)
            .map_err(Error::Invalid)?;
        self.environment.validate_execution(&execution)?;
        let volumes = creation::definitions(template, &slot.parameters, &execution.mounts)?;
        let mut fixed = slot.volumes.clone();
        let mut added = false;
        for volume in &volumes {
            let reference = self
                .environment
                .ensure_volume(tenant, &slot.environment_id, volume)
                .await?;
            if reference.id != volume.id
                || fixed
                    .get(&reference.id)
                    .is_some_and(|uid| uid != &reference.uid)
            {
                self.state
                    .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "volume_identity_changed")
                    .await?;
                return Ok(());
            }
            if fixed.get(&reference.id) != Some(&reference.uid) {
                added = true;
            }
            fixed.insert(reference.id, reference.uid);
        }
        if added {
            self.state
                .fix_pool_slot_volumes(tenant, pool_id, &slot.slot_id, &fixed)
                .await?;
        }
        for mount in &mut execution.mounts {
            if let Some(id) = &mount.volume_id {
                mount.volume_uid = Some(
                    fixed
                        .get(id)
                        .ok_or_else(|| Error::Invalid("pool Volume reference missing".into()))?
                        .clone(),
                );
            }
        }
        let observed = match self.environment.get(tenant, &slot.environment_id).await {
            Ok(Some(observed)) => observed,
            Ok(None) => match self
                .environment
                .create(&CreateEnvironment {
                    id: slot.environment_id.clone(),
                    tenant: tenant.into(),
                    execution,
                    deadline_unix_ms: Some(deadline_unix_ms),
                    creation_attempt: 0,
                })
                .await
            {
                Ok(observed) => observed,
                Err(EnvironmentError::SchedulingCancelled(_)) => return Ok(()),
                Err(EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_)) => {
                    return Ok(())
                }
                Err(error) => {
                    self.state
                        .quarantine_pool_slot(
                            tenant,
                            pool_id,
                            &slot.slot_id,
                            "environment_create_failed",
                        )
                        .await?;
                    return Err(error.into());
                }
            },
            Err(EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_)) => {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        };
        self.apply_pool_observation(tenant, pool_id, slot, &observed)
            .await
    }

    async fn apply_pool_observation(
        &self,
        tenant: &str,
        pool_id: &str,
        slot: &PoolSlot,
        observed: &EnvironmentObservation,
    ) -> Result<()> {
        if observed.id != slot.environment_id || observed.tenant != tenant {
            self.state
                .quarantine_pool_slot(
                    tenant,
                    pool_id,
                    &slot.slot_id,
                    "environment_identity_mismatch",
                )
                .await?;
            return Ok(());
        }
        match observed.phase {
            EnvironmentPhase::Running if observed.ready => {
                self.state
                    .mark_pool_slot_ready(tenant, pool_id, &slot.slot_id, SlotPhase::Running)
                    .await?
            }
            EnvironmentPhase::Paused => {
                self.state
                    .mark_pool_slot_ready(tenant, pool_id, &slot.slot_id, SlotPhase::Paused)
                    .await?
            }
            EnvironmentPhase::Failed | EnvironmentPhase::Deleted => {
                self.state
                    .quarantine_pool_slot(tenant, pool_id, &slot.slot_id, "platform_failed")
                    .await?
            }
            _ => {}
        }
        Ok(())
    }

    async fn cleanup_pool_slot(&self, tenant: &str, pool_id: &str, slot: &PoolSlot) -> Result<()> {
        match self.environment.delete(tenant, &slot.environment_id).await {
            Ok(observed)
                if observed.phase == EnvironmentPhase::Deleted
                    && observed.id == slot.environment_id
                    && observed.tenant == tenant => {}
            Ok(_) => {
                return Err(Error::OutcomeUnknown(
                    "Platform has not confirmed pool Environment deletion".into(),
                ))
            }
            Err(EnvironmentError::NotFound) if !slot.creation_attempted => {}
            Err(EnvironmentError::NotFound) => {
                return Err(Error::OutcomeUnknown(
                    "pool create may still complete; deletion is not confirmed".into(),
                ));
            }
            Err(EnvironmentError::Unavailable(_) | EnvironmentError::OutcomeUnknown(_)) => {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        }
        self.environment
            .release_volumes(tenant, &slot.environment_id)
            .await?;
        self.state
            .retire_pool_slot(tenant, pool_id, &slot.slot_id)
            .await?;
        Ok(())
    }
}

fn validate_limit(limit: usize) -> Result<()> {
    if !(1..=128).contains(&limit) {
        return Err(Error::Invalid("pool page limit must be 1..128".into()));
    }
    Ok(())
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolReconcileSettings {
    pub interval_seconds: u64,
}
impl Default for PoolReconcileSettings {
    fn default() -> Self {
        Self {
            interval_seconds: 5,
        }
    }
}

pub struct PoolReconciler {
    task: Option<JoinHandle<()>>,
}
impl Drop for PoolReconciler {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl PoolReconciler {
    pub async fn start(activator: Arc<Activator>, settings: PoolReconcileSettings) -> Result<Self> {
        if !(1..=300).contains(&settings.interval_seconds) {
            return Err(Error::Invalid(
                "pool reconcile interval must be 1..300 seconds".into(),
            ));
        }
        let holder = uuid::Uuid::new_v4().to_string();
        let task = tokio::spawn(async move {
            let interval = Duration::from_secs(settings.interval_seconds);
            loop {
                if activator
                    .renew_pool_lease(&holder, Duration::from_secs(30))
                    .await
                    .unwrap_or(false)
                {
                    let work = async {
                        for pool in activator.pool_configs().await? {
                            let deadline = adx_agent_core::transport::capped_deadline(
                                None,
                                Duration::from_secs(60),
                            );
                            let result = tokio::time::timeout(
                                Duration::from_secs(60),
                                activator.reconcile_pool(&pool.tenant, &pool.pool_id, deadline),
                            )
                            .await;
                            if !matches!(result, Ok(Ok(()))) {
                                tracing::warn!(pool_id = %pool.pool_id, "pool reconciliation incomplete");
                            }
                        }
                        Ok::<(), Error>(())
                    };
                    let renewal = async {
                        loop {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            if !activator
                                .renew_pool_lease(&holder, Duration::from_secs(30))
                                .await
                                .unwrap_or(false)
                            {
                                return;
                            }
                        }
                    };
                    tokio::select! {
                        result = work => if let Err(error) = result { tracing::warn!(%error, "pool reconciliation failed"); },
                        _ = renewal => tracing::warn!("pool lease lost; cancelling reconciliation"),
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
        Ok(Self { task: Some(task) })
    }

    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
