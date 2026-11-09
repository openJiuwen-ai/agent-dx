//! Gateway-side pool acquisition. Agent Store owns the claim terminal state; this module
//! records the account transaction, transfers the LiteLLM key once, and delegates readiness.
use crate::ingress::accounts::ClaimState;
use crate::ingress::accounts::{self, BusinessAuth, Error as AccountError, Principal};
use adx_agent_api::{activator::Control, managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{pool::*, Scope, TemplateRef};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::task::JoinHandle;

fn claim_budget() -> u64 {
    10
}
fn supplier_interval() -> u64 {
    5
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigitalMatePoolConfig {
    pub pools_by_version: BTreeMap<String, String>,
    #[serde(default = "claim_budget")]
    pub claim_budget_seconds: u64,
    #[serde(default = "supplier_interval")]
    pub supplier_interval_seconds: u64,
}
impl DigitalMatePoolConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.pools_by_version.is_empty()
            || !(1..=600).contains(&self.claim_budget_seconds)
            || !(1..=300).contains(&self.supplier_interval_seconds)
        {
            return Err(Error::Invalid(
                "invalid DigitalMate pool configuration".into(),
            ));
        }
        for (version, pool_id) in &self.pools_by_version {
            adx_agent_core::version_number(version)
                .map_err(|_| Error::Invalid("pool version must be numeric".into()))?;
            adx_agent_core::identifier(pool_id, "pool_id").map_err(Error::Invalid)?;
        }
        Ok(())
    }
}

pub enum PrepareError {
    Agent(Error),
    Account(AccountError),
}
impl From<Error> for PrepareError {
    fn from(value: Error) -> Self {
        Self::Agent(value)
    }
}
impl From<AccountError> for PrepareError {
    fn from(value: AccountError) -> Self {
        Self::Account(value)
    }
}

pub struct PoolRuntime {
    control: Arc<dyn Control>,
    auth: Arc<dyn BusinessAuth>,
    tenant: String,
    pools_by_version: BTreeMap<String, String>,
    claim_budget: Duration,
    _supplier: SupplierTask,
}
impl PoolRuntime {
    pub fn new(
        control: Arc<dyn Control>,
        auth: Arc<dyn BusinessAuth>,
        tenant: String,
        config: DigitalMatePoolConfig,
    ) -> Option<Arc<Self>> {
        if config.validate().is_err() {
            return None;
        }
        let models = auth.pool_models()?;
        let model_api_base = auth.model_api_base()?;
        let supplier = SupplierTask::start(
            control.clone(),
            models,
            tenant.clone(),
            config.pools_by_version.values().cloned().collect(),
            model_api_base,
            Duration::from_secs(config.supplier_interval_seconds),
        );
        Some(Arc::new(Self {
            control,
            auth,
            tenant,
            pools_by_version: config.pools_by_version,
            claim_budget: Duration::from_secs(config.claim_budget_seconds),
            _supplier: supplier,
        }))
    }

    /// Return Cold when the request must use the existing cold-start path. Any state after
    /// the account transaction is written returns an error instead of falling back.
    pub async fn prepare(
        &self,
        ctx: &Arc<RequestContext>,
        scope: &Scope,
        principal: &Principal,
        managed: &ManagedService,
    ) -> Result<(), PrepareError> {
        let deadline = ctx
            .deadline()
            .min(tokio::time::Instant::now() + self.claim_budget);
        let bounded = Arc::new(RequestContext::new(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
        ));
        tokio::time::timeout_at(
            deadline,
            self.prepare_inner(&bounded, scope, principal, managed),
        )
        .await
        .map_err(|_| {
            PrepareError::Agent(Error::OutcomeUnknown(
                "pool admission timed out; retry the original claim".into(),
            ))
        })?
    }

    async fn prepare_inner(
        &self,
        ctx: &Arc<RequestContext>,
        scope: &Scope,
        principal: &Principal,
        managed: &ManagedService,
    ) -> Result<(), PrepareError> {
        match self.control.binding(ctx, scope).await {
            Ok(_) => {
                // A commit may have succeeded while the assignment update was lost.
                if let Ok(state) = self.auth.claim_state(principal, scope).await {
                    if owned_by_claim(&state, &scope.binding_id) {
                        let _ = self
                            .auth
                            .commit_claim(principal, scope, &scope.binding_id)
                            .await;
                    }
                }
                return Ok(());
            }
            Err(Error::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let state = self
            .auth
            .claim_state(principal, scope)
            .await
            .map_err(PrepareError::Account)?;
        let own_assignment = state
            .assignment
            .as_ref()
            .is_some_and(|assignment| assignment.claim_id == scope.binding_id);
        let own_credential = state.credential_version.as_deref() == Some(&scope.binding_id);
        if own_assignment != own_credential {
            return Err(
                Error::Unavailable("incomplete account claim identity; retry".into()).into(),
            );
        }
        if state
            .assignment
            .as_ref()
            .is_some_and(|assignment| assignment.committed)
            || (!own_assignment && state.credential_version.is_some())
        {
            return Ok(());
        }
        let saved = match self
            .control
            .pool_claim_lookup(
                ctx,
                &ClaimLookupRequest {
                    tenant: scope.tenant.clone(),
                    claim_id: scope.binding_id.clone(),
                },
            )
            .await
        {
            Ok(claim) if claim.scope == *scope => Some(claim),
            Ok(_) => return Err(Error::Conflict("claim scope mismatch".into()).into()),
            Err(Error::NotFound) if !own_assignment => None,
            Err(error) => return Err(error.into()),
        };
        let pool_id = match saved {
            Some(claim) => claim.pool_id,
            None => {
                let version = match &scope.template_ref {
                    TemplateRef::Version(version) => version.clone(),
                    TemplateRef::Alias(_) => {
                        managed.binding_view(ctx, scope).await?.template.version
                    }
                };
                let Some(pool_id) = self.pools_by_version.get(&version) else {
                    return Ok(());
                };
                pool_id.clone()
            }
        };
        let outcome = match self
            .control
            .pool_claim(
                ctx,
                &ClaimRequest {
                    scope: scope.clone(),
                    pool_id: pool_id.clone(),
                    claim_id: scope.binding_id.clone(),
                },
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(Error::NotFound) if !own_assignment => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        match outcome {
            ClaimOutcome::Granted {
                claim_id,
                slot_id,
                parameters,
                ..
            } => {
                let key = parameters.get("model_api_key").ok_or_else(|| {
                    PrepareError::Agent(Error::Unavailable("pool claim has no model key".into()))
                })?;
                match self
                    .auth
                    .record_claim(
                        principal,
                        scope,
                        &claim_id,
                        &slot_id,
                        parameters
                            .get("workspace")
                            .map(String::as_str)
                            .unwrap_or_default(),
                        key,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(AccountError::ClaimConflict) => {
                        // The account transaction rolled back, so no workspace was fixed.
                        let terminal = self
                            .control
                            .pool_abort(
                                ctx,
                                &AbortRequest {
                                    tenant: self.tenant.clone(),
                                    pool_id: pool_id.clone(),
                                    claim_id: claim_id.clone(),
                                    reason: Some("account_conflict".into()),
                                },
                            )
                            .await?;
                        if matches!(terminal, ClaimOutcome::Aborted { .. }) {
                            return Ok(());
                        }
                        return Err(Error::Unavailable(
                            "account conflict with committed claim".into(),
                        )
                        .into());
                    }
                    Err(error) => return Err(PrepareError::Account(error)),
                }
                self.auth
                    .activate_claim(principal, scope, &claim_id, &slot_id, key)
                    .await
                    .map_err(PrepareError::Account)?;
                match self
                    .control
                    .pool_commit(
                        ctx,
                        &CommitRequest {
                            tenant: self.tenant.clone(),
                            pool_id: pool_id.clone(),
                            claim_id: claim_id.clone(),
                        },
                    )
                    .await
                {
                    Ok(ClaimOutcome::Committed { .. }) => {}
                    Ok(ClaimOutcome::Aborted { .. }) => {
                        return Err(PrepareError::Agent(Error::Unavailable(
                            "pool claim was aborted; retry after release".into(),
                        )))
                    }
                    Ok(ClaimOutcome::Granted { .. }) => {
                        return Err(PrepareError::Agent(Error::Unavailable(
                            "pool commit returned a non-terminal claim".into(),
                        )))
                    }
                    Err(Error::NotFound) => {
                        return Err(PrepareError::Agent(Error::Unavailable(
                            "pool commit outcome is unknown; retry".into(),
                        )))
                    }
                    Err(error) => return Err(error.into()),
                }
                self.auth
                    .commit_claim(principal, scope, &claim_id)
                    .await
                    .map_err(PrepareError::Account)?;
                Ok(())
            }
            ClaimOutcome::Committed { claim_id, .. } => {
                self.auth
                    .commit_claim(principal, scope, &claim_id)
                    .await
                    .map_err(PrepareError::Account)?;
                Ok(())
            }
            ClaimOutcome::Aborted { released, .. } => {
                if own_assignment && !released {
                    return Err(PrepareError::Agent(Error::Unavailable(
                        "pool claim was aborted; wait for workspace release".into(),
                    )));
                }
                Ok(())
            }
        }
    }
}

fn owned_by_claim(state: &ClaimState, claim_id: &str) -> bool {
    let credential = state.credential_version.as_deref() == Some(claim_id);
    let assignment = state
        .assignment
        .as_ref()
        .is_some_and(|assignment| assignment.claim_id == claim_id);
    credential && assignment
}

struct SupplierTask {
    task: Option<JoinHandle<()>>,
}
impl Drop for SupplierTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl SupplierTask {
    fn start(
        control: Arc<dyn Control>,
        models: Arc<dyn accounts::ModelProvider>,
        tenant: String,
        pools: Vec<String>,
        model_api_base: String,
        interval: Duration,
    ) -> Self {
        let task = tokio::spawn(async move {
            loop {
                let ctx = Arc::new(RequestContext::new(Duration::from_secs(30)));
                for pool_id in &pools {
                    if let Err(error) =
                        supply(&control, &models, &ctx, &tenant, pool_id, &model_api_base).await
                    {
                        tracing::warn!(
                            %error,
                            pool_id = %pool_id,
                            "DigitalMate pool supply step failed"
                        );
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
        Self { task: Some(task) }
    }
}

async fn supply(
    control: &Arc<dyn Control>,
    models: &Arc<dyn accounts::ModelProvider>,
    ctx: &Arc<RequestContext>,
    tenant: &str,
    pool_id: &str,
    model_api_base: &str,
) -> Result<(), Error> {
    let _ = control
        .pool_reserve(
            ctx,
            &ReserveRequest {
                tenant: tenant.into(),
                pool_id: pool_id.into(),
                model_api_base: model_api_base.into(),
            },
        )
        .await?;
    let pending = control
        .pool_pending_keys(
            ctx,
            &PendingKeysRequest {
                tenant: tenant.into(),
                pool_id: pool_id.into(),
                limit: 20,
            },
        )
        .await?;
    for key in pending.slots {
        models
            .ensure_pool(
                &key.model_owner,
                &key.model_api_key,
                &format!("pool-{}", key.slot_id),
            )
            .await
            .map_err(|_| Error::Unavailable("pool key confirmation failed".into()))?;
        control
            .pool_key_confirmed(
                ctx,
                &KeyConfirmedRequest {
                    tenant: tenant.into(),
                    pool_id: pool_id.into(),
                    slot_id: key.slot_id,
                    key_digest: key.key_digest,
                },
            )
            .await?;
    }
    let retired = control
        .pool_retired(
            ctx,
            &RetiredRequest {
                tenant: tenant.into(),
                pool_id: pool_id.into(),
                limit: 20,
            },
        )
        .await?;
    for key in retired.slots {
        if !key.preserve_key {
            models
                .release_pool(&key.model_owner, &key.model_api_key)
                .await
                .map_err(|_| Error::Unavailable("pool key release failed".into()))?;
        }
        control
            .pool_retired_ack(
                ctx,
                &AckRetiredRequest {
                    tenant: tenant.into(),
                    pool_id: pool_id.into(),
                    slot_id: key.slot_id,
                },
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "pool_tests.rs"]
mod tests;
