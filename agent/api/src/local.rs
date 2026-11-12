//! In-process Activator binding. Product state remains owned by the Activator.
use crate::{activator::Control, request::RequestContext, Result};
pub use adx_activator::registration::RegistrationConfig;
use adx_activator::Activator;
use adx_agent_core::{
    activator::{BindingList, BindingPage, Target},
    environment::Environment,
    pool::*,
    AgentBinding, Scope, TemplateVersion,
};
use adx_agent_store::{AgentState, RedisRepository};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub struct LocalControl {
    activator: Arc<Activator>,
    _reconciliation: Option<adx_activator::reconciliation::Reconciliation>,
    _pool_reconciler: Option<adx_activator::pool::PoolReconciler>,
}

impl LocalControl {
    pub async fn connect(
        redis_url: &str,
        namespace: &str,
        environment: Arc<dyn Environment>,
    ) -> Result<Self> {
        let store = RedisRepository::connect(redis_url, namespace, Duration::from_secs(3)).await?;
        let activator = Arc::new(Activator::new(
            AgentState::new(Arc::new(store)),
            environment,
        ));
        let pool = adx_activator::pool::PoolReconciler::start(
            activator.clone(),
            adx_activator::pool::PoolReconcileSettings::default(),
        )
        .await?;
        Ok(Self {
            activator,
            _reconciliation: None,
            _pool_reconciler: Some(pool),
        })
    }

    pub fn new(activator: Arc<Activator>) -> Self {
        Self {
            activator,
            _reconciliation: None,
            _pool_reconciler: None,
        }
    }
    pub async fn connect_reconciled(
        redis_url: &str,
        namespace: &str,
        environment: Arc<dyn Environment>,
        registration: adx_activator::registration::RegistrationConfig,
    ) -> Result<Self> {
        let store = RedisRepository::connect(redis_url, namespace, Duration::from_secs(3)).await?;
        let activator = Arc::new(Activator::new(
            AgentState::new(Arc::new(store.clone())),
            environment,
        ));
        let reconciliation = adx_activator::reconciliation::Reconciliation::start(
            activator.clone(),
            store,
            adx_agent_store::discovery::RedisRegistry::new(
                redis_url,
                namespace,
                Duration::from_secs(3),
            )?,
            registration,
            adx_activator::reconciliation::ReconcileSettings::default(),
        )
        .await?;
        let pool = adx_activator::pool::PoolReconciler::start(
            activator.clone(),
            adx_activator::pool::PoolReconcileSettings::default(),
        )
        .await?;
        Ok(Self {
            activator,
            _reconciliation: Some(reconciliation),
            _pool_reconciler: Some(pool),
        })
    }
}

#[async_trait]
impl Control for LocalControl {
    async fn pool_claim_lookup(
        &self,
        ctx: &RequestContext,
        request: &ClaimLookupRequest,
    ) -> Result<PoolClaim> {
        ctx.run(self.activator.lookup_pool_claim(request)).await
    }
    async fn pool_put(
        &self,
        ctx: &RequestContext,
        request: &PoolPutRequest,
    ) -> Result<PoolSnapshot> {
        ctx.start_write();
        ctx.run(self.activator.put_pool(request)).await
    }
    async fn pool_get(&self, ctx: &RequestContext, request: &PoolRequest) -> Result<PoolSnapshot> {
        ctx.run(self.activator.pool(&request.tenant, &request.pool_id))
            .await
    }
    async fn pool_status(&self, ctx: &RequestContext, request: &PoolRequest) -> Result<PoolStatus> {
        ctx.run(
            self.activator
                .pool_status(&request.tenant, &request.pool_id),
        )
        .await
    }
    async fn pool_drain(
        &self,
        ctx: &RequestContext,
        request: &PoolRequest,
    ) -> Result<PoolSnapshot> {
        ctx.start_write();
        ctx.run(self.activator.drain_pool(&request.tenant, &request.pool_id))
            .await
    }
    async fn pool_reserve(
        &self,
        ctx: &RequestContext,
        request: &ReserveRequest,
    ) -> Result<Option<ReservedSlot>> {
        ctx.start_write();
        ctx.run(self.activator.reserve_pool_slot(request)).await
    }
    async fn pool_pending_keys(
        &self,
        ctx: &RequestContext,
        request: &PendingKeysRequest,
    ) -> Result<PendingKeyPage> {
        ctx.run(self.activator.pending_pool_keys(request)).await
    }
    async fn pool_key_confirmed(
        &self,
        ctx: &RequestContext,
        request: &KeyConfirmedRequest,
    ) -> Result<SlotView> {
        ctx.start_write();
        ctx.run(self.activator.confirm_pool_key(request)).await
    }
    async fn pool_claim(
        &self,
        ctx: &RequestContext,
        request: &ClaimRequest,
    ) -> Result<ClaimOutcome> {
        ctx.start_write();
        ctx.run(self.activator.claim_pool(request)).await
    }
    async fn pool_commit(
        &self,
        ctx: &RequestContext,
        request: &CommitRequest,
    ) -> Result<ClaimOutcome> {
        ctx.start_write();
        ctx.run(self.activator.commit_pool(request)).await
    }
    async fn pool_abort(
        &self,
        ctx: &RequestContext,
        request: &AbortRequest,
    ) -> Result<ClaimOutcome> {
        ctx.start_write();
        ctx.run(self.activator.abort_pool(request)).await
    }
    async fn pool_retired(
        &self,
        ctx: &RequestContext,
        request: &RetiredRequest,
    ) -> Result<RetiredPage> {
        ctx.run(self.activator.retired_pool_keys(request)).await
    }
    async fn pool_retired_ack(
        &self,
        ctx: &RequestContext,
        request: &AckRetiredRequest,
    ) -> Result<()> {
        ctx.start_write();
        ctx.run(self.activator.ack_retired_pool_key(request)).await
    }
    async fn resolve(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::activator::ResolveRequest,
    ) -> Result<adx_agent_core::activator::ResolvedTarget> {
        ctx.start_write();
        self.activator
            .resolve(request, ctx.deadline_unix_ms())
            .await
    }
    async fn binding_view(
        &self,
        _ctx: &RequestContext,
        scope: &Scope,
    ) -> Result<adx_agent_core::activator::BindingView> {
        self.activator.binding_view(scope).await
    }
    async fn update_alias(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::activator::AliasUpdate,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        ctx.start_write();
        self.activator.update_alias(request).await
    }
    async fn alias_config(
        &self,
        _ctx: &RequestContext,
        request: &adx_agent_core::activator::AliasRequest,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        self.activator.alias_config(request).await
    }
    async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &adx_agent_core::TemplateDraft,
    ) -> Result<TemplateVersion> {
        ctx.start_write();
        self.activator.publish(tenant, template).await
    }
    async fn template(
        &self,
        _ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        self.activator.template(tenant, name, version).await
    }
    async fn binding(&self, _ctx: &RequestContext, scope: &Scope) -> Result<AgentBinding> {
        self.activator.binding(scope).await
    }
    async fn list_bindings(
        &self,
        _ctx: &RequestContext,
        query: &BindingList,
    ) -> Result<BindingPage> {
        self.activator.list_bindings(query).await
    }
    async fn delete_binding(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        ctx.start_write();
        self.activator.delete_binding(scope).await
    }
    async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target> {
        self.activate_with_cache(ctx, scope, expected_generation, false)
            .await
    }
    async fn activate_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
        bypass_cache: bool,
    ) -> Result<Target> {
        ctx.start_write();
        self.activator
            .activate_with_cache(
                scope,
                expected_generation,
                ctx.deadline_unix_ms(),
                bypass_cache,
            )
            .await
    }
    async fn activate_with_parameters(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
        bypass_cache: bool,
        parameters: Option<&[u8]>,
    ) -> Result<Target> {
        ctx.start_write();
        self.activator
            .activate_with_parameters(
                scope,
                expected_generation,
                ctx.deadline_unix_ms(),
                bypass_cache,
                parameters,
            )
            .await
    }
}
