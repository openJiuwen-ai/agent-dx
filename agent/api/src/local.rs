//! In-process Activator binding. Product state remains owned by the Activator.
use crate::{activator::Control, request::RequestContext, Result};
pub use adx_activator::config::ActivatorConfig;
use adx_activator::{reconciliation::Reconciliation, Activator};
use adx_agent_core::{
    activator::{BindingList, BindingPage, Target},
    environment::Environment,
    AgentBinding, Scope, TemplateVersion,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct LocalControl {
    activator: Arc<Activator>,
    _reconciliation: Option<Reconciliation>,
}

impl LocalControl {
    pub async fn connect(
        config: ActivatorConfig,
        environment: Arc<dyn Environment>,
    ) -> Result<Self> {
        let (activator, store) = config.connect(environment).await?;
        let reconciliation = config
            .start_reconciliation(activator.clone(), store)
            .await?;
        activator.start_snapshot_preparations();
        Ok(Self {
            activator,
            _reconciliation: reconciliation,
        })
    }

    pub fn new(activator: Arc<Activator>) -> Self {
        Self {
            activator,
            _reconciliation: None,
        }
    }
}

#[async_trait]
impl Control for LocalControl {
    async fn retire_golden(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::prewarm::GoldenRetirement,
    ) -> Result<()> {
        ctx.start_write();
        self.activator
            .retire_golden(&request.tenant, &request.snapshot_id)
            .await
    }
    async fn prewarm_policy(
        &self,
        _ctx: &RequestContext,
        scope: &adx_agent_core::prewarm::PolicyScope,
    ) -> Result<Option<adx_agent_core::prewarm::PolicySnapshot>> {
        self.activator.prewarm_policy(scope).await
    }
    async fn update_prewarm_policy(
        &self,
        ctx: &RequestContext,
        update: &adx_agent_core::prewarm::PolicyUpdate,
    ) -> Result<adx_agent_core::prewarm::PolicySnapshot> {
        ctx.start_write();
        self.activator.update_prewarm_policy(update).await
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
        self.activator
            .publish_with_id(tenant, template, ctx.publication_id())
            .await
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
