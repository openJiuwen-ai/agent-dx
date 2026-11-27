//! In-process Activator binding. Product state remains owned by the Activator.
use crate::{activator::Control, request::RequestContext, Error, Result};
pub use adx_activator::config::ActivatorConfig;
use adx_activator::{reconciliation::Reconciliation, registration::ConditionSource, Activator};
use adx_agent_core::{
    activator::{BindingList, BindingPage, Target},
    discovery::MemberCondition,
    environment::Environment,
    AgentBinding, Scope, TemplateVersion,
};
use async_trait::async_trait;
use std::sync::{atomic::Ordering, Arc};

pub struct LocalControl {
    activator: Arc<Activator>,
    _reconciliation: Option<Reconciliation>,
    draining: Arc<std::sync::atomic::AtomicBool>,
}

impl LocalControl {
    /// `draining` is the embedding process's shutdown flag: the member lease
    /// reports ready from the first renewal (the Activator is constructed
    /// here, dependencies already answered) and draining from the moment the
    /// host begins shutdown, so Gateway-side selection stops routing here
    /// within one lease heartbeat — mirroring the standalone Activator. The
    /// lease heartbeat also removes the member record on the draining flip,
    /// and the same flag rejects new work below (see `accepting`).
    pub async fn connect(
        config: ActivatorConfig,
        environment: Arc<dyn Environment>,
        draining: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self> {
        let (activator, store) = config.connect(environment).await?;
        let condition: ConditionSource = {
            let reported = draining.clone();
            Arc::new(move || MemberCondition {
                ready: true,
                draining: reported.load(Ordering::Acquire),
            })
        };
        let reconciliation = config
            .start_reconciliation(activator.clone(), store, condition)
            .await?;
        activator.start_snapshot_preparations();
        Ok(Self {
            activator,
            _reconciliation: reconciliation,
            draining,
        })
    }

    pub fn new(activator: Arc<Activator>) -> Self {
        Self {
            activator,
            _reconciliation: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Draining rejects new work with the same explicit, retryable error the
    /// standalone server's middleware returns, so callers retry another
    /// member instead of being cut off by process exit; already-running
    /// requests continue within the host's shutdown budget.
    fn accepting(&self) -> Result<()> {
        if self.draining.load(Ordering::Acquire) {
            return Err(Error::Unavailable(
                "Activator is draining; retry another member".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Control for LocalControl {
    async fn retire_golden(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::prewarm::GoldenRetirement,
    ) -> Result<()> {
        self.accepting()?;
        ctx.start_write();
        self.activator
            .retire_golden(&request.tenant, &request.snapshot_id)
            .await
    }
    async fn resolve(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::activator::ResolveRequest,
    ) -> Result<adx_agent_core::activator::ResolvedTarget> {
        self.accepting()?;
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
        self.accepting()?;
        self.activator.binding_view(scope).await
    }
    async fn update_alias(
        &self,
        ctx: &RequestContext,
        request: &adx_agent_core::activator::AliasUpdate,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        self.accepting()?;
        ctx.start_write();
        self.activator.update_alias(request).await
    }
    async fn alias_config(
        &self,
        _ctx: &RequestContext,
        request: &adx_agent_core::activator::AliasRequest,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        self.accepting()?;
        self.activator.alias_config(request).await
    }
    async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &adx_agent_core::TemplateDraft,
    ) -> Result<TemplateVersion> {
        self.accepting()?;
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
        self.accepting()?;
        self.activator.template(tenant, name, version).await
    }
    async fn binding(&self, _ctx: &RequestContext, scope: &Scope) -> Result<AgentBinding> {
        self.accepting()?;
        self.activator.binding(scope).await
    }
    async fn list_bindings(
        &self,
        _ctx: &RequestContext,
        query: &BindingList,
    ) -> Result<BindingPage> {
        self.accepting()?;
        self.activator.list_bindings(query).await
    }
    async fn delete_binding(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        self.accepting()?;
        ctx.start_write();
        self.activator.delete_binding(scope).await
    }
    async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target> {
        self.accepting()?;
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
        self.accepting()?;
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
        self.accepting()?;
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
