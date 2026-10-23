//! In-process Activator binding. Product state remains owned by the Activator.
use crate::{activator::Control, request::RequestContext, Result};
use adx_activator::Activator;
use adx_agent_core::{
    activator::{BindingList, BindingPage, Target},
    environment::Environment,
    AgentBinding, Scope, TemplateVersion,
};
use adx_agent_store::{AgentState, RedisRepository};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub struct LocalControl {
    activator: Arc<Activator>,
}

impl LocalControl {
    pub async fn connect(
        redis_url: &str,
        namespace: &str,
        environment: Arc<dyn Environment>,
    ) -> Result<Self> {
        let store = RedisRepository::connect(redis_url, namespace, Duration::from_secs(3)).await?;
        Ok(Self::new(Arc::new(Activator::new(
            AgentState::new(Arc::new(store)),
            environment,
        ))))
    }

    pub fn new(activator: Arc<Activator>) -> Self {
        Self { activator }
    }
}

#[async_trait]
impl Control for LocalControl {
    async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<()> {
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
