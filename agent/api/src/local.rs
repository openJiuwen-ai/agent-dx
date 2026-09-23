//! In-process Activator binding. Product state remains owned by the Activator.
use crate::{activator::Control, request::RequestContext, Result};
use adx_activator::Activator;
use adx_agent_core::{
    activator::{EnvironmentList, EnvironmentPage, Target},
    sandbox::Sandbox,
    Environment, Scope, TemplateVersion,
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
        sandbox: Arc<dyn Sandbox>,
    ) -> Result<Self> {
        let store = RedisRepository::connect(redis_url, namespace, Duration::from_secs(3)).await?;
        Ok(Self::new(Arc::new(Activator::new(
            AgentState::new(Arc::new(store)),
            sandbox,
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
    async fn environment(&self, _ctx: &RequestContext, scope: &Scope) -> Result<Environment> {
        self.activator.environment(scope).await
    }
    async fn list_environments(
        &self,
        _ctx: &RequestContext,
        query: &EnvironmentList,
    ) -> Result<EnvironmentPage> {
        self.activator.list_environments(query).await
    }
    async fn delete_environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        ctx.start_write();
        self.activator.delete_environment(scope).await
    }
    async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target> {
        ctx.start_write();
        self.activator
            .activate(scope, expected_generation, ctx.deadline_unix_ms())
            .await
    }
}
