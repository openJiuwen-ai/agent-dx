//! Gateway product facade. Redis and Sandbox lifecycle stay behind Activator.
use crate::{activator::Control, Error, Result};
use adx_agent_core::{activator::Target, *};
use std::sync::Arc;

pub struct ManagedService {
    control: Arc<dyn Control>,
}
impl ManagedService {
    pub fn new(control: Arc<dyn Control>) -> Self {
        Self { control }
    }
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        self.control.publish(tenant, template).await
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        let value = self.control.template(tenant, name, version).await?;
        if value.name != name || value.version != version {
            return Err(Error::Unavailable("template identity mismatch".into()));
        }
        value.validate().map_err(Error::Invalid)?;
        Ok(value)
    }
    pub async fn create_environment(&self, scope: &Scope) -> Result<Environment> {
        self.control.create_environment(scope).await
    }
    pub async fn environment(&self, scope: &Scope) -> Result<Environment> {
        self.control.environment(scope).await
    }
    pub async fn delete_environment(&self, scope: &Scope) -> Result<()> {
        self.control.delete_environment(scope).await
    }

    /// Each request validates the service and activates against authoritative product state.
    pub async fn resolve(
        &self,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
    ) -> Result<(Target, u16)> {
        scope.validate().map_err(Error::Invalid)?;
        let template = self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?;
        let ports: Vec<_> = template
            .service
            .iter()
            .filter(|s| s.protocol == protocol && port.is_none_or(|p| p == s.port))
            .map(|s| s.port)
            .collect();
        if ports.len() != 1 {
            return Err(Error::Invalid(
                "service must identify exactly one declared protocol/port".into(),
            ));
        }
        let target = self.control.activate(scope).await?;
        if target.environment.scope != *scope
            || target.environment.phase != EnvironmentPhase::Active
            || target.environment.generation.is_empty()
            || target.environment.sandbox_id.is_empty()
            || target.service != template.service
        {
            return Err(Error::Unavailable(
                "Activator target identity or service mismatch".into(),
            ));
        }
        Ok((target, ports[0]))
    }
}
