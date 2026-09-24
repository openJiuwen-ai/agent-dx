//! Gateway product facade. Product state and Sandbox lifecycle stay behind the Activator.
use crate::request::RequestContext;
use crate::{activator::Control, Error, Result};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{activator::Target, target::Target as AccessTarget, *};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};
type TemplateKey = (String, String, String);

pub struct ManagedService {
    control: Arc<dyn Control>,
    templates: Mutex<BoundedCache<TemplateKey, Arc<OnceCell<TemplateVersion>>>>,
}
impl ManagedService {
    pub fn new(control: Arc<dyn Control>) -> Self {
        Self {
            control,
            templates: Mutex::new(BoundedCache::new(1024)),
        }
    }
    /// Select an Environment identity without creating product or Sandbox state.
    /// Returns Invalid for an inline target or invalid scope components.
    pub fn environment_scope(tenant: &str, target: &AccessTarget) -> Result<Scope> {
        let (name, version, id) = match target {
            AccessTarget::Template { name, version } => {
                (name, version, uuid::Uuid::new_v4().to_string())
            }
            AccessTarget::Environment { name, version, id } => (name, version, id.clone()),
            AccessTarget::Instance(_) => {
                return Err(Error::Invalid(
                    "instance requires inline authentication".into(),
                ))
            }
        };
        let scope = Scope {
            tenant: tenant.into(),
            template: name.clone(),
            version: version.clone(),
            environment_id: id,
        };
        scope.validate().map_err(Error::Invalid)?;
        Ok(scope)
    }
    /// Select a declared service port before activation.
    /// Returns Invalid when the protocol and optional port do not identify exactly one service.
    pub fn select_service(
        template: &TemplateVersion,
        protocol: Protocol,
        port: Option<u16>,
    ) -> Result<u16> {
        let mut ports = template
            .service
            .iter()
            .filter(|s| s.protocol == protocol && port.is_none_or(|p| p == s.port))
            .map(|s| s.port);
        match (ports.next(), ports.next()) {
            (Some(port), None) => Ok(port),
            _ => Err(Error::Invalid(
                "service must identify exactly one declared protocol/port".into(),
            )),
        }
    }
    pub async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<()> {
        self.control.publish(ctx, tenant, template).await
    }
    pub async fn template(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        for value in [tenant, name, version] {
            identifier(value, "template scope").map_err(Error::Invalid)?;
        }
        ctx.run(async {
            let key = (tenant.to_owned(), name.to_owned(), version.to_owned());
            let cell = {
                let mut templates = self.templates.lock().await;
                if let Some(cell) = templates.get(&key) {
                    cell.clone()
                } else {
                    let cell = Arc::new(OnceCell::new());
                    templates.insert(key, cell.clone());
                    cell
                }
            };
            cell.get_or_try_init(|| async {
                let value = self.control.template(ctx, tenant, name, version).await?;
                if value.name != name || value.version != version {
                    return Err(Error::Unavailable("template identity mismatch".into()));
                }
                value.validate().map_err(Error::Invalid)?;
                Ok(value)
            })
            .await
            .cloned()
        })
        .await
    }

    pub async fn environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<Environment> {
        self.control.environment(ctx, scope).await
    }
    pub async fn list_environments(
        &self,
        ctx: &RequestContext,
        query: &activator::EnvironmentList,
    ) -> Result<activator::EnvironmentPage> {
        query.validate().map_err(Error::Invalid)?;
        self.control.list_environments(ctx, query).await
    }
    pub async fn delete_environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        self.control.delete_environment(ctx, scope).await
    }

    /// Each request validates the service; the Activator may reuse a cached Env binding.
    pub async fn resolve(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
    ) -> Result<(Target, u16)> {
        self.resolve_with_cache(ctx, scope, protocol, port, false)
            .await
    }
    /// Resolve with an explicit successful-binding cache bypass; immutable templates stay cached.
    pub async fn resolve_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        bypass_cache: bool,
    ) -> Result<(Target, u16)> {
        self.resolve_generation(ctx, scope, protocol, port, None, bypass_cache)
            .await
    }
    /// A retry of the same incoming request must not start a new lifecycle after deletion.
    pub async fn retry_resolve(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: u16,
        generation: &str,
    ) -> Result<(Target, u16)> {
        self.resolve_generation(ctx, scope, protocol, Some(port), Some(generation), true)
            .await
    }
    async fn resolve_generation(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        protocol: Protocol,
        port: Option<u16>,
        generation: Option<&str>,
        bypass_cache: bool,
    ) -> Result<(Target, u16)> {
        scope.validate().map_err(Error::Invalid)?;
        let template = self
            .template(ctx, &scope.tenant, &scope.template, &scope.version)
            .await?;
        let port = Self::select_service(&template, protocol, port)?;
        let target = self
            .control
            .activate_with_cache(ctx, scope, generation, bypass_cache)
            .await?;
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
        Ok((target, port))
    }
}
