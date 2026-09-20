//! Managed product operations. Dispatchers own allocation and lifecycle coordination.
use crate::{dispatcher::Dispatch, Error, Result};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{
    dispatcher::{ResolveRequest, Target},
    *,
};
use adx_agent_store::AgentState;
use std::sync::{Arc, Mutex};

pub struct ManagedService {
    state: AgentState,
    dispatcher: Arc<dyn Dispatch>,
    templates: Mutex<BoundedCache<(String, String, String), TemplateVersion>>,
}
impl ManagedService {
    pub fn new(state: AgentState, dispatcher: Arc<dyn Dispatch>) -> Self {
        Self {
            state,
            dispatcher,
            templates: Mutex::new(BoundedCache::new(limits::TEMPLATE_CACHE_ENTRIES)),
        }
    }
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        self.state
            .publish(tenant, template)
            .await
            .map_err(Into::into)
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        let key = (tenant.to_owned(), name.to_owned(), version.to_owned());
        if let Some(value) = self
            .templates
            .lock()
            .expect("template cache")
            .get(&key)
            .cloned()
        {
            return Ok(value);
        }
        let value = self
            .state
            .template(tenant, name, version)
            .await?
            .ok_or(Error::NotFound)?;
        let mut cache = self.templates.lock().expect("template cache");
        cache.insert(key, value.clone());
        Ok(value)
    }
    pub async fn create_session(&self, scope: Scope) -> Result<()> {
        self.state.create_session(scope).await.map_err(Into::into)
    }
    pub async fn session(&self, scope: &Scope) -> Result<Session> {
        self.state.session(scope).await?.ok_or(Error::NotFound)
    }
    pub async fn instances(&self, scope: &Scope) -> Result<Vec<Instance>> {
        let session = self.session(scope).await?;
        let mut instances = vec![];
        for id in session.instances {
            if let Some(instance) = self.state.instance(&scope.tenant, &id).await? {
                if instance.scope != *scope || instance.session_generation != session.generation {
                    return Err(Error::Unavailable("instance scope mismatch".into()));
                }
                instances.push(instance);
            }
        }
        Ok(instances)
    }
    pub async fn release(&self, scope: &Scope) -> Result<()> {
        self.session(scope).await?;
        self.dispatcher.release(scope).await
    }
    pub async fn instance(&self, scope: &Scope, id: &str) -> Result<Instance> {
        let session = self.session(scope).await?;
        let instance = self
            .state
            .instance(&scope.tenant, id)
            .await?
            .ok_or(Error::NotFound)?;
        if instance.scope != *scope || instance.session_generation != session.generation {
            return Err(Error::NotFound);
        }
        Ok(instance)
    }
    pub async fn release_instance(&self, scope: &Scope, id: &str) -> Result<()> {
        self.instance(scope, id).await?;
        self.dispatcher.release_instance(scope, id).await
    }
    /// Resolve before touching a user stream. Service is only protocol/port metadata.
    pub async fn resolve(
        &self,
        scope: &Scope,
        affinity: Option<String>,
        protocol: Protocol,
        port: Option<u16>,
    ) -> Result<(Target, u16)> {
        self.resolve_with_cache(scope, affinity, protocol, port, false)
            .await
    }
    pub async fn resolve_with_cache(
        &self,
        scope: &Scope,
        affinity: Option<String>,
        protocol: Protocol,
        port: Option<u16>,
        bypass_cache: bool,
    ) -> Result<(Target, u16)> {
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
        let target = self
            .dispatcher
            .resolve(&ResolveRequest {
                scope: scope.clone(),
                affinity_key: affinity,
                bypass_cache,
            })
            .await?;
        // Dispatcher is the authenticated selection authority. Re-reading Session/Instance
        // here would turn every cache hit back into Redis traffic. Fixed-ID access has its own checks.
        if target.scope != *scope
            || target.tenant != scope.tenant
            || target.session_generation.is_empty()
        {
            return Err(Error::Unavailable(
                "Dispatcher selected a different Session identity".into(),
            ));
        }
        Ok((target, ports[0]))
    }
}
