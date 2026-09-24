//! Stateless Environment activation. Platform owns Sandbox lifecycle and readiness.
pub mod server;
pub mod transport;
pub use adx_agent_core::error::{Error, Result};
use adx_agent_core::sandbox::{CreateSandbox, Sandbox, SandboxObservation, SandboxPhase};
use adx_agent_core::{activator::Target, Environment, EnvironmentPhase, Scope, TemplateVersion};
use adx_agent_store::AgentState;
use hashlink::LinkedHashMap;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Process-local Env cache limits. Zero capacity disables caching.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheSettings {
    pub capacity: usize,
    pub idle_seconds: u64,
}
impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            capacity: 200_000,
            idle_seconds: 18_000,
        }
    }
}
struct Binding {
    target: Option<Target>,
    attempt: Arc<()>,
    touched: Instant,
}

pub struct Activator {
    state: AgentState,
    sandbox: Arc<dyn Sandbox>,
    bindings: Mutex<LinkedHashMap<Scope, Binding>>,
    cache: CacheSettings,
}
impl Activator {
    pub fn new(state: AgentState, sandbox: Arc<dyn Sandbox>) -> Self {
        Self {
            state,
            sandbox,
            bindings: Mutex::new(LinkedHashMap::new()),
            cache: CacheSettings::default(),
        }
    }
    /// Returns Invalid for a zero idle timeout. Capacity zero disables the cache.
    pub fn with_cache(
        state: AgentState,
        sandbox: Arc<dyn Sandbox>,
        cache: CacheSettings,
    ) -> Result<Self> {
        if cache.idle_seconds == 0 {
            return Err(Error::Invalid(
                "Env cache idle_seconds must be positive".into(),
            ));
        }
        Ok(Self {
            cache,
            ..Self::new(state, sandbox)
        })
    }
    async fn invalidate(&self, scope: &Scope) {
        if let Some(binding) = self.bindings.lock().await.get_mut(scope) {
            binding.target = None;
            binding.attempt = Arc::new(());
        }
    }
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        self.sandbox.validate_execution(&template.into())?;
        self.state.publish(tenant, template).await?;
        Ok(())
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        self.state
            .template(tenant, name, version)
            .await?
            .ok_or(Error::NotFound)
    }
    pub async fn environment(&self, scope: &Scope) -> Result<Environment> {
        self.state.environment(scope).await?.ok_or(Error::NotFound)
    }
    pub async fn list_environments(
        &self,
        query: &adx_agent_core::activator::EnvironmentList,
    ) -> Result<adx_agent_core::activator::EnvironmentPage> {
        query.validate().map_err(Error::Invalid)?;
        self.template(&query.tenant, &query.template, &query.version)
            .await?;
        Ok(self.state.list_environments(query).await?)
    }
    fn validate_observation(
        environment: &Environment,
        observed: &SandboxObservation,
    ) -> Result<()> {
        if observed.id != environment.sandbox_id || observed.tenant != environment.scope.tenant {
            return Err(Error::Unavailable(
                "Sandbox response identity mismatch".into(),
            ));
        }
        Ok(())
    }
    pub async fn activate(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
    ) -> Result<Target> {
        self.activate_with_cache(scope, expected_generation, deadline_unix_ms, false)
            .await
    }
    /// Hot bindings renew an idle TTL without I/O. Bypass and misses read authoritative product
    /// state and Platform; failures never retain the previous successful binding. Immutable
    /// templates stay cached. Deletion concurrent with a cached read may succeed or fail.
    pub async fn activate_with_cache(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
        bypass_cache: bool,
    ) -> Result<Target> {
        scope.validate().map_err(Error::Invalid)?;
        let attempt = {
            let mut bindings = self.bindings.lock().await;
            if !bypass_cache {
                if let Some(binding) = bindings.to_back(scope) {
                    if binding.touched.elapsed() < Duration::from_secs(self.cache.idle_seconds) {
                        if let Some(target) = &binding.target {
                            if expected_generation
                                .is_some_and(|g| g != target.environment.generation)
                            {
                                return Err(Error::Conflict(
                                    "selected Environment lifecycle changed".into(),
                                ));
                            }
                            binding.touched = Instant::now();
                            return Ok(target.clone());
                        }
                    }
                }
            }
            // Fence older loaders before ANY I/O, including Redis errors and cancellation.
            let attempt = Arc::new(());
            if self.cache.capacity > 0 {
                bindings.insert(
                    scope.clone(),
                    Binding {
                        target: None,
                        attempt: attempt.clone(),
                        touched: Instant::now(),
                    },
                );
                while bindings.len() > self.cache.capacity {
                    bindings.pop_front();
                }
            }
            attempt
        };
        let template = self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?;
        let environment = match self.state.environment(scope).await? {
            Some(environment) => environment,
            None if expected_generation.is_some() => {
                return Err(Error::Conflict(
                    "selected Environment no longer exists".into(),
                ))
            }
            None => self.state.create_environment(scope.clone()).await?,
        };
        if expected_generation.is_some_and(|generation| generation != environment.generation) {
            return Err(Error::Conflict(
                "selected Environment lifecycle changed".into(),
            ));
        }
        if environment.phase != EnvironmentPhase::Active {
            return Err(Error::Conflict("environment is deleting".into()));
        }
        let observed = match self
            .sandbox
            .get(&scope.tenant, &environment.sandbox_id)
            .await?
        {
            Some(observed) => observed,
            None => {
                self.sandbox
                    .create(&CreateSandbox {
                        id: environment.sandbox_id.clone(),
                        tenant: scope.tenant.clone(),
                        execution: (&template).into(),
                        deadline_unix_ms: Some(deadline_unix_ms),
                    })
                    .await?
            }
        };
        Self::validate_observation(&environment, &observed)?;
        // Reject a delayed result after product deletion/recreation. Platform must also fence
        // create/delete ordering: a metadata check alone cannot revoke an already-sent create.
        let current = self.environment(scope).await?;
        if current != environment {
            return Err(Error::Conflict(
                "environment changed during activation".into(),
            ));
        }
        match observed.phase {
            SandboxPhase::Running if observed.ready => {
                let mut bindings = self.bindings.lock().await;
                if let Some(binding) = bindings.get_mut(scope) {
                    if Arc::ptr_eq(&binding.attempt, &attempt) {
                        binding.target = Some(Target {
                            environment: environment.clone(),
                            service: template.service.clone(),
                        });
                        binding.touched = Instant::now();
                    }
                }
                Ok(Target {
                    environment,
                    service: template.service,
                })
            }
            SandboxPhase::Creating | SandboxPhase::Running => Err(Error::NotReady(
                "Sandbox is not ready; retry the same Environment".into(),
            )),
            SandboxPhase::Failed => Err(Error::Conflict(
                "Sandbox failed; inspect Platform state".into(),
            )),
            SandboxPhase::Deleted => Err(Error::Conflict(
                "Sandbox was deleted; create a new Environment lifecycle".into(),
            )),
        }
    }
    pub async fn delete_environment(&self, scope: &Scope) -> Result<()> {
        scope.validate().map_err(Error::Invalid)?;
        self.invalidate(scope).await;
        self.environment(scope).await?;
        let environment = self.state.begin_delete(scope).await?;
        self.invalidate(scope).await;
        let observed = self
            .sandbox
            .delete(&scope.tenant, &environment.sandbox_id)
            .await?;
        Self::validate_observation(&environment, &observed)?;
        if observed.phase != SandboxPhase::Deleted {
            return Err(Error::OutcomeUnknown(
                "Platform has not confirmed deletion; retry this Environment deletion".into(),
            ));
        }
        self.state.finish_delete(&environment).await?;
        Ok(())
    }
}

#[cfg(test)]
mod cache_tests;
pub mod registration;
