//! Stateless AgentBinding activation. Platform owns Environment lifecycle and readiness.
mod creation;
pub mod environment_request;
pub mod local_environment;
pub mod server;
pub mod transport;
use adx_agent_core::environment::{
    Environment, EnvironmentError, EnvironmentObservation, EnvironmentPhase,
};
pub use adx_agent_core::error::{Error, Result};
use adx_agent_core::{activator::Target, AgentBinding, BindingPhase, Scope, TemplateVersion};
use adx_agent_store::AgentState;
use hashlink::LinkedHashMap;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Process-local AgentBinding cache limits. Zero capacity disables caching.
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
    environment: Arc<dyn Environment>,
    bindings: Mutex<LinkedHashMap<Scope, Binding>>,
    cache: CacheSettings,
}
impl Activator {
    pub fn new(state: AgentState, environment: Arc<dyn Environment>) -> Self {
        Self {
            state,
            environment,
            bindings: Mutex::new(LinkedHashMap::new()),
            cache: CacheSettings::default(),
        }
    }
    /// Returns Invalid for a zero idle timeout. Capacity zero disables the cache.
    pub fn with_cache(
        state: AgentState,
        environment: Arc<dyn Environment>,
        cache: CacheSettings,
    ) -> Result<Self> {
        if cache.idle_seconds == 0 {
            return Err(Error::Invalid(
                "AgentBinding cache idle_seconds must be positive".into(),
            ));
        }
        Ok(Self {
            cache,
            ..Self::new(state, environment)
        })
    }
    async fn invalidate(&self, scope: &Scope) {
        if let Some(binding) = self.bindings.lock().await.get_mut(scope) {
            binding.target = None;
            binding.attempt = Arc::new(());
        }
    }
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        let mut execution: adx_agent_core::environment::ExecutionSpec = template.into();
        // Mount syntax is checked at publication; concrete paths require Binding parameters.
        execution.mounts.clear();
        self.environment.validate_execution(&execution)?;
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
    pub async fn binding(&self, scope: &Scope) -> Result<AgentBinding> {
        self.state.binding(scope).await?.ok_or(Error::NotFound)
    }
    pub async fn list_bindings(
        &self,
        query: &adx_agent_core::activator::BindingList,
    ) -> Result<adx_agent_core::activator::BindingPage> {
        query.validate().map_err(Error::Invalid)?;
        self.template(&query.tenant, &query.template, &query.version)
            .await?;
        Ok(self.state.list_bindings(query).await?)
    }
    fn validate_observation(
        binding: &AgentBinding,
        observed: &EnvironmentObservation,
    ) -> Result<()> {
        if observed.id != binding.environment_id || observed.tenant != binding.scope.tenant {
            return Err(Error::Unavailable(
                "Environment response identity mismatch".into(),
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
        self.activate_with_parameters(
            scope,
            expected_generation,
            deadline_unix_ms,
            bypass_cache,
            None,
        )
        .await
    }
    /// Resolve initialization only after the authoritative Binding lookup misses.
    pub async fn activate_with_parameters(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
        bypass_cache: bool,
        parameters: Option<&[u8]>,
    ) -> Result<Target> {
        scope.validate().map_err(Error::Invalid)?;
        let attempt = {
            let mut bindings = self.bindings.lock().await;
            if !bypass_cache {
                if let Some(binding) = bindings.to_back(scope) {
                    if binding.touched.elapsed() < Duration::from_secs(self.cache.idle_seconds) {
                        if let Some(target) = &binding.target {
                            if expected_generation.is_some_and(|g| g != target.binding.generation) {
                                return Err(Error::Conflict(
                                    "selected AgentBinding lifecycle changed".into(),
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
        let binding = match self.state.binding(scope).await? {
            Some(binding) => binding,
            None if expected_generation.is_some() => {
                return Err(Error::Conflict(
                    "selected AgentBinding no longer exists".into(),
                ));
            }
            None => {
                let parameters = adx_agent_core::parameters::parse(parameters.unwrap_or_default())
                    .map_err(Error::Invalid)?;
                let mut execution: adx_agent_core::environment::ExecutionSpec = (&template).into();
                execution.mounts = template
                    .render_mounts(&parameters)
                    .map_err(Error::Invalid)?;
                execution.env = template.render_env(&parameters).map_err(Error::Invalid)?;
                creation::definitions(&template, &parameters, &execution.mounts)?;
                self.environment.validate_execution(&execution)?;
                self.state
                    .create_binding_with_parameters(scope.clone(), &parameters)
                    .await?
            }
        };
        if expected_generation.is_some_and(|generation| generation != binding.generation) {
            return Err(Error::Conflict(
                "selected AgentBinding lifecycle changed".into(),
            ));
        }
        if binding.phase != BindingPhase::Active {
            return Err(Error::Conflict("binding is deleting".into()));
        }
        let mut execution: adx_agent_core::environment::ExecutionSpec = (&template).into();
        execution.mounts = template
            .render_mounts(&binding.parameters)
            .map_err(Error::Invalid)?;
        execution.env = template
            .render_env(&binding.parameters)
            .map_err(Error::Invalid)?;
        self.environment.validate_execution(&execution)?;
        let volumes = creation::definitions(&template, &binding.parameters, &execution.mounts)?;
        let observed = self
            .activate_environment(&binding, execution, &volumes, deadline_unix_ms)
            .await;
        // Compensate a late activation using its saved identity. Never delete
        // the Environment belonging to a replacement Binding lifecycle.
        if self.state.binding(scope).await?.as_ref() != Some(&binding) {
            self.cleanup_binding(&binding).await?;
            return Err(Error::Conflict("binding changed during activation".into()));
        }
        let observed = observed?;
        Self::validate_observation(&binding, &observed)?;
        // A target is a stable identity, not proof of a live execution. Only the
        // authorized Gateway data path requests Platform EnsureRunning.
        match observed.phase {
            EnvironmentPhase::Running if observed.ready => (),
            EnvironmentPhase::Pausing | EnvironmentPhase::Paused | EnvironmentPhase::Resuming => (),
            EnvironmentPhase::Creating | EnvironmentPhase::Running => {
                return Err(Error::NotReady(
                    "Environment is not ready; retry the same AgentBinding".into(),
                ))
            }
            EnvironmentPhase::Failed => {
                return Err(Error::Conflict(
                    "Environment failed; inspect Platform state".into(),
                ))
            }
            EnvironmentPhase::Deleted => {
                return Err(Error::Conflict(
                    "Environment was deleted; create a new AgentBinding lifecycle".into(),
                ))
            }
        }
        let target = Target {
            binding,
            service: template.service,
        };
        let mut bindings = self.bindings.lock().await;
        if let Some(cached) = bindings.get_mut(scope) {
            if Arc::ptr_eq(&cached.attempt, &attempt) {
                cached.target = Some(target.clone());
                cached.touched = Instant::now();
            }
        }
        Ok(target)
    }

    pub async fn delete_binding(&self, scope: &Scope) -> Result<()> {
        scope.validate().map_err(Error::Invalid)?;
        self.invalidate(scope).await;
        self.binding(scope).await?;
        let binding = self.state.begin_delete(scope).await?;
        self.invalidate(scope).await;
        self.cleanup_binding(&binding).await?;
        self.state.finish_delete(&binding).await?;
        Ok(())
    }
    async fn cleanup_binding(&self, binding: &AgentBinding) -> Result<()> {
        match self
            .environment
            .delete(&binding.scope.tenant, &binding.environment_id)
            .await
        {
            Ok(observed) => {
                Self::validate_observation(binding, &observed)?;
                if observed.phase != EnvironmentPhase::Deleted {
                    return Err(Error::OutcomeUnknown(
                        "Platform has not confirmed deletion; retry this AgentBinding deletion"
                            .into(),
                    ));
                }
            }
            // Explicit absence completes the product deletion; transport failures do not.
            Err(EnvironmentError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        self.environment
            .release_volumes(&binding.scope.tenant, &binding.environment_id)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod cache_tests;
pub mod registration;
