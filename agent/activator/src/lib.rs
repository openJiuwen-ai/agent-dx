//! Stateless AgentBinding activation. Platform owns Environment lifecycle and readiness.
mod creation;
pub mod environment_request;
pub mod local_environment;
pub mod reconciliation;
pub mod server;
pub mod transport;
use adx_agent_core::environment::{
    Environment, EnvironmentError, EnvironmentObservation, EnvironmentPhase,
};
pub use adx_agent_core::error::{Error, Result};
use adx_agent_core::{
    activator::Target, AgentBinding, BindingPhase, EnvironmentRef, Scope, SecondaryIntent,
    TemplateVersion,
};
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
    subscription_ready: std::sync::atomic::AtomicBool,
}
impl Activator {
    pub fn new(state: AgentState, environment: Arc<dyn Environment>) -> Self {
        Self {
            state,
            environment,
            bindings: Mutex::new(LinkedHashMap::new()),
            cache: CacheSettings::default(),
            subscription_ready: std::sync::atomic::AtomicBool::new(true),
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
    async fn invalidate_all(&self) {
        self.bindings.lock().await.clear();
    }
    async fn reference_is_active(
        &self,
        saved: &AgentBinding,
        environment: &EnvironmentRef,
    ) -> Result<bool> {
        Ok(self
            .state
            .binding(&saved.scope)
            .await?
            .is_some_and(|binding| {
                binding.generation == saved.generation
                    && binding.phase == BindingPhase::Active
                    && (binding.current == *environment
                        || binding.secondary.as_ref().is_some_and(|secondary| {
                            secondary.intent == SecondaryIntent::Preparing
                                && secondary.environment == *environment
                        }))
            }))
    }

    /// Advance a single declarative version change. False means more work remains.
    /// Platform owns readiness and deletion; no Environment health recovery runs here.
    pub async fn reconcile_binding(&self, scope: &Scope, deadline: u64) -> Result<bool> {
        if self.state.binding(scope).await?.is_none() {
            return Ok(true);
        }
        let binding = self.state.prepare_binding(scope).await?;
        if binding.phase == BindingPhase::Deleting {
            self.delete_binding(scope).await?;
            return Ok(true);
        }
        let Some(secondary) = &binding.secondary else {
            return Ok(true);
        };
        if secondary.intent == SecondaryIntent::Retiring {
            self.cleanup_environment(&binding, &secondary.environment)
                .await?;
            self.state.clear_secondary(&binding).await?;
            self.invalidate(scope).await;
            return Ok(false);
        }
        let template = match self
            .template(
                &scope.tenant,
                &scope.template,
                &secondary.environment.version,
            )
            .await
        {
            Ok(template) => template,
            Err(Error::NotFound) => {
                tracing::error!(?scope, version = %secondary.environment.version,
                    environment_id = %secondary.environment.environment_id,
                    "candidate template is missing; retiring the saved candidate");
                let retired = self.state.retire_secondary(&binding).await?;
                self.cleanup_environment(&retired, &secondary.environment)
                    .await?;
                self.state.clear_secondary(&retired).await?;
                self.invalidate(scope).await;
                return Err(Error::Unavailable(format!(
                    "candidate template version {} does not exist",
                    secondary.environment.version
                )));
            }
            Err(error) => return Err(error),
        };
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
            .activate_environment(
                &binding,
                &secondary.environment,
                execution,
                &volumes,
                deadline,
            )
            .await;
        if !self
            .reference_is_active(&binding, &secondary.environment)
            .await?
        {
            self.cleanup_environment(&binding, &secondary.environment)
                .await?;
            return Err(Error::Conflict(
                "candidate retired during activation".into(),
            ));
        }
        let observed = observed?;
        Self::validate_observation(&binding, &secondary.environment, &observed)?;
        match observed.phase {
            EnvironmentPhase::Running if observed.ready => (),
            EnvironmentPhase::Paused => (),
            EnvironmentPhase::Failed | EnvironmentPhase::Deleted => {
                return Err(Error::Conflict(
                    "candidate Environment failed; inspect Platform state".into(),
                ));
            }
            _ => return Ok(false),
        }
        self.state.switch_current(&binding).await?;
        self.invalidate(scope).await;
        Ok(false)
    }
    pub fn ready(&self) -> bool {
        self.subscription_ready
            .load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn publish(
        &self,
        tenant: &str,
        template: &adx_agent_core::TemplateDraft,
    ) -> Result<TemplateVersion> {
        let snapshot = template.with_version("1".into());
        let mut execution: adx_agent_core::environment::ExecutionSpec = (&snapshot).into();
        // Mount syntax is checked at publication; concrete paths require Binding parameters.
        execution.mounts.clear();
        self.environment.validate_execution(&execution)?;
        self.state
            .publish(tenant, template)
            .await
            .map_err(Into::into)
    }
    pub async fn alias_config(
        &self,
        request: &adx_agent_core::activator::AliasRequest,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        let (revision, config) = self
            .state
            .alias(&request.tenant, &request.template, &request.alias)
            .await?
            .ok_or(Error::NotFound)?;
        Ok(adx_agent_core::activator::AliasSnapshot {
            config,
            revision: revision.as_str().into(),
        })
    }
    pub async fn update_alias(
        &self,
        request: &adx_agent_core::activator::AliasUpdate,
    ) -> Result<adx_agent_core::activator::AliasSnapshot> {
        let expected = request
            .expected_revision
            .as_deref()
            .map(adx_agent_store::Revision::parse)
            .transpose()?;
        let revision = self
            .state
            .put_alias(&request.config, expected.as_ref())
            .await?;
        Ok(adx_agent_core::activator::AliasSnapshot {
            config: request.config.clone(),
            revision: revision.as_str().into(),
        })
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
    /// Resolve metadata without creating a Binding or touching Platform lifecycle.
    pub async fn binding_view(
        &self,
        scope: &Scope,
    ) -> Result<adx_agent_core::activator::BindingView> {
        scope.validate().map_err(Error::Invalid)?;
        let binding = self.state.binding(scope).await?;
        let version = match &binding {
            Some(binding) => binding.current.version.clone(),
            None => self.state.selected_version(scope).await?,
        };
        let template = self
            .template(&scope.tenant, &scope.template, &version)
            .await?;
        Ok(adx_agent_core::activator::BindingView { binding, template })
    }
    /// Validate the requested service and activate the same selected Binding/version.
    pub async fn resolve(
        &self,
        request: &adx_agent_core::activator::ResolveRequest,
        deadline: u64,
    ) -> Result<adx_agent_core::activator::ResolvedTarget> {
        let activation = &request.activation;
        let target = self
            .activate_selected(
                &activation.scope,
                activation.expected_generation.as_deref(),
                deadline,
                activation.bypass_cache,
                activation.parameters.as_deref(),
                Some(request.service),
            )
            .await?;
        let port = request
            .service
            .select(&target.service)
            .map_err(Error::Invalid)?;
        Ok(adx_agent_core::activator::ResolvedTarget { target, port })
    }
    pub async fn binding(&self, scope: &Scope) -> Result<AgentBinding> {
        self.state.binding(scope).await?.ok_or(Error::NotFound)
    }
    pub async fn list_bindings(
        &self,
        query: &adx_agent_core::activator::BindingList,
    ) -> Result<adx_agent_core::activator::BindingPage> {
        query.validate().map_err(Error::Invalid)?;
        match &query.template_ref {
            adx_agent_core::TemplateRef::Version(version) => {
                self.template(&query.tenant, &query.template, version)
                    .await?;
            }
            adx_agent_core::TemplateRef::Alias(alias) => {
                self.state
                    .alias(&query.tenant, &query.template, alias)
                    .await?
                    .ok_or(Error::NotFound)?;
            }
        }
        Ok(self.state.list_bindings(query).await?)
    }
    fn validate_observation(
        binding: &AgentBinding,
        environment: &EnvironmentRef,
        observed: &EnvironmentObservation,
    ) -> Result<()> {
        if observed.id != environment.environment_id || observed.tenant != binding.scope.tenant {
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
        self.activate_selected(
            scope,
            expected_generation,
            deadline_unix_ms,
            bypass_cache,
            parameters,
            None,
        )
        .await
    }
    async fn activate_selected(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
        bypass_cache: bool,
        parameters: Option<&[u8]>,
        service: Option<adx_agent_core::activator::ServiceSelector>,
    ) -> Result<Target> {
        scope.validate().map_err(Error::Invalid)?;
        let attempt = {
            let mut bindings = self.bindings.lock().await;
            let subscription_ready = self
                .subscription_ready
                .load(std::sync::atomic::Ordering::Acquire);
            if !bypass_cache
                && (subscription_ready
                    || matches!(scope.template_ref, adx_agent_core::TemplateRef::Version(_)))
            {
                if let Some(binding) = bindings.to_back(scope) {
                    if binding.touched.elapsed() < Duration::from_secs(self.cache.idle_seconds) {
                        if let Some(target) = &binding.target {
                            if let Some(service) = service {
                                service.select(&target.service).map_err(Error::Invalid)?;
                            }
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
        let binding = match self.state.binding(scope).await? {
            Some(binding) => binding,
            None if expected_generation.is_some() => {
                return Err(Error::Conflict(
                    "selected AgentBinding no longer exists".into(),
                ));
            }
            None => {
                let version = self.state.selected_version(scope).await?;
                let template = self
                    .template(&scope.tenant, &scope.template, &version)
                    .await?;
                if let Some(service) = service {
                    service.select(&template.service).map_err(Error::Invalid)?;
                }
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
        let template = self
            .template(&scope.tenant, &scope.template, &binding.current.version)
            .await?;
        if let Some(service) = service {
            service.select(&template.service).map_err(Error::Invalid)?;
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
            .activate_environment(
                &binding,
                &binding.current,
                execution,
                &volumes,
                deadline_unix_ms,
            )
            .await;
        // Compensate a late activation using its saved identity. Never delete
        // the Environment belonging to a replacement Binding lifecycle.
        let binding = match self.state.binding(scope).await? {
            Some(latest)
                if latest.generation == binding.generation
                    && latest.phase == BindingPhase::Active
                    && latest.current == binding.current =>
            {
                latest
            }
            _ => {
                self.cleanup_environment(&binding, &binding.current).await?;
                return Err(Error::Conflict("binding changed during activation".into()));
            }
        };
        let observed = observed?;
        Self::validate_observation(&binding, &binding.current, &observed)?;
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
        self.cleanup_environment(&binding, &binding.current).await?;
        if let Some(secondary) = &binding.secondary {
            self.cleanup_environment(&binding, &secondary.environment)
                .await?;
        }
        self.state.finish_delete(&binding).await?;
        Ok(())
    }
    async fn cleanup_environment(
        &self,
        binding: &AgentBinding,
        environment: &EnvironmentRef,
    ) -> Result<()> {
        match self
            .environment
            .delete(&binding.scope.tenant, &environment.environment_id)
            .await
        {
            Ok(observed) => {
                Self::validate_observation(binding, environment, &observed)?;
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
            .release_volumes(&binding.scope.tenant, &environment.environment_id)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod cache_tests;
pub mod registration;
