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

/// Whether the Environment referenced by `current_env_id` is dormant right now:
/// Paused, Deleted, or absent. Absence counts as dormant: a removed Environment cannot
/// serve live traffic. Identity is validated so a transport mix-up cannot be mistaken
/// for an absent Environment. Shared by the creation gate (`creation.rs`) and the
/// switch-time recheck (`reconcile_binding`) so both stay on one definition of "dormant".
///
/// A `Failed` E1 is intentionally NOT dormant: Activator does not do health recovery or
/// second-guess Platform failure semantics, so a Failed E1 is treated as still-occupied
/// (the active path — keep E2 Running for zero-wake switch). If E1 later recovers or is
/// removed, the next reconcile pass re-evaluates. Paused/Deleted are the only phases that
/// prove E1 is not serving traffic.
async fn previous_environment_dormant(
    environment: &dyn Environment,
    tenant: &str,
    current_env_id: &str,
) -> std::result::Result<bool, EnvironmentError> {
    match environment.get(tenant, current_env_id).await? {
        None => Ok(true),
        Some(previous) => {
            if previous.id != current_env_id || previous.tenant != tenant {
                return Err(EnvironmentError::Unavailable(
                    "previous Environment identity mismatch".into(),
                ));
            }
            Ok(matches!(
                previous.phase,
                EnvironmentPhase::Paused | EnvironmentPhase::Deleted
            ))
        }
    }
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
            .await
            .map(|(observed, _pre_phase)| observed);
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
            EnvironmentPhase::Running if observed.ready => {
                // A dormant previous Environment means no user is currently on E1, so
                // the fresh candidate is checkpointed immediately instead of idling for
                // the template's pause timeout. An active E1 keeps E2 Running so live
                // traffic skips a wake restore on switch. Rechecked here, at the only
                // decision point, so a user who returned during E2's boot goes active.
                //
                // TOCTOU: this read of E1 and the subsequent pause of E2 are not atomic.
                // A request arriving between them can EnsureRunning E1 again; the switch
                // still commits to a Paused E2, so that user's next request pays one wake
                // restore. Accepted: closing the window would need cross-Environment
                // coordination, out of scope. The convergence loop self-corrects on the
                // next pass regardless of which side won the race.
                if previous_environment_dormant(
                    &*self.environment,
                    &binding.scope.tenant,
                    &binding.current.environment_id,
                )
                .await?
                {
                    let paused = self
                        .checkpoint_candidate(&binding, &secondary.environment, deadline)
                        .await?;
                    Self::validate_observation(&binding, &secondary.environment, &paused)?;
                }
            }
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

    /// Checkpoint a ready candidate to a Paused Environment before the switch. The
    /// timeout budget is derived from the reconciliation deadline. The result must be
    /// Paused: a non-paused reply is an unknown outcome that re-queries on the next pass.
    async fn checkpoint_candidate(
        &self,
        binding: &AgentBinding,
        environment: &EnvironmentRef,
        deadline: u64,
    ) -> Result<EnvironmentObservation> {
        // Decline to pause once the step's absolute deadline has passed: an expired
        // round must not fire a pointless capture request. pause_timeout_seconds clamps
        // an already-expired budget back to 1s, so the check must precede it. The next
        // reconcile pass observes the candidate's current phase and advances from there.
        if adx_agent_core::unix_time_millis() >= deadline {
            return Err(Error::OutcomeUnknown(
                "checkpoint deadline expired before pause; query the same AgentBinding".into(),
            ));
        }
        let timeout_seconds = environment_request::pause_timeout_seconds(deadline);
        let paused = self
            .environment
            .pause(
                &binding.scope.tenant,
                &environment.environment_id,
                timeout_seconds,
            )
            .await?;
        if paused.phase != EnvironmentPhase::Paused {
            return Err(Error::Unavailable(
                "checkpoint candidate did not commit Paused".into(),
            ));
        }
        Ok(paused)
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
        let (target, start) = self
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
        Ok(adx_agent_core::activator::ResolvedTarget {
            target,
            port,
            activation: start,
        })
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
        .map(|(target, _start)| target)
    }
    async fn activate_selected(
        &self,
        scope: &Scope,
        expected_generation: Option<&str>,
        deadline_unix_ms: u64,
        bypass_cache: bool,
        parameters: Option<&[u8]>,
        service: Option<adx_agent_core::activator::ServiceSelector>,
    ) -> Result<(Target, adx_agent_core::activator::ActivationKind)> {
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
                            return Ok((
                                target.clone(),
                                adx_agent_core::activator::ActivationKind::BindingReuse,
                            ));
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
        let (binding, created) = match self.state.binding(scope).await? {
            Some(binding) => (binding, false),
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
                (
                    self.state
                        .create_binding_with_parameters(scope.clone(), &parameters)
                        .await?,
                    true,
                )
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
        let (observed, pre_phase) = observed?;
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
        let activation = if created {
            adx_agent_core::activator::ActivationKind::Cold
        } else if matches!(
            pre_phase,
            Some(EnvironmentPhase::Pausing | EnvironmentPhase::Paused | EnvironmentPhase::Resuming)
        ) {
            adx_agent_core::activator::ActivationKind::Resumed
        } else {
            adx_agent_core::activator::ActivationKind::Unknown
        };
        Ok((target, activation))
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
