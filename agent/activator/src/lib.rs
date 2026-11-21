//! Stateless AgentBinding activation. Platform owns Environment lifecycle and readiness.
pub mod config;
mod creation;
pub mod environment_request;
mod image_resolution;
pub mod local_environment;
pub mod reconciliation;
pub mod server;
pub mod snapshot_preparation;
pub mod transport;
use adx_agent_core::environment::{
    Environment, EnvironmentError, EnvironmentObservation, EnvironmentPhase, SnapshotChange,
    SnapshotOwner,
};
pub use adx_agent_core::error::{Error, Result};
use adx_agent_core::{
    activator::{ActivationRequest, Target},
    AgentBinding, BindingPhase, EnvironmentRef, Scope, SecondaryIntent, TemplateVersion,
};
use adx_agent_store::{AgentState, PreparedTemplate, PublishedTemplate};
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
#[derive(Clone, PartialEq, Eq, Hash)]
enum BindingCacheKey {
    Scoped(Scope),
    Subject { tenant: String, binding_id: String },
}

impl BindingCacheKey {
    fn subject(scope: &Scope) -> Self {
        Self::Subject {
            tenant: scope.tenant.clone(),
            binding_id: scope.binding_id.clone(),
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
    image_resolver: Option<image_resolution::ImageResolver>,
    environment: Arc<dyn Environment>,
    bindings: Mutex<LinkedHashMap<BindingCacheKey, Binding>>,
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
            image_resolver: None,
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
        let mut bindings = self.bindings.lock().await;
        bindings.remove(&BindingCacheKey::Scoped(scope.clone()));
        bindings.remove(&BindingCacheKey::subject(scope));
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
        let Some(existing) = self.state.binding(scope).await? else {
            return Ok(true);
        };
        let selected = if existing.phase == BindingPhase::Active && existing.secondary.is_none() {
            let version = self.state.selected_version(scope).await?;
            if version == existing.current.version {
                None
            } else {
                let template = self
                    .template(&scope.tenant, &scope.template, &version)
                    .await?;
                let snapshot = self
                    .select_template_golden(
                        &scope.tenant,
                        &template,
                        &existing.parameters,
                        deadline,
                    )
                    .await?;
                Some((version, snapshot))
            }
        } else {
            None
        };
        let binding = self
            .state
            .prepare_binding_with_snapshot(
                scope,
                selected
                    .as_ref()
                    .map(|(version, snapshot)| (version.as_str(), snapshot.as_deref())),
            )
            .await?;
        let binding = self.settle_snapshot(binding, true, deadline).await?;
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
        let published = match self
            .state
            .published_template(
                &scope.tenant,
                &scope.template,
                &secondary.environment.version,
            )
            .await?
        {
            Some(published) => published,
            None => {
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
        };
        let execution =
            Self::binding_execution(&published, &binding.parameters, &secondary.environment)?;
        self.environment.validate_execution(&execution)?;
        let volumes =
            creation::definitions(&published.template, &binding.parameters, &execution.mounts)?;
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
        self.publish_with_id(tenant, template, None).await
    }
    pub async fn publish_with_id(
        &self,
        tenant: &str,
        template: &adx_agent_core::TemplateDraft,
        publication_id: Option<&str>,
    ) -> Result<TemplateVersion> {
        let prepared = PreparedTemplate::new(tenant, template)?;
        if let Some(id) = publication_id {
            if let Some(existing) = self.state.publication(&prepared, id).await? {
                return Ok(existing);
            }
        }
        let mut execution = adx_agent_core::environment::ExecutionSpec::from(prepared.template());
        // Mount syntax is checked at publication; concrete paths require Binding parameters.
        execution.mounts.clear();
        self.environment.validate_execution(&execution)?;
        let job = template
            .auto_prepare_snapshot
            .then(|| self.seed_job(tenant, prepared.template()))
            .transpose()?;
        let image = match &self.image_resolver {
            Some(resolver) => resolver.resolve(&template.image).await,
            None => None,
        };
        self.state
            .publish_with_preparation(prepared, image.as_deref(), job.as_ref(), publication_id)
            .await
            .map_err(Into::into)
    }
    async fn settle_snapshot(
        &self,
        binding: AgentBinding,
        candidate: bool,
        deadline: u64,
    ) -> Result<AgentBinding> {
        let target = if candidate {
            binding
                .secondary
                .as_ref()
                .filter(|s| s.intent == SecondaryIntent::Preparing)
                .map(|s| &s.environment)
        } else {
            Some(&binding.current)
        };
        let Some((target, snapshot)) =
            target.and_then(|r| r.pending_snapshot_id.as_deref().map(|id| (r, id)))
        else {
            return Ok(binding);
        };
        if binding.phase != BindingPhase::Active {
            return Ok(binding);
        }
        tokio::time::timeout(
            adx_agent_core::transport::remaining_time(deadline),
            self.acquire_pending_snapshot(&binding, target, snapshot),
        )
        .await
        .map_err(|_| {
            Error::OutcomeUnknown(
                "snapshot admission deadline exceeded; retry the same allocation".into(),
            )
        })?
    }

    // Recheck the same allocation after every revision read. Cleanup can fence a
    // delayed acquire between these reads and the write, even across Activators.
    async fn acquire_pending_snapshot(
        &self,
        binding: &AgentBinding,
        target: &EnvironmentRef,
        snapshot: &str,
    ) -> Result<AgentBinding> {
        let tenant = &binding.scope.tenant;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let revision = match self.environment.snapshot_revision(tenant, snapshot).await {
                Ok(revision) => revision,
                Err(EnvironmentError::NotFound) => {
                    return self
                        .state
                        .finish_snapshot_decision(binding, &target.environment_id, snapshot, false)
                        .await
                        .map_err(Into::into)
                }
                Err(error) => return Err(error.into()),
            };
            let current = self
                .state
                .binding(&binding.scope)
                .await?
                .filter(|current| {
                    current.generation == binding.generation
                        && current.phase == BindingPhase::Active
                })
                .ok_or_else(|| {
                    Error::Conflict("allocation retired before Catalog admission".into())
                })?;
            let allocation = if current.current.environment_id == target.environment_id {
                Some(&current.current)
            } else {
                current
                    .secondary
                    .as_ref()
                    .filter(|secondary| {
                        secondary.intent == SecondaryIntent::Preparing
                            && secondary.environment.environment_id == target.environment_id
                    })
                    .map(|secondary| &secondary.environment)
            };
            match allocation {
                Some(allocation) if allocation.pending_snapshot_id.is_none() => return Ok(current),
                Some(allocation) if allocation == target => {}
                _ => {
                    return Err(Error::Conflict(
                        "allocation changed before Catalog admission".into(),
                    ))
                }
            }
            let change = SnapshotChange::AcquireBinding {
                revision,
                owner: Self::snapshot_owner(binding, target),
            };
            let acquired = match self
                .environment
                .update_snapshot_reference(tenant, snapshot, &change)
                .await
            {
                Ok(()) => true,
                Err(EnvironmentError::Conflict(_) | EnvironmentError::NotFound) => false,
                Err(EnvironmentError::SnapshotAdmissionStale) => continue,
                Err(error) => return Err(error.into()),
            };
            return self
                .state
                .finish_snapshot_decision(binding, &target.environment_id, snapshot, acquired)
                .await
                .map_err(Into::into);
        }
        Err(EnvironmentError::SnapshotAdmissionStale.into())
    }

    fn snapshot_owner(binding: &AgentBinding, environment: &EnvironmentRef) -> SnapshotOwner {
        SnapshotOwner::Binding {
            binding_id: binding.scope.binding_id.clone(),
            environment_id: environment.environment_id.clone(),
        }
    }

    async fn select_template_golden(
        &self,
        tenant: &str,
        template: &TemplateVersion,
        parameters: &adx_agent_core::mounts::Parameters,
        deadline: u64,
    ) -> Result<Option<String>> {
        if template.auto_prepare_snapshot && snapshot_preparation::reserved_workspace(parameters) {
            return Err(Error::Invalid(
                "workspace name is reserved for anonymous Golden preparation".into(),
            ));
        }
        let Some(recipe) = adx_agent_core::prewarm::template_golden(template) else {
            return Ok(None);
        };
        if recipe.restore_profile_digest != snapshot_preparation::restore_digest(template)? {
            return Ok(None);
        }
        let checked = tokio::time::timeout(
            Duration::from_millis(500).min(adx_agent_core::transport::remaining_time(deadline)),
            self.environment.golden_available(
                tenant,
                &recipe.snapshot_id,
                &template.image,
                &template.isolation_runtime,
            ),
        )
        .await;
        Ok(matches!(checked, Ok(Ok(true))).then(|| recipe.snapshot_id.clone()))
    }

    fn binding_execution(
        published: &PublishedTemplate,
        parameters: &adx_agent_core::mounts::Parameters,
        environment: &EnvironmentRef,
    ) -> Result<adx_agent_core::environment::ExecutionSpec> {
        let template = &published.template;
        let mut execution = published.execution();
        execution.mounts = template.render_mounts(parameters).map_err(Error::Invalid)?;
        execution.env = template.render_env(parameters).map_err(Error::Invalid)?;
        if let Some(id) = &environment.snapshot_id {
            let recipe = template
                .prewarm_recipe
                .as_ref()
                .filter(|recipe| &recipe.snapshot_id == id)
                .ok_or_else(|| {
                    Error::Conflict("committed Golden recipe no longer matches template".into())
                })?;
            execution.snapshot_id = Some(id.clone());
            execution.env.extend(recipe.restore_env.clone());
        }
        Ok(execution)
    }

    pub async fn retire_golden(&self, tenant: &str, snapshot: &str) -> Result<()> {
        self.environment
            .update_snapshot_reference(tenant, snapshot, &SnapshotChange::Retire)
            .await?;
        Ok(())
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
            .activate_selected(activation, deadline, Some(request.service))
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
            &ActivationRequest {
                scope: scope.clone(),
                expected_generation: expected_generation.map(str::to_owned),
                bypass_cache,
                parameters: parameters.map(<[u8]>::to_vec),
                reuse_subject_binding: false,
            },
            deadline_unix_ms,
            None,
        )
        .await
        .map(|(target, _start)| target)
    }
    async fn activate_selected(
        &self,
        request: &ActivationRequest,
        deadline_unix_ms: u64,
        service: Option<adx_agent_core::activator::ServiceSelector>,
    ) -> Result<(Target, adx_agent_core::activator::ActivationKind)> {
        let scope = &request.scope;
        let expected_generation = request.expected_generation.as_deref();
        scope.validate().map_err(Error::Invalid)?;
        let cache_key = if request.reuse_subject_binding {
            BindingCacheKey::subject(scope)
        } else {
            BindingCacheKey::Scoped(scope.clone())
        };
        let attempt = {
            let mut bindings = self.bindings.lock().await;
            let subscription_ready = self
                .subscription_ready
                .load(std::sync::atomic::Ordering::Acquire);
            if !request.bypass_cache
                && (subscription_ready
                    || (!request.reuse_subject_binding
                        && matches!(scope.template_ref, adx_agent_core::TemplateRef::Version(_))))
            {
                if let Some(binding) = bindings.to_back(&cache_key) {
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
                    cache_key.clone(),
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
        let existing = if request.reuse_subject_binding {
            self.state.subject_binding(scope).await?
        } else {
            self.state.binding(scope).await?
        };
        let (binding, created) = match existing {
            Some(binding) => (binding, false),
            None if expected_generation.is_some() => {
                return Err(Error::Conflict(
                    "selected AgentBinding no longer exists".into(),
                ));
            }
            None => {
                let version = self.state.selected_version(scope).await?;
                let published = self
                    .state
                    .published_template(&scope.tenant, &scope.template, &version)
                    .await?
                    .ok_or(Error::NotFound)?;
                let mut execution = published.execution();
                let template = published.template;
                if let Some(service) = service {
                    service.select(&template.service).map_err(Error::Invalid)?;
                }
                let parameters = adx_agent_core::parameters::parse(
                    request.parameters.as_deref().unwrap_or_default(),
                )
                .map_err(Error::Invalid)?;
                execution.mounts = template
                    .render_mounts(&parameters)
                    .map_err(Error::Invalid)?;
                execution.env = template.render_env(&parameters).map_err(Error::Invalid)?;
                creation::definitions(&template, &parameters, &execution.mounts)?;
                self.environment.validate_execution(&execution)?;
                let chosen = self
                    .select_template_golden(&scope.tenant, &template, &parameters, deadline_unix_ms)
                    .await?;
                self.state
                    .create_binding_for_version(
                        scope.clone(),
                        &parameters,
                        request.reuse_subject_binding,
                        &version,
                        chosen.as_deref(),
                    )
                    .await?
            }
        };
        let binding = self
            .settle_snapshot(binding, false, deadline_unix_ms)
            .await?;
        let scope = &binding.scope;
        if expected_generation.is_some_and(|generation| generation != binding.generation) {
            return Err(Error::Conflict(
                "selected AgentBinding lifecycle changed".into(),
            ));
        }
        if binding.phase != BindingPhase::Active {
            return Err(Error::Conflict("binding is deleting".into()));
        }
        let published = self
            .state
            .published_template(&scope.tenant, &scope.template, &binding.current.version)
            .await?
            .ok_or(Error::NotFound)?;
        let template = &published.template;
        if let Some(service) = service {
            service.select(&template.service).map_err(Error::Invalid)?;
        }
        let execution = Self::binding_execution(&published, &binding.parameters, &binding.current)?;
        self.environment.validate_execution(&execution)?;
        let volumes = creation::definitions(template, &binding.parameters, &execution.mounts)?;
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
        let existing = if request.reuse_subject_binding {
            self.state.subject_binding(scope).await?
        } else {
            self.state.binding(scope).await?
        };
        let binding = match existing {
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
            service: published.template.service,
        };
        let mut bindings = self.bindings.lock().await;
        if let Some(cached) = bindings.get_mut(&cache_key) {
            if Arc::ptr_eq(&cached.attempt, &attempt) {
                cached.target = Some(target.clone());
                cached.touched = Instant::now();
            }
        }
        let activation = if created || pre_phase.is_none() {
            // A new Binding or a new/recreated Environment is a cold start. A CAS
            // loser reports `created = false` and falls through to phase inspection.
            adx_agent_core::activator::ActivationKind::Cold
        } else if matches!(
            pre_phase,
            Some(EnvironmentPhase::Pausing | EnvironmentPhase::Paused | EnvironmentPhase::Resuming)
        ) {
            adx_agent_core::activator::ActivationKind::Resumed
        } else if matches!(pre_phase, Some(EnvironmentPhase::Running)) && observed.ready {
            // Existing warm execution: no creation and no wake were needed.
            adx_agent_core::activator::ActivationKind::BindingReuse
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
        if let Some(snapshot) = environment
            .snapshot_id
            .as_ref()
            .or(environment.pending_snapshot_id.as_ref())
        {
            let owner = Self::snapshot_owner(binding, environment);
            let change = if environment.pending_snapshot_id.is_some() {
                let revision = match self
                    .environment
                    .snapshot_revision(&binding.scope.tenant, snapshot)
                    .await
                {
                    Ok(revision) => revision,
                    Err(EnvironmentError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
                SnapshotChange::CancelPending { owner, revision }
            } else {
                SnapshotChange::Release { owner }
            };
            match self
                .environment
                .update_snapshot_reference(&binding.scope.tenant, snapshot, &change)
                .await
            {
                Ok(()) | Err(EnvironmentError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod cache_tests;
pub mod registration;
