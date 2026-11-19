//! Product metadata transactions. Environment lifecycle belongs to Platform.
use crate::*;
use adx_agent_core::activator::{BindingList, BindingPage};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{limits, AgentBinding, BindingPhase, Scope, TemplateVersion};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

const TEMPLATE_CACHE_ENTRIES: usize = 1024;
type TemplateCell = Arc<OnceCell<TemplateVersion>>;

#[derive(Serialize, Deserialize)]
struct PageCursor {
    scope: [String; 3],
    after: String,
}

#[derive(Clone)]
pub struct AgentState {
    pub(crate) store: Arc<dyn Repository>,
    templates: Arc<Mutex<BoundedCache<Key, TemplateCell>>>,
}
pub(crate) fn binding_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new(
        "binding",
        &[
            &scope.tenant,
            &scope.template,
            scope.template_ref.kind(),
            scope.template_ref.name(),
            &scope.binding_id,
        ],
    )
}
pub(crate) fn check(key: &Key, record: &Record) -> Check {
    Check {
        key: key.clone(),
        expected: Some(record.revision.clone()),
    }
}
pub(crate) fn put<T: Serialize>(key: &Key, value: &T) -> Result<Put> {
    Ok(Put {
        key: key.clone(),
        record: Record::new(value)?,
    })
}
impl AgentState {
    pub fn new(store: Arc<dyn Repository>) -> Self {
        Self {
            store,
            templates: Arc::new(Mutex::new(BoundedCache::new(TEMPLATE_CACHE_ENTRIES))),
        }
    }
    /// Atomically publish a snapshot and advance its tenant/template sequence.
    /// An ambiguous commit is returned without replay; existing storage requires an
    /// explicitly initialized sequence, never an automatic legacy-data migration.
    pub async fn publish(
        &self,
        tenant: &str,
        draft: &adx_agent_core::TemplateDraft,
    ) -> Result<TemplateVersion> {
        self.publish_with_preparation(tenant, draft, None, None)
            .await
    }
    pub async fn publication(
        &self,
        tenant: &str,
        draft: &adx_agent_core::TemplateDraft,
        id: &str,
    ) -> Result<Option<TemplateVersion>> {
        adx_agent_core::identifier(id, "publication idempotency key").map_err(Error::Invalid)?;
        let key = Key::new("template-publication", &[tenant, id])?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(None);
        };
        let version = record.decode::<TemplateVersion>()?;
        if draft.with_version(version.version.clone()) != version {
            return Err(Error::Conflict(
                "publication idempotency key was used for another draft".into(),
            ));
        }
        self.project_preparation(tenant, &version).await.map(Some)
    }
    pub async fn publish_with_preparation(
        &self,
        tenant: &str,
        draft: &adx_agent_core::TemplateDraft,
        preparation: Option<&PreparationJob>,
        publication_id: Option<&str>,
    ) -> Result<TemplateVersion> {
        adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
        draft.validate().map_err(Error::Invalid)?;
        if draft.auto_prepare_snapshot != preparation.is_some() {
            return Err(Error::Invalid(
                "automatic preparation job is required for this draft".into(),
            ));
        }
        let sequence_key = Key::new("template-sequence", &[tenant, &draft.name])?;
        for _ in 0..limits::CAS_ATTEMPTS {
            if let Some(id) = publication_id {
                if let Some(replayed) = self.publication(tenant, draft, id).await? {
                    return Ok(replayed);
                }
            }
            let previous = self.store.get(&sequence_key).await?;
            let last = previous
                .as_ref()
                .map(|record| record.decode::<u64>())
                .transpose()?
                .unwrap_or(0);
            let next = last
                .checked_add(1)
                .ok_or_else(|| Error::Conflict("template version sequence exhausted".into()))?;
            let template = draft.with_version(next.to_string());
            let key = Key::new("template", &[tenant, &template.name, &template.version])?;
            let expected = previous.as_ref().map(|record| record.revision.clone());
            let mut checks = vec![
                Check {
                    key: sequence_key.clone(),
                    expected: expected.clone(),
                },
                Check {
                    key: key.clone(),
                    expected: None,
                },
            ];
            let mut puts = vec![put(&sequence_key, &next)?, put(&key, &template)?];
            if let Some(job) = preparation {
                let job = PreparationJob {
                    tenant: tenant.into(),
                    template: template.name.clone(),
                    version: template.version.clone(),
                    ..job.clone()
                };
                let key = crate::snapshot_preparation::pending_key(&job)?;
                puts.push(put(&key, &job)?);
                checks.push(Check {
                    key,
                    expected: None,
                });
            }
            if let Some(id) = publication_id {
                let key = Key::new("template-publication", &[tenant, id])?;
                puts.push(put(&key, &template)?);
                checks.push(Check {
                    key,
                    expected: None,
                });
            }
            if self.store.commit(&Transaction::new(checks, puts)?).await? {
                return Ok(template);
            }
            if let Some(id) = publication_id {
                if let Some(replayed) = self.publication(tenant, draft, id).await? {
                    return Ok(replayed);
                }
            }
            let observed = self.store.get(&sequence_key).await?;
            if observed.as_ref().map(|record| &record.revision) == expected.as_ref() {
                return Err(Error::Conflict("template version already exists; initialize the publication sequence through operations".into()));
            }
        }
        Err(Error::Conflict(
            "template publication changed concurrently".into(),
        ))
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<TemplateVersion>> {
        adx_agent_core::version_number(version).map_err(Error::Invalid)?;
        let key = Key::new("template", &[tenant, name, version])?;
        let cell = {
            let mut templates = self.templates.lock().await;
            match templates.get(&key) {
                Some(cell) => cell.clone(),
                None => {
                    let cell = Arc::new(OnceCell::new());
                    templates.insert(key.clone(), cell.clone());
                    cell
                }
            }
        };
        // Only immutable published values are retained. Missing/error results leave the cell
        // empty, so a later publication or recovered repository can be observed immediately.
        match cell
            .get_or_try_init(|| async {
                let record = self.store.get(&key).await.map_err(Some)?;
                record.ok_or(None)?.decode().map_err(Some)
            })
            .await
        {
            Ok(template) => self.project_preparation(tenant, template).await.map(Some),
            Err(None) => Ok(None),
            Err(Some(error)) => Err(error),
        }
    }
    pub async fn binding(&self, scope: &Scope) -> Result<Option<AgentBinding>> {
        self.store
            .get(&binding_key(scope)?)
            .await?
            .map(|r| r.decode())
            .transpose()
    }
    pub async fn list_bindings(&self, query: &BindingList) -> Result<BindingPage> {
        query.validate().map_err(Error::Invalid)?;
        let index = Index::bindings(&query.tenant, &query.template, &query.template_ref)?;
        let scope = [
            query.tenant.clone(),
            query.template.clone(),
            format!(
                "{}:{}",
                query.template_ref.kind(),
                query.template_ref.name()
            ),
        ];
        let after = query
            .page_token
            .as_ref()
            .map(|raw| {
                let bytes = URL_SAFE_NO_PAD
                    .decode(raw)
                    .map_err(|_| Error::Invalid("invalid page token".into()))?;
                let token: PageCursor = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Invalid("invalid page token".into()))?;
                if token.scope != scope || !token.after.starts_with("binding:") {
                    return Err(Error::Invalid(
                        "page token does not match query scope".into(),
                    ));
                }
                Ok(token.after)
            })
            .transpose()?;
        let mut records = self
            .store
            .page(&index, after.as_deref(), query.page_size + 1)
            .await?;
        let has_next = records.len() > query.page_size;
        records.truncate(query.page_size);
        let next_page_token = if has_next {
            records
                .last()
                .map(|(key, _)| {
                    serde_json::to_vec(&PageCursor {
                        scope,
                        after: key.clone(),
                    })
                    .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                    .map_err(|e| Error::Invalid(e.to_string()))
                })
                .transpose()?
        } else {
            None
        };
        let bindings = records
            .into_iter()
            .map(|(key, record)| {
                let value: AgentBinding = record.decode()?;
                if value.scope.tenant != query.tenant
                    || value.scope.template != query.template
                    || value.scope.template_ref != query.template_ref
                    || binding_key(&value.scope)?.as_str() != key
                {
                    return Err(Error::Corrupt("AgentBinding index scope mismatch".into()));
                }
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(BindingPage {
            bindings,
            next_page_token,
        })
    }
    /// Commit one stable Environment identity before any activation side effect.
    pub async fn create_binding(&self, scope: Scope) -> Result<AgentBinding> {
        self.create_binding_with_parameters(scope, &Default::default())
            .await
    }
    /// First successful creation fixes the parameters; all later requests reuse it.
    pub async fn create_binding_with_parameters(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
    ) -> Result<AgentBinding> {
        for _ in 0..limits::CAS_ATTEMPTS {
            match self
                .create_binding_with_decision(scope.clone(), parameters, None, None)
                .await
            {
                Err(Error::Conflict(cause)) if cause == "binding changed" => continue,
                result => return result,
            }
        }
        Err(Error::Conflict(
            "binding creation changed concurrently".into(),
        ))
    }

    /// First assignment and Binding are committed together; no Platform effects occur here.
    pub async fn create_product_binding(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
        subject: &adx_agent_core::prewarm::EligibleStart,
        checked_snapshot: Option<&str>,
    ) -> Result<AgentBinding> {
        for _ in 0..limits::CAS_ATTEMPTS {
            let canonical = self.product_binding_scope(&scope, subject).await?;
            if let Some(binding) = self.binding(&canonical).await? {
                if binding.phase != BindingPhase::Active {
                    return Err(Error::Conflict("binding is deleting".into()));
                }
                return Ok(binding);
            }
            match self
                .create_binding_with_decision(
                    scope.clone(),
                    parameters,
                    Some(subject),
                    checked_snapshot,
                )
                .await
            {
                Err(Error::Conflict(cause)) if cause == "binding changed" => continue,
                result => return result,
            }
        }
        Err(Error::Conflict(
            "first assignment changed concurrently".into(),
        ))
    }

    async fn create_binding_with_decision(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
        subject: Option<&adx_agent_core::prewarm::EligibleStart>,
        checked_snapshot: Option<&str>,
    ) -> Result<AgentBinding> {
        let key = binding_key(&scope)?;
        let (version, alias_check) = self.desired_version(&scope).await?;
        let template = self
            .template(&scope.tenant, &scope.template, &version)
            .await?
            .ok_or_else(|| Error::Invalid("template version does not exist".into()))?;
        template.render_mounts(parameters).map_err(Error::Invalid)?;
        template.render_env(parameters).map_err(Error::Invalid)?;
        let generation = uuid::Uuid::new_v4().to_string();
        let mut current =
            adx_agent_core::EnvironmentRef::new(version.clone(), format!("adx-{generation}"));
        let mut extra_checks = Vec::new();
        let mut extra_puts = Vec::new();
        let history = crate::prewarm::history_key(&scope)?;
        let previous_history = self.store.get(&history).await?;
        let never_assigned = previous_history.is_none();
        if previous_history.is_some() {
            if let Some(subject) = subject.filter(|facts| facts.product == "digitalmate") {
                let canonical = self.product_binding_scope(&scope, subject).await?;
                if canonical != scope {
                    if let Some(existing) = self.binding(&canonical).await? {
                        return if existing.phase == BindingPhase::Active {
                            Ok(existing)
                        } else {
                            Err(Error::Conflict("binding is deleting".into()))
                        };
                    }
                }
            }
        }
        extra_checks.push(Check {
            key: history.clone(),
            expected: previous_history.map(|record| record.revision),
        });
        if never_assigned {
            extra_puts.push(put(&history, &Some(&scope))?);
        }
        if let Some(subject) = subject {
            use crate::prewarm::{first_key, policy_key, FirstAssignment, FirstDecision};
            use adx_agent_core::prewarm::{PolicyScope, PrewarmPolicy};
            let first = first_key(&scope, subject)?;
            let prior = self.store.get(&first).await?;
            let mut initial = None;
            if let Some(previous) = &prior {
                let assignment: FirstAssignment = previous.decode()?;
                if assignment.binding.tenant != scope.tenant
                    || assignment.binding.binding_id != scope.binding_id
                {
                    return Err(Error::Corrupt("first-assignment identity mismatch".into()));
                }
                if let Some(binding) = self.binding(&assignment.binding).await? {
                    if binding.phase != BindingPhase::Active {
                        return Err(Error::Conflict("binding is deleting".into()));
                    }
                    return Ok(binding);
                }
                initial = Some(assignment.initial);
            }
            let policy = policy_key(&PolicyScope {
                tenant: scope.tenant.clone(),
                product: subject.product.clone(),
            })?;
            let rollout = self.store.get(&policy).await?;
            if prior.is_none() && never_assigned {
                if let (Some(saved), Some(recipe)) =
                    (rollout.as_ref(), template.prewarm_recipe.as_ref())
                {
                    let policy: PrewarmPolicy = saved.decode()?;
                    if policy.allows(subject)
                        && policy.template == scope.template
                        && policy.version == version
                        && checked_snapshot == Some(recipe.snapshot_id.as_str())
                        && (subject.product != "digitalmate"
                            || crate::prewarm::private_workspace(&template, parameters)?)
                    {
                        let protection =
                            crate::prewarm::protection_key(&scope.tenant, &recipe.snapshot_id)?;
                        if let Some(record) = self.open_snapshot_protection(&protection).await? {
                            current.snapshot_id = Some(recipe.snapshot_id.clone());
                            extra_checks.push(check(&protection, &record));
                        }
                    }
                }
            }
            extra_checks.push(Check {
                key: first.clone(),
                expected: prior.map(|r| r.revision),
            });
            let initial = initial.unwrap_or_else(|| FirstDecision {
                environment: current.clone(),
                binding_generation: generation.clone(),
                policy_revision: rollout
                    .as_ref()
                    .map(|record| record.revision.as_str().to_owned()),
                registered_at_ms: subject.registered_at_ms,
            });
            extra_checks.push(Check {
                key: policy,
                expected: rollout.map(|r| r.revision),
            });
            extra_puts.push(put(
                &first,
                &FirstAssignment {
                    binding: scope.clone(),
                    initial,
                },
            )?);
        }
        let value = AgentBinding {
            scope,
            current,
            secondary: None,
            generation,
            phase: BindingPhase::Active,
            parameters: parameters.clone(),
            volumes: Default::default(),
            creation_attempts: Default::default(),
        };
        let mut checks = vec![Check {
            key: key.clone(),
            expected: None,
        }];
        checks.extend(alias_check);
        checks.extend(extra_checks);
        let mut writes = vec![put(&key, &value)?];
        writes.extend(extra_puts);
        if self
            .store
            .commit(&Transaction::new(checks, writes)?)
            .await?
        {
            return Ok(value);
        }
        let existing: AgentBinding = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("binding changed".into()))?
            .decode()?;
        if existing.phase == BindingPhase::Active {
            Ok(existing)
        } else {
            Err(Error::Conflict("binding is deleting".into()))
        }
    }
    pub async fn begin_delete(&self, scope: &Scope) -> Result<AgentBinding> {
        let key = binding_key(scope)?;
        let generation = self
            .binding(scope)
            .await?
            .ok_or_else(|| Error::Conflict("binding missing".into()))?
            .generation;
        for _ in 0..limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("binding missing".into()))?;
            let mut value: AgentBinding = record.decode()?;
            if value.generation != generation {
                return Err(Error::Conflict("binding lifecycle changed".into()));
            }
            if value.phase == BindingPhase::Deleting {
                return Ok(value);
            }
            value.phase = BindingPhase::Deleting;
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &value)?],
                )?)
                .await?
            {
                return Ok(value);
            }
        }
        Err(Error::Conflict("binding deletion contention".into()))
    }
    /// After Platform confirms deletion or explicit absence. A late completion cannot erase a new lifecycle.
    pub async fn finish_delete(&self, binding: &AgentBinding) -> Result<()> {
        let key = binding_key(&binding.scope)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(());
        };
        let current: AgentBinding = record.decode()?;
        if current.generation != binding.generation {
            return Ok(());
        }
        if current.phase != BindingPhase::Deleting
            || current.current != binding.current
            || current.secondary != binding.secondary
        {
            return Err(Error::Conflict("binding deletion identity mismatch".into()));
        }
        let mut checks = vec![check(&key, &record)];
        let mut writes = Vec::new();
        let history = crate::prewarm::history_key(&binding.scope)?;
        if self.store.get(&history).await?.is_none() {
            checks.push(Check {
                key: history.clone(),
                expected: None,
            });
            writes.push(put(&history, &Some(&binding.scope))?);
        }
        let deletes = vec![key];
        if self
            .store
            .commit(&Transaction::with_deletes(checks, writes, deletes)?)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict("binding changed during deletion".into()))
        }
    }
}
