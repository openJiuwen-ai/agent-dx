//! Product metadata transactions. Environment lifecycle belongs to Platform.
use crate::*;
use adx_agent_core::activator::{BindingList, BindingPage};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{
    environment::ExecutionSpec, limits, AgentBinding, BindingPhase, Scope, TemplateDraft,
    TemplateVersion,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

const TEMPLATE_CACHE_ENTRIES: usize = 1024;
const EXECUTION_IMAGE_FIELD: &str = "execution_image";
type TemplateCell = Arc<OnceCell<PublishedTemplate>>;

/// A validated template draft ready for version allocation.
pub struct PreparedTemplate<'a> {
    tenant: &'a str,
    template: TemplateVersion,
}
impl<'a> PreparedTemplate<'a> {
    pub fn new(tenant: &'a str, draft: &TemplateDraft) -> Result<Self> {
        adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
        draft.validate().map_err(Error::Invalid)?;
        let template = draft.with_version("1".into());
        Ok(Self { tenant, template })
    }

    pub fn template(&self) -> &TemplateVersion {
        &self.template
    }
}

/// Immutable template metadata used by the Activator to construct an execution.
#[derive(Clone)]
pub struct PublishedTemplate {
    pub template: TemplateVersion,
    execution_image: Option<String>,
}
impl PublishedTemplate {
    pub fn execution(&self) -> ExecutionSpec {
        let mut execution = ExecutionSpec::from(&self.template);
        if let Some(image) = &self.execution_image {
            execution.image.clone_from(image);
        }
        execution
    }
}

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
    pub async fn publish(&self, tenant: &str, draft: &TemplateDraft) -> Result<TemplateVersion> {
        self.publish_prepared(PreparedTemplate::new(tenant, draft)?, None)
            .await
    }

    /// Allocate a version and commit its public template and private execution image together.
    pub async fn publish_prepared(
        &self,
        prepared: PreparedTemplate<'_>,
        execution_image: Option<&str>,
    ) -> Result<TemplateVersion> {
        self.publish_with_preparation(prepared, execution_image, None, None)
            .await
    }
    pub async fn publication(
        &self,
        prepared: &PreparedTemplate<'_>,
        id: &str,
    ) -> Result<Option<TemplateVersion>> {
        adx_agent_core::identifier(id, "publication idempotency key").map_err(Error::Invalid)?;
        let tenant = prepared.tenant;
        let key = Key::new("template-publication", &[tenant, id])?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(None);
        };
        let version = record.decode::<TemplateVersion>()?;
        let mut expected = prepared.template.clone();
        expected.version.clone_from(&version.version);
        if expected != version {
            return Err(Error::Conflict(
                "publication idempotency key was used for another draft".into(),
            ));
        }
        self.project_preparation(tenant, &version).await.map(Some)
    }
    pub async fn publish_with_preparation(
        &self,
        mut prepared: PreparedTemplate<'_>,
        execution_image: Option<&str>,
        preparation: Option<&PreparationJob>,
        publication_id: Option<&str>,
    ) -> Result<TemplateVersion> {
        if prepared.template.auto_prepare_snapshot != preparation.is_some() {
            return Err(Error::Invalid(
                "automatic preparation job is required for this draft".into(),
            ));
        }
        let tenant = prepared.tenant;
        let sequence_key = Key::new("template-sequence", &[tenant, &prepared.template.name])?;
        for _ in 0..limits::CAS_ATTEMPTS {
            if let Some(id) = publication_id {
                if let Some(replayed) = self.publication(&prepared, id).await? {
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
            let template = &mut prepared.template;
            template.version = next.to_string();
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
            let mut publication = put(&key, template)?;
            if let Some(image) = execution_image {
                let fields = publication.record.value.as_object_mut().ok_or_else(|| {
                    Error::Invalid("template version must serialize as an object".into())
                })?;
                fields.insert(EXECUTION_IMAGE_FIELD.into(), Value::String(image.into()));
            }
            let mut puts = vec![put(&sequence_key, &next)?, publication];
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
                puts.push(put(&key, template)?);
                checks.push(Check {
                    key,
                    expected: None,
                });
            }
            if self.store.commit(&Transaction::new(checks, puts)?).await? {
                return Ok(prepared.template);
            }
            if let Some(id) = publication_id {
                if let Some(replayed) = self.publication(&prepared, id).await? {
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
        Ok(self
            .published_template(tenant, name, version)
            .await?
            .map(|published| published.template))
    }
    pub async fn published_template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<PublishedTemplate>> {
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
                let mut record = self.store.get(&key).await.map_err(Some)?.ok_or(None)?;
                let image = record
                    .value
                    .as_object_mut()
                    .and_then(|fields| fields.remove(EXECUTION_IMAGE_FIELD))
                    .unwrap_or(Value::Null);
                Ok(PublishedTemplate {
                    template: record.decode().map_err(Some)?,
                    execution_image: serde_json::from_value(image)
                        .map_err(|error| Some(Error::Corrupt(error.to_string())))?,
                })
            })
            .await
        {
            Ok(published) => {
                let template = self
                    .project_preparation(tenant, &published.template)
                    .await?;
                Ok(Some(PublishedTemplate {
                    template,
                    execution_image: published.execution_image.clone(),
                }))
            }
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
        self.commit_binding(scope, parameters, false, None, None)
            .await
    }

    /// Fix the version checked by the caller together with the Environment startup decision.
    pub async fn create_binding_for_version(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
        reuse_subject_binding: bool,
        version: &str,
        checked_snapshot: Option<&str>,
    ) -> Result<AgentBinding> {
        self.commit_binding(
            scope,
            parameters,
            reuse_subject_binding,
            Some(version),
            checked_snapshot,
        )
        .await
    }

    async fn commit_binding(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
        reuse_subject_binding: bool,
        selected_version: Option<&str>,
        checked_snapshot: Option<&str>,
    ) -> Result<AgentBinding> {
        for _ in 0..limits::CAS_ATTEMPTS {
            match self
                .create_binding_with_decision(
                    scope.clone(),
                    parameters,
                    reuse_subject_binding,
                    selected_version,
                    checked_snapshot,
                )
                .await
            {
                Ok(Some(binding)) => return Ok(binding),
                Ok(None) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::Conflict(
            "binding creation changed concurrently".into(),
        ))
    }

    async fn create_binding_with_decision(
        &self,
        scope: Scope,
        parameters: &adx_agent_core::mounts::Parameters,
        reuse_subject_binding: bool,
        selected_version: Option<&str>,
        checked_snapshot: Option<&str>,
    ) -> Result<Option<AgentBinding>> {
        let key = binding_key(&scope)?;
        let history = crate::prewarm::history_key(&scope)?;
        let previous_history = self.store.get(&history).await?;
        // The caller already resolved historical records before choosing a version.
        // Recheck a current pointer here to catch a concurrent commit under another Alias.
        if reuse_subject_binding {
            if let Some(binding) = self
                .binding_from_history(&scope, previous_history.as_ref())
                .await?
            {
                return if binding.phase == BindingPhase::Active {
                    Ok(Some(binding))
                } else {
                    Err(Error::Conflict("binding is deleting".into()))
                };
            }
        }
        let (version, alias_check) = self.desired_version(&scope).await?;
        if selected_version.is_some_and(|selected| selected != version) {
            return Err(Error::Conflict(
                "selected template version changed; retry activation".into(),
            ));
        }
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
        let template_golden = adx_agent_core::prewarm::template_golden(&template);
        if let Some(recipe) = template_golden {
            current.snapshot_id = self
                .select_snapshot(&scope.tenant, recipe, checked_snapshot, &mut extra_checks)
                .await?;
        }
        let history_empty = previous_history
            .as_ref()
            .map(Record::decode::<Option<Scope>>)
            .transpose()?
            .flatten()
            .is_none();
        extra_checks.push(Check {
            key: history.clone(),
            expected: previous_history.map(|record| record.revision),
        });
        if reuse_subject_binding || history_empty {
            extra_puts.push(put(&history, &Some(&scope))?);
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
            return Ok(Some(value));
        }
        let Some(existing) = self.store.get(&key).await? else {
            return Ok(None);
        };
        let existing: AgentBinding = existing.decode()?;
        if existing.phase == BindingPhase::Active {
            Ok(Some(existing))
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
        if let Some(record) = self.store.get(&history).await? {
            if record.decode::<Option<Scope>>()?.as_ref() == Some(&binding.scope) {
                checks.push(check(&history, &record));
                writes.push(put(&history, &Option::<Scope>::None)?);
            }
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
