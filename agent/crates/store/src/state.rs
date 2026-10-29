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
        adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
        draft.validate().map_err(Error::Invalid)?;
        let sequence_key = Key::new("template-sequence", &[tenant, &draft.name])?;
        for _ in 0..limits::CAS_ATTEMPTS {
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
            if self
                .store
                .commit(&Transaction::new(
                    vec![
                        Check {
                            key: sequence_key.clone(),
                            expected: expected.clone(),
                        },
                        Check {
                            key: key.clone(),
                            expected: None,
                        },
                    ],
                    vec![put(&sequence_key, &next)?, put(&key, &template)?],
                )?)
                .await?
            {
                return Ok(template);
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
            Ok(template) => Ok(Some(template.clone())),
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
        let key = binding_key(&scope)?;
        let (version, alias_check) = self.desired_version(&scope).await?;
        let template = self
            .template(&scope.tenant, &scope.template, &version)
            .await?
            .ok_or_else(|| Error::Invalid("template version does not exist".into()))?;
        template.render_mounts(parameters).map_err(Error::Invalid)?;
        template.render_env(parameters).map_err(Error::Invalid)?;
        let generation = uuid::Uuid::new_v4().to_string();
        let value = AgentBinding {
            scope,
            current: adx_agent_core::EnvironmentRef::new(version, format!("adx-{generation}")),
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
        if self
            .store
            .commit(&Transaction::new(checks, vec![put(&key, &value)?])?)
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
        let checks = vec![check(&key, &record)];
        let deletes = vec![key];
        if self
            .store
            .commit(&Transaction::with_deletes(checks, vec![], deletes)?)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict("binding changed during deletion".into()))
        }
    }
}
