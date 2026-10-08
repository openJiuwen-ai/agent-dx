//! Product metadata transactions. Sandbox lifecycle belongs to Platform.
use crate::*;
use adx_agent_core::activator::{EnvironmentList, EnvironmentPage};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::{limits, Environment, EnvironmentPhase, Scope, TemplateVersion};
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
    store: Arc<dyn Repository>,
    templates: Arc<Mutex<BoundedCache<Key, TemplateCell>>>,
}
pub(crate) fn environment_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new(
        "environment",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.environment_id,
        ],
    )
}
fn check(key: &Key, record: &Record) -> Check {
    Check {
        key: key.clone(),
        expected: Some(record.revision.clone()),
    }
}
fn put<T: Serialize>(key: &Key, value: &T) -> Result<Put> {
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
    pub async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
        template.validate().map_err(Error::Invalid)?;
        let key = Key::new("template", &[tenant, &template.name, &template.version])?;
        if self
            .store
            .commit(&Transaction::new(
                vec![Check {
                    key: key.clone(),
                    expected: None,
                }],
                vec![put(&key, template)?],
            )?)
            .await?
        {
            return Ok(());
        }
        let existing = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("template changed".into()))?;
        if existing.decode::<TemplateVersion>()? == *template {
            Ok(())
        } else {
            Err(Error::Conflict("template versions are immutable".into()))
        }
    }
    pub async fn template(
        &self,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<TemplateVersion>> {
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
    pub async fn environment(&self, scope: &Scope) -> Result<Option<Environment>> {
        self.store
            .get(&environment_key(scope)?)
            .await?
            .map(|r| r.decode())
            .transpose()
    }
    pub async fn list_environments(&self, query: &EnvironmentList) -> Result<EnvironmentPage> {
        query.validate().map_err(Error::Invalid)?;
        let index = Index::environments(&query.tenant, &query.template, &query.version)?;
        let scope = [
            query.tenant.clone(),
            query.template.clone(),
            query.version.clone(),
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
                if token.scope != scope || !token.after.starts_with("environment:") {
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
        let environments = records
            .into_iter()
            .map(|(key, record)| {
                let value: Environment = record.decode()?;
                if value.scope.tenant != query.tenant
                    || value.scope.template != query.template
                    || value.scope.version != query.version
                    || environment_key(&value.scope)?.as_str() != key
                {
                    return Err(Error::Corrupt("Environment index scope mismatch".into()));
                }
                Ok(value)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(EnvironmentPage {
            environments,
            next_page_token,
        })
    }
    /// Commit one stable Sandbox identity before any activation side effect.
    pub async fn create_environment(&self, scope: Scope) -> Result<Environment> {
        let key = environment_key(&scope)?;
        if self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?
            .is_none()
        {
            return Err(Error::Invalid("template version does not exist".into()));
        }
        let generation = uuid::Uuid::new_v4().to_string();
        let value = Environment {
            scope,
            sandbox_id: format!("adx-{generation}"),
            generation,
            phase: EnvironmentPhase::Active,
        };
        if self
            .store
            .commit(&Transaction::new(
                vec![Check {
                    key: key.clone(),
                    expected: None,
                }],
                vec![put(&key, &value)?],
            )?)
            .await?
        {
            return Ok(value);
        }
        let existing: Environment = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| Error::Conflict("environment changed".into()))?
            .decode()?;
        if existing.phase == EnvironmentPhase::Active {
            Ok(existing)
        } else {
            Err(Error::Conflict("environment is deleting".into()))
        }
    }
    pub async fn begin_delete(&self, scope: &Scope) -> Result<Environment> {
        let key = environment_key(scope)?;
        let generation = self
            .environment(scope)
            .await?
            .ok_or_else(|| Error::Conflict("environment missing".into()))?
            .generation;
        for _ in 0..limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("environment missing".into()))?;
            let mut value: Environment = record.decode()?;
            if value.generation != generation {
                return Err(Error::Conflict("environment lifecycle changed".into()));
            }
            if value.phase == EnvironmentPhase::Deleting {
                return Ok(value);
            }
            value.phase = EnvironmentPhase::Deleting;
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
        Err(Error::Conflict("environment deletion contention".into()))
    }
    /// Only after Platform confirms deletion. A late completion cannot erase a new lifecycle.
    pub async fn finish_delete(&self, environment: &Environment) -> Result<()> {
        let key = environment_key(&environment.scope)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(());
        };
        let current: Environment = record.decode()?;
        if current.generation != environment.generation {
            return Ok(());
        }
        if current.phase != EnvironmentPhase::Deleting
            || current.sandbox_id != environment.sandbox_id
        {
            return Err(Error::Conflict(
                "environment deletion identity mismatch".into(),
            ));
        }
        if self
            .store
            .commit(&Transaction::with_deletes(
                vec![check(&key, &record)],
                vec![],
                vec![key],
            )?)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict(
                "environment changed during deletion".into(),
            ))
        }
    }
}
