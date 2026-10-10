//! Product metadata transactions. Sandbox lifecycle belongs to Platform.
use crate::*;
use adx_agent_core::activator::{BindingList, BindingPage};
use adx_agent_core::cache::BoundedCache;
use adx_agent_core::launch::{CredentialCipher, LaunchConfig};
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
    store: Arc<dyn Repository>,
    templates: Arc<Mutex<BoundedCache<Key, TemplateCell>>>,
    credential_cipher: Option<Arc<CredentialCipher>>,
}
pub(crate) fn binding_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new(
        "binding",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.binding_id,
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
            credential_cipher: None,
            templates: Arc::new(Mutex::new(BoundedCache::new(TEMPLATE_CACHE_ENTRIES))),
        }
    }
    pub fn with_credential_key(mut self, key: [u8; 32]) -> Self {
        self.credential_cipher = Some(Arc::new(CredentialCipher::new(key)));
        self
    }
    /// Optional for generic Agents; private model bindings fail closed when unconfigured.
    pub fn with_deployment_credentials(self) -> Result<Self> {
        let Some(path) = std::env::var_os("ADX_CREDENTIAL_KEY_FILE") else {
            return Ok(self);
        };
        let text = std::fs::read_to_string(path)
            .map_err(|_| Error::Unavailable("credential key file unreadable".into()))?;
        let text = text.trim();
        if text.len() != 64 || !text.is_ascii() {
            return Err(Error::Invalid(
                "credential key must be 64 hex characters".into(),
            ));
        }
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
                .map_err(|_| Error::Invalid("invalid credential key".into()))?;
        }
        Ok(self.with_credential_key(key))
    }
    fn cipher(&self) -> Result<&CredentialCipher> {
        self.credential_cipher.as_deref().ok_or_else(|| {
            Error::Unavailable("binding credential encryption is not configured".into())
        })
    }
    pub async fn launch_config(&self, binding: &AgentBinding) -> Result<Option<LaunchConfig>> {
        let key = launch_key(&binding.scope)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(None);
        };
        let encrypted: Vec<u8> = record.decode()?;
        let aad = serde_json::to_vec(&(binding.scope.clone(), &binding.generation))
            .map_err(|_| Error::Corrupt("invalid binding".into()))?;
        let plain = self
            .cipher()?
            .open(&aad, &encrypted)
            .map_err(|e| Error::Corrupt(e.into()))?;
        let launch: LaunchConfig = serde_json::from_slice(&plain)
            .map_err(|_| Error::Corrupt("invalid private launch configuration".into()))?;
        launch.validate().map_err(Error::Corrupt)?;
        Ok(Some(launch))
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
    pub async fn binding(&self, scope: &Scope) -> Result<Option<AgentBinding>> {
        self.store
            .get(&binding_key(scope)?)
            .await?
            .map(|r| r.decode())
            .transpose()
    }
    pub async fn list_bindings(&self, query: &BindingList) -> Result<BindingPage> {
        query.validate().map_err(Error::Invalid)?;
        let index = Index::bindings(&query.tenant, &query.template, &query.version)?;
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
                    || value.scope.version != query.version
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
    /// Commit one stable Sandbox identity before any activation side effect.
    pub async fn create_binding(&self, scope: Scope) -> Result<AgentBinding> {
        self.create_binding_with_launch(scope, None).await
    }
    pub async fn create_binding_with_launch(
        &self,
        scope: Scope,
        launch: Option<&LaunchConfig>,
    ) -> Result<AgentBinding> {
        if let Some(launch) = launch {
            launch.validate().map_err(Error::Invalid)?;
            self.cipher()?;
        }
        let key = binding_key(&scope)?;
        if self
            .template(&scope.tenant, &scope.template, &scope.version)
            .await?
            .is_none()
        {
            return Err(Error::Invalid("template version does not exist".into()));
        }
        let generation = uuid::Uuid::new_v4().to_string();
        let value = AgentBinding {
            scope,
            sandbox_id: format!("adx-{generation}"),
            generation,
            phase: BindingPhase::Active,
        };
        let private_key = launch_key(&value.scope)?;
        let mut checks = vec![
            Check {
                key: key.clone(),
                expected: None,
            },
            Check {
                key: private_key.clone(),
                expected: None,
            },
        ];
        let mut puts = vec![put(&key, &value)?];
        if let Some(launch) = launch {
            let aad = serde_json::to_vec(&(value.scope.clone(), &value.generation))
                .map_err(|_| Error::Invalid("invalid binding".into()))?;
            let plain = serde_json::to_vec(launch)
                .map_err(|_| Error::Invalid("invalid launch configuration".into()))?;
            let encrypted = self
                .cipher()?
                .seal(&aad, &plain)
                .map_err(|e| Error::Unavailable(e.into()))?;
            puts.push(put(&private_key, &encrypted)?);
        }
        if self
            .store
            .commit(&Transaction::new(std::mem::take(&mut checks), puts)?)
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
            if let Some(launch) = launch {
                if self.launch_config(&existing).await?.as_ref() != Some(launch) {
                    return Err(Error::Conflict(
                        "binding startup configuration is immutable".into(),
                    ));
                }
            }
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
    /// Only after Platform confirms deletion. A late completion cannot erase a new lifecycle.
    pub async fn finish_delete(&self, binding: &AgentBinding) -> Result<()> {
        let key = binding_key(&binding.scope)?;
        let Some(record) = self.store.get(&key).await? else {
            return Ok(());
        };
        let current: AgentBinding = record.decode()?;
        if current.generation != binding.generation {
            return Ok(());
        }
        if current.phase != BindingPhase::Deleting || current.sandbox_id != binding.sandbox_id {
            return Err(Error::Conflict("binding deletion identity mismatch".into()));
        }
        let private_key = launch_key(&binding.scope)?;
        let private = self.store.get(&private_key).await?;
        let mut checks = vec![check(&key, &record)];
        let mut deletes = vec![key];
        if let Some(record) = private {
            checks.push(check(&private_key, &record));
            deletes.push(private_key);
        }
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

fn launch_key(scope: &Scope) -> Result<Key> {
    Key::new(
        "binding-launch",
        &[
            &scope.tenant,
            &scope.template,
            &scope.version,
            &scope.binding_id,
        ],
    )
}
