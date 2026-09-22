//! Product metadata transactions. Sandbox lifecycle belongs to Platform.
use crate::*;
use adx_agent_core::{limits, Environment, EnvironmentPhase, Scope, TemplateVersion};
use std::sync::Arc;

#[derive(Clone)]
pub struct AgentState {
    store: Arc<dyn Repository>,
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
        Self { store }
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
        self.store
            .get(&Key::new("template", &[tenant, name, version])?)
            .await?
            .map(|r| r.decode())
            .transpose()
    }
    pub async fn environment(&self, scope: &Scope) -> Result<Option<Environment>> {
        self.store
            .get(&environment_key(scope)?)
            .await?
            .map(|r| r.decode())
            .transpose()
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
