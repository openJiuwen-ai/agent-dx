//! Golden retirement protection and durable subject Binding pointers.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::{prewarm::PrewarmRecipe, AgentBinding, Scope};

pub(crate) fn history_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new("binding-history", &[&scope.tenant, &scope.binding_id])
}

#[derive(Serialize, Deserialize)]
struct Protection {
    proof: String,
    retiring: bool,
}

pub(crate) fn protection_key(tenant: &str, snapshot: &str) -> Result<Key> {
    adx_agent_core::identifier(tenant, "tenant").map_err(Error::Invalid)?;
    adx_agent_core::identifier(snapshot, "snapshot").map_err(Error::Invalid)?;
    if matches!(snapshot, "." | "..") || snapshot.contains(['/', '\\']) {
        return Err(Error::Invalid("invalid snapshot identity".into()));
    }
    Key::new("snapshot-protection", &[tenant, snapshot])
}

impl AgentState {
    /// Reserve a durable proof before contacting Platform. The retirement fence
    /// survives both lost responses and failed/abandoned publication attempts.
    pub async fn snapshot_protection(
        &self,
        tenant: &str,
        snapshot: &str,
    ) -> Result<Option<String>> {
        self.set_snapshot_protection(tenant, snapshot, false).await
    }

    async fn set_snapshot_protection(
        &self,
        tenant: &str,
        snapshot: &str,
        retire: bool,
    ) -> Result<Option<String>> {
        let key = protection_key(tenant, snapshot)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let stored = self.store.get(&key).await?;
            let mut protection: Protection = stored
                .as_ref()
                .map(Record::decode)
                .transpose()?
                .unwrap_or_else(|| Protection {
                    proof: uuid::Uuid::new_v4().to_string(),
                    retiring: false,
                });
            if protection.retiring && !retire {
                return Ok(None);
            }
            if stored.is_some() && protection.retiring == retire {
                return Ok(Some(protection.proof));
            }
            protection.retiring = retire;
            let predicate = Check {
                key: key.clone(),
                expected: stored.map(|record| record.revision),
            };
            if self
                .store
                .commit(&Transaction::new(
                    vec![predicate],
                    vec![put(&key, &protection)?],
                )?)
                .await?
            {
                return Ok(Some(protection.proof));
            }
        }
        Err(Error::Conflict("Golden snapshot protection changed".into()))
    }

    pub async fn begin_snapshot_retirement(&self, tenant: &str, snapshot: &str) -> Result<String> {
        // Fence all new Binding commits before scanning. Every new Golden
        // decision checks the same record revision in its Binding transaction.
        let proof = self
            .set_snapshot_protection(tenant, snapshot, true)
            .await?
            .ok_or_else(|| Error::Corrupt("retirement fence was not established".into()))?;
        let prefix = format!("binding:{}", adx_agent_core::encode_key_prefix(&[tenant]));
        let mut after = prefix.clone();
        loop {
            let page = self
                .store
                .page(&Index::all_bindings(), Some(&after), 128)
                .await?;
            for (key, record) in &page {
                if !key.starts_with(&prefix) {
                    return Ok(proof);
                }
                let binding: AgentBinding = record.decode()?;
                if binding.scope.tenant != tenant {
                    return Err(Error::Corrupt("Binding index tenant mismatch".into()));
                }
                let mut references = std::iter::once(&binding.current).chain(
                    binding
                        .secondary
                        .iter()
                        .map(|reference| &reference.environment),
                );
                if let Some(reference) =
                    references.find(|reference| reference.snapshot_id.as_deref() == Some(snapshot))
                {
                    return Err(Error::Conflict(format!(
                        "Golden snapshot retained by Binding Environment {}",
                        reference.environment_id
                    )));
                }
            }
            if page.len() < 128 {
                return Ok(proof);
            }
            if let Some((key, _)) = page.into_iter().last() {
                after = key;
            }
        }
    }

    /// Fence allocation with the same snapshot protection revision used by retirement.
    pub(crate) async fn select_snapshot(
        &self,
        tenant: &str,
        recipe: &PrewarmRecipe,
        checked_snapshot: Option<&str>,
        checks: &mut Vec<Check>,
    ) -> Result<Option<String>> {
        if checked_snapshot != Some(recipe.snapshot_id.as_str()) {
            return Ok(None);
        }
        let key = protection_key(tenant, &recipe.snapshot_id)?;
        let Some(record) = self.open_snapshot_protection(&key).await? else {
            return Ok(None);
        };
        checks.push(check(&key, &record));
        Ok(Some(recipe.snapshot_id.clone()))
    }

    pub(crate) async fn open_snapshot_protection(&self, key: &Key) -> Result<Option<Record>> {
        let Some(record) = self.store.get(key).await? else {
            return Ok(None);
        };
        let protection: Protection = record.decode()?;
        Ok((!protection.retiring).then_some(record))
    }

    /// Resolve the durable subject pointer with bounded point reads.
    pub async fn subject_binding(&self, scope: &Scope) -> Result<Option<AgentBinding>> {
        let history = self.store.get(&history_key(scope)?).await?;
        self.binding_from_history(scope, history.as_ref()).await
    }

    pub(crate) async fn binding_from_history(
        &self,
        scope: &Scope,
        history: Option<&Record>,
    ) -> Result<Option<AgentBinding>> {
        let prior = history
            .map(Record::decode::<Option<Scope>>)
            .transpose()?
            .flatten();
        if let Some(prior) = prior {
            if prior.tenant != scope.tenant || prior.binding_id != scope.binding_id {
                return Err(Error::Corrupt("binding history identity mismatch".into()));
            }
            if prior != *scope {
                if let Some(binding) = self.binding(&prior).await? {
                    return Ok(Some(binding));
                }
            }
        }
        self.binding(scope).await
    }
}
