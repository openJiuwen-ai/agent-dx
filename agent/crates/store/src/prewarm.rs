//! Tenant/product policy and durable first-assignment index.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::{prewarm::*, AgentBinding, Scope};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FirstAssignment {
    pub(crate) binding: Scope,
    pub(crate) initial: FirstDecision,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FirstDecision {
    pub(crate) environment: adx_agent_core::EnvironmentRef,
    pub(crate) binding_generation: String,
    pub(crate) policy_revision: Option<String>,
    pub(crate) registered_at_ms: Option<u64>,
}

pub(crate) fn policy_key(scope: &PolicyScope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new("prewarm-policy", &[&scope.tenant, &scope.product])
}

pub(crate) fn history_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new("binding-history", &[&scope.tenant, &scope.binding_id])
}

pub(crate) fn first_key(scope: &Scope, facts: &EligibleStart) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    adx_agent_core::identifier(&facts.product, "product").map_err(Error::Invalid)?;
    Key::new(
        "first-assignment",
        &[&scope.tenant, &facts.product, &scope.binding_id],
    )
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
    pub async fn snapshot_protection(&self, tenant: &str, snapshot: &str) -> Result<String> {
        self.set_snapshot_protection(tenant, snapshot, false).await
    }

    async fn set_snapshot_protection(
        &self,
        tenant: &str,
        snapshot: &str,
        retire: bool,
    ) -> Result<String> {
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
                return Err(Error::Conflict("Golden snapshot is retiring".into()));
            }
            if stored.is_some() && protection.retiring == retire {
                return Ok(protection.proof);
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
                return Ok(protection.proof);
            }
        }
        Err(Error::Conflict("Golden snapshot protection changed".into()))
    }

    pub async fn begin_snapshot_retirement(&self, tenant: &str, snapshot: &str) -> Result<String> {
        // Fence all new Binding commits before scanning. Every new Golden
        // decision checks the same record revision in its Binding transaction.
        let proof = self.set_snapshot_protection(tenant, snapshot, true).await?;
        let mut after = None;
        loop {
            let page = self
                .store
                .page(&Index::all_bindings(), after.as_deref(), 128)
                .await?;
            if page.is_empty() {
                return Ok(proof);
            }
            for (key, record) in page {
                after = Some(key);
                let binding: adx_agent_core::AgentBinding = record.decode()?;
                if binding.scope.tenant == tenant
                    && (binding.current.snapshot_id.as_deref() == Some(snapshot)
                        || binding.secondary.as_ref().is_some_and(|reference| {
                            reference.environment.snapshot_id.as_deref() == Some(snapshot)
                        }))
                {
                    return Err(Error::Conflict(format!(
                        "Golden snapshot retained by Binding Environment {}",
                        binding.current.environment_id
                    )));
                }
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

    pub async fn prewarm_policy(&self, scope: &PolicyScope) -> Result<Option<PolicySnapshot>> {
        self.store
            .get(&policy_key(scope)?)
            .await?
            .map(|record| {
                Ok(PolicySnapshot {
                    policy: record.decode()?,
                    revision: record.revision.as_str().into(),
                })
            })
            .transpose()
    }

    /// Atomically change the desired policy; never move a previously committed cutover.
    pub async fn update_prewarm_policy(
        &self,
        update: &PolicyUpdate,
        protected_snapshot: Option<&str>,
    ) -> Result<PolicySnapshot> {
        update.validate().map_err(Error::Invalid)?;
        let key = policy_key(&update.scope)?;
        let current = self.store.get(&key).await?;
        if current.as_ref().map(|v| v.revision.as_str()) != update.expected_revision.as_deref() {
            return Err(Error::Conflict("prewarm policy revision changed".into()));
        }
        let previous: Option<PrewarmPolicy> = current.as_ref().map(Record::decode).transpose()?;
        let enabled_at = update.enabled.then(adx_agent_core::unix_time_millis);
        let first = previous
            .as_ref()
            .and_then(|p| p.first_enabled_at_ms)
            .or(enabled_at);
        let assignment_tracking_since_ms = previous
            .and_then(|p| p.assignment_tracking_since_ms)
            .or(enabled_at);
        let policy = PrewarmPolicy {
            enabled: update.enabled,
            first_enabled_at_ms: first,
            assignment_tracking_since_ms,
            template: update.template.clone(),
            version: update.version.clone(),
            validation_ref: update.validation_ref.clone(),
        };
        let change = put(&key, &policy)?;
        let revision = change.record.revision.as_str().to_owned();
        let predicate = current.as_ref().map_or_else(
            || Check {
                key: key.clone(),
                expected: None,
            },
            |record| check(&key, record),
        );
        let mut checks = vec![predicate];
        if update.enabled {
            let snapshot = protected_snapshot.ok_or_else(|| {
                Error::Invalid("enabled policy requires Golden protection".into())
            })?;
            let key = protection_key(&update.scope.tenant, snapshot)?;
            let protection = self
                .open_snapshot_protection(&key)
                .await?
                .ok_or_else(|| Error::Conflict("Golden snapshot is retiring".into()))?;
            checks.push(check(&key, &protection));
        }
        if !self
            .store
            .commit(&Transaction::new(checks, vec![change])?)
            .await?
        {
            return Err(Error::Conflict("prewarm policy revision changed".into()));
        }
        Ok(PolicySnapshot { revision, policy })
    }

    /// At the first enablement with tracking, retain unique pre-existing
    /// DigitalMate scopes and fence ambiguous subject histories.
    pub async fn backfill_digitalmate_assignments(&self, tenant: &str) -> Result<()> {
        let mut scopes = std::collections::BTreeMap::<String, Option<Scope>>::new();
        let mut after = None;
        loop {
            let page = self
                .store
                .page(&Index::all_bindings(), after.as_deref(), 128)
                .await?;
            if page.is_empty() {
                break;
            }
            for (key, record) in page {
                after = Some(key);
                let binding: adx_agent_core::AgentBinding = record.decode()?;
                if binding.scope.tenant == tenant
                    && binding
                        .scope
                        .binding_id
                        .strip_prefix("digitalmate-")
                        .is_some_and(adx_agent_core::prewarm::workspace_digest)
                {
                    scopes
                        .entry(binding.scope.binding_id.clone())
                        .and_modify(|entry| *entry = None)
                        .or_insert(Some(binding.scope));
                }
            }
        }
        for (id, scope) in scopes {
            let key = Key::new("binding-history", &[tenant, &id])?;
            // An already-recorded creation/deletion wins the race with backfill.
            self.store
                .commit(&Transaction::new(
                    vec![Check {
                        key: key.clone(),
                        expected: None,
                    }],
                    vec![put(&key, &scope)?],
                )?)
                .await?;
        }
        Ok(())
    }

    /// Resolve an already assigned identity across all template references of this product.
    pub async fn product_binding_scope(
        &self,
        scope: &Scope,
        facts: &EligibleStart,
    ) -> Result<Scope> {
        if facts.product == "digitalmate" {
            let history = self.store.get(&history_key(scope)?).await?;
            return Ok(self
                .digitalmate_binding(scope, history.as_ref())
                .await?
                .map_or_else(|| scope.clone(), |binding| binding.scope));
        }
        let Some(record) = self.store.get(&first_key(scope, facts)?).await? else {
            return Ok(scope.clone());
        };
        let assignment: FirstAssignment = record.decode()?;
        if assignment.binding.tenant != scope.tenant
            || assignment.binding.binding_id != scope.binding_id
        {
            return Err(Error::Corrupt("first-assignment identity mismatch".into()));
        }
        if self.binding(&assignment.binding).await?.is_some() {
            Ok(assignment.binding)
        } else {
            // Keep the record as a tombstone even if the original Binding is gone.
            Ok(scope.clone())
        }
    }

    pub(crate) async fn digitalmate_binding(
        &self,
        scope: &Scope,
        history: Option<&Record>,
    ) -> Result<Option<AgentBinding>> {
        let prior = match history {
            Some(record) => record.decode::<Option<Scope>>()?,
            None => None,
        };
        if let Some(prior) = prior {
            if prior.tenant != scope.tenant || prior.binding_id != scope.binding_id {
                return Err(Error::Corrupt("binding history identity mismatch".into()));
            }
            if let Some(binding) = self.binding(&prior).await? {
                return Ok(Some(binding));
            }
        }
        if let Some(binding) = self.binding(scope).await? {
            return Ok(Some(binding));
        }

        // Replace the hex-encoded closing bracket with a comma to start at this
        // tenant's entries in the ordered index of Binding keys.
        let tenant_key = Key::new("binding", &[&scope.tenant])?;
        let tenant_prefix = format!(
            "{}2c",
            tenant_key
                .as_str()
                .strip_suffix("5d")
                .ok_or_else(|| Error::Corrupt("invalid Binding tenant key".into()))?
        );
        // Older writers had no pointer or kept the original scope after deletion.
        let mut candidate = None;
        let mut after = tenant_prefix.clone();
        let index = Index::all_bindings();
        loop {
            let records = self.store.page(&index, Some(&after), 128).await?;
            let finished = records.len() < 128;
            for (key, record) in records {
                if !key.starts_with(&tenant_prefix) {
                    return Ok(candidate);
                }
                after = key;
                let binding: AgentBinding = record.decode()?;
                if binding.scope.tenant == scope.tenant
                    && binding.scope.binding_id == scope.binding_id
                {
                    if candidate.is_some() {
                        return Err(Error::Conflict(
                            "multiple existing Bindings for the DigitalMate subject".into(),
                        ));
                    }
                    candidate = Some(binding);
                }
            }
            if finished {
                return Ok(candidate);
            }
        }
    }
}
