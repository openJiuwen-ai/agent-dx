//! Tenant/product policy and durable first-assignment index.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::{prewarm::*, Scope};

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

pub(crate) fn first_key(scope: &Scope, facts: &EligibleStart) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    adx_agent_core::identifier(&facts.product, "product").map_err(Error::Invalid)?;
    Key::new(
        "first-assignment",
        &[&scope.tenant, &facts.product, &scope.binding_id],
    )
}

impl AgentState {
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
    pub async fn update_prewarm_policy(&self, update: &PolicyUpdate) -> Result<PolicySnapshot> {
        update.validate().map_err(Error::Invalid)?;
        let key = policy_key(&update.scope)?;
        let current = self.store.get(&key).await?;
        if current.as_ref().map(|v| v.revision.as_str()) != update.expected_revision.as_deref() {
            return Err(Error::Conflict("prewarm policy revision changed".into()));
        }
        let first = current
            .as_ref()
            .map(Record::decode::<PrewarmPolicy>)
            .transpose()?
            .and_then(|p| p.first_enabled_at_ms)
            .or_else(|| update.enabled.then(adx_agent_core::unix_time_millis));
        let policy = PrewarmPolicy {
            enabled: update.enabled,
            first_enabled_at_ms: first,
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
        if !self
            .store
            .commit(&Transaction::new(vec![predicate], vec![change])?)
            .await?
        {
            return Err(Error::Conflict("prewarm policy revision changed".into()));
        }
        Ok(PolicySnapshot { revision, policy })
    }

    /// Resolve an already assigned identity across all template references of this product.
    pub async fn product_binding_scope(
        &self,
        scope: &Scope,
        facts: &EligibleStart,
    ) -> Result<Scope> {
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
}
