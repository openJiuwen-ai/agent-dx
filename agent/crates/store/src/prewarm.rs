//! Golden retirement protection and durable subject Binding pointers.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::{AgentBinding, Scope};

pub(crate) fn history_key(scope: &Scope) -> Result<Key> {
    scope.validate().map_err(Error::Invalid)?;
    Key::new("binding-history", &[&scope.tenant, &scope.binding_id])
}

impl AgentState {
    /// Finalize the exact allocation whose owner was registered in Catalog. A retry observes
    /// the winning decision; a retired or replaced allocation cannot acquire a new decision.
    pub async fn finish_snapshot_decision(
        &self,
        saved: &AgentBinding,
        id: &str,
        snapshot: &str,
        acquired: bool,
    ) -> Result<AgentBinding> {
        let key = crate::state::binding_key(&saved.scope)?;
        for _ in 0..adx_agent_core::limits::CAS_ATTEMPTS {
            let record = self
                .store
                .get(&key)
                .await?
                .ok_or_else(|| Error::Conflict("allocation no longer exists".into()))?;
            let mut binding: AgentBinding = record.decode()?;
            if binding.generation != saved.generation
                || binding.phase != adx_agent_core::BindingPhase::Active
            {
                return Err(Error::Conflict("allocation retired".into()));
            }
            let target = if binding.current.environment_id == id {
                &mut binding.current
            } else {
                &mut binding
                    .secondary
                    .as_mut()
                    .filter(|secondary| {
                        secondary.environment.environment_id == id
                            && secondary.intent == adx_agent_core::SecondaryIntent::Preparing
                    })
                    .ok_or_else(|| Error::Conflict("candidate retired".into()))?
                    .environment
            };
            if target.pending_snapshot_id.is_none() {
                return Ok(binding);
            }
            if target.pending_snapshot_id.as_deref() != Some(snapshot) {
                return Err(Error::Conflict("snapshot decision changed".into()));
            }
            target.snapshot_id = if acquired {
                target.pending_snapshot_id.take()
            } else {
                target.pending_snapshot_id = None;
                None
            };
            if self
                .store
                .commit(&Transaction::new(
                    vec![check(&key, &record)],
                    vec![put(&key, &binding)?],
                )?)
                .await?
            {
                return Ok(binding);
            }
        }
        Err(Error::Conflict("snapshot decision contention".into()))
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
