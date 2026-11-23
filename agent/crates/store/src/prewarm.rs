//! Golden snapshot decisions.
use crate::{
    state::{check, put},
    *,
};
use adx_agent_core::AgentBinding;

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
}
