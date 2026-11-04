//! Agent-only Activator membership and deterministic AgentBinding routing.
use crate::{transport, Scope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatorEndpoint {
    pub id: String,
    pub url: String,
}
impl ActivatorEndpoint {
    pub fn validate(&self, allow_plaintext: bool) -> crate::ValidationResult {
        crate::identifier(&self.id, "Activator instance id")?;
        transport::service_origin(&self.url, allow_plaintext)?;
        Ok(())
    }
}

/// Membership condition carried in registry records and honored by Gateway
/// member selection. Fields absent from records written by older Activators
/// default to ready-and-not-draining, so old and new versions interoperate
/// during a rolling upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberCondition {
    #[serde(default = "ready_by_default")]
    pub ready: bool,
    #[serde(default)]
    pub draining: bool,
}
fn ready_by_default() -> bool {
    true
}
impl Default for MemberCondition {
    fn default() -> Self {
        Self {
            ready: true,
            draining: false,
        }
    }
}
impl MemberCondition {
    /// A member is selectable only when initialized (ready) and not draining.
    pub fn selectable(&self) -> bool {
        self.ready && !self.draining
    }
}

/// One registry member: its endpoint plus the readiness condition that
/// Gateway-side selection filters on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub endpoint: ActivatorEndpoint,
    pub condition: MemberCondition,
}

/// Rendezvous ordering is independent of discovery order and process-local hash seeds.
/// Hash fields are length-delimited; generation is deliberately excluded from the route key.
pub fn ranked_endpoints(scope: &Scope, endpoints: &[ActivatorEndpoint]) -> Vec<ActivatorEndpoint> {
    let key = crate::encode_key(&[
        &scope.tenant,
        &scope.template,
        scope.template_ref.kind(),
        scope.template_ref.name(),
        &scope.binding_id,
    ]);
    let mut ranked: Vec<_> = endpoints
        .iter()
        .map(|endpoint| {
            let mut hash = Sha256::new();
            hash.update(key.as_bytes());
            hash.update([0]);
            hash.update(endpoint.id.as_bytes());
            (hash.finalize(), endpoint)
        })
        .collect();
    ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));
    ranked
        .into_iter()
        .map(|(_, endpoint)| endpoint.clone())
        .collect()
}
