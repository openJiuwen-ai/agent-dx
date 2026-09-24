//! Agent-only Activator membership and deterministic Env routing.
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

/// Rendezvous ordering is independent of discovery order and process-local hash seeds.
/// Hash fields are length-delimited; generation is deliberately excluded from the route key.
pub fn ranked_endpoints(scope: &Scope, endpoints: &[ActivatorEndpoint]) -> Vec<ActivatorEndpoint> {
    let key = crate::encode_key(&[
        &scope.tenant,
        &scope.template,
        &scope.version,
        &scope.environment_id,
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
