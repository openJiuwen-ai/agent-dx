//! Internal Gateway/Dispatcher wire types. Tenant is supplied by authenticated Gateway.
use crate::{Instance, Scope};
use serde::{Deserialize, Serialize};

/// Authenticated internal RPC deadline, shared across Gateway failover attempts.
pub const DEADLINE_HEADER: &str = "x-adx-deadline-ms";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveRequest {
    pub scope: Scope,
    pub affinity_key: Option<String>,
    #[serde(default)]
    pub bypass_cache: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub instance_id: String,
    pub sandbox_id: String,
    pub tenant: String,
    pub scope: Scope,
    pub session_generation: String,
}
impl Target {
    pub fn from_instance(instance: &Instance) -> Self {
        Self {
            instance_id: instance.id.clone(),
            sandbox_id: instance.sandbox_id.clone(),
            tenant: instance.tenant.clone(),
            scope: instance.scope.clone(),
            session_generation: instance.session_generation.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeRequest {
    pub scope: Scope,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseInstanceRequest {
    pub scope: Scope,
    pub instance_id: String,
}
