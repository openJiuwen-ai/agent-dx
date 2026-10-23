//! Deployment ACL uses IPv4 CIDRs supported by the current sandboxd backend.
//! Deployment-owned network policy for template-created sandboxes.
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxNetworkPolicy {
    /// All sandbox address pools; explicit service exceptions may not overlap them.
    pub sandbox_cidrs: Vec<String>,
    /// Additional cluster/host networks excluded from public egress.
    pub internal_cidrs: Vec<String>,
    pub public_egress: bool,
    pub egress_allow: Vec<EgressAllow>,
    /// Explicit deployment choices; absent runtimes retain the compiled ACL.
    #[serde(default)]
    pub runtime_policies: BTreeMap<String, RuntimeNetworkPolicy>,
}

/// Selected by the operator, never inferred from a backend rejection.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeNetworkPolicy {
    Deployment,
    /// Omit ADX filtering for this runtime; native runtime policy still applies.
    Unrestricted,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressAllow {
    pub cidr: String,
    pub protocol: EgressProtocol,
    pub ports: Option<PortRange>,
}
#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EgressProtocol {
    Tcp,
    Udp,
    Icmp,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub first: u16,
    pub last: u16,
}
