//! Private parent-selected management-session bootstrap; no binary policy.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessBootstrap {
    pub owner: crate::control::MaintenanceOwner,
    pub node_session: String,
    pub deadline_unix_millis: u64,
}
