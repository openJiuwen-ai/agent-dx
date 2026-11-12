use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discovery {
    pub redis_url: String,
    pub namespace: String,
    #[serde(default = "poll")]
    pub poll_seconds: u64,
}
fn poll() -> u64 {
    2
}
fn unknown_create_retention() -> u64 {
    600
}
#[derive(Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CreateMode {
    #[default]
    Central,
    LocalFirst,
}
#[derive(Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngressMode {
    #[default]
    Embedded,
    Standalone,
}
fn workspace_affinity_timeout() -> u64 {
    3
}
/// Behavior when the Workspace directory cannot answer ownership queries.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAffinityErrorPolicy {
    /// Reject the request instead of placing an Environment without affinity.
    FailClosed,
    /// Fall back to ordinary scheduling when ownership cannot be resolved.
    #[default]
    FailOpen,
}
/// Optional Workspace ownership affinity; disabled when the section is absent.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceAffinity {
    /// Directory prefix that contains one child directory per Workspace owner.
    pub root: PathBuf,
    /// Workspace directory endpoints, tried in order. Empty selects the local node.
    #[serde(default)]
    pub endpoints: Vec<String>,
    #[serde(default = "workspace_affinity_timeout")]
    pub timeout_seconds: u64,
    /// Behavior when ownership lookup fails; defaults to `fail_open`.
    #[serde(default)]
    pub on_error: WorkspaceAffinityErrorPolicy,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub e2b: Option<crate::e2b::Config>,
    #[serde(default)]
    pub runtime_profile: Option<adx_core::runtime_profile::RuntimeProfile>,
    #[serde(default)]
    pub preparation_files: Option<adx_preparation_files::FileStorageConfig>,
    #[serde(default)]
    pub local_rootfs_templates:
        std::collections::BTreeMap<String, adx_core::local_rootfs::LocalRootfsTemplate>,
    pub listen: SocketAddr,
    /// Private Prometheus listener, independent of the public HTTP API.
    #[serde(default)]
    pub metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    pub create_mode: CreateMode,
    /// Persist a pre-execution Claim when local-first placement is enabled.
    #[serde(default)]
    pub persist_local_claim: bool,
    #[serde(default)]
    pub loopback_http: bool,
    #[serde(default)]
    pub coordinator_address: String,
    pub discovery: Option<Discovery>,
    #[serde(default)]
    pub internal_security: adx_transport::rpc::SecurityMode,
    #[serde(default)]
    pub ca: PathBuf,
    #[serde(default)]
    pub certificate: PathBuf,
    #[serde(default)]
    pub private_key: PathBuf,
    #[serde(default)]
    pub server_name: String,
    pub rpc_timeout_seconds: u64,
    pub cache_entries: usize,
    #[serde(default = "unknown_create_retention")]
    pub create_unknown_retention_seconds: u64,
    pub auth_cache_ttl_seconds: u64,
    #[serde(default)]
    pub agent_address: String,
    #[serde(default)]
    pub ingress_mode: IngressMode,
    #[serde(default)]
    pub ingress_control: Option<data_plane_gateway::ingress::coordinator_routes::ControlConfig>,
    #[serde(default)]
    pub workspace_affinity: Option<WorkspaceAffinity>,
}
impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(files) = &self.preparation_files {
            files.validate()?;
        }
        if let Some(e2b) = &self.e2b {
            if let Err(error) = e2b.validate(self.runtime_profile.as_ref()) {
                return Err(error.message.into());
            }
        }
        for (name, template) in &self.local_rootfs_templates {
            adx_core::local_rootfs::capability_label(name)?;
            template.validate()?;
        }
        if let Some(e) = &self.runtime_profile {
            e.validate()?;
        }
        if self.coordinator_address.is_empty() == self.discovery.is_none()
            || (self.internal_security == adx_transport::rpc::SecurityMode::Mtls
                && self.server_name.is_empty())
            || self.rpc_timeout_seconds == 0
            || self.auth_cache_ttl_seconds == 0
            || self.cache_entries == 0
            || self.create_unknown_retention_seconds == 0
            || self.discovery.as_ref().is_some_and(|d| d.poll_seconds == 0)
        {
            return Err("positive RPC/cache bounds, TLS name and exactly one Coordinator discovery source required".into());
        }
        if self.loopback_http && !self.listen.ip().is_loopback() {
            return Err("loopback_http requires literal loopback listen address".into());
        }
        match (self.ingress_mode, self.ingress_control.is_some()) {
            (IngressMode::Embedded, false) => {
                return Err("embedded Ingress requires ingress_control".into())
            }
            (IngressMode::Standalone, true) => {
                return Err("standalone Ingress must not configure ingress_control".into())
            }
            _ => {}
        }
        if !self.agent_address.is_empty() {
            let url = url::Url::parse(&self.agent_address)?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err("invalid Agent endpoint".into());
            }
        }
        if let Some(affinity) = &self.workspace_affinity {
            // A bare root would turn every absolute local mount into an owner.
            if !affinity.root.is_absolute() || affinity.root.as_os_str() == "/" {
                return Err("Workspace affinity root must be a concrete absolute directory".into());
            }
            if affinity.timeout_seconds == 0 {
                return Err("Workspace affinity requires a positive timeout".into());
            }
            for endpoint in &affinity.endpoints {
                let url = url::Url::parse(endpoint)?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    return Err("invalid Workspace directory endpoint".into());
                }
            }
        }
        Ok(())
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_timeout_seconds)
    }
}
