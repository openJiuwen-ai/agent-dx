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
#[derive(Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CreateMode {
    #[default]
    Central,
    LocalFirst,
}
#[derive(Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeMode {
    #[default]
    Embedded,
    Standalone,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub environment: Option<adx_core::environment::EnvironmentSpec>,
    pub listen: SocketAddr,
    #[serde(default)]
    pub create_mode: CreateMode,
    #[serde(default)]
    pub loopback_http: bool,
    #[serde(default)]
    pub master_address: String,
    pub discovery: Option<Discovery>,
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub server_name: String,
    pub rpc_timeout_seconds: u64,
    pub cache_entries: usize,
    pub auth_cache_ttl_seconds: u64,
    #[serde(default)]
    pub agent_address: String,
    #[serde(default)]
    pub edge_mode: EdgeMode,
    #[serde(default)]
    pub edge_control: Option<data_plane_gateway::edge::master_routes::ControlConfig>,
}
impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(e) = &self.environment {
            e.validate()?;
        }
        if self.master_address.is_empty() == self.discovery.is_none()
            || self.server_name.is_empty()
            || self.rpc_timeout_seconds == 0
            || self.auth_cache_ttl_seconds == 0
            || self.cache_entries == 0
            || self.discovery.as_ref().is_some_and(|d| d.poll_seconds == 0)
        {
            return Err("positive RPC/cache bounds, TLS name and exactly one Master discovery source required".into());
        }
        if self.loopback_http && !self.listen.ip().is_loopback() {
            return Err("loopback_http requires literal loopback listen address".into());
        }
        match (self.edge_mode, self.edge_control.is_some()) {
            (EdgeMode::Embedded, false) => return Err("embedded Edge requires edge_control".into()),
            (EdgeMode::Standalone, true) => {
                return Err("standalone Edge must not configure edge_control".into())
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
        Ok(())
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_timeout_seconds)
    }
}
