use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Source {
    pub endpoint: String,
    pub bucket: String,
    pub object: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub access_key_secret: String,
}

impl S3Source {
    pub fn validate(&self) -> Result<()> {
        if [&self.endpoint, &self.bucket, &self.object]
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(Error::Invalid(
                "S3 endpoint, bucket and object are required".into(),
            ));
        }
        if self.access_key_id.is_empty() != self.access_key_secret.is_empty() {
            return Err(Error::Invalid(
                "S3 access key and secret must be supplied together".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageSource {
    Image(String),
    S3(S3Source),
    Local(String),
}

impl StorageSource {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Image(value) | Self::Local(value) if value.trim().is_empty() => {
                Err(Error::Invalid("storage source is required".into()))
            }
            Self::S3(value) => value.validate(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rootfs {
    pub readonly: bool,
    pub source: StorageSource,
}

impl Rootfs {
    pub fn validate(&self) -> Result<()> {
        self.source.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    pub kind: String,
    pub target: String,
    #[serde(default)]
    pub options: Vec<String>,
    pub source: StorageSource,
}

impl Mount {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.kind.as_str(), "bind" | "erofs" | "tmpfs")
            || !self.target.starts_with('/')
            || self.target.contains("/../")
            || self.target.ends_with("/..")
            || self.options.iter().any(|value| value.trim().is_empty())
        {
            return Err(Error::Invalid("invalid sandbox mount".into()));
        }
        self.source.validate()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkAction {
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkDirection {
    Ingress,
    Egress,
    #[default]
    Both,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkProtocol {
    #[default]
    Any,
    Tcp,
    Udp,
    Icmp,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrafficMode {
    Stateless,
    #[default]
    Stateful,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortRange {
    pub first: u32,
    pub last: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPeer {
    pub address: String,
    pub port: u32,
    pub cidr: String,
    pub domain: String,
    pub port_range: Option<PortRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkRule {
    pub action: NetworkAction,
    pub direction: NetworkDirection,
    pub protocol: NetworkProtocol,
    #[serde(default)]
    pub peer: NetworkPeer,
    pub sandbox_port: u32,
    pub sandbox_port_range: Option<PortRange>,
    pub priority: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficPolicy {
    pub ingress_default_action: NetworkAction,
    pub egress_default_action: NetworkAction,
    #[serde(default)]
    pub rules: Vec<NetworkRule>,
    pub mode: TrafficMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsRule {
    pub action: NetworkAction,
    pub pattern: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsPolicy {
    pub default_action: NetworkAction,
    #[serde(default)]
    pub rules: Vec<DnsRule>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPolicy {
    pub traffic: Option<TrafficPolicy>,
    pub dns: Option<DnsPolicy>,
}

impl NetworkPolicy {
    pub fn validate(&self) -> Result<()> {
        let port = |value: u32| value <= u16::MAX as u32;
        if let Some(traffic) = &self.traffic {
            for rule in &traffic.rules {
                if rule.priority == 0
                    || rule.priority == u32::MAX
                    || !port(rule.peer.port)
                    || !port(rule.sandbox_port)
                    || rule.peer.port_range.as_ref().is_some_and(|range| {
                        range.first == 0 || range.first > range.last || !port(range.last)
                    })
                    || rule.sandbox_port_range.as_ref().is_some_and(|range| {
                        range.first == 0 || range.first > range.last || !port(range.last)
                    })
                {
                    return Err(Error::Invalid(
                        "invalid network policy rule priority or port".into(),
                    ));
                }
            }
        }
        if self
            .dns
            .as_ref()
            .is_some_and(|dns| dns.rules.iter().any(|rule| rule.pattern.trim().is_empty()))
        {
            return Err(Error::Invalid("invalid DNS policy pattern".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_with_priority(priority: u32) -> NetworkPolicy {
        NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Allow,
                rules: vec![NetworkRule {
                    action: NetworkAction::Allow,
                    direction: NetworkDirection::Ingress,
                    protocol: NetworkProtocol::Tcp,
                    peer: NetworkPeer::default(),
                    sandbox_port: 8080,
                    sandbox_port_range: None,
                    priority,
                }],
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        }
    }

    #[test]
    fn network_policy_reserves_zero_and_maximum_rule_priorities() {
        assert!(policy_with_priority(1).validate().is_ok());
        assert!(policy_with_priority(u32::MAX - 1).validate().is_ok());
        assert!(policy_with_priority(0).validate().is_err());
        assert!(policy_with_priority(u32::MAX).validate().is_err());
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataPlaneSecurityMode {
    #[default]
    Inherit,
    Tls,
    TlsToken,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataPlanePolicy {
    pub tunnel: DataPlaneSecurityMode,
    pub port_forward: DataPlaneSecurityMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxOptions {
    pub rootfs: Option<Rootfs>,
    #[serde(default)]
    pub mounts: Vec<Mount>,
    pub network: Option<NetworkPolicy>,
    #[serde(default)]
    pub data_plane: DataPlanePolicy,
    #[serde(default)]
    pub ports: Vec<u16>,
    pub failover: bool,
    pub inherit_entrypoint: bool,
    #[serde(default)]
    pub limits: ResourceLimits,
    #[serde(default)]
    pub extra_config: String,
}

impl SandboxOptions {
    pub fn validate(&self, requests: &crate::Resources) -> Result<()> {
        if let Some(rootfs) = &self.rootfs {
            rootfs.validate()?;
        }
        for mount in &self.mounts {
            mount.validate()?;
        }
        if let Some(network) = &self.network {
            network.validate()?;
        }
        if self.ports.contains(&0) {
            return Err(Error::Invalid("published port must be positive".into()));
        }
        let limits = &self.limits;
        if (limits.cpu_millis != 0 && limits.cpu_millis < requests.cpu_millis)
            || (limits.memory_bytes != 0 && limits.memory_bytes < requests.memory_bytes)
            || (limits.disk_bytes != 0 && limits.disk_bytes < requests.disk_bytes)
        {
            return Err(Error::Invalid(
                "execution limits cannot be lower than resource requests".into(),
            ));
        }
        if self.inherit_entrypoint
            && !matches!(
                self.rootfs.as_ref().map(|rootfs| &rootfs.source),
                Some(StorageSource::Image(_))
            )
        {
            return Err(Error::Invalid(
                "inherited entrypoint requires an image rootfs".into(),
            ));
        }
        Ok(())
    }
}
