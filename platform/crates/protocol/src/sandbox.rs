use crate::control as pb;
use adx_core::{sandbox as core, Error, Result};

fn invalid(name: &str) -> Error {
    Error::Invalid(format!("invalid {name}"))
}

impl From<core::S3Source> for pb::S3Source {
    fn from(value: core::S3Source) -> Self {
        Self {
            endpoint: value.endpoint,
            bucket: value.bucket,
            object: value.object,
            access_key_id: value.access_key_id,
            access_key_secret: value.access_key_secret,
        }
    }
}
impl From<pb::S3Source> for core::S3Source {
    fn from(value: pb::S3Source) -> Self {
        Self {
            endpoint: value.endpoint,
            bucket: value.bucket,
            object: value.object,
            access_key_id: value.access_key_id,
            access_key_secret: value.access_key_secret,
        }
    }
}

impl From<core::StorageSource> for pb::StorageSource {
    fn from(value: core::StorageSource) -> Self {
        use pb::storage_source::Source;
        Self {
            source: Some(match value {
                core::StorageSource::Image(value) => Source::Image(value),
                core::StorageSource::S3(value) => Source::S3(value.into()),
                core::StorageSource::Local(value) => Source::Local(value),
            }),
        }
    }
}
impl TryFrom<pb::StorageSource> for core::StorageSource {
    type Error = Error;
    fn try_from(value: pb::StorageSource) -> Result<Self> {
        use pb::storage_source::Source;
        Ok(
            match value.source.ok_or_else(|| invalid("storage source"))? {
                Source::Image(value) => Self::Image(value),
                Source::S3(value) => Self::S3(value.into()),
                Source::Local(value) => Self::Local(value),
            },
        )
    }
}

impl From<core::Rootfs> for pb::RootfsSpec {
    fn from(value: core::Rootfs) -> Self {
        Self {
            readonly: value.readonly,
            source: Some(value.source.into()),
        }
    }
}
impl TryFrom<pb::RootfsSpec> for core::Rootfs {
    type Error = Error;
    fn try_from(value: pb::RootfsSpec) -> Result<Self> {
        Ok(Self {
            readonly: value.readonly,
            source: value
                .source
                .ok_or_else(|| invalid("rootfs source"))?
                .try_into()?,
        })
    }
}

impl From<core::Mount> for pb::MountSpec {
    fn from(value: core::Mount) -> Self {
        Self {
            r#type: value.kind,
            target: value.target,
            options: value.options,
            source: Some(value.source.into()),
        }
    }
}
impl TryFrom<pb::MountSpec> for core::Mount {
    type Error = Error;
    fn try_from(value: pb::MountSpec) -> Result<Self> {
        Ok(Self {
            kind: value.r#type,
            target: value.target,
            options: value.options,
            source: value
                .source
                .ok_or_else(|| invalid("mount source"))?
                .try_into()?,
        })
    }
}

fn action(value: i32) -> Result<core::NetworkAction> {
    match pb::NetworkAction::try_from(value).map_err(|_| invalid("network action"))? {
        pb::NetworkAction::Allow => Ok(core::NetworkAction::Allow),
        pb::NetworkAction::Deny => Ok(core::NetworkAction::Deny),
        _ => Err(invalid("network action")),
    }
}
fn action_pb(value: core::NetworkAction) -> i32 {
    match value {
        core::NetworkAction::Allow => pb::NetworkAction::Allow as i32,
        core::NetworkAction::Deny => pb::NetworkAction::Deny as i32,
    }
}
fn direction(value: i32) -> Result<core::NetworkDirection> {
    match pb::NetworkDirection::try_from(value).map_err(|_| invalid("network direction"))? {
        pb::NetworkDirection::Ingress => Ok(core::NetworkDirection::Ingress),
        pb::NetworkDirection::Egress => Ok(core::NetworkDirection::Egress),
        pb::NetworkDirection::Both => Ok(core::NetworkDirection::Both),
        _ => Err(invalid("network direction")),
    }
}
fn direction_pb(value: core::NetworkDirection) -> i32 {
    match value {
        core::NetworkDirection::Ingress => pb::NetworkDirection::Ingress as i32,
        core::NetworkDirection::Egress => pb::NetworkDirection::Egress as i32,
        core::NetworkDirection::Both => pb::NetworkDirection::Both as i32,
    }
}
fn protocol(value: i32) -> Result<core::NetworkProtocol> {
    match pb::NetworkProtocol::try_from(value).map_err(|_| invalid("network protocol"))? {
        pb::NetworkProtocol::Any => Ok(core::NetworkProtocol::Any),
        pb::NetworkProtocol::Tcp => Ok(core::NetworkProtocol::Tcp),
        pb::NetworkProtocol::Udp => Ok(core::NetworkProtocol::Udp),
        pb::NetworkProtocol::Icmp => Ok(core::NetworkProtocol::Icmp),
        _ => Err(invalid("network protocol")),
    }
}
fn protocol_pb(value: core::NetworkProtocol) -> i32 {
    match value {
        core::NetworkProtocol::Any => pb::NetworkProtocol::Any as i32,
        core::NetworkProtocol::Tcp => pb::NetworkProtocol::Tcp as i32,
        core::NetworkProtocol::Udp => pb::NetworkProtocol::Udp as i32,
        core::NetworkProtocol::Icmp => pb::NetworkProtocol::Icmp as i32,
    }
}
fn range(value: core::PortRange) -> pb::PortRange {
    pb::PortRange {
        first: value.first,
        last: value.last,
    }
}
fn range_core(value: pb::PortRange) -> core::PortRange {
    core::PortRange {
        first: value.first,
        last: value.last,
    }
}

impl From<core::NetworkRule> for pb::NetworkRule {
    fn from(value: core::NetworkRule) -> Self {
        Self {
            action: action_pb(value.action),
            direction: direction_pb(value.direction),
            protocol: protocol_pb(value.protocol),
            peer: Some(pb::NetworkPeer {
                address: value.peer.address,
                port: value.peer.port,
                cidr: value.peer.cidr,
                domain: value.peer.domain,
                port_range: value.peer.port_range.map(range),
            }),
            sandbox_port: value.sandbox_port,
            sandbox_port_range: value.sandbox_port_range.map(range),
            priority: value.priority,
        }
    }
}
impl TryFrom<pb::NetworkRule> for core::NetworkRule {
    type Error = Error;
    fn try_from(value: pb::NetworkRule) -> Result<Self> {
        let peer = value.peer.unwrap_or_default();
        Ok(Self {
            action: action(value.action)?,
            direction: direction(value.direction)?,
            protocol: protocol(value.protocol)?,
            peer: core::NetworkPeer {
                address: peer.address,
                port: peer.port,
                cidr: peer.cidr,
                domain: peer.domain,
                port_range: peer.port_range.map(range_core),
            },
            sandbox_port: value.sandbox_port,
            sandbox_port_range: value.sandbox_port_range.map(range_core),
            priority: value.priority,
        })
    }
}

impl From<core::NetworkPolicy> for pb::NetworkPolicy {
    fn from(value: core::NetworkPolicy) -> Self {
        Self {
            traffic: value.traffic.map(|traffic| pb::TrafficPolicy {
                ingress_default_action: action_pb(traffic.ingress_default_action),
                egress_default_action: action_pb(traffic.egress_default_action),
                rules: traffic.rules.into_iter().map(Into::into).collect(),
                mode: match traffic.mode {
                    core::TrafficMode::Stateless => pb::TrafficMode::Stateless as i32,
                    core::TrafficMode::Stateful => pb::TrafficMode::Stateful as i32,
                },
            }),
            dns: value.dns.map(|dns| pb::DnsPolicy {
                default_action: action_pb(dns.default_action),
                rules: dns
                    .rules
                    .into_iter()
                    .map(|rule| pb::DnsRule {
                        action: action_pb(rule.action),
                        pattern: rule.pattern,
                    })
                    .collect(),
            }),
        }
    }
}
impl TryFrom<pb::NetworkPolicy> for core::NetworkPolicy {
    type Error = Error;
    fn try_from(value: pb::NetworkPolicy) -> Result<Self> {
        Ok(Self {
            traffic: value
                .traffic
                .map(|traffic| {
                    Ok(core::TrafficPolicy {
                        ingress_default_action: action(traffic.ingress_default_action)?,
                        egress_default_action: action(traffic.egress_default_action)?,
                        rules: traffic
                            .rules
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect::<Result<_>>()?,
                        mode: match pb::TrafficMode::try_from(traffic.mode)
                            .map_err(|_| invalid("traffic mode"))?
                        {
                            pb::TrafficMode::Stateless => core::TrafficMode::Stateless,
                            pb::TrafficMode::Stateful => core::TrafficMode::Stateful,
                            _ => return Err(invalid("traffic mode")),
                        },
                    })
                })
                .transpose()?,
            dns: value
                .dns
                .map(|dns| {
                    Ok(core::DnsPolicy {
                        default_action: action(dns.default_action)?,
                        rules: dns
                            .rules
                            .into_iter()
                            .map(|rule| {
                                Ok(core::DnsRule {
                                    action: action(rule.action)?,
                                    pattern: rule.pattern,
                                })
                            })
                            .collect::<Result<_>>()?,
                    })
                })
                .transpose()?,
        })
    }
}

fn security(value: i32) -> Result<core::DataPlaneSecurityMode> {
    match pb::DataPlaneSecurityMode::try_from(value)
        .map_err(|_| invalid("data-plane security mode"))?
    {
        pb::DataPlaneSecurityMode::DataPlaneSecurityInherit => {
            Ok(core::DataPlaneSecurityMode::Inherit)
        }
        pb::DataPlaneSecurityMode::DataPlaneSecurityTls => Ok(core::DataPlaneSecurityMode::Tls),
        pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken => {
            Ok(core::DataPlaneSecurityMode::TlsToken)
        }
    }
}
fn security_pb(value: core::DataPlaneSecurityMode) -> i32 {
    match value {
        core::DataPlaneSecurityMode::Inherit => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityInherit as i32
        }
        core::DataPlaneSecurityMode::Tls => pb::DataPlaneSecurityMode::DataPlaneSecurityTls as i32,
        core::DataPlaneSecurityMode::TlsToken => {
            pb::DataPlaneSecurityMode::DataPlaneSecurityTlsToken as i32
        }
    }
}

impl From<core::SandboxOptions> for pb::SandboxOptions {
    fn from(value: core::SandboxOptions) -> Self {
        Self {
            rootfs: value.rootfs.map(Into::into),
            mounts: value.mounts.into_iter().map(Into::into).collect(),
            network: value.network.map(Into::into),
            data_plane: Some(pb::DataPlanePolicy {
                tunnel: security_pb(value.data_plane.tunnel),
                port_forward: security_pb(value.data_plane.port_forward),
            }),
            ports: value.ports.into_iter().map(u32::from).collect(),
            failover: value.failover,
            inherit_entrypoint: value.inherit_entrypoint,
            limits: Some(pb::ResourceLimits {
                cpu_millis: value.limits.cpu_millis,
                memory_bytes: value.limits.memory_bytes,
                disk_bytes: value.limits.disk_bytes,
            }),
            extra_config: value.extra_config,
        }
    }
}
impl TryFrom<pb::SandboxOptions> for core::SandboxOptions {
    type Error = Error;
    fn try_from(value: pb::SandboxOptions) -> Result<Self> {
        let data_plane = value.data_plane.unwrap_or_default();
        let limits = value.limits.unwrap_or_default();
        Ok(Self {
            rootfs: value.rootfs.map(TryInto::try_into).transpose()?,
            mounts: value
                .mounts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
            network: value.network.map(TryInto::try_into).transpose()?,
            data_plane: core::DataPlanePolicy {
                tunnel: security(data_plane.tunnel)?,
                port_forward: security(data_plane.port_forward)?,
            },
            ports: value
                .ports
                .into_iter()
                .map(|port| u16::try_from(port).map_err(|_| invalid("published port")))
                .collect::<Result<_>>()?,
            failover: value.failover,
            inherit_entrypoint: value.inherit_entrypoint,
            limits: core::ResourceLimits {
                cpu_millis: limits.cpu_millis,
                memory_bytes: limits.memory_bytes,
                disk_bytes: limits.disk_bytes,
            },
            extra_config: value.extra_config,
        })
    }
}
