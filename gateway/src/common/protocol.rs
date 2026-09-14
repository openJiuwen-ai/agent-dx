use http::HeaderMap;
use ipnet::IpNet;
use std::net::{IpAddr, SocketAddr};
use thiserror::Error;

// Canonical documentation spelling follows Frontend's existing style
// (`X-Adx-Instance-Id`, `X-Request-Id`). HTTP field names are case-insensitive,
// and RFC 9113 requires their HTTP/2 wire encoding to be lowercase, so these
// constants intentionally contain the wire spelling used by h2/http crates.
pub const H_INSTANCE_ID: &str = "x-adx-instance-id";
pub const H_WORKLOAD_ID: &str = "x-adx-workload-id";
pub const H_TARGET_IP: &str = "x-adx-target-ip";
pub const H_TARGET_PORT: &str = "x-adx-target-port";
pub const H_REQUEST_ID: &str = "x-request-id";
pub const H_ACCESS_KIND: &str = "x-adx-access-kind";
pub const H_ACTIVITY_CLASS: &str = "x-adx-activity-class";
pub const ACTIVITY_CLASS_PASSIVE: &str = "passive";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectTarget {
    pub instance_id: String,
    pub workload_id: String,
    pub target_ip: IpAddr,
    pub target_port: u16,
    pub request_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("missing or invalid header {0}")]
    InvalidHeader(&'static str),
    #[error("target is outside the gateway's allowed networks")]
    TargetOutsideAllowedNetworks,
    #[error("target address is reserved")]
    ReservedTarget,
}

fn header_text<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, ProtocolError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .ok_or(ProtocolError::InvalidHeader(name))
}

impl ConnectTarget {
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, ProtocolError> {
        let instance_id = header_text(headers, H_INSTANCE_ID)?.to_owned();
        let workload_id = header_text(headers, H_WORKLOAD_ID)?.to_owned();
        let target_ip = header_text(headers, H_TARGET_IP)?
            .parse()
            .map_err(|_| ProtocolError::InvalidHeader(H_TARGET_IP))?;
        let target_port = header_text(headers, H_TARGET_PORT)?
            .parse::<u16>()
            .ok()
            .filter(|v| *v != 0)
            .ok_or(ProtocolError::InvalidHeader(H_TARGET_PORT))?;
        let request_id = headers
            .get(H_REQUEST_ID)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        Ok(Self {
            instance_id,
            workload_id,
            target_ip,
            target_port,
            request_id,
        })
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.target_ip, self.target_port)
    }
}

#[derive(Debug, Clone)]
pub struct GatewayPolicy {
    allowed_networks: Vec<IpNet>,
    allow_reserved_targets: bool,
}

impl GatewayPolicy {
    pub fn new(allowed_networks: Vec<IpNet>) -> Self {
        Self {
            allowed_networks,
            allow_reserved_targets: false,
        }
    }

    #[cfg(feature = "mock-e2e")]
    pub fn for_local_mock(allowed_networks: Vec<IpNet>) -> Self {
        Self {
            allowed_networks,
            allow_reserved_targets: true,
        }
    }

    pub fn validate(&self, target: &ConnectTarget) -> Result<(), ProtocolError> {
        if !self
            .allowed_networks
            .iter()
            .any(|network| network.contains(&target.target_ip))
        {
            return Err(ProtocolError::TargetOutsideAllowedNetworks);
        }
        if !self.allow_reserved_targets
            && (target.target_ip.is_loopback()
                || target.target_ip.is_unspecified()
                || match target.target_ip {
                    IpAddr::V4(addr) => addr.is_link_local(),
                    IpAddr::V6(addr) => addr.is_unicast_link_local(),
                }
                || is_metadata_address(target.target_ip))
        {
            return Err(ProtocolError::ReservedTarget);
        }
        Ok(())
    }
}

fn is_metadata_address(addr: IpAddr) -> bool {
    matches!(addr, IpAddr::V4(v4) if v4.octets() == [169, 254, 169, 254])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(H_INSTANCE_ID, "inst".parse().unwrap());
        h.insert(H_WORKLOAD_ID, "sb".parse().unwrap());
        h.insert(H_TARGET_IP, "10.0.0.2".parse().unwrap());
        h.insert(H_TARGET_PORT, "22".parse().unwrap());
        h
    }

    #[test]
    fn parses_connect_metadata() {
        let target = ConnectTarget::from_headers(&headers()).unwrap();
        assert_eq!(target.socket_addr().to_string(), "10.0.0.2:22");
    }

    #[test]
    fn h2_wire_headers_share_the_lowercase_x_adx_namespace() {
        for name in [H_INSTANCE_ID, H_WORKLOAD_ID, H_TARGET_IP, H_TARGET_PORT] {
            assert!(
                name.starts_with("x-adx-"),
                "unexpected header namespace: {name}"
            );
        }
        assert_eq!(H_REQUEST_ID, "x-request-id");
    }

    #[test]
    fn accepts_full_tcp_port_range() {
        let mut h = headers();
        h.insert(H_TARGET_PORT, "65535".parse().unwrap());
        assert_eq!(ConnectTarget::from_headers(&h).unwrap().target_port, 65535);
        h.insert(H_TARGET_PORT, "0".parse().unwrap());
        assert!(ConnectTarget::from_headers(&h).is_err());
    }

    #[test]
    fn rejects_unbounded_target() {
        let target = ConnectTarget::from_headers(&headers()).unwrap();
        let policy = GatewayPolicy::new(vec!["10.0.0.0/24".parse().unwrap()]);
        assert!(policy.validate(&target).is_ok());
        let mut bad = target;
        bad.target_ip = "127.0.0.1".parse().unwrap();
        assert!(matches!(
            policy.validate(&bad),
            Err(ProtocolError::TargetOutsideAllowedNetworks)
        ));
    }
}
