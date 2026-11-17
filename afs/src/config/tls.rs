//! TLS identity fields, trusted Node certificates and certificate parsing.

use super::{Config, TlsMode, invalid};
use afs_error::Result;

/// Resolve the trust mode from CLI over TOML. An explicit `tls_mode` always
/// wins; without one the mode is `Mutual`, and a missing certificate set is
/// reported by [`validate`] as a missing certificate, never silently
/// downgraded.
pub(super) fn resolve_mode(cli: &super::Cli, file: &super::FileConfig) -> TlsMode {
    cli.tls_mode.or(file.tls_mode).unwrap_or(TlsMode::Mutual)
}

fn tls_field_count(cfg: &Config) -> usize {
    [
        cfg.tls_ca_certificate.is_some(),
        cfg.tls_identity_certificate.is_some(),
        cfg.tls_identity_private_key.is_some(),
        cfg.tls_server_name.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count()
}

/// All four `tls_*` fields together, and absolute trusted certificate paths.
pub(super) fn validate(role: super::Role, cfg: &Config) -> Result<()> {
    let tls_count = tls_field_count(cfg);
    match cfg.tls_mode {
        TlsMode::Mutual => {
            if cfg.tls_encryption_enabled && tls_count == 0 {
                return Err(invalid(
                    "tls_encryption_enabled requires all four tls_* identity fields",
                ));
            }
            if tls_count != 0 && tls_count != 4 {
                return Err(invalid("all four tls_* fields are required together"));
            }
            if !cfg.trusted_node_ips.is_empty() {
                return Err(invalid(
                    "trusted_node_ips is only valid with tls_mode = \"insecure\"",
                ));
            }
        }
        TlsMode::Insecure => validate_insecure(role, cfg, tls_count)?,
    }
    for path in cfg.trusted_node_certs.values() {
        if !path.is_absolute() {
            return Err(invalid("trusted_node_certs paths must be absolute"));
        }
    }
    Ok(())
}

/// `insecure` has no certificate identity, so any certificate setting is a
/// contradiction rather than a stronger configuration. Rejecting the mixture
/// keeps "how is a peer authenticated" answerable from one place.
fn validate_insecure(role: super::Role, cfg: &Config, tls_count: usize) -> Result<()> {
    if tls_count != 0 {
        return Err(invalid(
            "tls_mode = \"insecure\" cannot be combined with any tls_* field",
        ));
    }
    if cfg.tls_encryption_enabled {
        return Err(invalid(
            "tls_mode = \"insecure\" cannot enable tls_encryption_enabled",
        ));
    }
    if !cfg.trusted_node_certs.is_empty() {
        return Err(invalid(
            "tls_mode = \"insecure\" cannot be combined with trusted_node_certs; use trusted_node_ips",
        ));
    }
    for (node_id, address) in &cfg.trusted_node_ips {
        if !super::is_safe_node_id(node_id) {
            return Err(invalid(format!(
                "trusted_node_ips key {node_id:?} must contain 1..128 letters, digits, _ or -"
            )));
        }
        // The deployment boundary is IPv4 today: adxlet's own control-stream
        // check rejects a non-IPv4 literal for the same reason, so accepting
        // IPv6 here would create two different notions of a node address.
        if address.is_ipv6() {
            return Err(invalid(format!(
                "trusted_node_ips value for {node_id:?} must be an IPv4 address"
            )));
        }
    }
    // Only a Node observes the source address of an incoming peer, because the
    // peer authenticator runs on the Node-side services. Meta trusts the node
    // id a request declares in this mode, so requiring a table it never reads
    // would be documentation pretending to be enforcement.
    if role == super::Role::Node && (cfg.ownerfs || cfg.dfs) && cfg.trusted_node_ips.is_empty() {
        return Err(invalid(
            "tls_mode = \"insecure\" requires trusted_node_ips for peer identity",
        ));
    }
    let mut seen: std::collections::HashMap<std::net::IpAddr, &String> =
        std::collections::HashMap::new();
    for (node_id, address) in &cfg.trusted_node_ips {
        if let Some(previous) = seen.insert(*address, node_id) {
            return Err(invalid(format!(
                "trusted_node_ips maps the same address to both {previous} and {node_id}"
            )));
        }
    }
    // `TlsConfig::Disabled` returns the endpoint URI unchanged, so an https://
    // endpoint would only fail when the pool dials it. Reject it at startup.
    for (name, endpoint) in [
        ("meta_endpoint", &cfg.meta_endpoint),
        ("advertise_endpoint", &cfg.advertise_endpoint),
        ("peer_endpoint", &cfg.peer_endpoint),
    ] {
        if let Some(endpoint) = endpoint
            && !endpoint.starts_with("http://")
        {
            return Err(invalid(format!(
                "{name} must be an http:// URI with tls_mode = \"insecure\""
            )));
        }
    }
    Ok(())
}

impl Config {
    /// gRPC builders共用安全配置；只有配置了完整证书集合才启用 mTLS。
    /// Meta authority and node-to-node filesystem RPCs also require
    /// `trusted_node_certs` to bind a verified certificate to one Node ID.
    pub fn tls_config(&self) -> afs_transport::grpc::TlsConfig {
        if self.tls_mode == TlsMode::Insecure {
            return afs_transport::grpc::TlsConfig::Disabled;
        }
        match (
            &self.tls_ca_certificate,
            &self.tls_identity_certificate,
            &self.tls_identity_private_key,
            &self.tls_server_name,
        ) {
            (Some(ca), Some(cert), Some(key), Some(server_name)) => {
                afs_transport::grpc::TlsConfig::MutualTls {
                    encryption_enabled: self.tls_encryption_enabled,
                    ca_certificate: ca.clone(),
                    identity_certificate: cert.clone(),
                    identity_private_key: key.clone(),
                    server_name: server_name.clone(),
                }
            }
            _ => afs_transport::grpc::TlsConfig::Disabled,
        }
    }
}

/// Normalize a configured certificate file to the leaf DER identity exposed by tonic.
/// Explicit service launchers reuse this parser instead of maintaining a second decoder.
pub fn read_certificate_der(path: &std::path::Path) -> Result<Vec<u8>> {
    let bytes = std::fs::read(path).map_err(|source| {
        invalid(format!(
            "failed to read trusted node cert {}: {source}",
            path.display()
        ))
    })?;
    if let Some(der) = first_pem_certificate_der(&bytes)? {
        Ok(der)
    } else if bytes.is_empty() {
        Err(invalid(format!(
            "trusted node cert {} is empty",
            path.display()
        )))
    } else {
        Ok(bytes)
    }
}

fn first_pem_certificate_der(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => return Ok(None),
    };
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let Some(begin) = text.find(BEGIN) else {
        return Ok(None);
    };
    let body_start = begin + BEGIN.len();
    let Some(relative_end) = text[body_start..].find(END) else {
        return Err(invalid("PEM certificate is missing END CERTIFICATE"));
    };
    let body = text[body_start..body_start + relative_end]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect::<String>();
    Ok(Some(decode_base64(&body)?))
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0u8;
    let mut padding = false;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => u32::from(byte - b'A'),
            b'a'..=b'z' => u32::from(byte - b'a' + 26),
            b'0'..=b'9' => u32::from(byte - b'0' + 52),
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                padding = true;
                continue;
            }
            _ => return Err(invalid("PEM certificate contains invalid base64")),
        };
        if padding {
            return Err(invalid("PEM certificate has data after base64 padding"));
        }
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    if out.is_empty() {
        return Err(invalid("PEM certificate has empty base64 body"));
    }
    Ok(out)
}
