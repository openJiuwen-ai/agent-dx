//! TLS identity fields, trusted Node certificates and certificate parsing.

use super::{Config, invalid};
use afs_error::Result;

/// All four `tls_*` fields together, and absolute trusted certificate paths.
pub(super) fn validate(cfg: &Config) -> Result<()> {
    let tls_count = [
        cfg.tls_ca_certificate.is_some(),
        cfg.tls_identity_certificate.is_some(),
        cfg.tls_identity_private_key.is_some(),
        cfg.tls_server_name.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    if cfg.tls_encryption_enabled && tls_count == 0 {
        return Err(invalid(
            "tls_encryption_enabled requires all four tls_* identity fields",
        ));
    }
    if tls_count != 0 && tls_count != 4 {
        return Err(invalid("all four tls_* fields are required together"));
    }
    for path in cfg.trusted_node_certs.values() {
        if !path.is_absolute() {
            return Err(invalid("trusted_node_certs paths must be absolute"));
        }
    }
    Ok(())
}

impl Config {
    /// gRPC builders共用安全配置；只有配置了完整证书集合才启用 mTLS。
    /// Meta authority and node-to-node filesystem RPCs also require
    /// `trusted_node_certs` to bind a verified certificate to one Node ID.
    pub fn tls_config(&self) -> afs_transport::grpc::TlsConfig {
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
