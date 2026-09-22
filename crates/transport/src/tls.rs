//! Deployment-owned TLS files and shared HTTP/gRPC TLS builders.
use adx_protocol::auth::{Peers, Principal};
use serde::Deserialize;
use std::{collections::BTreeMap, fs::File, io::BufReader, path::PathBuf, sync::Arc};
use tokio_rustls::TlsAcceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub server_name: String,
    pub peers: BTreeMap<String, PathBuf>,
}

fn peer_principal(role: &str) -> Result<Principal, &'static str> {
    match role {
        "master" => Ok(Principal::Master),
        "api-server" => Ok(Principal::ApiServer),
        "edge" => Ok(Principal::Edge),
        "node-pool" => Ok(Principal::NodePool),
        value if value.starts_with("node:") && value.len() > 5 => {
            Ok(Principal::Node(value[5..].into()))
        }
        _ => Err("invalid TLS peer role"),
    }
}

pub fn grpc_client_config(
    ca: impl Into<PathBuf>,
    certificate: impl Into<PathBuf>,
    private_key: impl Into<PathBuf>,
    server_name: &str,
) -> Result<ClientTlsConfig, Box<dyn std::error::Error>> {
    crate::install_crypto_provider();
    if server_name.trim().is_empty() {
        return Err("TLS server_name required".into());
    }
    Ok(ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(std::fs::read(ca.into())?))
        .identity(Identity::from_pem(
            std::fs::read(certificate.into())?,
            std::fs::read(private_key.into())?,
        ))
        .domain_name(server_name))
}

/// Load one rustls server identity with optional client certificate
/// verification. Callers select ALPN explicitly for HTTP/1.1 or HTTP/2.
pub fn http_server_acceptor(
    certificate: impl Into<PathBuf>,
    private_key: impl Into<PathBuf>,
    client_ca: Option<PathBuf>,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    crate::install_crypto_provider();
    let certificate = certificate.into();
    let private_key = private_key.into();
    let mut cert_reader = BufReader::new(File::open(certificate)?);
    let certificates = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err("TLS certificate file is empty".into());
    }
    let mut key_reader = BufReader::new(File::open(private_key)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or("TLS private key is empty")?;
    let builder = rustls::ServerConfig::builder();
    let mut config = match client_ca {
        None => builder
            .with_no_client_auth()
            .with_single_cert(certificates, key)?,
        Some(client_ca) => {
            let mut ca_reader = BufReader::new(File::open(client_ca)?);
            let ca_certificates =
                rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
            if ca_certificates.is_empty() {
                return Err("TLS client CA file is empty".into());
            }
            let mut roots = rustls::RootCertStore::empty();
            for certificate in ca_certificates {
                roots.add(certificate)?;
            }
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, key)?
        }
    };
    config.alpn_protocols = alpn_protocols;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
impl TlsFiles {
    pub fn load(
        &self,
    ) -> Result<(ServerTlsConfig, ClientTlsConfig, Peers), Box<dyn std::error::Error>> {
        crate::install_crypto_provider();
        if self.server_name.trim().is_empty() {
            return Err("TLS server_name required".into());
        }
        let identity = Identity::from_pem(
            std::fs::read(&self.certificate)?,
            std::fs::read(&self.private_key)?,
        );
        let mut peers = Vec::new();
        for (role, path) in &self.peers {
            let principal = peer_principal(role)?;
            peers.push((std::fs::read(path)?, principal));
        }
        if peers.is_empty() {
            return Err("TLS peer identities required".into());
        }
        let client = grpc_client_config(
            &self.ca,
            &self.certificate,
            &self.private_key,
            &self.server_name,
        )?;
        Ok((
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(Certificate::from_pem(std::fs::read(&self.ca)?)),
            client,
            Peers::new(peers),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::peer_principal;
    use adx_protocol::auth::Principal;

    #[test]
    fn parses_exact_and_pool_node_roles() {
        assert_eq!(
            peer_principal("node:a").unwrap(),
            Principal::Node("a".into())
        );
        assert_eq!(peer_principal("node-pool").unwrap(), Principal::NodePool);
        assert!(peer_principal("node:").is_err());
        assert!(peer_principal("node").is_err());
    }
}
