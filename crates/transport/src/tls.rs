//! Deployment-owned TLS files and shared HTTP/gRPC TLS builders.
use adx_protocol::auth::{Peers, Principal};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio_rustls::TlsAcceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    #[serde(default)]
    pub mode: crate::rpc::SecurityMode,
    #[serde(default)]
    pub ca: PathBuf,
    #[serde(default)]
    pub certificate: PathBuf,
    #[serde(default)]
    pub private_key: PathBuf,
    #[serde(default)]
    pub server_name: String,
    #[serde(default)]
    pub peers: BTreeMap<String, PathBuf>,
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
    let ca = client_ca.map(std::fs::read).transpose()?;
    http_server_acceptor_pem(
        &std::fs::read(certificate.into())?,
        &std::fs::read(private_key.into())?,
        ca.as_deref(),
        alpn_protocols,
    )
}

/// Same ADX TLS policy for callers that validate private files before parsing them.
/// Returns an error for empty/invalid PEM, mismatched keys or invalid client trust.
pub fn http_server_acceptor_pem(
    mut certificate: &[u8],
    mut private_key: &[u8],
    client_ca: Option<&[u8]>,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    crate::install_crypto_provider();
    let certificates = rustls_pemfile::certs(&mut certificate).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err("TLS certificate file is empty".into());
    }
    let key = rustls_pemfile::private_key(&mut private_key)?.ok_or("TLS private key is empty")?;
    let builder = rustls::ServerConfig::builder();
    let mut config = match client_ca {
        None => builder
            .with_no_client_auth()
            .with_single_cert(certificates, key)?,
        Some(mut client_ca) => {
            let ca_certificates =
                rustls_pemfile::certs(&mut client_ca).collect::<Result<Vec<_>, _>>()?;
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

/// HTTP client trust with optional mTLS identity. Callers retain endpoint/name validation.
/// Rejects empty/invalid CA material and invalid client identities; never disables verification.
pub fn http_client_config(
    mut ca: &[u8],
    identity: Option<(&[u8], &[u8])>,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<rustls::ClientConfig, Box<dyn std::error::Error + Send + Sync>> {
    crate::install_crypto_provider();
    let certificates = rustls_pemfile::certs(&mut ca).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err("TLS CA contains no certificates".into());
    }
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate)?;
    }
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    let mut config = match identity {
        None => builder.with_no_client_auth(),
        Some((mut certificate, mut key)) => {
            let certificates =
                rustls_pemfile::certs(&mut certificate).collect::<Result<Vec<_>, _>>()?;
            let key = rustls_pemfile::private_key(&mut key)?.ok_or("TLS client key is empty")?;
            builder.with_client_auth_cert(certificates, key)?
        }
    };
    config.alpn_protocols = alpn_protocols;
    Ok(config)
}
impl TlsFiles {
    pub fn load_rpc(
        &self,
        principal: Principal,
    ) -> Result<(Option<ServerTlsConfig>, crate::rpc::RpcClient, Peers), Box<dyn std::error::Error>>
    {
        if self.mode == crate::rpc::SecurityMode::Network {
            return Ok((
                None,
                crate::rpc::RpcClient::network(principal),
                Peers::network(),
            ));
        }
        let (server, client, peers) = self.load()?;
        Ok((Some(server), client.into(), peers))
    }

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
            let principal = match role.as_str() {
                "coordinator" => Principal::Coordinator,
                "apiserver" => Principal::ApiServer,
                "ingress" => Principal::Ingress,
                value if value.starts_with("node:") && value.len() > 5 => {
                    Principal::Node(value[5..].into())
                }
                _ => return Err("invalid TLS peer role".into()),
            };
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
