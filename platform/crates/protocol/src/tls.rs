//! Deployment-owned TLS files; callers configure role identities explicitly.
use crate::auth::{Peers, Principal};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub server_name: String,
    pub peers: BTreeMap<String, PathBuf>,
}
impl TlsFiles {
    pub fn load(
        &self,
    ) -> Result<(ServerTlsConfig, ClientTlsConfig, Peers), Box<dyn std::error::Error>> {
        if self.server_name.trim().is_empty() {
            return Err("TLS server_name required".into());
        }
        let ca = Certificate::from_pem(std::fs::read(&self.ca)?);
        let identity = Identity::from_pem(
            std::fs::read(&self.certificate)?,
            std::fs::read(&self.private_key)?,
        );
        let mut peers = Vec::new();
        for (role, path) in &self.peers {
            let principal = match role.as_str() {
                "master" => Principal::Master,
                "api-server" => Principal::ApiServer,
                "edge" => Principal::Edge,
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
        Ok((
            ServerTlsConfig::new()
                .identity(identity.clone())
                .client_ca_root(ca.clone()),
            ClientTlsConfig::new()
                .identity(identity)
                .ca_certificate(ca)
                .domain_name(&self.server_name),
            Peers::new(peers),
        ))
    }
}
