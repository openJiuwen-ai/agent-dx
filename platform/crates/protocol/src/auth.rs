//! Component identity: verified certificates by default; explicit network mode
//! accepts a caller declaration on deployment-isolated internal listeners.
use crate::control::CallerContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tonic::{Request, Status};
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMode {
    #[default]
    Mtls,
    Network,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    Coordinator,
    ApiServer,
    Ingress,
    Node(String),
}
#[derive(Clone, Default)]
pub struct Peers {
    certificates: Arc<BTreeMap<Vec<u8>, Principal>>,
    network: bool,
}
impl Peers {
    /// DER leaf certificates are supplied by the deployment alongside the CA.
    pub fn new(certificates: impl IntoIterator<Item = (Vec<u8>, Principal)>) -> Self {
        Self {
            certificates: Arc::new(
                certificates
                    .into_iter()
                    .map(|(der, p)| (Sha256::digest(der).to_vec(), p))
                    .collect(),
            ),
            network: false,
        }
    }
    /// Trust caller declarations; only enable on an isolated internal network.
    pub fn network() -> Self {
        Self {
            network: true,
            ..Self::default()
        }
    }
    // Keep tonic's native error type at the RPC authentication boundary.
    #[allow(clippy::result_large_err)]
    pub fn authenticate<T>(&self, request: &Request<T>) -> Result<Principal, Status> {
        if self.network {
            return match request
                .metadata()
                .get("adx-component")
                .and_then(|v| v.to_str().ok())
            {
                Some("coordinator") => Ok(Principal::Coordinator),
                Some("apiserver") => Ok(Principal::ApiServer),
                Some("ingress") => Ok(Principal::Ingress),
                Some("node") => {
                    let id = request
                        .metadata()
                        .get_bin("adx-node-id-bin")
                        .and_then(|v| v.to_bytes().ok())
                        .and_then(|v| String::from_utf8(v.to_vec()).ok())
                        .filter(|v| !v.trim().is_empty() && v.len() <= 256)
                        .ok_or_else(|| Status::unauthenticated("node identity required"))?;
                    Ok(Principal::Node(id))
                }
                _ => Err(Status::unauthenticated("component identity required")),
            };
        }
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("mutual TLS required"))?;
        let certificate = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("peer certificate required"))?;
        self.certificates
            .get(Sha256::digest(certificate.as_ref()).as_slice())
            .cloned()
            .ok_or_else(|| Status::permission_denied("component certificate not authorized"))
    }
}
// Callers propagate this directly from tonic service handlers.
#[allow(clippy::result_large_err)]
pub fn tenant(caller: Option<&CallerContext>, owner: &str) -> Result<(), Status> {
    let caller =
        caller.ok_or_else(|| Status::unauthenticated("validated caller context required"))?;
    if caller.tenant_id.trim().is_empty() {
        return Err(Status::unauthenticated("tenant context required"));
    }
    if !caller.administrator && caller.tenant_id != owner {
        return Err(Status::permission_denied(
            "environment belongs to another tenant",
        ));
    }
    Ok(())
}
