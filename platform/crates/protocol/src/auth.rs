//! Component identities come from verified TLS peer certificates, not metadata.
use crate::control::CallerContext;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tonic::{Request, Status};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    Master,
    ApiServer,
    Edge,
    Node(String),
    /// A deployment-managed certificate shared by a set of Node Managers.
    /// The concrete node remains explicit in every node-originated request.
    NodePool,
}
impl Principal {
    /// Resolve the concrete node authorized by this mTLS identity.
    ///
    /// Exact node certificates may omit the request node for compatibility.
    /// Pool certificates always require a non-empty request node so callers do
    /// not silently collapse a multi-node deployment onto one identity.
    pub fn authorized_node<'a>(&'a self, requested: &'a str) -> Option<&'a str> {
        match self {
            Self::Node(bound) if requested.is_empty() || requested == bound => Some(bound),
            Self::NodePool if !requested.trim().is_empty() => Some(requested),
            _ => None,
        }
    }
}
#[derive(Clone, Default)]
pub struct Peers(Arc<BTreeMap<Vec<u8>, Principal>>);
impl Peers {
    /// DER leaf certificates are supplied by the deployment alongside the CA.
    pub fn new(certificates: impl IntoIterator<Item = (Vec<u8>, Principal)>) -> Self {
        Self(Arc::new(
            certificates
                .into_iter()
                .map(|(der, p)| (Sha256::digest(der).to_vec(), p))
                .collect(),
        ))
    }
    // Keep tonic's native error type at the RPC authentication boundary.
    #[allow(clippy::result_large_err)]
    pub fn authenticate<T>(&self, request: &Request<T>) -> Result<Principal, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("mutual TLS required"))?;
        let certificate = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("peer certificate required"))?;
        self.0
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
            "capsule belongs to another tenant",
        ));
    }
    Ok(())
}
