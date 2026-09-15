//! Minimal API key verification. Redis stores only SHA256 digests and identities.
use crate::storage::Session;
use adx_core::{Error, Result};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb, status,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credential {
    pub tenant_id: String,
    pub administrator: bool,
    pub expires_at_unix_seconds: u64,
}
impl Credential {
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.trim().is_empty() {
            return Err(Error::Invalid("credential tenant required".into()));
        }
        Ok(())
    }
}
pub fn digest(key: &str) -> Result<String> {
    if key.len() < 32 || key.len() > 512 {
        return Err(Error::Invalid("API key must contain 32..512 bytes".into()));
    }
    Ok(format!("{:x}", Sha256::digest(key.as_bytes())))
}
#[derive(Clone)]
pub struct AuthRpc {
    session: Session,
    peers: Peers,
}
impl AuthRpc {
    pub fn new(session: Session, peers: Peers) -> Self {
        Self { session, peers }
    }
}
#[tonic::async_trait]
impl pb::auth_service_server::AuthService for AuthRpc {
    async fn verify_api_key(
        &self,
        request: Request<pb::VerifyApiKeyRequest>,
    ) -> std::result::Result<Response<pb::VerifyApiKeyResponse>, Status> {
        if !matches!(
            self.peers.authenticate(&request)?,
            Principal::Frontend | Principal::Edge
        ) {
            return Err(Status::permission_denied(
                "trusted ingress identity required",
            ));
        }
        let hash = digest(&request.into_inner().api_key)
            .map_err(|_| Status::unauthenticated("invalid API key"))?;
        let credential = self.session.credential(&hash).await.map_err(|e| match e {
            Error::NotFound => Status::unauthenticated("invalid API key"),
            e => status(e),
        })?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::unavailable("clock unavailable"))?
            .as_secs();
        if credential.expires_at_unix_seconds != 0 && now >= credential.expires_at_unix_seconds {
            return Err(Status::unauthenticated("expired API key"));
        }
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(pb::CallerContext {
                tenant_id: credential.tenant_id,
                administrator: credential.administrator,
            }),
            expires_at_unix_seconds: credential.expires_at_unix_seconds,
        }))
    }
}
