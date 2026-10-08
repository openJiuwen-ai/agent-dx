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
        if self.tenant_id.trim().is_empty()
            || self.tenant_id.len() > 256
            || self.tenant_id.chars().any(char::is_control)
        {
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
            Principal::ApiServer | Principal::Ingress
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

impl AuthRpc {
    #[allow(clippy::result_large_err)] // Tonic RPC authentication preserves Status.
    fn admin<T>(
        &self,
        request: &Request<T>,
        caller: Option<&pb::CallerContext>,
    ) -> std::result::Result<(), Status> {
        if self.peers.authenticate(request)? != Principal::ApiServer {
            return Err(Status::permission_denied("Frontend identity required"));
        }
        if !caller.is_some_and(|c| c.administrator && !c.tenant_id.trim().is_empty()) {
            return Err(Status::permission_denied("administrator required"));
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl pb::credential_service_server::CredentialService for AuthRpc {
    async fn create_tenant_key(
        &self,
        request: Request<pb::CreateTenantKeyRequest>,
    ) -> std::result::Result<Response<pb::CreateTenantKeyResponse>, Status> {
        self.admin(&request, request.get_ref().caller.as_ref())?;
        let r = request.into_inner();
        let credential = Credential {
            tenant_id: r.tenant_id,
            administrator: false,
            expires_at_unix_seconds: r.expires_at_unix_seconds,
        };
        credential.validate().map_err(status)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::unavailable("clock unavailable"))?
            .as_secs();
        if credential.expires_at_unix_seconds != 0
            && (credential.expires_at_unix_seconds <= now
                || credential.expires_at_unix_seconds > i64::MAX as u64)
        {
            return Err(Status::invalid_argument(
                "expiry must be a future Unix timestamp or zero",
            ));
        }
        let api_key = format!(
            "adx_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        self.session
            .bootstrap_credential(&api_key, &credential)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::CreateTenantKeyResponse {
            key: Some(pb::TenantKey {
                id: digest(&api_key).map_err(status)?,
                tenant_id: credential.tenant_id,
                expires_at_unix_seconds: credential.expires_at_unix_seconds,
            }),
            api_key,
        }))
    }
    async fn list_tenant_keys(
        &self,
        request: Request<pb::ListTenantKeysRequest>,
    ) -> std::result::Result<Response<pb::ListTenantKeysResponse>, Status> {
        self.admin(&request, request.get_ref().caller.as_ref())?;
        let r = request.into_inner();
        if r.page_size > 1000
            || (!r.page_token.is_empty()
                && (r.page_token.len() != 64
                    || !r.page_token.bytes().all(|b| b.is_ascii_hexdigit())))
        {
            return Err(Status::invalid_argument("invalid credential page"));
        }
        let size = if r.page_size == 0 {
            100
        } else {
            r.page_size as usize
        };
        let mut keys: Vec<_> = self
            .session
            .list_credentials()
            .await
            .map_err(status)?
            .into_iter()
            .filter(|(id, c)| {
                !c.administrator
                    && *id > r.page_token
                    && (r.tenant_id.is_empty() || c.tenant_id == r.tenant_id)
            })
            .take(size + 1)
            .map(|(id, c)| pb::TenantKey {
                id,
                tenant_id: c.tenant_id,
                expires_at_unix_seconds: c.expires_at_unix_seconds,
            })
            .collect();
        let next_page_token = if keys.len() > size {
            keys.pop();
            keys.last().map(|key| key.id.clone()).unwrap_or_default()
        } else {
            String::new()
        };
        Ok(Response::new(pb::ListTenantKeysResponse {
            keys,
            next_page_token,
        }))
    }
    async fn revoke_tenant_key(
        &self,
        request: Request<pb::RevokeTenantKeyRequest>,
    ) -> std::result::Result<Response<pb::RevokeTenantKeyResponse>, Status> {
        self.admin(&request, request.get_ref().caller.as_ref())?;
        self.session
            .revoke_credential(&request.into_inner().id)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::RevokeTenantKeyResponse {}))
    }
}
