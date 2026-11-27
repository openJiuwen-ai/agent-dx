//! Minimal API key verification. Redis stores only SHA256 digests and identities.
use crate::storage::Session;
use adx_core::{Error, Result};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb, not_accepted, status,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credential {
    pub tenant_id: String,
    pub administrator: bool,
    pub expires_at_unix_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<CredentialMetadata>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialMetadata {
    pub public_id: String,
    pub name: String,
    pub created_at_unix_ms: u64,
    pub prefix: String,
    pub value_length: u32,
    pub masked_value_prefix: String,
    pub masked_value_suffix: String,
}
impl CredentialMetadata {
    fn new(name: String, api_key: &str) -> Result<Self> {
        let value = api_key
            .strip_prefix("adx_")
            .filter(|value| value.len() == 64 && value.bytes().all(is_lower_hex))
            .ok_or_else(|| Error::Invalid("invalid generated credential".into()))?;
        let metadata = Self {
            public_id: uuid::Uuid::new_v4().to_string(),
            name,
            created_at_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Unavailable("clock unavailable".into()))?
                .as_millis()
                .try_into()
                .map_err(|_| Error::Unavailable("clock unavailable".into()))?,
            prefix: "adx_".into(),
            value_length: 64,
            masked_value_prefix: value[..2].into(),
            masked_value_suffix: value[60..].into(),
        };
        metadata.validate()?;
        Ok(metadata)
    }

    fn validate(&self) -> Result<()> {
        if uuid::Uuid::parse_str(&self.public_id).is_err()
            || self.name.trim().is_empty()
            || self.name.chars().count() > 256
            || self.name.chars().any(char::is_control)
            || self.created_at_unix_ms == 0
            || self.created_at_unix_ms > i64::MAX as u64
            // Keep metadata for previously persisted compatibility keys readable.
            || !matches!(self.prefix.as_str(), "adx_" | "e2b_")
            || self.value_length != 64
            || self.masked_value_prefix.len() != 2
            || self.masked_value_suffix.len() != 4
            || !self
                .masked_value_prefix
                .bytes()
                .chain(self.masked_value_suffix.bytes())
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::Invalid("invalid credential metadata".into()));
        }
        Ok(())
    }
}
impl Credential {
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.trim().is_empty()
            || self.tenant_id.len() > 256
            || self.tenant_id.chars().any(char::is_control)
        {
            return Err(Error::Invalid("credential tenant required".into()));
        }
        if let Some(metadata) = &self.metadata {
            if self.administrator {
                return Err(Error::Invalid(
                    "administrator metadata is not supported".into(),
                ));
            }
            metadata.validate()?;
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
fn new_api_key() -> String {
    format!(
        "adx_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

#[derive(Clone)]
pub struct AuthRpc {
    session: Session,
    peers: Peers,
    draining: tokio::sync::watch::Receiver<bool>,
}
impl AuthRpc {
    pub fn new(session: Session, peers: Peers) -> Self {
        Self {
            session,
            peers,
            draining: tokio::sync::watch::channel(false).1,
        }
    }
    /// Construct with the handover draining flag shared with the RPC
    /// admission gate (see `CoordinatorRpc::begin_drain` and
    /// `CoordinatorRpc::draining_receiver`).
    pub fn with_draining(
        session: Session,
        peers: Peers,
        draining: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            session,
            peers,
            draining,
        }
    }
    /// Handover admission gate for credential writes; same contract as
    /// `CoordinatorRpc::admission`.
    fn admission(&self) -> std::result::Result<(), Status> {
        if *self.draining.borrow() {
            Err(not_accepted(
                "Coordinator handover in progress; operation not accepted",
            ))
        } else {
            Ok(())
        }
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
        self.admission()?;
        let r = request.into_inner();
        let api_key = new_api_key();
        let metadata = r
            .name
            .map(|name| CredentialMetadata::new(name, &api_key))
            .transpose()
            .map_err(status)?;
        let credential = Credential {
            tenant_id: r.tenant_id,
            administrator: false,
            expires_at_unix_seconds: r.expires_at_unix_seconds,
            metadata,
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
        self.session
            .bootstrap_credential(&api_key, &credential)
            .await
            .map_err(status)?;
        Ok(Response::new(pb::CreateTenantKeyResponse {
            key: Some(pb::TenantKey {
                id: digest(&api_key).map_err(status)?,
                tenant_id: credential.tenant_id,
                expires_at_unix_seconds: credential.expires_at_unix_seconds,
                metadata: credential.metadata.as_ref().map(metadata_proto),
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
                metadata: c.metadata.as_ref().map(metadata_proto),
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
        self.admission()?;
        let r = request.into_inner();
        if ((!r.id.is_empty()) as u8 + (!r.public_id.is_empty()) as u8) != 1 {
            return Err(Status::invalid_argument(
                "exactly one credential identifier required",
            ));
        }
        if !r.public_id.is_empty() {
            let caller = r
                .caller
                .ok_or_else(|| Status::permission_denied("administrator required"))?;
            if r.tenant_id != caller.tenant_id {
                return Err(Status::not_found("credential not found"));
            }
            self.session
                .revoke_credential_public(&r.public_id, &r.tenant_id)
                .await
                .map_err(|e| match e {
                    Error::NotFound => Status::not_found("credential not found"),
                    other => status(other),
                })?
        } else {
            self.session
                .revoke_credential(&r.id)
                .await
                .map_err(status)?;
        }
        Ok(Response::new(pb::RevokeTenantKeyResponse {}))
    }
}
fn metadata_proto(metadata: &CredentialMetadata) -> pb::CredentialMetadata {
    pb::CredentialMetadata {
        public_id: metadata.public_id.clone(),
        name: metadata.name.clone(),
        created_at_unix_ms: metadata.created_at_unix_ms,
        prefix: metadata.prefix.clone(),
        value_length: metadata.value_length,
        masked_value_prefix: metadata.masked_value_prefix.clone(),
        masked_value_suffix: metadata.masked_value_suffix.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{new_api_key, CredentialMetadata};

    fn metadata(prefix: &str) -> CredentialMetadata {
        CredentialMetadata {
            public_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            name: "managed key".into(),
            created_at_unix_ms: 1,
            prefix: prefix.into(),
            value_length: 64,
            masked_value_prefix: "aa".into(),
            masked_value_suffix: "aaaa".into(),
        }
    }

    #[test]
    fn new_credentials_use_the_native_format_with_matching_public_masks() {
        let key = new_api_key();
        let value = key.strip_prefix("adx_").unwrap();
        assert_eq!(value.len(), 64);
        assert!(value.bytes().all(super::is_lower_hex));
        let metadata = CredentialMetadata::new("managed key".into(), &key).unwrap();
        assert_eq!(metadata.prefix, "adx_");
        assert_eq!(metadata.masked_value_prefix, &value[..2]);
        assert_eq!(metadata.masked_value_suffix, &value[60..]);
    }

    #[test]
    fn credential_metadata_allows_only_known_key_prefixes() {
        assert!(metadata("adx_").validate().is_ok());
        assert!(metadata("e2b_").validate().is_ok());
        assert!(metadata("custom_").validate().is_err());
    }
}
