//! Business accounts and sessions. Independent of ADX tenant/service authentication.
mod providers;
mod store;
use adx_agent_core::launch::{CredentialCipher, LaunchConfig};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use http::{header, HeaderMap, StatusCode};
pub use providers::{Huawei, IdentityProvider, LiteLlm, ModelProvider};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
pub use store::AccountStore;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid login request")]
    Invalid,
    #[error("business session is invalid or expired")]
    Unauthorized,
    #[error("account is disabled or access is forbidden")]
    Forbidden,
    #[error("registration agreement is required")]
    AgreementRequired,
    #[error("identity provider or account service unavailable")]
    Unavailable,
    #[error("invalid account service configuration")]
    Configuration,
}
impl Error {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden | Self::AgreementRequired => StatusCode::FORBIDDEN,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid => "INVALID_LOGIN_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Forbidden => "FORBIDDEN",
            Self::AgreementRequired => "AGREEMENT_REQUIRED",
            _ => "AUTH_UNAVAILABLE",
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoginRequest {
    pub authorization_code: String,
    pub agreement_version: Option<String>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub user_id: String,
    pub token: String,
    pub token_type: &'static str,
    pub expires_in: u64,
}
#[derive(Clone)]
pub struct Principal {
    pub tenant: String,
    pub user_id: String,
    pub expires_at: i64,
    pub(crate) token_digest: String,
}
/// Called by Jiuwen only. Production has no fixed-user or anonymous implementation.
#[async_trait]
pub trait BusinessAuth: Send + Sync {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse>;
    async fn authenticate(&self, token: &str) -> Result<Principal>;
    async fn check(&self, principal: &Principal) -> Result<()>;
    async fn logout(&self, token: &str) -> Result<()>;
    async fn launch_config(&self, principal: &Principal) -> Result<LaunchConfig>;
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    pub tenant: String,
    pub developer_scope: String,
    pub agreement_version: String,
    pub database_url_env: String,
    pub database_ca_file: Option<String>,
    #[serde(default)]
    pub database_allow_plaintext: bool,
    #[serde(default = "session_ttl")]
    pub session_ttl_seconds: u64,
    pub litellm: ModelConfig,
}
fn session_ttl() -> u64 {
    604800
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub management_url: String,
    pub api_base: String,
    pub admin_key_env: String,
    pub model: String,
    pub max_budget: f64,
    pub budget_duration: String,
    pub rpm_limit: u32,
    pub tpm_limit: u32,
    #[serde(default)]
    pub allow_plaintext: bool,
}
pub struct AccountService {
    pub(crate) store: AccountStore,
    identity: Arc<dyn IdentityProvider>,
    models: Arc<dyn ModelProvider>,
    cipher: CredentialCipher,
    config: AccountConfig,
    client_id: String,
}
impl AccountService {
    pub async fn connect(config: AccountConfig) -> Result<Self> {
        let client_id =
            std::env::var("HUAWEI_OAUTH_CLIENT_ID").map_err(|_| Error::Configuration)?;
        let secret =
            std::env::var("HUAWEI_OAUTH_CLIENT_SECRET").map_err(|_| Error::Configuration)?;
        let path = std::env::var_os("ADX_CREDENTIAL_KEY_FILE").ok_or(Error::Configuration)?;
        let text = std::fs::read_to_string(path).map_err(|_| Error::Configuration)?;
        let bytes = hex::decode(text.trim()).map_err(|_| Error::Configuration)?;
        let key: [u8; 32] = bytes.try_into().map_err(|_| Error::Configuration)?;
        let store = AccountStore::connect(&config).await?;
        let identity = Arc::new(Huawei::new(client_id.clone(), secret)?);
        let models = Arc::new(LiteLlm::new(config.litellm.clone())?);
        Self::new(config, client_id, store, identity, models, key)
    }
    pub fn new(
        config: AccountConfig,
        client_id: String,
        store: AccountStore,
        identity: Arc<dyn IdentityProvider>,
        models: Arc<dyn ModelProvider>,
        key: [u8; 32],
    ) -> Result<Self> {
        for value in [
            &config.tenant,
            &config.developer_scope,
            &config.agreement_version,
            &client_id,
        ] {
            adx_agent_core::identifier(value, "account configuration")
                .map_err(|_| Error::Configuration)?;
        }
        if !(60..=604800).contains(&config.session_ttl_seconds) {
            return Err(Error::Configuration);
        }
        config.litellm.validate()?;
        Ok(Self {
            store,
            identity,
            models,
            cipher: CredentialCipher::new(key),
            config,
            client_id,
        })
    }
}
#[async_trait]
impl BusinessAuth for AccountService {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse> {
        if request.authorization_code.is_empty()
            || request.authorization_code.len() > 8192
            || request.authorization_code.chars().any(char::is_control)
        {
            return Err(Error::Invalid);
        }
        let union_id = self.identity.exchange(&request.authorization_code).await?;
        if union_id.is_empty() || union_id.len() > 2048 || union_id.chars().any(char::is_control) {
            return Err(Error::Unauthorized);
        }
        let token = random_token()?;
        let (user_id, _) = self
            .store
            .login(
                &self.config,
                &self.client_id,
                &union_id,
                request.agreement_version.as_deref(),
                &digest(&token),
            )
            .await?;
        Ok(LoginResponse {
            user_id,
            token,
            token_type: "Bearer",
            expires_in: self.config.session_ttl_seconds,
        })
    }
    async fn authenticate(&self, token: &str) -> Result<Principal> {
        if token.len() > 512 || token.len() < 32 {
            return Err(Error::Unauthorized);
        }
        self.store
            .authenticate(&self.config.tenant, &digest(token))
            .await
    }
    async fn check(&self, principal: &Principal) -> Result<()> {
        if principal.tenant != self.config.tenant {
            return Err(Error::Unauthorized);
        }
        let current = self
            .store
            .authenticate(&principal.tenant, &principal.token_digest)
            .await?;
        if current.user_id != principal.user_id || current.expires_at != principal.expires_at {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }
    async fn logout(&self, token: &str) -> Result<()> {
        // Idempotent logout, including an already revoked/expired session.
        self.store.logout(&self.config.tenant, &digest(token)).await
    }
    async fn launch_config(&self, principal: &Principal) -> Result<LaunchConfig> {
        self.check(principal).await?;
        let aad = serde_json::to_vec(&("litellm", &principal.tenant, &principal.user_id))
            .map_err(|_| Error::Unavailable)?;
        let candidate = format!("sk-{}", random_token()?);
        let encrypted = self
            .cipher
            .seal(&aad, candidate.as_bytes())
            .map_err(|_| Error::Unavailable)?;
        let record = self.store.credential(principal, encrypted).await?;
        let key = String::from_utf8(
            self.cipher
                .open(&aad, &record.encrypted_key)
                .map_err(|_| Error::Unavailable)?,
        )
        .map_err(|_| Error::Unavailable)?;
        if !record.ready {
            // Persist the random key BEFORE remote creation. Every retry and replica uses
            // this exact key, so a lost provider response cannot mint a second credential.
            let owner = litellm_user(&principal.tenant, &principal.user_id);
            self.models.ensure(&owner, &key).await?;
            self.store.mark_ready(principal, &record.version).await?;
        }
        self.check(principal).await?;
        Ok(LaunchConfig {
            credential_version: record.version,
            env: BTreeMap::from([
                ("API_KEY".into(), key),
                ("API_BASE".into(), self.config.litellm.api_base.clone()),
                ("MODEL_NAME".into(), self.config.litellm.model.clone()),
                ("MODEL_PROVIDER".into(), "OpenAI".into()),
            ]),
        })
    }
}
pub fn bearer(headers: &HeaderMap) -> Result<&str> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next().ok_or(Error::Unauthorized)?;
    if values.next().is_some() || headers.contains_key("x-auth") {
        return Err(Error::Unauthorized);
    }
    let value = value.to_str().map_err(|_| Error::Unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or(Error::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.len() > 512
        || token.bytes().any(|c| c.is_ascii_whitespace())
    {
        return Err(Error::Unauthorized);
    }
    Ok(token)
}
pub(crate) fn digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}
pub(crate) fn litellm_user(tenant: &str, user_id: &str) -> String {
    format!(
        "adx-{}",
        digest(&serde_json::json!([tenant, user_id]).to_string())
    )
}
pub(crate) fn random_token() -> Result<String> {
    let mut bytes = [0; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| Error::Unavailable)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}
pub(crate) const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests;

pub struct SessionGuard {
    pub auth: Arc<dyn BusinessAuth>,
    pub principal: Principal,
}
impl SessionGuard {
    pub async fn check(&self) -> Result<()> {
        tokio::time::timeout(IO_TIMEOUT, self.auth.check(&self.principal))
            .await
            .map_err(|_| Error::Unavailable)?
    }
    pub async fn watch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.as_secs())
            .unwrap_or(u64::MAX);
        let remaining = (self.principal.expires_at.max(0) as u64).saturating_sub(now);
        let expiry = tokio::time::sleep(Duration::from_secs(remaining));
        tokio::pin!(expiry);
        let checks = async {
            loop {
                if self.check().await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        };
        tokio::select! {_=&mut expiry=>{},_=checks=>{}}
    }
}
