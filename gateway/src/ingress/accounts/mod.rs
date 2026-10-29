//! Business accounts and sessions. Independent of ADX tenant/service authentication.
mod credentials;
mod providers;
mod store;
use adx_agent_core::mounts::Parameters;
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
pub use credentials::CredentialCipher;
use http::{header, HeaderMap, StatusCode};
pub use providers::{Agc, AgcIdentityProvider, Huawei, IdentityProvider, LiteLlm, ModelProvider};
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
    #[error("AGC access token is invalid, expired, or revoked")]
    InvalidAgcAccessToken,
    #[error("account is disabled or access is forbidden")]
    Forbidden,
    #[error("account is disabled or deleted")]
    AccountDisabled,
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
            Self::Unauthorized | Self::InvalidAgcAccessToken => StatusCode::UNAUTHORIZED,
            Self::Forbidden | Self::AccountDisabled | Self::AgreementRequired => {
                StatusCode::FORBIDDEN
            }
            _ => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid => "INVALID_LOGIN_REQUEST",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::InvalidAgcAccessToken => "INVALID_AGC_ACCESS_TOKEN",
            Self::Forbidden => "FORBIDDEN",
            Self::AccountDisabled => "ACCOUNT_DISABLED",
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
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgcLoginRequest {
    pub agc_access_token: String,
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
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ModelQuota {
    pub total: f64,
    pub balance: f64,
    pub used: f64,
    pub low_balance: bool,
    pub exhausted: bool,
    pub reset_period: String,
    pub reset_at: String,
}
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct QuotaResponse {
    pub ok: bool,
    pub available: bool,
    pub quota: ModelQuota,
}
/// Identity and optional profile supplied by a verified identity provider.
#[derive(Debug, PartialEq)]
pub struct VerifiedIdentity {
    pub subject: String,
    pub display_name: Option<String>,
}

pub(crate) struct ScopedIdentity<'a> {
    pub provider: &'a str,
    pub scope: &'a str,
    pub client_id: &'a str,
    pub identity: &'a VerifiedIdentity,
}

#[derive(Clone)]
pub struct Principal {
    pub user_id: String,
    pub expires_at: i64,
    pub(crate) token_digest: String,
}
/// Called by DigitalMateGateway only. Production has no fixed-user or anonymous implementation.
#[async_trait]
pub trait BusinessAuth: Send + Sync {
    async fn login(&self, request: LoginRequest) -> Result<LoginResponse>;
    async fn agc_login(&self, _request: AgcLoginRequest) -> Result<LoginResponse> {
        Err(Error::Unavailable)
    }
    async fn authenticate(&self, token: &str) -> Result<Principal>;
    async fn check(&self, principal: &Principal) -> Result<()>;
    async fn logout(&self, token: &str) -> Result<()>;
    async fn quota(&self, principal: &Principal) -> Result<QuotaResponse>;
    async fn model_parameters(&self, principal: &Principal) -> Result<Parameters>;
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountConfig {
    pub developer_scope: String,
    pub agreement_version: String,
    pub database_url_env: String,
    pub database_ca_file: Option<String>,
    #[serde(default)]
    pub database_allow_plaintext: bool,
    #[serde(default = "session_ttl")]
    pub session_ttl_seconds: u64,
    #[serde(default)]
    pub agc: Option<AgcConfig>,
    pub litellm: ModelConfig,
}
fn session_ttl() -> u64 {
    604800
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgcConfig {
    pub project_id: String,
    pub api_client_id: String,
    pub api_client_secret_env: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub management_url: String,
    pub api_base: String,
    pub admin_key_env: String,
    pub max_budget: f64,
    /// Displayed points per LiteLLM budget unit; does not alter billing or key budgets.
    #[serde(default = "default_points_per_budget_unit")]
    pub points_per_budget_unit: f64,
    #[serde(default)]
    pub budget_duration: Option<String>,
    pub rpm_limit: u32,
    pub tpm_limit: u32,
    #[serde(default)]
    pub allow_plaintext: bool,
}
fn default_points_per_budget_unit() -> f64 {
    1.0
}
pub struct AccountService {
    pub(crate) store: AccountStore,
    identity: Arc<dyn IdentityProvider>,
    agc_identity: Option<Arc<dyn AgcIdentityProvider>>,
    models: Arc<dyn ModelProvider>,
    cipher: CredentialCipher,
    config: AccountConfig,
    client_id: String,
}
impl AccountService {
    /// Connect business accounts and sessions.
    /// The supplied fixed namespace determines model owners and encryption context.
    /// Returns Configuration for invalid configuration/schema and Unavailable for dependency failure.
    pub async fn connect(config: AccountConfig, model_namespace: &str) -> Result<Self> {
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
        let agc_identity = match &config.agc {
            Some(agc) => {
                let secret =
                    std::env::var(&agc.api_client_secret_env).map_err(|_| Error::Configuration)?;
                Some(Arc::new(Agc::new(agc.clone(), secret)?) as Arc<dyn AgcIdentityProvider>)
            }
            None => None,
        };
        let models = Arc::new(LiteLlm::new(config.litellm.clone())?);
        Self::new_with_agc(
            config,
            client_id,
            store,
            identity,
            agc_identity,
            models,
            CredentialCipher::new(key, model_namespace)?,
        )
    }
    /// Returns Configuration for invalid account settings or inconsistent provider wiring.
    pub fn new_with_agc(
        config: AccountConfig,
        client_id: String,
        store: AccountStore,
        identity: Arc<dyn IdentityProvider>,
        agc_identity: Option<Arc<dyn AgcIdentityProvider>>,
        models: Arc<dyn ModelProvider>,
        cipher: CredentialCipher,
    ) -> Result<Self> {
        for value in [
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
        if let Some(agc) = &config.agc {
            for value in [&agc.project_id, &agc.api_client_id] {
                adx_agent_core::identifier(value, "AGC account configuration")
                    .map_err(|_| Error::Configuration)?;
            }
            if agc.api_client_secret_env.trim().is_empty() || agc_identity.is_none() {
                return Err(Error::Configuration);
            }
        } else if agc_identity.is_some() {
            return Err(Error::Configuration);
        }
        config.litellm.validate()?;
        Ok(Self {
            store,
            identity,
            agc_identity,
            models,
            cipher,
            config,
            client_id,
        })
    }

    async fn create_session(
        &self,
        provider: &str,
        provider_scope: &str,
        client_id: &str,
        identity: &VerifiedIdentity,
        agreement_version: Option<&str>,
    ) -> Result<LoginResponse> {
        let token = random_token()?;
        let identity = ScopedIdentity {
            provider,
            scope: provider_scope,
            client_id,
            identity,
        };
        let (user_id, _) = self
            .store
            .login(&self.config, identity, agreement_version, &digest(&token))
            .await?;
        Ok(LoginResponse {
            user_id,
            token,
            token_type: "Bearer",
            expires_in: self.config.session_ttl_seconds,
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
        let identity = self.identity.exchange(&request.authorization_code).await?;
        let union_id = &identity.subject;
        if union_id.is_empty() || union_id.len() > 2048 || union_id.chars().any(char::is_control) {
            return Err(Error::Unauthorized);
        }
        self.create_session(
            "huawei",
            &self.config.developer_scope,
            &self.client_id,
            &identity,
            request.agreement_version.as_deref(),
        )
        .await
    }
    async fn agc_login(&self, request: AgcLoginRequest) -> Result<LoginResponse> {
        if !valid_agc_access_token(&request.agc_access_token) {
            return Err(Error::Invalid);
        }
        let provider = self.agc_identity.as_ref().ok_or(Error::Unavailable)?;
        let config = self.config.agc.as_ref().ok_or(Error::Unavailable)?;
        let identity = provider.verify(&request.agc_access_token).await?;
        if identity.subject.is_empty()
            || identity.subject.len() > 1024
            || identity
                .subject
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(Error::InvalidAgcAccessToken);
        }
        self.create_session(
            "agc",
            &config.project_id,
            &config.api_client_id,
            &identity,
            request.agreement_version.as_deref(),
        )
        .await
    }
    async fn authenticate(&self, token: &str) -> Result<Principal> {
        if token.len() > 512 || token.len() < 32 {
            return Err(Error::Unauthorized);
        }
        self.store.authenticate(&digest(token)).await
    }
    async fn check(&self, principal: &Principal) -> Result<()> {
        let current = self.store.authenticate(&principal.token_digest).await?;
        if current.user_id != principal.user_id || current.expires_at != principal.expires_at {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }
    async fn logout(&self, token: &str) -> Result<()> {
        // Idempotent logout, including an already revoked/expired session.
        self.store.logout(&digest(token)).await
    }
    async fn quota(&self, principal: &Principal) -> Result<QuotaResponse> {
        let launch = self.model_parameters(principal).await?;
        let key = launch.get("model_api_key").ok_or(Error::Unavailable)?;
        let owner = self.cipher.model_owner(&principal.user_id);
        let quota = self.models.quota(&owner, key).await?;
        self.check(principal).await?;
        Ok(quota)
    }
    async fn model_parameters(&self, principal: &Principal) -> Result<Parameters> {
        self.check(principal).await?;
        let aad = self
            .cipher
            .associated_data(&principal.user_id)
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
        // Reconcile model access and account alias even for a ready credential when preparing a new
        // binding or serving quota. Warm Sandbox traffic never enters this path.
        // Persist the key BEFORE remote creation so retries cannot mint another key.
        let owner = self.cipher.model_owner(&principal.user_id);
        let alias = self.store.model_key_alias(principal).await?;
        self.models.ensure(&owner, &key, &alias).await?;
        if !record.ready {
            self.store.mark_ready(principal, &record.version).await?;
        }
        self.check(principal).await?;
        Ok(BTreeMap::from([
            ("model_api_key".into(), key),
            (
                "model_api_base".into(),
                self.config.litellm.api_base.clone(),
            ),
        ]))
    }
}
fn valid_agc_access_token(value: &str) -> bool {
    !value.is_empty() && value.len() <= 8192 && value.bytes().all(|byte| byte.is_ascii_graphic())
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
fn model_key_alias(provider: &str, user_id: &str, display_name: Option<&str>) -> String {
    let provider = if provider == "agc" { "agc" } else { "huawei" };
    let short_id: String = user_id.chars().take(8).collect();
    let nickname: String = display_name
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
        .take(128)
        .collect();
    let nickname = nickname.trim();
    if nickname.is_empty() {
        format!("{provider}-{short_id}")
    } else {
        format!("{provider}-{nickname}-{short_id}")
    }
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
