//! Bounded server-to-server clients. No redirects or caller-selected Huawei endpoints.
use super::*;
use base64::engine::general_purpose::URL_SAFE;
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::Instant;

const HUAWEI_ORIGIN: &str = "https://oauth-login.cloud.huawei.com";
const HUAWEI_ISSUER: &str = "https://accounts.huawei.com";
const AGC_CN_ORIGIN: &str = "https://connect-api.cloud.huawei.com";
const AGC_INVALID_ACCESS_TOKEN: i64 = 203817985;
const AGC_TOKEN_FORMAT_ERROR: i64 = 203818355;
const AGC_EXPIRED_ACCESS_TOKEN: i64 = 203818357;
const AGC_REVOKED_ACCESS_TOKEN: i64 = 203818359;
const AGC_THIRD_PARTY_AUTH_FAILED: i64 = 205524994;
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    async fn exchange(&self, code: &str) -> Result<VerifiedIdentity>;
}
#[async_trait]
pub trait AgcIdentityProvider: Send + Sync {
    async fn verify(&self, access_token: &str) -> Result<VerifiedIdentity>;
}
#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn ensure(&self, user_id: &str, key: &str, key_alias: &str) -> Result<()>;
    async fn quota(&self, user_id: &str, key: &str) -> Result<QuotaResponse>;
}
fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(IO_TIMEOUT)
        .build()
        .map_err(|_| Error::Configuration)
}
async fn json_response(mut response: reqwest::Response) -> Result<Value> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
        if body.len() + chunk.len() > 256 * 1024 {
            return Err(Error::Unavailable);
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| Error::Unavailable)
}
pub struct Huawei {
    client: reqwest::Client,
    client_id: String,
    secret: String,
    keys: Mutex<Option<(Instant, JwkSet)>>,
}
#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    display_name: Option<String>,
}
impl Huawei {
    pub fn new(client_id: String, secret: String) -> Result<Self> {
        if client_id.is_empty() || secret.is_empty() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            client: client()?,
            client_id,
            secret,
            keys: Mutex::new(None),
        })
    }
    async fn verify(&self, token: &str) -> Result<VerifiedIdentity> {
        let header = decode_header(token).map_err(|_| Error::Unauthorized)?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::PS256) {
            return Err(Error::Unauthorized);
        }
        let kid = header.kid.ok_or(Error::Unauthorized)?;
        let mut cache = self.keys.lock().await;
        let refresh = cache.as_ref().is_none_or(|(at, keys)| {
            at.elapsed() > Duration::from_secs(3600) || keys.find(&kid).is_none()
        });
        if refresh {
            let response = self
                .client
                .get(format!("{HUAWEI_ORIGIN}/.well-known/openid-configuration"))
                .send()
                .await
                .map_err(|_| Error::Unavailable)?;
            if !response.status().is_success() {
                return Err(Error::Unavailable);
            }
            let discovery = json_response(response).await?;
            let uri = discovery["jwks_uri"].as_str().ok_or(Error::Unavailable)?;
            let url = url::Url::parse(uri).map_err(|_| Error::Unavailable)?;
            // Discovery must not redirect credentials or key fetches to arbitrary origins.
            if url.scheme() != "https"
                || url.host_str() != Some("oauth-login.cloud.huawei.com")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.port_or_known_default() != Some(443)
            {
                return Err(Error::Unavailable);
            }
            let response = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|_| Error::Unavailable)?;
            if !response.status().is_success() {
                return Err(Error::Unavailable);
            }
            let keys: JwkSet = serde_json::from_value(json_response(response).await?)
                .map_err(|_| Error::Unavailable)?;
            *cache = Some((Instant::now(), keys));
        }
        let jwk = cache
            .as_ref()
            .and_then(|(_, keys)| keys.find(&kid))
            .ok_or(Error::Unauthorized)?;
        let key = DecodingKey::from_jwk(jwk).map_err(|_| Error::Unauthorized)?;
        verify_claims(token, &key, header.alg, &self.client_id)
    }
}
#[async_trait]
impl IdentityProvider for Huawei {
    async fn exchange(&self, code: &str) -> Result<VerifiedIdentity> {
        let response = self
            .client
            .post(format!("{HUAWEI_ORIGIN}/oauth2/v3/token"))
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.secret.as_str()),
                ("supportAlg", "PS256"),
            ])
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        let status = response.status();
        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::Unavailable);
        }
        if !status.is_success() {
            return Err(Error::Unauthorized);
        }
        let value = json_response(response).await?;
        let token = value["id_token"].as_str().ok_or(Error::Unauthorized)?;
        if token.len() > 32768 {
            return Err(Error::Unauthorized);
        }
        self.verify(token).await
    }
}

struct AgcServiceToken {
    value: String,
    expires_at: Instant,
}

#[derive(Debug)]
struct AgcVerifyFailure {
    error: Error,
    stage: &'static str,
    reason: &'static str,
    http_status: Option<u16>,
    upstream_code: Option<i64>,
}

type AgcResult<T> = std::result::Result<T, AgcVerifyFailure>;

impl AgcVerifyFailure {
    fn invalid(stage: &'static str, reason: &'static str) -> Self {
        Self {
            error: Error::InvalidAgcAccessToken,
            stage,
            reason,
            http_status: None,
            upstream_code: None,
        }
    }

    fn unavailable(stage: &'static str, reason: &'static str) -> Self {
        Self {
            error: Error::Unavailable,
            stage,
            reason,
            http_status: None,
            upstream_code: None,
        }
    }

    fn with_status(mut self, status: StatusCode) -> Self {
        self.http_status = Some(status.as_u16());
        self
    }

    fn with_upstream_code(mut self, code: i64) -> Self {
        self.upstream_code = Some(code);
        self
    }

    fn into_public(self) -> Error {
        tracing::warn!(
            event = "agc_token_verification_failed",
            stage = self.stage,
            reason = self.reason,
            http_status = ?self.http_status,
            upstream_code = ?self.upstream_code,
            public_error = self.error.code(),
            "AGC user access token verification failed"
        );
        self.error
    }
}

/// AppGallery Connect Auth server verifier for the CN project configured by the deployment.
pub struct Agc {
    client: reqwest::Client,
    config: AgcConfig,
    secret: String,
    origin: String,
    service_token: Mutex<Option<AgcServiceToken>>,
}

impl Agc {
    pub fn new(config: AgcConfig, secret: String) -> Result<Self> {
        Self::with_origin(config, secret, AGC_CN_ORIGIN)
    }

    fn with_origin(config: AgcConfig, secret: String, origin: &str) -> Result<Self> {
        if config.project_id.is_empty()
            || config.api_client_id.is_empty()
            || secret.is_empty()
            || origin.is_empty()
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            client: client()?,
            config,
            secret,
            origin: origin.trim_end_matches('/').to_owned(),
            service_token: Mutex::new(None),
        })
    }

    async fn service_token(&self) -> AgcResult<String> {
        let mut cached = self.service_token.lock().await;
        let now = Instant::now();
        if let Some(token) = cached.as_ref() {
            if token.expires_at > now + Duration::from_secs(30) {
                return Ok(token.value.clone());
            }
        }
        let response = self
            .client
            .post(format!("{}/api/oauth2/v1/token", self.origin))
            .header("algorithm_type", "1")
            .json(&json!({
                "grant_type": "client_credentials",
                "client_id": self.config.api_client_id,
                "client_secret": self.secret,
            }))
            .send()
            .await
            .map_err(|_| {
                AgcVerifyFailure::unavailable("service_token_request", "request_failed")
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(
                AgcVerifyFailure::unavailable("service_token_response", "http_status")
                    .with_status(status),
            );
        }
        let value = json_response(response).await.map_err(|_| {
            AgcVerifyFailure::unavailable("service_token_response", "invalid_response_body")
                .with_status(status)
        })?;
        let token = value["access_token"]
            .as_str()
            .filter(|token| !token.is_empty() && token.len() <= 8192)
            .ok_or_else(|| {
                AgcVerifyFailure::unavailable("service_token_response", "missing_access_token")
                    .with_status(status)
            })?;
        let expires_in = value["expires_in"]
            .as_u64()
            .filter(|seconds| *seconds > 30)
            .ok_or_else(|| {
                AgcVerifyFailure::unavailable("service_token_response", "invalid_expiry")
                    .with_status(status)
            })?;
        let expires_at = now
            .checked_add(Duration::from_secs(expires_in))
            .ok_or_else(|| {
                AgcVerifyFailure::unavailable("service_token_response", "invalid_expiry")
                    .with_status(status)
            })?;
        *cached = Some(AgcServiceToken {
            value: token.to_owned(),
            expires_at,
        });
        Ok(token.to_owned())
    }

    async fn verify_remote(&self, access_token: &str) -> AgcResult<String> {
        let service_token = self.service_token().await?;
        let response = self
            .client
            .get(format!("{}/api/oauth2/third/v1/verify-token", self.origin))
            .query(&[("productId", self.config.project_id.as_str())])
            .header("accessToken", access_token)
            .header("client_id", &self.config.api_client_id)
            .bearer_auth(service_token)
            .send()
            .await
            .map_err(|_| AgcVerifyFailure::unavailable("verify_token_request", "request_failed"))?;
        let status = response.status();
        if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
            return Err(AgcVerifyFailure::unavailable(
                "verify_token_response",
                "upstream_unavailable",
            )
            .with_status(status));
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(
                AgcVerifyFailure::invalid("verify_token_response", "unauthorized_status")
                    .with_status(status),
            );
        }
        let value = json_response(response).await.map_err(|_| {
            AgcVerifyFailure::unavailable("verify_token_response", "invalid_response_body")
                .with_status(status)
        })?;
        let code = value["ret"]["code"]
            .as_i64()
            .or_else(|| value["ret"]["code"].as_str()?.parse().ok())
            .ok_or_else(|| {
                AgcVerifyFailure::unavailable("verify_token_response", "missing_upstream_code")
                    .with_status(status)
            })?;
        if code != 0 {
            return if matches!(
                code,
                AGC_INVALID_ACCESS_TOKEN
                    | AGC_TOKEN_FORMAT_ERROR
                    | AGC_EXPIRED_ACCESS_TOKEN
                    | AGC_REVOKED_ACCESS_TOKEN
                    | AGC_THIRD_PARTY_AUTH_FAILED
            ) {
                Err(
                    AgcVerifyFailure::invalid("verify_token_response", "upstream_rejected")
                        .with_status(status)
                        .with_upstream_code(code),
                )
            } else {
                Err(
                    AgcVerifyFailure::unavailable("verify_token_response", "unknown_upstream_code")
                        .with_status(status)
                        .with_upstream_code(code),
                )
            };
        }
        if !status.is_success() {
            return Err(AgcVerifyFailure::invalid(
                "verify_token_response",
                "unexpected_http_status",
            )
            .with_status(status)
            .with_upstream_code(code));
        }
        if value["productId"].as_str() != Some(self.config.project_id.as_str()) {
            return Err(
                AgcVerifyFailure::invalid("verify_token_response", "project_mismatch")
                    .with_status(status)
                    .with_upstream_code(code),
            );
        }
        identity_value(&value["uid"]).ok_or_else(|| {
            AgcVerifyFailure::invalid("verify_token_response", "missing_uid")
                .with_status(status)
                .with_upstream_code(code)
        })
    }
}

#[async_trait]
impl AgcIdentityProvider for Agc {
    async fn verify(&self, access_token: &str) -> Result<VerifiedIdentity> {
        let uid = self
            .verify_remote(access_token)
            .await
            .map_err(AgcVerifyFailure::into_public)?;
        validate_agc_claims(access_token, &self.config.project_id, &uid)
            .map_err(AgcVerifyFailure::into_public)?;
        Ok(VerifiedIdentity {
            subject: uid,
            display_name: None,
        })
    }
}

#[derive(Deserialize)]
struct AgcClaims {
    iss: String,
    aud: Value,
    sub: Value,
    exp: u64,
}

fn validate_agc_claims(token: &str, project_id: &str, uid: &str) -> AgcResult<()> {
    let mut segments = token.split('.');
    let _header = segments
        .next()
        .ok_or_else(|| AgcVerifyFailure::invalid("local_claims", "token_format"))?;
    let payload = segments
        .next()
        .ok_or_else(|| AgcVerifyFailure::invalid("local_claims", "token_format"))?;
    let _signature = segments
        .next()
        .ok_or_else(|| AgcVerifyFailure::invalid("local_claims", "token_format"))?;
    if segments.next().is_some() {
        return Err(AgcVerifyFailure::invalid("local_claims", "token_format"));
    }
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .map_err(|_| AgcVerifyFailure::invalid("local_claims", "payload_encoding"))?;
    let claims_value: Value = serde_json::from_slice(&payload)
        .map_err(|_| AgcVerifyFailure::invalid("local_claims", "claims_json"))?;
    let claims: AgcClaims = serde_json::from_value(claims_value)
        .map_err(|_| AgcVerifyFailure::invalid("local_claims", "claims_json"))?;
    let expected_issuer = format!("https://agc.developer.huawei.com/{project_id}");
    let now = adx_agent_core::unix_time_millis() / 1000;
    if claims.iss != expected_issuer {
        return Err(AgcVerifyFailure::invalid("local_claims", "issuer_mismatch"));
    }
    if identity_value(&claims.aud).as_deref() != Some(project_id) {
        return Err(AgcVerifyFailure::invalid(
            "local_claims",
            "audience_mismatch",
        ));
    }
    if identity_value(&claims.sub).as_deref() != Some(uid) {
        return Err(AgcVerifyFailure::invalid(
            "local_claims",
            "subject_mismatch",
        ));
    }
    if claims.exp <= now {
        return Err(AgcVerifyFailure::invalid("local_claims", "expired"));
    }
    Ok(())
}

fn identity_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

impl ModelConfig {
    pub fn validate(&self) -> Result<()> {
        for value in [&self.management_url, &self.api_base] {
            let url = url::Url::parse(value).map_err(|_| Error::Configuration)?;
            if url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || !(url.scheme() == "https" || (self.allow_plaintext && url.scheme() == "http"))
            {
                return Err(Error::Configuration);
            }
        }
        if !self.points_per_budget_unit.is_finite() || self.points_per_budget_unit <= 0.0 {
            return Err(Error::Configuration);
        }
        if !self.max_budget.is_finite()
            || self.max_budget <= 0.0
            || self.rpm_limit == 0
            || self.tpm_limit == 0
            || self.budget_duration.as_ref().is_some_and(String::is_empty)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}
pub struct LiteLlm {
    client: reqwest::Client,
    config: ModelConfig,
    admin_key: String,
}
impl LiteLlm {
    pub fn new(config: ModelConfig) -> Result<Self> {
        let key = std::env::var(&config.admin_key_env).map_err(|_| Error::Configuration)?;
        Self::with_key(config, key)
    }
    pub fn with_key(config: ModelConfig, admin_key: String) -> Result<Self> {
        config.validate()?;
        if admin_key.is_empty() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            client: client()?,
            config,
            admin_key,
        })
    }
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.config.management_url.trim_end_matches('/'))
    }
    async fn key_info(&self, user_id: &str, key: &str) -> Result<Option<Value>> {
        // Query by hash to avoid logging the usable key in provider access URLs.
        let response = self
            .client
            .get(self.url("/key/info"))
            .bearer_auth(&self.admin_key)
            .query(&[("key", digest(key))])
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        let value = json_response(response).await?;
        let info = &value["info"];
        if info["user_id"].as_str() != Some(user_id) || info["blocked"].as_bool() == Some(true) {
            return Err(Error::Forbidden);
        }
        Ok(Some(info.clone()))
    }
    async fn user_info(&self, user_id: &str) -> Result<Option<Value>> {
        let response = self
            .client
            .get(self.url("/user/info"))
            .bearer_auth(&self.admin_key)
            .query(&[("user_id", user_id)])
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        let value = json_response(response).await?;
        let info = value.get("user_info").unwrap_or(&value);
        if info["user_id"].as_str() != Some(user_id) && value["user_id"].as_str() != Some(user_id) {
            return Err(Error::Unavailable);
        }
        Ok(Some(info.clone()))
    }
    async fn user(&self, user_id: &str) -> Result<()> {
        if self.user_info(user_id).await?.is_some() {
            return Ok(());
        }
        let response = self
            .client
            .post(self.url("/user/new"))
            .bearer_auth(&self.admin_key)
            .json(&json!({"user_id":user_id,"user_role":"internal_user","auto_create_key":false}))
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        // Another request may have created the same user after our lookup.
        if !response.status().is_success() && response.status() != StatusCode::CONFLICT {
            return Err(Error::Unavailable);
        }
        self.user_info(user_id)
            .await?
            .map(|_| ())
            .ok_or(Error::Unavailable)
    }
    fn quota_from_info(&self, info: &Value) -> Result<QuotaResponse> {
        fn amount(value: &Value, factor: f64) -> Result<Option<f64>> {
            let Some(parsed) = value
                .as_f64()
                .or_else(|| value.as_str()?.parse().ok())
                .filter(|value| value.is_finite() && *value >= 0.0)
            else {
                return Ok(None);
            };
            let points = parsed * factor;
            if !points.is_finite() {
                return Err(Error::Unavailable);
            }
            Ok(Some(points))
        }
        let total = amount(&info["max_budget"], self.config.points_per_budget_unit)?;
        let used = amount(&info["spend"], self.config.points_per_budget_unit)?;
        let known = total.is_some() && used.is_some();
        let balance = match (total, used) {
            (Some(total), Some(used)) => {
                ((total - used).max(0.0) * 1_000_000.0).round() / 1_000_000.0
            }
            _ => 0.0,
        };
        if !balance.is_finite() {
            return Err(Error::Unavailable);
        }
        let reset_period = info["budget_duration"].as_str().unwrap_or("").to_owned();
        let reset_at = if reset_period.is_empty() {
            String::new()
        } else {
            info["budget_reset_at"].as_str().unwrap_or("").to_owned()
        };
        Ok(QuotaResponse {
            ok: true,
            available: true,
            quota: ModelQuota {
                total: total.unwrap_or(-1.0),
                balance,
                used: used.unwrap_or(-1.0),
                low_balance: matches!((total, used), (Some(total), Some(used)) if total > 0.0 && used >= total * 0.9),
                exhausted: known && balance <= 0.0,
                reset_period,
                reset_at,
            },
        })
    }
}
#[async_trait]
impl ModelProvider for LiteLlm {
    async fn ensure(&self, user_id: &str, key: &str, key_alias: &str) -> Result<()> {
        if let Some(info) = self.key_info(user_id, key).await? {
            if has_proxy_access(&info) && info["key_alias"].as_str() == Some(key_alias) {
                return Ok(());
            }
            // Reconcile only this verified owner's access and display alias.
            // Preserve the key, budget, spend and limits, including after lost responses.
            let mut update = json!({"key":digest(key)});
            if !has_proxy_access(&info) {
                update["models"] = json!(["all-proxy-models"]);
            }
            if info["key_alias"].as_str() != Some(key_alias) {
                update["key_alias"] = json!(key_alias);
            }
            let response = self
                .client
                .post(self.url("/key/update"))
                .bearer_auth(&self.admin_key)
                .json(&update)
                .send()
                .await
                .map_err(|_| Error::Unavailable)?;
            if !response.status().is_success() {
                return Err(Error::Unavailable);
            }
            return match self.key_info(user_id, key).await? {
                Some(info)
                    if has_proxy_access(&info) && info["key_alias"].as_str() == Some(key_alias) =>
                {
                    Ok(())
                }
                _ => Err(Error::Unavailable),
            };
        }
        self.user(user_id).await?;
        let result=self.client.post(self.url("/key/generate")).bearer_auth(&self.admin_key).json(&json!({
            "key":key,"user_id":user_id,"key_alias":key_alias,"key_type":"llm_api",
            "models":["all-proxy-models"],"max_budget":self.config.max_budget,"budget_duration":self.config.budget_duration,
            "rpm_limit":self.config.rpm_limit,"tpm_limit":self.config.tpm_limit
        })).send().await;
        // Never generate a different key after conflict, timeout or a lost response.
        // A subsequent request reconciles this same persisted identity.
        match result {
            Ok(r) if r.status().is_success() => {}
            _ => return Err(Error::Unavailable),
        }
        match self.key_info(user_id, key).await? {
            Some(info)
                if has_proxy_access(&info) && info["key_alias"].as_str() == Some(key_alias) =>
            {
                Ok(())
            }
            _ => Err(Error::Unavailable),
        }
    }
    async fn quota(&self, user_id: &str, key: &str) -> Result<QuotaResponse> {
        let info = self
            .key_info(user_id, key)
            .await?
            .ok_or(Error::Unavailable)?;
        self.quota_from_info(&info)
    }
}

fn has_proxy_access(info: &Value) -> bool {
    info["models"].as_array().is_some_and(|models| {
        models
            .iter()
            .any(|model| model.as_str() == Some("all-proxy-models"))
    })
}

fn verify_claims(
    token: &str,
    key: &DecodingKey,
    algorithm: Algorithm,
    client_id: &str,
) -> Result<VerifiedIdentity> {
    let mut validation = Validation::new(algorithm);
    validation.set_audience(&[client_id]);
    validation.set_issuer(&[HUAWEI_ISSUER]);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.validate_nbf = true;
    validation.leeway = 30;
    let claims_value = decode::<Value>(token, key, &validation)
        .map_err(|_| Error::Unauthorized)?
        .claims;
    let claims: Claims = serde_json::from_value(claims_value).map_err(|_| Error::Unauthorized)?;
    if claims.sub.is_empty() {
        return Err(Error::Unauthorized);
    }
    let display_name = claims.display_name.and_then(|name| {
        let name = name.trim();
        if name.is_empty() || name.len() > 512 || name.chars().count() > 128
            || name.chars().any(|c| c.is_control() || matches!(c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')) {
            None
        } else {
            Some(name.to_owned())
        }
    });
    Ok(VerifiedIdentity {
        subject: claims.sub,
        display_name,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        http::HeaderMap,
        routing::{get, post},
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicI64, AtomicUsize, Ordering},
        Arc,
    };

    fn agc_token(project_id: &str, uid: &str, provider: i64) -> String {
        let now = adx_agent_core::unix_time_millis() / 1000;
        let header = URL_SAFE_NO_PAD.encode(json!({"alg":"RS256","typ":"JWT"}).to_string());
        let payload = URL_SAFE_NO_PAD.encode(
            json!({
                "iss": format!("https://agc.developer.huawei.com/{project_id}"),
                "aud": project_id,
                "sub": uid,
                "iat": now,
                "exp": now + 300,
                "agc": {"sign_in_provider": provider},
            })
            .to_string(),
        );
        format!("{header}.{payload}.synthetic-signature")
    }

    #[test]
    fn agc_claims_bind_verified_uid_project_issuer_and_expiry() {
        let token = agc_token("project", "uid-1", 11);
        validate_agc_claims(&token, "project", "uid-1").unwrap();
        let failure = validate_agc_claims(&token, "other-project", "uid-1").unwrap_err();
        assert_eq!(failure.stage, "local_claims");
        assert_eq!(failure.reason, "issuer_mismatch");
        assert!(validate_agc_claims(&token, "project", "uid-2").is_err());
        validate_agc_claims(&agc_token("project", "uid-1", 0), "project", "uid-1").unwrap();
    }

    struct AgcMock {
        token_requests: AtomicUsize,
        verify_code: AtomicI64,
        user_token: String,
    }

    async fn agc_service_token(
        State(state): State<Arc<AgcMock>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        assert_eq!(headers["algorithm_type"], "1");
        assert_eq!(body["grant_type"], "client_credentials");
        assert_eq!(body["client_id"], "api-client");
        assert_eq!(body["client_secret"], "api-secret");
        state.token_requests.fetch_add(1, Ordering::SeqCst);
        Json(json!({"access_token":"server-token","expires_in":3600}))
    }

    async fn agc_verify_token(
        State(state): State<Arc<AgcMock>>,
        Query(query): Query<std::collections::HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        assert_eq!(query["productId"], "project");
        assert_eq!(headers["authorization"], "Bearer server-token");
        assert_eq!(headers["client_id"], "api-client");
        assert_eq!(headers["accesstoken"].to_str().unwrap(), state.user_token);
        let code = state.verify_code.load(Ordering::SeqCst);
        Json(json!({
            "ret":{"code":code,"msg":"mock"},
            "productId":"project",
            "uid":"uid-1",
            "accessToken":{"validPeriod":3600},
        }))
    }

    #[tokio::test]
    async fn agc_uses_server_verification_with_revocation_check_and_caches_only_service_token() {
        let user_token = agc_token("project", "uid-1", 11);
        let state = Arc::new(AgcMock {
            token_requests: AtomicUsize::new(0),
            verify_code: AtomicI64::new(0),
            user_token: user_token.clone(),
        });
        let app = Router::new()
            .route("/api/oauth2/v1/token", post(agc_service_token))
            .route("/api/oauth2/third/v1/verify-token", get(agc_verify_token))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider = Agc::with_origin(
            AgcConfig {
                project_id: "project".into(),
                api_client_id: "api-client".into(),
                api_client_secret_env: "UNUSED".into(),
            },
            "api-secret".into(),
            &origin,
        )
        .unwrap();
        assert_eq!(provider.verify(&user_token).await.unwrap().subject, "uid-1");
        assert_eq!(provider.verify(&user_token).await.unwrap().subject, "uid-1");
        assert_eq!(state.token_requests.load(Ordering::SeqCst), 1);
        state.verify_code.store(203818355, Ordering::SeqCst);
        let failure = provider.verify_remote(&user_token).await.unwrap_err();
        assert_eq!(failure.stage, "verify_token_response");
        assert_eq!(failure.reason, "upstream_rejected");
        assert_eq!(failure.http_status, Some(200));
        assert_eq!(failure.upstream_code, Some(AGC_TOKEN_FORMAT_ERROR));
        assert!(matches!(
            provider.verify(&user_token).await,
            Err(Error::InvalidAgcAccessToken)
        ));
        state
            .verify_code
            .store(AGC_REVOKED_ACCESS_TOKEN, Ordering::SeqCst);
        assert!(matches!(
            provider.verify(&user_token).await,
            Err(Error::InvalidAgcAccessToken)
        ));
        server.abort();
    }

    #[test]
    fn huawei_signed_profile_provides_nickname_without_changing_identity() {
        let signing = jsonwebtoken::EncodingKey::from_rsa_pem(include_bytes!(
            "../../../tests/fixtures/huawei-test-key.pem"
        ))
        .unwrap();
        let verify = DecodingKey::from_rsa_pem(include_bytes!(
            "../../../tests/fixtures/huawei-test-public.pem"
        ))
        .unwrap();
        let now = adx_agent_core::unix_time_millis() / 1000;
        for nickname in [None, Some(" 小明 "), Some("坏\n名字")] {
            let claims = json!({"sub":"stable-subject","display_name":nickname,
                "aud":"app","iss":HUAWEI_ISSUER,"exp":now+300});
            let token = jsonwebtoken::encode(
                &jsonwebtoken::Header::new(Algorithm::RS256),
                &claims,
                &signing,
            )
            .unwrap();
            let identity = verify_claims(&token, &verify, Algorithm::RS256, "app").unwrap();
            assert_eq!(identity.subject, "stable-subject");
            assert_eq!(
                identity.display_name.as_deref(),
                if nickname == Some(" 小明 ") {
                    Some("小明")
                } else {
                    None
                }
            );
            assert!(verify_claims(&token, &verify, Algorithm::RS256, "wrong-app").is_err());
        }
    }

    #[test]
    fn key_spend_is_displayed_as_points_with_configured_conversion() {
        let config: ModelConfig = serde_json::from_value(json!({
            "management_url":"https://litellm.example.com",
            "api_base":"https://litellm.example.com/v1",
            "admin_key_env":"UNUSED",
            "max_budget":15.0,
            "points_per_budget_unit":2.0,
            "budget_duration":"1d",
            "rpm_limit":30,
            "tpm_limit":30000
        }))
        .unwrap();
        let provider = LiteLlm::with_key(config.clone(), "admin".into()).unwrap();
        for (spend, expected, low_balance, exhausted) in [
            (3.75, (30.0, 7.5, 22.5), false, false),
            (13.5, (30.0, 27.0, 3.0), true, false),
            (15.0, (30.0, 30.0, 0.0), true, true),
            (16.0, (30.0, 32.0, 0.0), true, true),
            (20.0, (30.0, 40.0, 0.0), true, true),
        ] {
            let quota = provider
                .quota_from_info(&json!({
                    "max_budget":15.0,"spend":spend,
                    "budget_duration":"1d","budget_reset_at":"2026-10-30T00:00:00+08:00"
                }))
                .unwrap();
            assert_eq!(
                (quota.quota.total, quota.quota.used, quota.quota.balance),
                expected
            );
            assert_eq!(quota.quota.low_balance, low_balance);
            assert_eq!(quota.quota.exhausted, exhausted);
            assert_eq!(quota.quota.reset_period, "1d");
            assert_eq!(quota.quota.reset_at, "2026-10-30T00:00:00+08:00");
        }
        let unknown = provider.quota_from_info(&json!({})).unwrap();
        assert_eq!(
            (
                unknown.quota.total,
                unknown.quota.used,
                unknown.quota.balance
            ),
            (-1.0, -1.0, 0.0)
        );
        assert!(!unknown.quota.exhausted);
        assert!(provider
            .quota_from_info(&json!({"max_budget":f64::MAX,"spend":0.0}))
            .is_err());
        for factor in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            let mut invalid = config.clone();
            invalid.points_per_budget_unit = factor;
            assert!(matches!(invalid.validate(), Err(Error::Configuration)));
        }
    }
    #[test]
    fn huawei_identity_requires_signature_application_issuer_and_expiry() {
        // Publicly committed synthetic RSA key, used only to sign test identity tokens.
        let signing = jsonwebtoken::EncodingKey::from_rsa_pem(include_bytes!(
            "../../../tests/fixtures/huawei-test-key.pem"
        ))
        .unwrap();
        let verify = DecodingKey::from_rsa_pem(include_bytes!(
            "../../../tests/fixtures/huawei-test-public.pem"
        ))
        .unwrap();
        let now = adx_agent_core::unix_time_millis() / 1000;
        let claims = json!({"sub":"UnionID-CaseSensitive","aud":"app","iss":HUAWEI_ISSUER,"exp":now+300,"iat":now});
        for alg in [Algorithm::RS256, Algorithm::PS256] {
            let token =
                jsonwebtoken::encode(&jsonwebtoken::Header::new(alg), &claims, &signing).unwrap();
            assert_eq!(
                verify_claims(&token, &verify, alg, "app").unwrap().subject,
                "UnionID-CaseSensitive"
            );
            assert!(verify_claims(&token, &verify, alg, "other-app").is_err());
            for (field, value) in [
                ("iss", json!("https://attacker.invalid")),
                ("exp", json!(now - 120)),
                ("sub", json!("")),
                ("nbf", json!(now + 3600)),
            ] {
                let mut bad = claims.clone();
                bad[field] = value;
                let token =
                    jsonwebtoken::encode(&jsonwebtoken::Header::new(alg), &bad, &signing).unwrap();
                assert!(
                    verify_claims(&token, &verify, alg, "app").is_err(),
                    "{field}"
                );
            }
            let mut parts = token.split('.').map(str::to_owned).collect::<Vec<_>>();
            parts[1] = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                json!({"sub":"other","aud":"app","iss":HUAWEI_ISSUER,"exp":now+300}).to_string(),
            );
            assert!(verify_claims(&parts.join("."), &verify, alg, "app").is_err());
        }
    }
}
