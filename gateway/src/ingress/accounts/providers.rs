//! Bounded server-to-server clients. No redirects or caller-selected Huawei endpoints.
use super::*;
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::Instant;

const HUAWEI_ORIGIN: &str = "https://oauth-login.cloud.huawei.com";
const HUAWEI_ISSUER: &str = "https://accounts.huawei.com";
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    async fn exchange(&self, code: &str) -> Result<String>;
}
#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn ensure(&self, user_id: &str, key: &str) -> Result<()>;
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
    async fn verify(&self, token: &str) -> Result<String> {
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
    async fn exchange(&self, code: &str) -> Result<String> {
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
        if self.model.trim().is_empty()
            || self.model.len() > 256
            || self.model == "*"
            || !self.max_budget.is_finite()
            || self.max_budget <= 0.0
            || self.rpm_limit == 0
            || self.tpm_limit == 0
            || self.budget_duration.is_empty()
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
    async fn key_exists(&self, user_id: &str, key: &str) -> Result<bool> {
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
            return Ok(false);
        }
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        let value = json_response(response).await?;
        let info = &value["info"];
        if info["user_id"].as_str() != Some(user_id)
            || info["blocked"].as_bool() == Some(true)
            || !info["models"].as_array().is_some_and(|models| {
                models
                    .iter()
                    .any(|m| m.as_str() == Some(self.config.model.as_str()))
            })
        {
            return Err(Error::Forbidden);
        }
        Ok(true)
    }
    async fn user(&self, user_id: &str) -> Result<()> {
        let response = self
            .client
            .get(self.url("/user/info"))
            .bearer_auth(&self.admin_key)
            .query(&[("user_id", user_id)])
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if response.status().is_success() {
            let value = json_response(response).await?;
            if value["user_info"]["user_id"].as_str() == Some(user_id)
                || value["user_id"].as_str() == Some(user_id)
            {
                return Ok(());
            }
            return Err(Error::Unavailable);
        }
        if response.status() != StatusCode::NOT_FOUND {
            return Err(Error::Unavailable);
        }
        let response = self
            .client
            .post(self.url("/user/new"))
            .bearer_auth(&self.admin_key)
            .json(&json!({"user_id":user_id,"user_role":"internal_user","auto_create_key":false}))
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        if !response.status().is_success() {
            return Err(Error::Unavailable);
        }
        Ok(())
    }
}
#[async_trait]
impl ModelProvider for LiteLlm {
    async fn ensure(&self, user_id: &str, key: &str) -> Result<()> {
        self.user(user_id).await?;
        if self.key_exists(user_id, key).await? {
            return Ok(());
        }
        let result=self.client.post(self.url("/key/generate")).bearer_auth(&self.admin_key).json(&json!({
            "key":key,"user_id":user_id,"key_alias":user_id,"key_type":"llm_api",
            "models":[self.config.model],"max_budget":self.config.max_budget,"budget_duration":self.config.budget_duration,
            "rpm_limit":self.config.rpm_limit,"tpm_limit":self.config.tpm_limit
        })).send().await;
        // Never generate a different key after conflict, timeout or a lost response.
        // A subsequent request reconciles this same persisted identity.
        match result {
            Ok(r) if r.status().is_success() => {}
            _ => return Err(Error::Unavailable),
        }
        if self.key_exists(user_id, key).await? {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
}

fn verify_claims(
    token: &str,
    key: &DecodingKey,
    algorithm: Algorithm,
    client_id: &str,
) -> Result<String> {
    let mut validation = Validation::new(algorithm);
    validation.set_audience(&[client_id]);
    validation.set_issuer(&[HUAWEI_ISSUER]);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.validate_nbf = true;
    validation.leeway = 30;
    let claims = decode::<Claims>(token, key, &validation)
        .map_err(|_| Error::Unauthorized)?
        .claims;
    if claims.sub.is_empty() {
        return Err(Error::Unauthorized);
    }
    Ok(claims.sub)
}
#[cfg(test)]
mod tests {
    use super::*;
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
                verify_claims(&token, &verify, alg, "app").unwrap(),
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
