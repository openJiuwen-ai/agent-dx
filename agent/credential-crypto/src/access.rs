//! A pooled, verify-only connection to the existing AgentBinding authority.
use crate::{
    storage::{read_private, StorageError},
    Context,
};
use adx_agent_core::{
    service_grants::{ServiceGrantDecision, VerifyServiceGrant},
    transport::{service_origin, validate_service_token},
};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessConfig {
    pub url: String,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub allow_loopback_http: bool,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}
fn default_timeout() -> u64 {
    adx_agent_core::limits::SERVICE_VERIFY_TIMEOUT.as_secs()
}

#[derive(Clone)]
pub struct AccessClient {
    client: Client,
    base: Url,
    token_file: PathBuf,
}

impl AccessClient {
    /// Uses a server-side private service credential, never a platform tenant key from a sandbox.
    pub fn new(config: &AccessConfig) -> Result<Self, StorageError> {
        let base = service_origin(&config.url, config.allow_loopback_http)
            .map_err(|_| StorageError::Invalid)?;
        if base.scheme() == "http"
            && !matches!(
                base.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
        {
            return Err(StorageError::Invalid);
        }
        if !(1..=30).contains(&config.timeout_seconds) {
            return Err(StorageError::Invalid);
        }
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(config.timeout_seconds))
            .timeout(Duration::from_secs(config.timeout_seconds))
            .pool_max_idle_per_host(16);
        if let Some(path) = &config.ca_file {
            let certificate = reqwest::Certificate::from_pem(&read_private(path, 65_536)?)
                .map_err(|_| StorageError::Invalid)?;
            builder = builder.add_root_certificate(certificate);
        }
        let client = Self {
            client: builder.build().map_err(|_| StorageError::Invalid)?,
            base,
            token_file: config.token_file.clone(),
        };
        client.identity()?;
        Ok(client)
    }
    fn identity(&self) -> Result<Zeroizing<String>, StorageError> {
        let raw = read_private(&self.token_file, 1024)?;
        let token = Zeroizing::new(
            std::str::from_utf8(&raw)
                .map_err(|_| StorageError::Invalid)?
                .trim()
                .to_owned(),
        );
        validate_service_token(&token).map_err(|_| StorageError::Invalid)?;
        if token
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(StorageError::Invalid);
        }
        Ok(token)
    }

    /// Verify only; grant issuance/revocation remain in the existing Activator control API.
    pub async fn allows(&self, credential: &str, expected: &Context) -> Result<bool, StorageError> {
        let request = VerifyServiceGrant {
            credential: credential.into(),
            expected: expected.clone(),
        };
        let token = self.identity()?;
        let url = self
            .base
            .join("/internal/adx/v1/service-grants/verify")
            .map_err(|_| StorageError::Invalid)?;
        let mut response = self
            .client
            .post(url)
            .bearer_auth(token.as_str())
            .json(&request)
            .send()
            .await
            .map_err(|_| StorageError::Authority)?;
        let limit = adx_agent_core::limits::SERVICE_GRANT_JSON_BYTES;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|length| length > limit as u64)
        {
            return Err(StorageError::Authority);
        }
        let mut body = Zeroizing::new(Vec::new());
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StorageError::Authority)?
        {
            if body.len() + chunk.len() > limit {
                return Err(StorageError::Authority);
            }
            body.extend_from_slice(&chunk);
        }
        let decision: ServiceGrantDecision =
            serde_json::from_slice(&body).map_err(|_| StorageError::Authority)?;
        Ok(decision.allowed)
    }
}
