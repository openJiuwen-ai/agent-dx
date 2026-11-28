//! HTTP-only clients, with deployment-owned origins and tenant keys.
use crate::config::{ConsoleConfig, Tenant};
use reqwest::{Method, StatusCode};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct Clients { pub http: reqwest::Client, pub trust: crate::tls::Trust }
pub struct UpstreamError { pub status: StatusCode, pub kind: &'static str }
impl Clients {
    pub async fn accounts(&self, config: &crate::config::AccountsApi, method: Method, segments: &[&str], query: &str, body: Option<&Value>, actor: &str) -> Result<Value, UpstreamError> {
        let token = std::fs::read_to_string(&config.token_file).map_err(|_|UpstreamError { status:StatusCode::SERVICE_UNAVAILABLE,kind:"upstream_credential_unavailable" })?;
        let token = token.trim();
        if token.is_empty() || token.chars().any(char::is_control) { return Err(UpstreamError {status:StatusCode::SERVICE_UNAVAILABLE,kind:"upstream_credential_invalid"}); }
        let mut url = Self::url(&config.base_url,segments)?;
        if !query.is_empty() { url.set_query(Some(query)); }
        let mut request = self.http.request(method,url).bearer_auth(token).header("x-adx-console-actor",actor).timeout(Duration::from_millis(config.timeout_ms));
        if let Some(body) = body { request = request.json(body); }
        let mut response = request.send().await.map_err(|_|UpstreamError {status:StatusCode::SERVICE_UNAVAILABLE,kind:"accounts_unavailable"})?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_|UpstreamError {status:StatusCode::SERVICE_UNAVAILABLE,kind:"accounts_unavailable"})? {
            if bytes.len().saturating_add(chunk.len()) > 4*1024*1024 { return Err(UpstreamError {status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid"}); }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_|UpstreamError {status:StatusCode::BAD_GATEWAY,kind:"upstream_response_invalid"})?;
        if !status.is_success() {
            let kind = match value["error"]["kind"].as_str() {
                Some("key_identity_conflict") => "key_identity_conflict",
                Some("not_recoverable") => "not_recoverable",
                Some("user_not_found") => "user_not_found",
                Some("invalid_query") => "invalid_query",
                _ => "accounts_unavailable",
            };
            return Err(UpstreamError { status,kind });
        }
        Ok(value)
    }
    pub fn new() -> Result<Self, String> {
        Self::with_ca(None)
    }
    pub fn with_ca(path: Option<&str>) -> Result<Self, String> {
        let trust = crate::tls::Trust::load(path)?;
        Ok(Self { http: trust.http_builder()?.redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10)).build().map_err(|_| "HTTP client initialization failed")?, trust })
    }
    pub fn url(base: &str, segments: &[&str]) -> Result<url::Url, UpstreamError> {
        let error = || UpstreamError { status: StatusCode::BAD_GATEWAY, kind:"upstream_configuration_invalid" };
        let mut url = url::Url::parse(base).map_err(|_| error())?;
        url.path_segments_mut().map_err(|_| error())?.pop_if_empty().extend(segments);
        Ok(url)
    }
    pub fn token(tenant: &Tenant) -> Result<String, UpstreamError> {
        let token = std::fs::read_to_string(&tenant.upstream_token_file).map_err(|_| UpstreamError { status:StatusCode::SERVICE_UNAVAILABLE, kind:"upstream_credential_unavailable" })?;
        let token = token.trim();
        if token.is_empty() || token.chars().any(char::is_control) { return Err(UpstreamError { status:StatusCode::SERVICE_UNAVAILABLE, kind:"upstream_credential_invalid" }); }
        Ok(token.into())
    }
    pub async fn json(&self, tenant: &Tenant, method: Method, url: url::Url, body: Option<&Value>, timeout_ms: u64) -> Result<Value, UpstreamError> {
        let token = Self::token(tenant)?;
        let mut request = self.http.request(method, url).bearer_auth(token).timeout(Duration::from_millis(timeout_ms));
        if let Some(body) = body { request = request.json(body); }
        let response = request.send().await.map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY, kind:"upstream_outcome_unknown" })?;
        let status = response.status();
        if !status.is_success() { return Err(UpstreamError { status, kind:"upstream_request_failed" }); }
        response.json().await.map_err(|_| UpstreamError { status:StatusCode::BAD_GATEWAY, kind:"upstream_response_invalid" })
    }
    pub async fn agent(&self, config: &ConsoleConfig, tenant: &Tenant, method: Method, segments: &[&str], query: &str, body: Option<&Value>) -> Result<Value, UpstreamError> {
        let mut url = Self::url(&config.agent_api.base_url, segments)?;
        if !query.is_empty() { url.set_query(Some(query)); }
        self.json(tenant, method, url, body, config.agent_api.timeout_ms).await
    }
}
