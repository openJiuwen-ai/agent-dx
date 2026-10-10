//! Standalone Activator adapter for the existing API Server management API.
use crate::sandbox_request::{self, observation, validate_identity};
use adx_agent_core::{
    limits,
    sandbox::*,
    transport::{capped_deadline, remaining_time, service_origin},
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

pub const SANDBOX_PATH: &str = "/api/sandbox/v1/sandboxes";

pub struct HttpSandbox {
    client: Client,
    base: Url,
    api_keys: BTreeMap<String, String>,
    timeout: Duration,
}
impl HttpSandbox {
    /// Keys must be tenant-scoped Platform API keys; API Server derives ownership from the key.
    pub fn new(
        base: &str,
        api_keys: BTreeMap<String, String>,
        timeout: Duration,
        ca_pem: Option<&[u8]>,
        allow_http: bool,
    ) -> Result<Self, SandboxError> {
        let base = service_origin(base, allow_http).map_err(SandboxError::Invalid)?;
        if api_keys.is_empty() {
            return Err(SandboxError::Invalid(
                "Sandbox tenant API keys required".into(),
            ));
        }
        for (tenant, key) in &api_keys {
            validate_identity(tenant)?;
            if key.is_empty()
                || key.len() > limits::HTTP_JSON_BYTES
                || key
                    .bytes()
                    .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                || reqwest::header::HeaderValue::from_str(key).is_err()
            {
                return Err(SandboxError::Invalid("invalid Platform API key".into()));
            }
        }
        sandbox_request::validate_timeout(timeout)?;
        let mut builder = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(limits::CONNECT_TIMEOUT))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| SandboxError::Invalid("invalid API Server CA".into()))?,
            );
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|_| SandboxError::Invalid("HTTP client initialization failed".into()))?,
            base,
            api_keys,
            timeout,
        })
    }

    async fn request(
        &self,
        method: Method,
        tenant: &str,
        id: &str,
        payload: Option<Value>,
        deadline: Option<u64>,
    ) -> Result<Option<Value>, SandboxError> {
        validate_identity(tenant)?;
        validate_identity(id)?;
        let key = self.api_keys.get(tenant).ok_or(SandboxError::NotFound)?;
        let read = method == Method::GET;
        let create = method == Method::POST;
        let mut url = self
            .base
            .join(if read { "/api/instances" } else { SANDBOX_PATH })
            .map_err(|_| SandboxError::Invalid("invalid API Server URL".into()))?;
        if read {
            url.query_pairs_mut().append_pair("instance_id", id);
        } else if !create {
            url.path_segments_mut()
                .map_err(|_| SandboxError::Invalid("invalid API Server URL".into()))?
                .push(id);
        }
        let remaining = remaining_time(capped_deadline(deadline, self.timeout));
        if remaining.is_zero() {
            return Err(SandboxError::Unavailable(
                "Sandbox deadline expired before submission".into(),
            ));
        }
        let mut request = self
            .client
            .request(method, url)
            .bearer_auth(key)
            .timeout(remaining);
        if !read {
            let action = if create { "create" } else { "delete" };
            request = request.header(
                "x-request-id",
                sandbox_request::operation_id(action, tenant, id)?,
            );
        }
        if let Some(payload) = payload {
            request = request.json(&payload);
        }
        let uncertain = || {
            if read {
                SandboxError::Unavailable("Sandbox read failed".into())
            } else {
                SandboxError::OutcomeUnknown(
                    "Sandbox write response unavailable; query the original ID".into(),
                )
            }
        };
        let mut response = request.send().await.map_err(|error| {
            if error.is_connect() {
                SandboxError::Unavailable("API Server connection failed".into())
            } else {
                uncertain()
            }
        })?;
        let status = response.status();
        if read && status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response
            .content_length()
            .is_some_and(|n| n > limits::HTTP_JSON_BYTES as u64)
        {
            return Err(uncertain());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| uncertain())? {
            if chunk.len() > limits::HTTP_JSON_BYTES.saturating_sub(body.len()) {
                return Err(uncertain());
            }
            body.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            // Do not expose server messages or interpret absence as completed deletion.
            return Err(match status {
                StatusCode::BAD_REQUEST => {
                    SandboxError::Invalid("API Server rejected request".into())
                }
                StatusCode::CONFLICT => {
                    SandboxError::Conflict("Sandbox identity/specification conflict".into())
                }
                StatusCode::FORBIDDEN => SandboxError::NotFound,
                StatusCode::UNAUTHORIZED => {
                    SandboxError::Unavailable("API Server credential rejected".into())
                }
                StatusCode::NOT_IMPLEMENTED => {
                    SandboxError::Unsupported("Sandbox capability unavailable".into())
                }
                _ => uncertain(),
            });
        }
        let value: Value = serde_json::from_slice(&body).map_err(|_| uncertain())?;
        if read {
            return Ok(Some(value));
        }
        if value.get("code").and_then(Value::as_u64) != Some(200) {
            return Err(uncertain());
        }
        let data = value.get("data").ok_or_else(uncertain)?;
        if create {
            let encoded = data.as_str().ok_or_else(uncertain)?;
            let decoded = STANDARD.decode(encoded).map_err(|_| uncertain())?;
            let result: Value = serde_json::from_slice(&decoded).map_err(|_| uncertain())?;
            sandbox_request::created(tenant, id, &result)?;
            Ok(Some(result))
        } else if data.is_null() {
            // API Server returns success only after deletion confirmation is published.
            Ok(Some(Value::Null))
        } else {
            Err(uncertain())
        }
    }
}
#[async_trait]
impl Sandbox for HttpSandbox {
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), SandboxError> {
        sandbox_request::validate_execution(execution)
    }
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let payload = sandbox_request::create_input(request, self.timeout)?;
        self.request(
            Method::POST,
            &request.tenant,
            &request.id,
            Some(payload),
            request.deadline_unix_ms,
        )
        .await?;
        Ok(observation(
            &request.tenant,
            &request.id,
            SandboxPhase::Running,
        ))
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        let Some(value) = self.request(Method::GET, tenant, id, None, None).await? else {
            return Ok(None);
        };
        let rows = value
            .as_array()
            .filter(|rows| rows.len() == 1)
            .ok_or_else(|| {
                SandboxError::Unavailable("invalid API Server instance response".into())
            })?;
        sandbox_request::observed(tenant, id, &rows[0]).map(Some)
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.request(
            Method::DELETE,
            tenant,
            id,
            Some(json!({"timeoutSeconds":self.timeout.as_secs().max(1)})),
            None,
        )
        .await?;
        Ok(observation(tenant, id, SandboxPhase::Deleted))
    }
}
