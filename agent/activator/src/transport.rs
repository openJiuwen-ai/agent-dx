//! Standalone Activator adapter for the existing API Server management API.
use crate::environment_request::{self, observation, validate_identity};
use adx_agent_core::network::SandboxNetworkPolicy;
use adx_agent_core::{
    environment::*,
    limits,
    transport::{capped_deadline, remaining_time, service_origin},
};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

pub const ENVIRONMENT_PATH: &str = "/api/environment/v1/environments";

pub struct HttpEnvironment {
    client: Client,
    base: Url,
    api_keys: BTreeMap<String, String>,
    timeout: Duration,
    network: SandboxNetworkPolicy,
}
impl HttpEnvironment {
    /// Keys must be tenant-scoped Platform API keys; API Server derives ownership from the key.
    pub fn new(
        base: &str,
        api_keys: BTreeMap<String, String>,
        timeout: Duration,
        ca_pem: Option<&[u8]>,
        allow_http: bool,
        network: SandboxNetworkPolicy,
    ) -> Result<Self, EnvironmentError> {
        let base = service_origin(base, allow_http).map_err(EnvironmentError::Invalid)?;
        if api_keys.is_empty() {
            return Err(EnvironmentError::Invalid(
                "Environment tenant API keys required".into(),
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
                return Err(EnvironmentError::Invalid("invalid Platform API key".into()));
            }
        }
        environment_request::validate_timeout(timeout)?;
        environment_request::network_input(&network)?;
        let mut builder = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(limits::CONNECT_TIMEOUT))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| EnvironmentError::Invalid("invalid API Server CA".into()))?,
            );
        }
        Ok(Self {
            client: builder.build().map_err(|_| {
                EnvironmentError::Invalid("HTTP client initialization failed".into())
            })?,
            base,
            api_keys,
            timeout,
            network,
        })
    }

    async fn volume_request(
        &self,
        tenant: &str,
        owner: &str,
        spec: Option<&adx_agent_core::volumes::VolumeSpec>,
    ) -> Result<Value, EnvironmentError> {
        validate_identity(tenant)?;
        validate_identity(owner)?;
        let key = self.api_keys.get(tenant).ok_or_else(|| {
            EnvironmentError::Invalid("Platform tenant credential is not configured".into())
        })?;
        let uncertain = || {
            EnvironmentError::OutcomeUnknown(
                "Volume management response unavailable; retry same identity".into(),
            )
        };
        let mut url = self
            .base
            .join(if spec.is_some() {
                "/api/environment/v1/volumes"
            } else {
                "/api/environment/v1/volume-owners"
            })
            .map_err(|_| EnvironmentError::Invalid("invalid Volume URL".into()))?;
        let mut request = if let Some(spec) = spec {
            spec.validate().map_err(EnvironmentError::Invalid)?;
            let mut input = serde_json::to_value(spec)
                .map_err(|_| EnvironmentError::Invalid("invalid Volume definition".into()))?;
            input["owner_id"] = json!(owner);
            self.client.post(url).json(&input)
        } else {
            url.path_segments_mut()
                .map_err(|_| EnvironmentError::Invalid("invalid Volume URL".into()))?
                .push(owner);
            self.client.delete(url)
        }
        .bearer_auth(key)
        .timeout(self.timeout);
        request = request.header(
            "x-request-id",
            environment_request::operation_id("volumes", tenant, owner)?,
        );
        let mut response = request.send().await.map_err(|_| uncertain())?;
        let status = response.status();
        if !status.is_success() {
            return Err(match status {
                StatusCode::BAD_REQUEST => {
                    EnvironmentError::Invalid("Volume request rejected".into())
                }
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    EnvironmentError::Invalid("Volume request credentials rejected".into())
                }
                StatusCode::NOT_FOUND => EnvironmentError::NotFound,
                StatusCode::CONFLICT => {
                    EnvironmentError::Conflict("Volume is still referenced or occupied".into())
                }
                StatusCode::NOT_IMPLEMENTED => {
                    EnvironmentError::Unsupported("Volume management unavailable".into())
                }
                _ => uncertain(),
            });
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| uncertain())? {
            if chunk.len() > limits::HTTP_JSON_BYTES.saturating_sub(body.len()) {
                return Err(uncertain());
            }
            body.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&body).map_err(|_| uncertain())?;
        if value["code"] != 200 {
            return Err(uncertain());
        }
        match &value["data"] {
            Value::String(encoded) => {
                serde_json::from_slice(&STANDARD.decode(encoded).map_err(|_| uncertain())?)
                    .map_err(|_| uncertain())
            }
            Value::Null if spec.is_none() => Ok(Value::Null),
            _ => Err(uncertain()),
        }
    }

    async fn request(
        &self,
        method: Method,
        tenant: &str,
        id: &str,
        payload: Option<Value>,
        deadline: Option<u64>,
        creation_attempt: u64,
    ) -> Result<Option<Value>, EnvironmentError> {
        validate_identity(tenant)?;
        validate_identity(id)?;
        let key = self.api_keys.get(tenant).ok_or_else(|| {
            if method == Method::GET {
                EnvironmentError::NotFound
            } else {
                EnvironmentError::Unavailable("Platform tenant credential is not configured".into())
            }
        })?;
        let read = method == Method::GET;
        let create = method == Method::POST;
        let delete = method == Method::DELETE;
        let mut url = self
            .base
            .join(if read {
                "/api/instances"
            } else {
                ENVIRONMENT_PATH
            })
            .map_err(|_| EnvironmentError::Invalid("invalid API Server URL".into()))?;
        if read {
            url.query_pairs_mut().append_pair("instance_id", id);
        } else if !create {
            url.path_segments_mut()
                .map_err(|_| EnvironmentError::Invalid("invalid API Server URL".into()))?
                .push(id);
        }
        let remaining = remaining_time(capped_deadline(deadline, self.timeout));
        if remaining.is_zero() {
            return Err(EnvironmentError::Unavailable(
                "Environment deadline expired before submission".into(),
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
                if create {
                    environment_request::creation_operation_id(tenant, id, creation_attempt)?
                } else {
                    environment_request::operation_id(action, tenant, id)?
                },
            );
        }
        if let Some(payload) = payload {
            request = request.json(&payload);
        }
        let uncertain = || {
            if read {
                EnvironmentError::Unavailable("Environment read failed".into())
            } else {
                EnvironmentError::OutcomeUnknown(
                    "Environment write response unavailable; query the original ID".into(),
                )
            }
        };
        let mut response = request.send().await.map_err(|error| {
            if error.is_connect() {
                EnvironmentError::Unavailable("API Server connection failed".into())
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
            if create && status == StatusCode::GATEWAY_TIMEOUT {
                if let Ok(value) = serde_json::from_slice::<Value>(&body) {
                    if let Some(error) = environment_request::scheduling_cancellation(&value) {
                        return Err(error);
                    }
                }
            }
            // Keep explicit DELETE absence distinct from authentication and transport failures.
            return Err(match status {
                StatusCode::BAD_REQUEST => {
                    EnvironmentError::Invalid("API Server rejected request".into())
                }
                StatusCode::CONFLICT => {
                    EnvironmentError::Conflict("Environment identity/specification conflict".into())
                }
                StatusCode::NOT_FOUND if delete => EnvironmentError::NotFound,
                StatusCode::FORBIDDEN if read => EnvironmentError::NotFound,
                StatusCode::FORBIDDEN => {
                    EnvironmentError::Unavailable("API Server permission denied".into())
                }
                StatusCode::UNAUTHORIZED => {
                    EnvironmentError::Unavailable("API Server credential rejected".into())
                }
                StatusCode::NOT_IMPLEMENTED => {
                    EnvironmentError::Unsupported("Environment capability unavailable".into())
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
            environment_request::created(tenant, id, &result)?;
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
impl Environment for HttpEnvironment {
    async fn ensure_volume(
        &self,
        tenant: &str,
        owner: &str,
        spec: &adx_agent_core::volumes::VolumeSpec,
    ) -> Result<adx_agent_core::volumes::VolumeRef, EnvironmentError> {
        environment_request::volume_reference(
            &self.volume_request(tenant, owner, Some(spec)).await?,
            tenant,
            &spec.id,
        )
    }
    async fn release_volumes(&self, tenant: &str, owner: &str) -> Result<(), EnvironmentError> {
        self.volume_request(tenant, owner, None).await.map(|_| ())
    }
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<(), EnvironmentError> {
        environment_request::validate_execution(execution)
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let payload = environment_request::create_input(request, self.timeout, &self.network)?;
        self.request(
            Method::POST,
            &request.tenant,
            &request.id,
            Some(payload),
            request.deadline_unix_ms,
            request.creation_attempt,
        )
        .await?;
        Ok(observation(
            &request.tenant,
            &request.id,
            EnvironmentPhase::Running,
        ))
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        let Some(value) = self.request(Method::GET, tenant, id, None, None, 0).await? else {
            return Ok(None);
        };
        let rows = value
            .as_array()
            .filter(|rows| rows.len() == 1)
            .ok_or_else(|| {
                EnvironmentError::Unavailable("invalid API Server instance response".into())
            })?;
        environment_request::observed(tenant, id, &rows[0]).map(Some)
    }
    async fn delete(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.request(
            Method::DELETE,
            tenant,
            id,
            Some(json!({"timeoutSeconds":self.timeout.as_secs().max(1)})),
            None,
            0,
        )
        .await?;
        Ok(observation(tenant, id, EnvironmentPhase::Deleted))
    }
}
