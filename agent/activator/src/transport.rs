//! HTTP binding of the common Gateway Sandbox API; no Platform client lives here.
use adx_agent_core::{
    limits,
    sandbox::*,
    transport::{service_origin, validate_service_token},
};
use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode, Url};
use std::time::Duration;

pub const SANDBOX_PATH: &str = "/api/sandbox/v2/instances";

pub struct HttpSandbox {
    client: Client,
    base: Url,
    token: String,
    timeout: Duration,
}
impl HttpSandbox {
    pub fn new(
        base: &str,
        token: String,
        timeout: Duration,
        ca_pem: Option<&[u8]>,
        allow_http: bool,
    ) -> Result<Self, SandboxError> {
        let base = service_origin(base, allow_http).map_err(SandboxError::Invalid)?;
        validate_service_token(&token).map_err(SandboxError::Invalid)?;
        if timeout.is_zero() {
            return Err(SandboxError::Invalid(
                "Sandbox timeout must be positive".into(),
            ));
        }
        let mut builder = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(limits::CONNECT_TIMEOUT))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| SandboxError::Invalid("invalid Gateway CA".into()))?,
            );
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|_| SandboxError::Invalid("HTTP client initialization failed".into()))?,
            base,
            token,
            timeout,
        })
    }
    async fn request(
        &self,
        method: Method,
        tenant: &str,
        id: Option<&str>,
        payload: Option<&CreateSandbox>,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        if tenant.is_empty()
            || tenant.len() > limits::IDENTIFIER_BYTES
            || id.is_some_and(|id| {
                id.is_empty() || id.len() > limits::IDENTIFIER_BYTES || id == "." || id == ".."
            })
        {
            return Err(SandboxError::Invalid("invalid Sandbox identity".into()));
        }
        let read = method == Method::GET;
        let mut url = self.base.join(SANDBOX_PATH).expect("static Sandbox path");
        if let Some(id) = id {
            url.path_segments_mut().expect("HTTP URL").push(id);
        }
        url.query_pairs_mut().append_pair("tenant", tenant);
        let mut request = self.client.request(method, url).bearer_auth(&self.token);
        if let Some(payload) = payload {
            let mut payload = payload.clone();
            let deadline =
                adx_agent_core::transport::capped_deadline(payload.deadline_unix_ms, self.timeout);
            let remaining = adx_agent_core::transport::remaining_time(deadline);
            if remaining.is_zero() {
                return Err(SandboxError::Unavailable(
                    "Sandbox create deadline expired before submission".into(),
                ));
            }
            payload.deadline_unix_ms = Some(deadline);
            request = request.timeout(remaining).json(&payload);
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
                SandboxError::Unavailable("Sandbox connection failed".into())
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
            if body.len() + chunk.len() > limits::HTTP_JSON_BYTES {
                return Err(uncertain());
            }
            body.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            // Only typed responses from the authenticated Sandbox boundary are trusted.
            return Err(
                serde_json::from_slice::<SandboxError>(&body).unwrap_or_else(|_| match status {
                    StatusCode::BAD_REQUEST => {
                        SandboxError::Invalid("Sandbox rejected request".into())
                    }
                    StatusCode::CONFLICT => {
                        SandboxError::Conflict("Sandbox identity/specification conflict".into())
                    }
                    StatusCode::NOT_IMPLEMENTED => {
                        SandboxError::Unsupported("Sandbox capability unavailable".into())
                    }
                    _ => uncertain(),
                }),
            );
        }
        let observed: SandboxObservation =
            serde_json::from_slice(&body).map_err(|_| uncertain())?;
        if observed.tenant != tenant
            || observed.id != id.unwrap_or_else(|| &payload.expect("create payload").id)
        {
            return Err(uncertain());
        }
        Ok(Some(observed))
    }
}
#[async_trait]
impl Sandbox for HttpSandbox {
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        request
            .execution
            .validate()
            .map_err(SandboxError::Invalid)?;
        if request.id.is_empty() || request.id.len() > limits::IDENTIFIER_BYTES {
            return Err(SandboxError::Invalid("invalid Sandbox ID".into()));
        }
        self.request(Method::POST, &request.tenant, None, Some(request))
            .await?
            .ok_or_else(|| SandboxError::OutcomeUnknown("missing create result".into()))
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        self.request(Method::GET, tenant, Some(id), None).await
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.request(Method::DELETE, tenant, Some(id), None)
            .await?
            .ok_or_else(|| SandboxError::OutcomeUnknown("missing delete result".into()))
    }
}
