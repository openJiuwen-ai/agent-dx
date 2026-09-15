//! Node-local HTTP cooperation with the Instance runtime.
use adx_core::{runtime::*, Error, InstanceRecord, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{header::HeaderValue, Method, Request, StatusCode};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{net::SocketAddr, time::Duration};

pub struct RuntimeControlClient {
    client: Client<HttpConnector, Full<Bytes>>,
    port: u16,
    timeout: Duration,
    token: Option<HeaderValue>,
}
impl RuntimeControlClient {
    pub fn new(port: u16, timeout: Duration) -> Result<Self> {
        if port == 0 || timeout.is_zero() {
            return Err(Error::Invalid(
                "runtime HTTP port and timeout must be positive".into(),
            ));
        }
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(Some(timeout));
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
            port,
            timeout,
            token: None,
        })
    }
    pub fn with_token(mut self, token: &str) -> Result<Self> {
        if token.is_empty() {
            return Err(Error::Invalid("runtime token must not be empty".into()));
        }
        let mut value = HeaderValue::from_str(token)
            .map_err(|_| Error::Invalid("invalid runtime HTTP token".into()))?;
        value.set_sensitive(true);
        self.token = Some(value);
        Ok(self)
    }
    pub fn identity(record: &InstanceRecord) -> RuntimeIdentity {
        RuntimeIdentity {
            instance_id: record.spec.id.clone(),
            runtime_id: record.runtime_id.clone(),
            ownership_generation: record.assignment.generation,
        }
    }
    async fn request(
        &self,
        record: &InstanceRecord,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<RuntimeStatus> {
        let expected = Self::identity(record);
        expected.validate()?;
        let address = SocketAddr::new(
            record
                .runtime_ip
                .ok_or_else(|| Error::Invalid("runtime IP is required".into()))?,
            self.port,
        );
        let mut builder = Request::builder()
            .method(if body.is_some() {
                Method::POST
            } else {
                Method::GET
            })
            .uri(format!("http://{address}/control/v1/{path}"));
        if let Some(token) = &self.token {
            builder = builder.header("X-Auth", token);
        }
        let request = builder
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body.unwrap_or_default())))
            .map_err(|_| Error::Invalid("invalid runtime HTTP request".into()))?;
        tokio::time::timeout(self.timeout, async {
            let response = self.client.request(request).await.map_err(unavailable)?;
            match response.status() {
                StatusCode::OK => {}
                StatusCode::CONFLICT => return Err(Error::Conflict),
                status => return Err(unavailable(format!("HTTP {status}"))),
            }
            let body = Limited::new(response.into_body(), 65_536)
                .collect()
                .await
                .map_err(unavailable)?
                .to_bytes();
            let status: RuntimeStatus = serde_json::from_slice(&body).map_err(unavailable)?;
            if status.identity != expected || status.revision == 0 {
                return Err(Error::Conflict);
            }
            Ok(status)
        })
        .await
        .map_err(|_| unavailable("deadline exceeded"))?
    }
    pub async fn status(&self, record: &InstanceRecord) -> Result<RuntimeStatus> {
        self.request(record, "status", None).await
    }
    /// Prepared acknowledges the runtime barrier only. The caller must then invoke
    /// the execution backend and persist checkpoint artifacts/metadata separately.
    pub async fn prepare(
        &self,
        record: &InstanceRecord,
        operation_id: &str,
        expected_revision: u64,
    ) -> Result<RuntimeStatus> {
        let request = PrepareCheckpoint {
            identity: Self::identity(record),
            operation_id: operation_id.into(),
            expected_revision,
        };
        let status = self
            .request(
                record,
                "checkpoint/prepare",
                Some(serde_json::to_vec(&request).map_err(unavailable)?),
            )
            .await?;
        if status.phase != RuntimePhase::Prepared
            || status.checkpoint.as_ref().is_none_or(|c| {
                c.operation_id != operation_id || c.phase != CheckpointPhase::Prepared
            })
        {
            return Err(unavailable(
                "runtime did not acknowledge checkpoint preparation",
            ));
        }
        Ok(status)
    }
    /// Only after the execution backend confirms it did not begin checkpointing.
    pub async fn abort_unstarted(
        &self,
        record: &InstanceRecord,
        operation_id: &str,
        expected_revision: u64,
    ) -> Result<RuntimeStatus> {
        let request = AbortCheckpoint {
            identity: Self::identity(record),
            operation_id: operation_id.into(),
            expected_revision,
        };
        let status = self
            .request(
                record,
                "checkpoint/abort-unstarted",
                Some(serde_json::to_vec(&request).map_err(unavailable)?),
            )
            .await?;
        if status.phase != RuntimePhase::Running
            || status.checkpoint.as_ref().is_none_or(|c| {
                c.operation_id != operation_id || c.phase != CheckpointPhase::Aborted
            })
        {
            return Err(unavailable("runtime did not acknowledge checkpoint abort"));
        }
        Ok(status)
    }
}
fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("runtime HTTP control: {error}"))
}
