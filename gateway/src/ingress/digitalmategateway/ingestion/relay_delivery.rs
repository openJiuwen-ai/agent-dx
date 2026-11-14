//! Scope-based readiness and HTTP delivery over the actual shared ADX
//! Ingress/Relay stream. No direct node address, alternate lifecycle or URL proxy.
use super::{DeliveryBatch, DeliveryFailure, DeliveryPort, DurableAck};
use crate::ingress::agent_service::{AgentV2Access, ServiceAccessError, ServiceSelector};
use crate::ingress::server::Ingress;
use adx_agent_api::{managed::ManagedService, request::RequestContext};
use adx_agent_core::{Protocol, Scope};
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use std::{sync::Arc, time::Duration};

const BATCH_PATH: &str = "/api/digitalmate/ingestion/v1/batches";

struct AbortConnection(tokio::task::JoinHandle<()>);
impl AbortConnection {
    fn abort(&self) {
        self.0.abort();
    }
}
impl Drop for AbortConnection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The actual identity/purpose owner supplies a fresh narrowly scoped credential.
/// No crypto-bearer or JSON epoch fallback is available. Assembly must provide
/// its published implementation; this module never mints another identity.
#[async_trait]
pub trait DeliveryAuthorization: Send + Sync {
    /// Identity owner validates the persisted work and admits only its existing
    /// logical workspace. The returned value exposes no Environment route.
    async fn scope(&self, batch: &DeliveryBatch) -> Result<Scope, DeliveryFailure>;
    async fn headers(&self, batch: &DeliveryBatch) -> Result<HeaderMap, DeliveryFailure>;
}

pub struct RelayDelivery {
    access: AgentV2Access,
    ingress: Arc<Ingress>,
    authorization: Arc<dyn DeliveryAuthorization>,
    timeout: Duration,
    service_port: u16,
}

impl RelayDelivery {
    pub fn new(
        managed: Arc<ManagedService>,
        ingress: Arc<Ingress>,
        authorization: Arc<dyn DeliveryAuthorization>,
        timeout: Duration,
        service_port: u16,
    ) -> Result<Self, DeliveryFailure> {
        if service_port == 0
            || timeout < Duration::from_secs(1)
            || timeout > Duration::from_secs(120)
        {
            return Err(DeliveryFailure::Rejected);
        }
        Ok(Self {
            access: AgentV2Access::new(managed),
            ingress,
            authorization,
            timeout,
            service_port,
        })
    }

    async fn send(
        &self,
        batch: &DeliveryBatch,
        method: Method,
        scope: &Scope,
    ) -> Result<(StatusCode, Option<DurableAck>), DeliveryFailure> {
        let context = Arc::new(RequestContext::new(self.timeout));
        let selection = self
            .access
            .select_with_initializer(
                context.clone(),
                scope.clone(),
                ServiceSelector {
                    protocol: Protocol::Http,
                    port: Some(self.service_port),
                },
                async {
                    Err::<adx_agent_core::mounts::Parameters, _>(adx_agent_api::Error::NotFound)
                },
            )
            .await
            .map_err(|error| match error {
                crate::ingress::agent_service::InitializationError::Agent(
                    adx_agent_api::Error::Conflict(_)
                    | adx_agent_api::Error::NotFound
                    | adx_agent_api::Error::Invalid(_),
                )
                | crate::ingress::agent_service::InitializationError::Source(_) => {
                    DeliveryFailure::Rejected
                }
                _ => DeliveryFailure::Unknown,
            })?;
        let stream = self
            .access
            .connect_service_when_ready(&self.ingress, selection)
            .await
            .map_err(|error| match error {
                ServiceAccessError::Agent(
                    adx_agent_api::Error::Conflict(_)
                    | adx_agent_api::Error::NotFound
                    | adx_agent_api::Error::Invalid(_),
                ) => DeliveryFailure::Rejected,
                _ => DeliveryFailure::Unknown,
            })?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|_| DeliveryFailure::Unknown)?;
        let connection_task = AbortConnection(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let path = if method == Method::GET {
            format!("{BATCH_PATH}/{}", batch.batch_id)
        } else {
            BATCH_PATH.into()
        };
        let payload = if method == Method::POST {
            serde_json::to_vec(batch).map_err(|_| DeliveryFailure::Rejected)?
        } else {
            Vec::new()
        };
        if payload.len() > 2_097_152 {
            connection_task.abort();
            return Err(DeliveryFailure::Rejected);
        }
        // Readiness may take longer than a credential's lifetime. Obtain fresh
        // current-purpose proof after readiness for each physical HTTP request.
        let headers = self.authorization.headers(batch).await?;
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "digitalmate-sandbox")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))
            .map_err(|_| DeliveryFailure::Rejected)?;
        for (name, value) in &headers {
            if matches!(
                name.as_str(),
                "host" | "content-length" | "transfer-encoding" | "connection" | "upgrade"
            ) {
                connection_task.abort();
                return Err(DeliveryFailure::Rejected);
            }
            request.headers_mut().insert(name.clone(), value.clone());
        }
        let response = tokio::time::timeout_at(context.deadline(), sender.send_request(request))
            .await
            .map_err(|_| DeliveryFailure::Unknown)?
            .map_err(|_| DeliveryFailure::Unknown)?;
        let status = response.status();
        let retry = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5)
            .min(3600);
        if status == StatusCode::TOO_MANY_REQUESTS {
            connection_task.abort();
            return Err(DeliveryFailure::RetryAfter(Duration::from_secs(retry)));
        }
        if status == StatusCode::NOT_FOUND {
            connection_task.abort();
            return Ok((status, None));
        }
        if !status.is_success() {
            connection_task.abort();
            return Err(if status.is_server_error() {
                DeliveryFailure::Unknown
            } else {
                DeliveryFailure::Rejected
            });
        }
        let bytes = tokio::time::timeout_at(
            context.deadline(),
            Limited::new(response.into_body(), 4096).collect(),
        )
        .await
        .map_err(|_| DeliveryFailure::Unknown)?
        .map_err(|_| DeliveryFailure::Unknown)?
        .to_bytes();
        connection_task.abort();
        let ack = serde_json::from_slice(&bytes).map_err(|_| DeliveryFailure::Unknown)?;
        Ok((status, Some(ack)))
    }
}

#[async_trait]
impl DeliveryPort for RelayDelivery {
    async fn deliver(&self, batch: &DeliveryBatch) -> Result<DurableAck, DeliveryFailure> {
        batch.validate().map_err(|_| DeliveryFailure::Rejected)?;
        let operation = async {
            // Refuse stale/revoked work before requesting platform readiness.
            // The resulting credential is not reused after the readiness wait.
            let scope = self.authorization.scope(batch).await?;
            if let (_, Some(ack)) = self.send(batch, Method::GET, &scope).await? {
                return Ok(ack);
            }
            self.send(batch, Method::POST, &scope)
                .await?
                .1
                .ok_or(DeliveryFailure::Unknown)
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| DeliveryFailure::Unknown)?
    }
}
