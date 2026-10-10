//! E2A handshake and driver over the shared authorized Agent/Relay connection.
use super::{
    driver::{self, DriverConfig, DriverError},
    protocol::Request,
};
use crate::ingress::{
    agent_service::{AgentV2Access, ServiceAccessError, ServiceSelection},
    server::{wait_for_route_cancellation, Ingress, IngressStream},
};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{tungstenite::client::IntoClientRequest, WebSocketStream};

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Access(#[from] ServiceAccessError),
    #[error(transparent)]
    Driver(#[from] DriverError),
    #[error("AgentServer WebSocket handshake exceeded the admission deadline")]
    HandshakeTimeout,
    #[error("AgentServer route was revoked; in-flight results may be unknown")]
    Revoked,
}

/// One authorized AgentServer connection, consumed by exactly one driver.
/// The caller authenticates the user and selects that user's binding first.
/// Dropping this object releases the Ingress session guard and Relay stream.
pub struct Connection {
    backend: WebSocketStream<IngressStream>,
    cancelled: watch::Receiver<bool>,
}
impl Connection {
    /// Use shared route authorization and the selection's original deadline.
    /// Each frontend opens its own stream, even for an already-connected binding.
    /// Business concurrency is delegated to the backend. Returns access, WS
    /// handshake, deadline or route-revocation errors. Only
    /// shared pre-write route/open failures may retry. A refused business port waits
    /// within the same deadline; HTTP upgrades never retry.
    /// The URI/Origin describe the Sandbox-local service, not a dial destination:
    /// all bytes travel over the already-selected Relay stream. No caller headers,
    /// frontend credentials or public URL are forwarded to AgentServer.
    pub async fn connect(
        ingress: &Ingress,
        access: &AgentV2Access,
        selection: ServiceSelection,
    ) -> Result<Self, ConnectionError> {
        let port = selection
            .websocket_port()
            .map_err(ServiceAccessError::Agent)?;
        let deadline = selection.deadline();
        let stream = access
            .connect_service_when_ready(ingress, selection)
            .await?;
        let cancelled = stream.cancellation();
        let mut request = format!("ws://127.0.0.1:{port}/")
            .into_client_request()
            .map_err(DriverError::from)?;
        let origin = format!("http://127.0.0.1:{port}")
            .parse()
            .map_err(|_| super::protocol::ProtocolError::Invalid("invalid backend Origin"))
            .map_err(DriverError::from)?;
        request.headers_mut().insert(http::header::ORIGIN, origin);
        let handshake = tokio_tungstenite::client_async_with_config(
            request,
            stream,
            Some(driver::websocket_config()),
        );
        let (backend, _) = tokio::select! {
            biased;
            _ = wait_for_route_cancellation(Some(cancelled.clone())) => return Err(ConnectionError::Revoked),
            result = tokio::time::timeout_at(deadline, handshake) => {
                result.map_err(|_| ConnectionError::HandshakeTimeout)?.map_err(DriverError::from)?
            }
        };
        Ok(Self { backend, cancelled })
    }

    /// Run for this binding's already-authenticated user. The frontend owner must
    /// close its socket when this returns (including route revocation), releasing
    /// both channel ends and reporting unknown outcomes without replaying work.
    /// Returns driver errors or Revoked; the admission deadline no longer applies.
    pub async fn run(
        self,
        user_id: String,
        config: DriverConfig,
        requests: mpsc::Receiver<Request>,
        output: mpsc::Sender<Value>,
    ) -> Result<(), ConnectionError> {
        let Self { backend, cancelled } = self;
        tokio::select! {
            biased;
            _ = wait_for_route_cancellation(Some(cancelled)) => Err(ConnectionError::Revoked),
            result = driver::run(backend, user_id, config, requests, output) => result.map_err(ConnectionError::from),
        }
    }
}
