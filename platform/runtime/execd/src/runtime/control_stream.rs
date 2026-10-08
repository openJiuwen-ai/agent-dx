//! Execd initiates a node-local stream after boot. Reconnect sends the complete
//! current control state; durable checkpoint acknowledgement is operation based.
use super::control::Controller;
use adx_core::{runtime::RuntimeStatus, Error, Result};
use adx_protocol::{
    runtime as pb,
    runtime_stream::{ControlOperation, MAX_CONTROL_MESSAGE_BYTES},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

#[derive(Clone)]
pub struct ControlStreamConfig {
    pub address: String,
    pub token: String,
}
impl ControlStreamConfig {
    pub fn from_environment() -> Result<Option<Self>> {
        let Some(address) = std::env::var("ADX_RUNTIME_CONTROL_ADDRESS")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let token = std::env::var("ADX_RUNTIME_CONTROL_TOKEN")
            .map_err(|_| Error::Invalid("runtime control token required".into()))?;
        let config = Self { address, token };
        config.validate()?;
        Ok(Some(config))
    }
    fn validate(&self) -> Result<()> {
        if !self.address.starts_with("http://") || self.token.len() != 64 {
            return Err(Error::Invalid(
                "node-local runtime control requires http address and execution credential".into(),
            ));
        }
        Endpoint::from_shared(self.address.clone())
            .map_err(|_| Error::Invalid("invalid runtime control address".into()))?;
        Ok(())
    }
}
/// Lifecycle is independent of this transport. Read current deployment credentials
/// again after restore, which may change the target node and execution identity.
pub async fn run<F>(controller: Arc<Controller>, configuration: F)
where
    F: Fn() -> Result<ControlStreamConfig> + Send + Sync,
{
    let mut retry = Duration::from_millis(25);
    loop {
        match configuration() {
            Ok(config) => {
                let started = tokio::time::Instant::now();
                if let Err(error) = session(controller.clone(), config).await {
                    // Never print credentials, payloads, or URI query strings.
                    execd_warn!("[execd-control] disconnected: {error}");
                    if error == Error::Conflict {
                        retry = Duration::from_millis(25);
                        continue;
                    }
                }
                if started.elapsed() >= Duration::from_secs(2) {
                    retry = Duration::from_millis(25);
                }
            }
            Err(_) => execd_warn!("[execd-control] reconnect configuration unavailable"),
        }
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(Duration::from_secs(1));
    }
}
async fn session(controller: Arc<Controller>, config: ControlStreamConfig) -> Result<()> {
    config.validate()?;
    let mut changes = controller.subscribe();
    let status = controller.status();
    let identity = status.identity.clone();
    let channel = Endpoint::from_shared(config.address)
        .map_err(|_| unavailable("invalid endpoint"))?
        .connect_timeout(Duration::from_secs(2))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect()
        .await
        .map_err(|_| unavailable("node control connection failed"))?;
    let mut client = pb::runtime_control_service_client::RuntimeControlServiceClient::new(channel)
        .max_decoding_message_size(MAX_CONTROL_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_CONTROL_MESSAGE_BYTES);
    let (events, receiver) = mpsc::channel(16);
    events
        .send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
                token: config.token,
                status_json: encode(&status)?,
            })),
        })
        .await
        .map_err(|_| unavailable("control hello closed"))?;
    let mut commands = tokio::time::timeout(
        Duration::from_secs(2),
        client.open_control(ReceiverStream::new(receiver)),
    )
    .await
    .map_err(|_| unavailable("control registration timed out"))?
    .map_err(|_| unavailable("control registration rejected"))?
    .into_inner();
    execd_info!(
        "[execd-control] registered environment={} generation={}",
        identity.environment_id,
        identity.ownership_generation
    );
    loop {
        tokio::select! {
            changed = changes.changed() => {
                changed.map_err(|_| unavailable("runtime controller stopped"))?;
                let status = controller.status();
                if status.identity != identity { return Err(Error::Conflict); }
                events.send(pb::RuntimeEvent { event: Some(pb::runtime_event::Event::StatusJson(encode(&status)?)) }).await.map_err(|_| unavailable("control status stream closed"))?;
            },
            command = commands.message() => {
                let command = command.map_err(|_| unavailable("node control stream failed"))?.ok_or_else(|| unavailable("node control stream closed"))?;
                let result = match serde_json::from_slice::<ControlOperation>(&command.operation_json) {
                    Ok(ControlOperation::Status) => Ok(controller.status()),
                    Ok(ControlOperation::Prepare(request)) => controller.prepare(request).await,
                    Ok(ControlOperation::Abort(request)) => controller.abort_unstarted(&request.operation_id, &request.identity, request.expected_revision),
                    Ok(ControlOperation::Finish(request)) => controller.finish_checkpoint(request),
                    Err(_) => Err(Error::Invalid("invalid runtime control operation".into())),
                };
                let mut reply = pb::RuntimeReply { request_id: command.request_id, ..Default::default() };
                match result {
                    Ok(status) => reply.status_json = encode(&status)?,
                    Err(error) => {
                        reply.error_code = match &error { Error::Invalid(_) => "INVALID", Error::Conflict => "CONFLICT", _ => "UNAVAILABLE" }.into();
                        reply.error_message = error.to_string();
                    }
                }
                events.send(pb::RuntimeEvent { event: Some(pb::runtime_event::Event::Reply(reply)) }).await.map_err(|_| unavailable("control response stream closed"))?;
            }
        }
    }
}
fn encode(status: &RuntimeStatus) -> Result<Vec<u8>> {
    serde_json::to_vec(status).map_err(|_| unavailable("runtime status encoding failed"))
}
fn unavailable(message: &str) -> Error {
    Error::Unavailable(message.into())
}
