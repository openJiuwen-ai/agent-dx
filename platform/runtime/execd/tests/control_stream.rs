//! Real bidirectional gRPC checks; no HTTP control listener or periodic observer.
use adx_core::runtime::*;
use adx_execd::runtime::{
    control::{CheckpointHooks, Controller, Handoff, HandoffOutcome},
    control_stream::{run, ControlStreamConfig},
};
use adx_protocol::{
    runtime::runtime_control_service_server::RuntimeControlServiceServer,
    runtime_stream::{execution_token, ControlOperation},
};
use adxlet::control_stream::RuntimeControlHub;
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";
fn identity() -> RuntimeIdentity {
    RuntimeIdentity {
        environment_id: "i".into(),
        runtime_id: "i-1".into(),
        ownership_generation: 1,
    }
}
struct Hooks(Mutex<Option<oneshot::Receiver<HandoffOutcome>>>);
impl CheckpointHooks for Hooks {
    fn open(&self) -> io::Result<Handoff> {
        let rx = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| io::Error::other("already opened"))?;
        Ok(Box::pin(async { rx.await.map_err(io::Error::other) }))
    }
    fn restore(&self, old: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        Ok(old.clone())
    }
}
struct Test {
    hub: RuntimeControlHub,
    address: String,
    controller: Arc<Controller>,
    client: tokio::task::JoinHandle<()>,
    server: tokio::task::JoinHandle<()>,
    resume: Option<oneshot::Sender<HandoffOutcome>>,
}
impl Drop for Test {
    fn drop(&mut self) {
        self.client.abort();
        self.server.abort();
    }
}
async fn setup(token: Option<String>, armed: bool) -> Test {
    let hub = RuntimeControlHub::new(SECRET.to_vec(), Duration::from_secs(2)).unwrap();
    if armed {
        hub.expect(&identity()).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let service = RuntimeControlServiceServer::new(hub.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let (tx, rx) = oneshot::channel();
    let controller = Controller::new(identity(), Arc::new(Hooks(Mutex::new(Some(rx))))).unwrap();
    let config = ControlStreamConfig {
        address: address.clone(),
        token: token.unwrap_or_else(|| execution_token(SECRET, &identity()).unwrap()),
    };
    let current = controller.clone();
    let client = tokio::spawn(async move {
        run(current, move || Ok(config.clone())).await;
    });
    Test {
        address,
        hub,
        controller,
        client,
        server,
        resume: Some(tx),
    }
}
#[tokio::test]
async fn ready_notification_is_retained_before_the_waiter_starts() {
    let test = setup(None, true).await;
    test.hub.wait_ready(&identity()).await.unwrap();
    let status = test
        .hub
        .request(&identity(), ControlOperation::Status)
        .await
        .unwrap();
    assert_eq!(status.phase, RuntimePhase::Running);
    assert_eq!(status.identity, identity());
}
#[tokio::test]
async fn workload_checkpoint_is_notified_without_a_monitor_tick() {
    let test = setup(None, true).await;
    let mut events = test.hub.take_events().unwrap();
    test.hub.wait_ready(&identity()).await.unwrap();
    let controller = test.controller.clone();
    let caller = tokio::spawn(async move {
        controller
            .request_checkpoint("workload".into(), Some(Duration::from_secs(2)))
            .await
    });
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event, identity());
    assert_eq!(
        test.hub
            .observed(&identity())
            .unwrap()
            .unwrap()
            .requested_checkpoint
            .as_deref(),
        Some("workload")
    );
    assert!(
        !caller.is_finished(),
        "notification is not a durable completion ACK"
    );
    test.hub
        .request(
            &identity(),
            ControlOperation::Finish(FinishWorkloadCheckpoint {
                identity: identity(),
                operation_id: "workload".into(),
                error: Some("test failure".into()),
            }),
        )
        .await
        .unwrap();
    assert!(caller.await.unwrap().is_err());
}
#[tokio::test]
async fn prepare_and_completion_use_the_stream_and_reconnect_preserves_pending_request() {
    let mut test = setup(None, true).await;
    test.hub.wait_ready(&identity()).await.unwrap();
    let controller = test.controller.clone();
    let caller = tokio::spawn(async move {
        controller
            .request_checkpoint("save".into(), Some(Duration::from_secs(5)))
            .await
    });
    let mut changes = test.controller.subscribe();
    while test.controller.status().requested_checkpoint.is_none() {
        changes.changed().await.unwrap();
    }
    test.hub.disconnect(&identity()).unwrap();
    test.hub.wait_ready(&identity()).await.unwrap();
    let status = test
        .hub
        .request(&identity(), ControlOperation::Status)
        .await
        .unwrap();
    assert_eq!(status.requested_checkpoint.as_deref(), Some("save"));
    let prepared = test
        .hub
        .request(
            &identity(),
            ControlOperation::Prepare(PrepareCheckpoint {
                identity: identity(),
                operation_id: "save".into(),
                expected_revision: status.revision,
            }),
        )
        .await
        .unwrap();
    assert_eq!(prepared.phase, RuntimePhase::Prepared);
    test.resume
        .take()
        .unwrap()
        .send(HandoffOutcome::Resume)
        .unwrap();
    let mut changes = test.controller.subscribe();
    while test.controller.status().phase != RuntimePhase::Running {
        changes.changed().await.unwrap();
    }
    test.hub.disconnect(&identity()).unwrap();
    test.hub.wait_ready(&identity()).await.unwrap();
    let ack = ControlOperation::Finish(FinishWorkloadCheckpoint {
        identity: identity(),
        operation_id: "save".into(),
        error: None,
    });
    test.hub.request(&identity(), ack.clone()).await.unwrap();
    test.hub.request(&identity(), ack).await.unwrap();
    caller.await.unwrap().unwrap();
}
#[tokio::test]
async fn bad_credentials_and_unowned_runtime_never_become_ready() {
    for (token, armed) in [(Some("0".repeat(64)), true), (None, false)] {
        let test = setup(token, armed).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(150), test.hub.wait_ready(&identity()))
                .await
                .is_err()
                || test.hub.observed(&identity()).is_err()
        );
        assert!(test.hub.observed(&identity()).ok().flatten().is_none());
    }
}
#[tokio::test]
async fn retirement_rejects_late_reconnect_and_releases_directory_entry() {
    let test = setup(None, true).await;
    test.hub.wait_ready(&identity()).await.unwrap();
    test.hub.retire(&identity());
    assert!(test
        .hub
        .request(&identity(), ControlOperation::Status)
        .await
        .is_err());
    assert_eq!(test.hub.len(), 0);
}

#[path = "control_stream/process.rs"]
mod process;

#[tokio::test]
async fn invalid_hello_does_not_replace_a_healthy_control_connection() {
    use adx_protocol::runtime as pb;
    let test = setup(None, true).await;
    test.hub.wait_ready(&identity()).await.unwrap();
    let mut invalid = test.controller.status();
    invalid.revision = 0;
    let mut client = pb::runtime_control_service_client::RuntimeControlServiceClient::connect(
        test.address.clone(),
    )
    .await
    .unwrap();
    let rejected = client
        .open_control(tokio_stream::iter([pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
                token: execution_token(SECRET, &identity()).unwrap(),
                status_json: serde_json::to_vec(&invalid).unwrap(),
            })),
        }]))
        .await;
    assert!(rejected.is_err());
    assert!(
        test.hub.observed(&identity()).unwrap().is_some(),
        "invalid Hello must not invalidate the healthy session"
    );
    test.hub
        .request(&identity(), ControlOperation::Status)
        .await
        .unwrap();
}
