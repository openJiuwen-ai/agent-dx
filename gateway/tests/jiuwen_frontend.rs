#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::jiuwen::{
    connection::ConnectionError,
    driver::{self, DriverError},
    frontend::{self, FrontendConfig},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};

fn config() -> FrontendConfig {
    FrontendConfig {
        queue_capacity: 2,
        write_timeout: Duration::from_millis(100),
        idle_timeout: Duration::from_secs(2),
        ping_interval: Duration::from_secs(1),
    }
}
async fn pair() -> (
    WebSocketStream<tokio::io::DuplexStream>,
    WebSocketStream<tokio::io::DuplexStream>,
) {
    let (client, server) = tokio::io::duplex(4096);
    (
        WebSocketStream::from_raw_socket(client, Role::Client, Some(driver::websocket_config()))
            .await,
        WebSocketStream::from_raw_socket(server, Role::Server, Some(driver::websocket_config()))
            .await,
    )
}
async fn frame(ws: &mut WebSocketStream<tokio::io::DuplexStream>) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) => ws.flush().await.unwrap(),
            other => panic!("expected JSON frame, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn invalid_frames_stay_local_and_generic_methods_reach_backend() {
    let (mut client, server) = pair().await;
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        |mut requests, output| async move {
            output.send(json!({"type":"event","event":"connection.ack","payload":{"protocol_version":"1.0"}})).await.unwrap();
            let request = requests.recv().await.unwrap();
            assert_eq!(request.id(), "unknown");
            assert_eq!(request.method_name(), "config.write");
            output
                .send(json!({"type":"res","id":request.id(),"ok":true,"payload":{}}))
                .await
                .unwrap();
            let request = requests.recv().await.unwrap();
            assert_eq!(request.id(), "valid");
            output
                .send(json!({"type":"res","id":request.id(),"ok":true,"payload":[]}))
                .await
                .unwrap();
            std::future::pending::<Result<(), ConnectionError>>().await
        },
    ));
    assert_eq!(frame(&mut client).await["event"], "connection.ack");
    client.send(Message::Text("{".into())).await.unwrap();
    let bad = frame(&mut client).await;
    assert_eq!(bad["code"], "BAD_REQUEST");
    assert_eq!(bad["id"], "");
    client
        .send(Message::Text(
            json!({"type":"req","id":"unknown","method":"config.write","params":{}}).to_string(),
        ))
        .await
        .unwrap();
    let forwarded = frame(&mut client).await;
    assert_eq!(forwarded["id"], "unknown");
    assert_eq!(forwarded["ok"], true);
    client
        .send(Message::Text(
            json!({"type":"req","id":"valid","method":"session.list","params":{}}).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(frame(&mut client).await["id"], "valid");
    client.close(None).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn frontend_disconnect_cancels_owned_backend() {
    let (mut client, server) = pair().await;
    let released = Arc::new(AtomicBool::new(false));
    let guard = Dropped(released.clone());
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        move |_requests, output| async move {
            let _guard = guard;
            output.send(json!({"ready":true})).await.unwrap();
            std::future::pending::<Result<(), ConnectionError>>().await
        },
    ));
    assert_eq!(frame(&mut client).await["ready"], true);
    client.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn backend_failure_closes_frontend_without_success_response() {
    let (mut client, server) = pair().await;
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        |_requests, _output| async { Err(ConnectionError::Driver(DriverError::Disconnected)) },
    ));
    let message = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(message, Message::Close(Some(frame)) if u16::from(frame.code)==1011));
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn slow_frontend_times_out_and_releases_backend() {
    let (_client, server) = pair().await;
    let released = Arc::new(AtomicBool::new(false));
    let guard = Dropped(released.clone());
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        move |_requests, output| async move {
            let _guard = guard;
            output
                .send(json!({"payload":"x".repeat(32768)}))
                .await
                .unwrap();
            std::future::pending::<Result<(), ConnectionError>>().await
        },
    ));
    assert!(tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn caller_cancellation_releases_backend_without_detached_work() {
    let (mut client, server) = pair().await;
    let released = Arc::new(AtomicBool::new(false));
    let guard = Dropped(released.clone());
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        move |_requests, output| async move {
            let _guard = guard;
            output.send(json!({"ready": true})).await.unwrap();
            std::future::pending::<Result<(), ConnectionError>>().await
        },
    ));
    assert_eq!(frame(&mut client).await["ready"], true);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pending_backend_cannot_accumulate_unbounded_frontend_requests() {
    let (mut client, server) = pair().await;
    let released = Arc::new(AtomicBool::new(false));
    let guard = Dropped(released.clone());
    let task = tokio::spawn(frontend::run(
        server,
        config(),
        move |requests, output| async move {
            let _guard = guard;
            let _requests = requests;
            output.send(json!({"ready": true})).await.unwrap();
            std::future::pending::<Result<(), ConnectionError>>().await
        },
    ));
    assert_eq!(frame(&mut client).await["ready"], true);
    for id in 0..3 {
        client
            .send(Message::Text(
                json!({"type":"req","id":id.to_string(),"method":"session.list","params":{}})
                    .to_string(),
            ))
            .await
            .unwrap();
    }
    let message = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(message, Message::Close(Some(frame)) if u16::from(frame.code)==1011));
    assert!(matches!(
        task.await.unwrap(),
        Err(frontend::FrontendError::Capacity)
    ));
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test]
async fn frontend_and_e2a_driver_preserve_user_and_runtime_request_correlation() {
    use data_plane_gateway::ingress::jiuwen::{driver::DriverConfig, session::SessionLimits};
    let (mut client, frontend_socket) = pair().await;
    let (backend_socket, mut agent) = pair().await;
    let task = tokio::spawn(frontend::run(
        frontend_socket,
        config(),
        move |requests, output| async move {
            driver::run(
                backend_socket,
                "verified-user".into(),
                DriverConfig {
                    limits: SessionLimits {
                        pending: 4,
                        recent: 4,
                        busy: 4,
                    },
                    write_timeout: Duration::from_millis(100),
                    idle_timeout: Duration::from_secs(3),
                    ping_interval: Duration::from_secs(1),
                    request_timeout: Duration::from_secs(2),
                },
                requests,
                output,
            )
            .await
            .map_err(ConnectionError::from)
        },
    ));
    agent
        .send(Message::Text(
            json!({"type":"event","event":"connection.ack","payload":{"status":"warming"}})
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(frame(&mut client).await["payload"]["status"], "warming");
    client.send(Message::Text(json!({"type":"req","id":"chat","method":"chat.send","params":{"session_id":"s","content":"hello","user_id":"forged"}}).to_string())).await.unwrap();
    let envelope = frame(&mut agent).await;
    assert_eq!(envelope["user_id"], "verified-user");
    assert_ne!(envelope["request_id"], "chat");
    let ack = frame(&mut client).await;
    assert_eq!(ack["id"], "chat");
    assert_eq!(ack["payload"]["accepted"], true);
    agent.send(Message::Text(json!({"protocol_version":"1.0","request_id":envelope["request_id"],"sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{"event_type":"runtime.accepted","request_id":envelope["request_id"]}}}).to_string())).await.unwrap();
    let receipt = frame(&mut client).await;
    assert_eq!(receipt["event"], "runtime.accepted");
    assert_eq!(receipt["payload"]["request_id"], "chat");
    client.close(None).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
}
