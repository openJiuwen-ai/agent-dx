#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::jiuwen::{
    driver::{run, DriverConfig, DriverError},
    protocol::Request,
    session::SessionLimits,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{io::DuplexStream, sync::mpsc};
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};
fn config() -> DriverConfig {
    DriverConfig {
        limits: SessionLimits {
            pending: 4,
            recent: 4,
            busy: 4,
        },
        write_timeout: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(2),
        ping_interval: Duration::from_millis(10),
        request_timeout: Duration::from_millis(60),
    }
}
fn request(id: &str, method: &str) -> Request {
    Request::parse(
        &serde_json::to_vec(
            &json!({"type":"req","id":id,"method":method,"params":{"session_id":"s"}}),
        )
        .unwrap(),
    )
    .unwrap()
}
async fn pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
    let (a, b) = tokio::io::duplex(65536);
    (
        WebSocketStream::from_raw_socket(
            a,
            Role::Client,
            Some(data_plane_gateway::ingress::jiuwen::driver::websocket_config()),
        )
        .await,
        WebSocketStream::from_raw_socket(b, Role::Server, None).await,
    )
}
async fn text(socket: &mut WebSocketStream<DuplexStream>) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(data) => return serde_json::from_str(&data).unwrap(),
            Message::Ping(_) => socket.flush().await.unwrap(),
            _ => {}
        }
    }
}
async fn ack(socket: &mut WebSocketStream<DuplexStream>) {
    socket.send(Message::Text(json!({"type":"event","event":"connection.ack","payload":{"status":"warming","readiness":"WARMING"}}).to_string())).await.unwrap();
}
#[tokio::test]
async fn one_connection_carries_requests_events_and_stops_without_replay() {
    let (socket, mut backend) = pair().await;
    let (send, requests) = mpsc::channel(4);
    let (output, mut frames) = mpsc::channel(8);
    let task = tokio::spawn(run(socket, "user".into(), config(), requests, output));
    ack(&mut backend).await;
    assert_eq!(frames.recv().await.unwrap()["event"], "connection.ack");
    send.send(request("client", "chat.send")).await.unwrap();
    let envelope = text(&mut backend).await;
    assert_eq!(envelope["user_id"], "user");
    assert_ne!(envelope["request_id"], "client");
    assert_eq!(frames.recv().await.unwrap()["payload"]["accepted"], true);
    backend.send(Message::Text(json!({"protocol_version":"1.0","request_id":envelope["request_id"],"sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{"event_type":"runtime.accepted","request_id":envelope["request_id"]}}}).to_string())).await.unwrap();
    let event = frames.recv().await.unwrap();
    assert_eq!(event["event"], "runtime.accepted");
    assert_eq!(event["payload"]["request_id"], "client");
    send.send(request("meta", "session.get_metadata"))
        .await
        .unwrap();
    assert_eq!(text(&mut backend).await["method"], "session.get_metadata");
    backend.close(None).await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(DriverError::Disconnected)
    ));
    assert!(frames.recv().await.is_none());
    assert!(send.send(request("late", "chat.send")).await.is_err());
}
#[tokio::test]
async fn project_list_uses_the_existing_e2a_connection_without_a_gateway_ack() {
    let (socket, mut backend) = pair().await;
    let (send, requests) = mpsc::channel(2);
    let (output, mut frames) = mpsc::channel(4);
    let task = tokio::spawn(run(socket, "user".into(), config(), requests, output));
    ack(&mut backend).await;
    assert_eq!(frames.recv().await.unwrap()["event"], "connection.ack");
    let request = Request::parse(
        br#"{"type":"req","id":"projects","method":"project.list","params":{"filter":"all"}}"#,
    )
    .unwrap();
    send.send(request).await.unwrap();
    let envelope = text(&mut backend).await;
    assert_eq!(envelope["method"], "project.list");
    assert_eq!(envelope["params"]["filter"], "all");
    assert!(envelope["session_id"].is_null());
    assert!(frames.try_recv().is_err());
    backend
        .send(Message::Text(
            json!({"protocol_version":"1.0","request_id":envelope["request_id"],"sequence":0,"is_final":true,"status":"succeeded","response_kind":"e2a.complete","body":{"result":{"projects":[]}}}).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        frames.recv().await.unwrap(),
        json!({"type":"res","id":"projects","ok":true,"payload":{"projects":[]}})
    );
    drop(send);
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn slow_frontend_is_bounded_and_does_not_replay_sent_request() {
    let (socket, mut backend) = pair().await;
    let (send, requests) = mpsc::channel(2);
    let (output, _frames) = mpsc::channel(1);
    let task = tokio::spawn(run(socket, "user".into(), config(), requests, output));
    ack(&mut backend).await;
    send.send(request("one", "chat.send")).await.unwrap();
    assert_eq!(text(&mut backend).await["method"], "chat.send");
    assert!(matches!(
        task.await.unwrap(),
        Err(DriverError::SlowConsumer)
    ));
}
#[tokio::test]
async fn unary_timeout_retires_only_that_request_without_closing_the_connection() {
    let (socket, mut backend) = pair().await;
    let (send, requests) = mpsc::channel(2);
    let (output, mut frames) = mpsc::channel(4);
    let task = tokio::spawn(run(socket, "user".into(), config(), requests, output));
    ack(&mut backend).await;
    frames.recv().await.unwrap();
    send.send(request("query", "session.get_metadata"))
        .await
        .unwrap();
    text(&mut backend).await;
    let error = tokio::time::timeout(Duration::from_secs(1), frames.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error["code"], "AGENT_SERVER_TIMEOUT");
    assert_eq!(error["id"], "query");
    send.send(request("next", "chat.send")).await.unwrap();
    assert_eq!(text(&mut backend).await["method"], "chat.send");
    assert_eq!(frames.recv().await.unwrap()["id"], "next");
    drop(send);
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn pong_does_not_replace_the_required_front_ack() {
    let (socket, mut backend) = pair().await;
    let (_send, requests) = mpsc::channel(1);
    let (output, _frames) = mpsc::channel(1);
    let mut config = config();
    config.idle_timeout = Duration::from_millis(40);
    let peer = tokio::spawn(async move {
        while let Some(Ok(_)) = backend.next().await {
            if backend.flush().await.is_err() {
                break;
            }
        }
    });
    let mut task = tokio::spawn(run(socket, "user".into(), config, requests, output));
    let result = tokio::time::timeout(Duration::from_millis(250), &mut task).await;
    task.abort();
    peer.abort();
    assert!(matches!(result, Ok(Ok(Err(DriverError::Timeout)))));
}
