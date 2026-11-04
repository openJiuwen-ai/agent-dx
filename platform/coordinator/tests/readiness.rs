//! The readiness endpoint reports the process role, and readiness only
//! distinguishes "initialization not complete": a standby whose
//! synchronization goes stale stays ready, while a draining or lost
//! primary is not ready.
use adx_coordinator::readiness::{self, Readiness, Role};

/// One raw HTTP/1.1 GET over a fresh connection, returning the status
/// code and body.
async fn get(address: std::net::SocketAddr) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let text = String::from_utf8_lossy(&response).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("HTTP status line");
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (status, body)
}

#[tokio::test]
async fn readiness_endpoint_reports_each_role_and_never_unreadies_a_synced_standby() {
    let readiness = Readiness::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(readiness::serve(listener, readiness.clone()));

    // Preparing: not ready.
    let (status, body) = get(address).await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"role":"preparing","ready":false}"#);

    // Standby before the first successful synchronization: not ready.
    readiness.standby(false);
    let (status, body) = get(address).await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"role":"standby","ready":false}"#);

    // First successful synchronization latches ready.
    readiness.standby(true);
    let (status, body) = get(address).await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"role":"standby","ready":true}"#);

    // A later stale refresh never un-readies a synced standby.
    readiness.standby(false);
    let (status, _) = get(address).await;
    assert_eq!(status, 200);

    // Promotion reports the primary role; late standby updates must not
    // demote it.
    readiness.promote();
    let (status, body) = get(address).await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"role":"primary","ready":true}"#);
    readiness.standby(true);
    let (status, body) = get(address).await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"role":"primary","ready":true}"#);

    // Draining is not ready: the orchestrator replaces this process.
    readiness.drain();
    let (status, body) = get(address).await;
    assert_eq!(status, 503);
    assert_eq!(body, r#"{"role":"draining","ready":false}"#);
}

#[test]
fn role_readiness_is_a_pure_function_of_the_role() {
    assert!(!Role::Preparing.ready());
    assert!(!Role::Standby { synced: false }.ready());
    assert!(Role::Standby { synced: true }.ready());
    assert!(Role::Primary.ready());
    assert!(!Role::Draining.ready());
    assert_eq!(Role::Preparing.label(), "preparing");
    assert_eq!(Role::Standby { synced: false }.label(), "standby");
    assert_eq!(Role::Primary.label(), "primary");
    assert_eq!(Role::Draining.label(), "draining");
}

#[tokio::test]
async fn repeated_probes_are_served_each_on_its_own_connection() {
    let readiness = Readiness::new();
    readiness.promote();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(readiness::serve(listener, readiness.clone()));
    for _ in 0..8 {
        let (status, _) = get(address).await;
        assert_eq!(status, 200);
    }
}
