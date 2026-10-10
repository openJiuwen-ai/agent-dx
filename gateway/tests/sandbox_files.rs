#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::sandbox_files::{
    DirectoryConfig, ExecdAccess, HttpDirectory, SandboxDirectory,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn directory_uses_existing_instance_api_without_returning_credentials() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let backend = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_headers(&mut stream).await;
        assert!(request.starts_with("GET /api/instances?instance_id=sandbox-a HTTP/1.1"));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer management-test"));
        assert!(!request.contains("execd-private"));
        let body = r#"[{"id":"sandbox-a","status":"running"}]"#;
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    });
    let directory = HttpDirectory::new(
        DirectoryConfig {
            url: format!("http://{address}"),
            allow_http: true,
            ca_file: None,
        },
        BTreeMap::from([("tenant-a".into(), "management-test".into())]),
    )
    .unwrap();
    assert!(directory.authorize("tenant-b", "sandbox-a").await.is_err());
    directory.authorize("tenant-a", "sandbox-a").await.unwrap();
    backend.await.unwrap();
    // Credentials belong to shared transport configuration, never the directory response.
    assert!(ExecdAccess::new(Arc::new(directory), 50090, "execd-private".into()).is_ok());
}

#[tokio::test]
async fn directory_rejects_non_running_or_substituted_instances() {
    for body in [
        r#"[{"id":"other","status":"running"}]"#,
        r#"[{"id":"sandbox-a","status":"paused"}]"#,
        r#"[]"#,
        r#"[{"id":"sandbox-a","status":"running"},{"id":"other","status":"running"}]"#,
        r#"{"id":"sandbox-a","status":"running"}"#,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let backend = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _request = read_headers(&mut stream).await;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let directory = HttpDirectory::new(
            DirectoryConfig {
                url: format!("http://{address}"),
                allow_http: true,
                ca_file: None,
            },
            BTreeMap::from([("tenant-a".into(), "management-test".into())]),
        )
        .unwrap();
        assert!(directory.authorize("tenant-a", "sandbox-a").await.is_err());
        backend.await.unwrap();
    }
}

async fn read_headers(stream: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.windows(4).any(|v| v == b"\r\n\r\n") {
        let mut chunk = [0; 512];
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0 && bytes.len() + n <= 8192);
        bytes.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8(bytes).unwrap()
}
