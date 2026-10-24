use super::handle_one_request;
use crate::runtime::control::{self, CheckpointHooks, Controller, Handoff};
use adx_core::runtime::RuntimeIdentity;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::{io, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Hooks;
impl CheckpointHooks for Hooks {
    fn open(&self) -> io::Result<Handoff> {
        Err(io::Error::other("checkpoint is not used in this test"))
    }
    fn restore(&self, previous: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        Ok(previous.clone())
    }
}

async fn request(method: &str, path: &str, parent: Option<&str>, body: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        Box::pin(handle_one_request(&mut stream, None, &mut false))
            .await
            .unwrap();
    };
    let client = async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let parent = parent.map_or(String::new(), |value| format!("traceparent: {value}\r\n"));
        stream
            .write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n{parent}Content-Length: {}\r\n\r\n{body}", body.len()).as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(server, client).1
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn passive_polls_do_not_export_but_requests_keep_context() {
    // The global tracer provider and runtime controller belong to this test process.
    const ISOLATED: &str = "ADX_EXECD_PASSIVE_TRACE_TEST_ISOLATED";
    if std::env::var_os(ISOLATED).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("runtime::httpserver::trace_tests::passive_polls_do_not_export_but_requests_keep_context")
            .args(["--exact", "--test-threads=1"])
            .env(ISOLATED, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    control::install(
        Controller::new(
            RuntimeIdentity {
                environment_id: "trace-test".into(),
                runtime_id: "trace-test-1".into(),
                ownership_generation: 1,
            },
            Arc::new(Hooks),
        )
        .unwrap(),
    )
    .unwrap();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for _ in 0..3 {
        for path in ["/healthz", "/metrics", "/control/v1/status?probe=1"] {
            let response = request("GET", path, None, "").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        }
    }
    provider.force_flush().unwrap();
    assert!(exporter.get_finished_spans().unwrap().is_empty());

    let parent = "00-11111111111111111111111111111111-2222222222222222-01";
    assert!(request("GET", "/control/v1/status", Some(parent), "")
        .await
        .starts_with("HTTP/1.1 200"));
    assert!(
        request("POST", "/control/v1/checkpoint/prepare", None, "{}")
            .await
            .starts_with("HTTP/1.1 400")
    );
    assert!(request("POST", "/invoke", None, r#"{"action":"ping"}"#)
        .await
        .starts_with("HTTP/1.1 200"));
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 3);
    assert_eq!(spans[0].name, "GET /control/v1/status");
    assert_eq!(spans[0].parent_span_id.to_string(), "2222222222222222");
    // Checkpoint control currently uses the bounded fallback HTTP route name.
    assert_eq!(spans[1].name, "POST /unmatched");
    assert_eq!(spans[2].name, "POST /invoke");
    provider.shutdown().unwrap();
}
