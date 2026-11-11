//! HTTP/authentication integration with controlled Redis connection failures.
use super::*;
use crate::http::Api;
use futures_util::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use tonic::Request;

#[path = "../../tests/support/http.rs"]
mod http;
#[path = "../../../../platform/crates/discovery/tests/support/mod.rs"]
mod redis_peer;

#[derive(Default)]
struct Auth {
    calls: AtomicUsize,
    delay_ms: AtomicU64,
    node_watch_started: AtomicBool,
}

struct AuthService(Arc<Auth>);

#[tonic::async_trait]
impl pb::auth_service_server::AuthService for AuthService {
    async fn verify_api_key(
        &self,
        request: Request<pb::VerifyApiKeyRequest>,
    ) -> Result<Response<pb::VerifyApiKeyResponse>, Status> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.0.delay_ms.load(Ordering::SeqCst),
        ))
        .await;
        if request.into_inner().api_key != "a".repeat(40) {
            return Err(Status::unauthenticated("invalid key"));
        }
        Ok(Response::new(pb::VerifyApiKeyResponse {
            caller: Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            }),
            expires_at_unix_seconds: 0,
        }))
    }
}

struct Directory;
#[tonic::async_trait]
impl pb::environment_directory_service_server::EnvironmentDirectoryService for Directory {
    type WatchEnvironmentsStream =
        Pin<Box<dyn Stream<Item = Result<pb::EnvironmentDirectoryFrame, Status>> + Send>>;
    async fn watch_environments(
        &self,
        _: Request<pb::WatchEnvironmentsRequest>,
    ) -> Result<Response<Self::WatchEnvironmentsStream>, Status> {
        let frame = pb::EnvironmentDirectoryFrame {
            epoch: 7,
            revision: 1,
            reset: true,
            ..Default::default()
        };
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async { Ok(frame) }).chain(futures_util::stream::pending()),
        )))
    }
}

struct Fixture {
    clients: Arc<Clients>,
    peer: redis_peer::RedisPeer,
    auth: Arc<Auth>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        adx_transport::install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let auth = Arc::new(Auth::default());
        let service = AuthService(auth.clone());
        let observed = auth.clone();
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .trace_fn(move |request| {
                    if request.uri().path().ends_with("/WatchNodes") {
                        observed.node_watch_started.store(true, Ordering::SeqCst);
                    }
                    tracing::Span::none()
                })
                .add_service(pb::auth_service_server::AuthServiceServer::new(service))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(Directory))
                .serve_with_incoming(incoming).await.unwrap();
        });
        let peer = redis_peer::RedisPeer::new(&format!("http://{address}")).await;
        let config: Config = serde_json::from_value(serde_json::json!({
            "listen":"127.0.0.1:0", "internal_security":"network", "ingress_mode":"standalone",
            "discovery":{"redis_url":peer.url,"namespace":"test","poll_seconds":1},
            "rpc_timeout_seconds":1, "cache_entries":8,"auth_cache_ttl_seconds":1
        }))
        .unwrap();
        let clients = Clients::new(config).unwrap();
        clients.authenticate_fresh(&"a".repeat(40)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while clients
                .environments
                .lock()
                .await
                .get("absent")
                .unwrap_err()
                .code()
                != Code::NotFound
                || !auth.node_watch_started.load(Ordering::SeqCst)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Self {
            clients,
            peer,
            auth,
            server,
        }
    }

    async fn expire_endpoint(&self) {
        // Force the same endpoint-cache miss as an idle request, without a real
        // sleep that would let the background watcher warm discovery first.
        *self.clients.endpoint.lock().await = Cache::new(1);
    }

    async fn request(&self, key: &str) -> hyper::Response<Vec<u8>> {
        http::request(
            Api::new(self.clients.clone()).unwrap(),
            "GET",
            "/api/instances",
            &[("Authorization", format!("Bearer {key}"))],
            "",
        )
        .await
    }
}

#[tokio::test]
async fn idle_discovery_close_recovers_before_http_authentication() {
    let fixture = Fixture::new().await;
    let before = fixture.auth.calls.load(Ordering::SeqCst);
    fixture.peer.disconnect().await;
    fixture.expire_endpoint().await;
    let response = fixture.request(&"a".repeat(40)).await;
    assert_eq!(response.status(), 200, "{:?}", response.body());
    assert_eq!(fixture.auth.calls.load(Ordering::SeqCst), before + 1);
    assert_eq!(fixture.peer.state.connections.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.request(&"b".repeat(40)).await.status(), 401);
}

#[tokio::test]
async fn missing_record_rejects_http_before_verify_key() {
    let fixture = Fixture::new().await;
    let before = fixture.auth.calls.load(Ordering::SeqCst);
    fixture.expire_endpoint().await;
    *fixture.peer.state.reply.lock().unwrap() =
        redis_peer::Reply::Raw(b"*2\r\n$-1\r\n$-1\r\n".to_vec());
    assert_eq!(fixture.request(&"a".repeat(40)).await.status(), 503);
    assert_eq!(fixture.auth.calls.load(Ordering::SeqCst), before);
    assert_eq!(fixture.peer.state.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn authentication_discovery_and_rpc_share_the_original_budget() {
    let fixture = Fixture::new().await;
    fixture.expire_endpoint().await;
    *fixture.peer.state.reply.lock().unwrap() =
        redis_peer::Reply::Delay(Duration::from_millis(600));
    fixture.auth.delay_ms.store(600, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let error = fixture
        .clients
        .authenticate_fresh(&"a".repeat(40))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(started.elapsed() < Duration::from_millis(1150));
}

#[tokio::test]
async fn hanging_discovery_preserves_http_503_and_never_calls_verify_key() {
    let fixture = Fixture::new().await;
    fixture.expire_endpoint().await;
    *fixture.peer.state.reply.lock().unwrap() = redis_peer::Reply::Delay(Duration::from_secs(10));
    let before = fixture.auth.calls.load(Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let response = fixture.request(&"a".repeat(40)).await;
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["error"]["code"], "UNAVAILABLE");
    assert_eq!(body["error"]["retry"], "after_backoff");
    assert_eq!(fixture.auth.calls.load(Ordering::SeqCst), before);
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[tokio::test]
async fn hanging_auth_rpc_preserves_http_503() {
    let fixture = Fixture::new().await;
    fixture.auth.delay_ms.store(2000, Ordering::SeqCst);
    let before = fixture.auth.calls.load(Ordering::SeqCst);
    let response = fixture.request(&"a".repeat(40)).await;
    assert_eq!(response.status(), 503);
    let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(body["error"]["code"], "UNAVAILABLE");
    assert_eq!(fixture.auth.calls.load(Ordering::SeqCst), before + 1);
}

#[tokio::test]
async fn unrepresentable_authentication_budget_is_an_error_instead_of_a_panic() {
    let fixture = Fixture::new().await;
    let mut config = fixture.clients.config.clone();
    config.rpc_timeout_seconds = u64::MAX;
    let clients = Clients::new(config).unwrap();
    let error = clients
        .authenticate_fresh(&"a".repeat(40))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("timeout is too large"));
    let error = fixture
        .clients
        .rpc_with_timeout("test.unrepresentable", Duration::MAX, async {
            Ok(Response::new(()))
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable);
    assert!(error.message().contains("timeout is too large"));
}

#[tokio::test]
async fn rpc_and_auth_timeouts_record_failure_attribution() {
    // Keep the single global test tracer in one test; concurrent replacements
    // would make export assertions depend on the test runner's scheduling.
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let fixture = Fixture::new().await;
    let error = fixture
        .clients
        .rpc_with_timeout::<()>(
            "test.timeout",
            Duration::from_millis(20),
            std::future::pending::<Result<Response<()>, Status>>(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(error
        .message()
        .contains("control RPC response deadline exceeded"));

    fixture.expire_endpoint().await;
    *fixture.peer.state.reply.lock().unwrap() =
        redis_peer::Reply::Delay(Duration::from_millis(600));
    fixture.auth.delay_ms.store(600, Ordering::SeqCst);
    let parent = trace::Trace::child("test.auth_timeout");
    let trace_id = parent.trace_id().unwrap();
    let error = parent
        .run_result(fixture.clients.authenticate_fresh(&"a".repeat(40)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    for name in ["test.timeout", "apiserver.verify_key"] {
        let span = spans
            .iter()
            .find(|span| {
                span.name == name
                    && (name == "test.timeout"
                        || span.span_context.trace_id().to_string() == trace_id)
            })
            .expect("the expired RPC span must be exported");
        assert!(
            span.attributes.iter().any(|attribute| {
                attribute.key.as_str() == "result" && attribute.value.to_string() == "fail"
            }),
            "{name}: missing failure result"
        );
        assert!(
            span.attributes.iter().any(|attribute| {
                attribute.key.as_str() == "error.code"
                    && attribute.value.to_string() == i32::from(Code::DeadlineExceeded).to_string()
            }),
            "{name}: missing deadline code"
        );
        assert!(
            span.attributes.iter().any(|attribute| {
                attribute.key.as_str() == "error.message"
                    && attribute.value.to_string().contains("deadline exceeded")
            }),
            "{name}: missing deadline message"
        );
        assert!(
            matches!(span.status, opentelemetry::trace::Status::Error { .. }),
            "{name}: span not marked failed"
        );
    }
    provider.shutdown().unwrap();
}
