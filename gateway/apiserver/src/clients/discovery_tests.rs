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
