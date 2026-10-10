//! Embedded lifecycle calls must return errors without overflowing a normal worker stack.
use adx_activator::{local_sandbox::LocalSandbox, Activator};
use adx_agent_api::{
    activator::Control, local::LocalControl, managed::ManagedService, request::RequestContext,
};
use adx_agent_core::{sandbox::Sandbox, TemplateVersion};
use adx_agent_store::{AgentState, MemoryRepository};
use adx_apiserver::{clients::Clients, config::Config, sandbox_service::SandboxService};
use adx_protocol::control as pb;
use data_plane_gateway::ingress::{
    auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
    DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator, IngressRouteResolver,
    RouteStore,
};
use futures_util::{Stream, StreamExt};
use serde_json::json;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Debug)]
struct Identity;
#[async_trait::async_trait]
impl CredentialVerifier for Identity {
    async fn verify(&self, token: &str) -> Result<AuthenticatedIdentity, AuthError> {
        assert_eq!(token, "test-user");
        Ok(AuthenticatedIdentity {
            tenant_id: "tenant".into(),
            expires_at_unix: None,
        })
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
        let reset = pb::EnvironmentDirectoryFrame {
            epoch: 1,
            revision: 1,
            reset: true,
            ..Default::default()
        };
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async { Ok(reset) }).chain(futures_util::stream::pending()),
        )))
    }
}

#[test]
fn embedded_create_returns_rpc_failure_on_normal_worker_stack() {
    const CHILD: &str = "ADX_TEST_EMBEDDED_STACK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "embedded_create_returns_rpc_failure_on_normal_worker_stack",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "embedded worker failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
            // Synchronize an empty directory so create reaches the RPC call.
            let endpoint = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = endpoint.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let incoming = futures_util::stream::unfold(endpoint, |listener| async {
                    let socket = listener.accept().await.map(|(socket, _)| socket);
                    Some((socket, listener))
                });
                tonic::transport::Server::builder()
                    .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(Directory))
                    .serve_with_incoming(incoming).await.unwrap();
            });
            let config: Config = serde_json::from_value(json!({
                "listen":"127.0.0.1:0", "coordinator_address":format!("http://{address}"),
                "internal_security":"network", "ingress_mode":"standalone",
                "rpc_timeout_seconds":1, "cache_entries":16, "auth_cache_ttl_seconds":1
            })).unwrap();
            let mut config = config;
            let profile = serde_json::from_value(json!({
                "rootfs":{"runtime_class":"runc","type":"image","image":format!("runtime@sha256:{}", "0".repeat(64)),"readonly":false},
                "bootstrap":{"type":"image","image":format!("runtime@sha256:{}", "0".repeat(64)),"target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]},
                "env":{"EXECD_HTTP_PORT":"50090"}
            })).unwrap();
            config.runtime_profile = Some(profile);
            let clients = Clients::new(config).unwrap();
            let service = SandboxService::new(clients);
            let adapter = Arc::new(LocalSandbox::new(service, std::time::Duration::from_secs(60)).unwrap());
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                loop {
                    if matches!(adapter.get("tenant", "embedded-stack").await, Ok(None)) { break; }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }).await.expect("directory must synchronize before create");
            let template: TemplateVersion = serde_json::from_value(json!({
                "name":"app", "version":"1", "image":"app:1", "isolation_runtime":"runc",
                "resources":{"cpu_millis":1000,"memory_mib":512}, "service":[{"protocol":"ws","port":18092}]
            })).unwrap();
            let control = Arc::new(LocalControl::new(Arc::new(Activator::new(
                AgentState::new(Arc::new(MemoryRepository::default())), adapter,
            ))));
            let context = RequestContext::new(std::time::Duration::from_secs(5));
            control.publish(&context, "tenant", &template).await.unwrap();
            let api = Arc::new(data_plane_gateway::ingress::agent_api::AgentApi {
                managed: Arc::new(ManagedService::new(control)),
                request_timeout: std::time::Duration::from_secs(5),
            });
            adx_transport::install_crypto_provider();
            let certificate = include_bytes!("../../tests/fixtures/ingress-cert.pem");
            let certs = rustls_pemfile::certs(&mut &certificate[..]).collect::<Result<Vec<_>, _>>().unwrap();
            let key = rustls_pemfile::private_key(&mut &include_bytes!("../../tests/fixtures/ingress-key.pem")[..]).unwrap().unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs.clone(), key).unwrap()));
            let mut roots = rustls::RootCertStore::empty();
            for cert in certs { roots.add(cert).unwrap(); }
            let connector = tokio_rustls::TlsConnector::from(Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()));
            let routes = Arc::new(RouteStore::new()); routes.set_ready(true);
            let ingress = Arc::new(Ingress::new(
                Arc::new(IngressRouteResolver::new(routes)), DataPlaneL4Connector::new(H2PoolConfig::default()),
                IngressAuthenticator::with_verifier(Arc::new(Identity)), 50090, 8765, "127.0.0.1:1", vec![],
            ).with_agent_api(api).with_client_acl(vec!["127.0.0.1/32".parse().unwrap()],false));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_stop, shutdown) = tokio::sync::watch::channel(false);
            let http_server = tokio::spawn(ingress.serve_http_tls(listener, acceptor, shutdown));
            let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
            let client_io = connector.connect("localhost".try_into().unwrap(), tcp).await.unwrap();
            let (mut sender, connection) = hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(client_io)).await.unwrap();
            let client = tokio::spawn(connection);
            let request = hyper::Request::post("/api/agent/v2/templates/app/versions/1/bindings/embedded-stack/resolve")
                .header("Host", "localhost")
                .header("Authorization", "Bearer test-user")
                .header("Content-Type", "application/json")
                .body(http_body_util::Full::new(bytes::Bytes::from_static(br#"{"protocol":"ws"}"#))).unwrap();
            let response = tokio::time::timeout(std::time::Duration::from_secs(5), sender.send_request(request)).await.expect("HTTP create must remain bounded").unwrap();
            // The directory is synchronized, but this fixture deliberately has no create RPC.
            assert_eq!(response.status(), hyper::StatusCode::NOT_IMPLEMENTED);
            client.abort();
            http_server.abort();
            server.abort();
        }).await.unwrap();
    });
}
