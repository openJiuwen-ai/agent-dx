//! One Edge lifecycle shared by standalone and API Server embedded deployment.
use super::{
    master_routes::{ControlConfig, MasterConnection},
    CommandWatchConfig, DataPlaneL4Connector, EdgeAuthenticator, EdgeFrontend, EdgeRouteResolver,
    RouteChange, RouteStore,
};
use crate::config::EdgeFrontendConfig;
use std::{future::Future, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    task::{JoinError, JoinSet},
};
use tokio_rustls::TlsAcceptor;

pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;

pub struct EdgeFrontendService {
    config: EdgeFrontendConfig,
    tls_listener: TcpListener,
    plain_listener: TcpListener,
    health_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    gateway: Arc<EdgeFrontend>,
    watcher: Arc<MasterConnection>,
    store: Arc<RouteStore>,
    route_changes: broadcast::Receiver<RouteChange>,
}

impl EdgeFrontendService {
    /// Bind all public listeners before the owning process reports readiness.
    pub async fn bind(
        config: EdgeFrontendConfig,
        control: ControlConfig,
    ) -> Result<Self, ServiceError> {
        #[cfg(not(feature = "agent-api"))]
        if std::env::var_os("ADX_SANDBOX_CONFIG").is_some()
            || std::env::var_os("ADX_AGENT_CONFIG").is_some()
        {
            return Err(
                "ADX_SANDBOX_CONFIG requires a Gateway built with --features agent-api".into(),
            );
        }
        let tls_listener = TcpListener::bind(config.tls_bind).await?;
        let plain_listener = TcpListener::bind(config.plain_bind).await?;
        let health_listener = TcpListener::bind(config.health_bind).await?;
        let tls_acceptor = load_tls_acceptor(&config.tls_cert, &config.tls_key)?;
        let store = Arc::new(RouteStore::new());
        let route_changes = store.subscribe();
        let watcher = Arc::new(MasterConnection::new(control).map_err(send_error)?);
        let resolver = Arc::new(EdgeRouteResolver::new(store.clone()).stream_only());
        let connector = DataPlaneL4Connector::new(config.h2_pool_config()?);
        let authenticator = EdgeAuthenticator::with_verifier(watcher.clone());
        #[cfg(feature = "agent-api")]
        let sandbox_api = if let Ok(path) = std::env::var("ADX_SANDBOX_CONFIG") {
            use super::sandbox_api::{PlatformSandbox, SandboxApi, SandboxConfig};
            let settings: SandboxConfig = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| "invalid Sandbox configuration")?;
            let backend = Arc::new(
                PlatformSandbox::new(settings, std::env::var("ADX_SANDBOX_RRT_TOKEN")?)
                    .map_err(send_error)?,
            );
            Some(Arc::new(SandboxApi::new(
                backend,
                &std::env::var("ADX_SANDBOX_SERVICE_TOKEN")?,
            )?))
        } else {
            None
        };
        #[cfg(feature = "agent-api")]
        let agent_api = if let Ok(path) = std::env::var("ADX_AGENT_CONFIG") {
            use super::agent_api::{AgentApi, AgentConfig};
            let settings: AgentConfig = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| "invalid Agent configuration")?;
            let sandbox = sandbox_api
                .as_ref()
                .ok_or("Agent APIs require ADX_SANDBOX_CONFIG")?;
            Some(Arc::new(
                AgentApi::new(settings, sandbox.backend.clone())
                    .await
                    .map_err(send_error)?,
            ))
        } else {
            None
        };
        let gateway = EdgeFrontend::new(
            resolver,
            connector,
            authenticator,
            config.default_direct_port,
            config.default_tunnel_port,
            config.frontend_address.clone(),
            config.control_plane_routes.clone(),
        )
        .with_backend_http_pool_config(config.backend_http_pool_config())
        .with_reverse_proxy_config(config.reverse_proxy.clone())
        .with_proxy_routes(config.proxy_routes.clone())
        .with_command_watch_config(CommandWatchConfig {
            max_subscriptions_per_connection: config.command_watch_max_subscriptions,
            queue_capacity: config.command_watch_queue_capacity,
            max_frame_bytes: config.command_watch_max_frame_bytes,
            ping_interval: config.command_watch_ping_interval,
        })
        .with_client_acl(
            config.allowed_client_networks.clone(),
            config.allow_any_client,
        );
        #[cfg(feature = "agent-api")]
        let gateway = if let Some(api) = &sandbox_api {
            gateway.with_sandbox_api(api.clone())
        } else {
            gateway
        };
        #[cfg(feature = "agent-api")]
        let gateway = if let Some(api) = &agent_api {
            gateway.with_agent_api(api.clone())
        } else {
            gateway
        };
        Ok(Self {
            config,
            tls_listener,
            plain_listener,
            health_listener,
            tls_acceptor,
            gateway: Arc::new(gateway),
            watcher,
            store,
            route_changes,
        })
    }

    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ServiceError> {
        let Self {
            config,
            tls_listener,
            plain_listener,
            health_listener,
            tls_acceptor,
            gateway,
            watcher,
            store,
            route_changes,
        } = self;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let watcher_task = tokio::spawn(watcher.run(store));
        let route_reconciler_task =
            tokio::spawn(gateway.clone().run_route_reconciler(route_changes));
        let mut listeners = JoinSet::new();
        let gateway_for_tls = gateway.clone();
        let shutdown_for_tls = shutdown_rx.clone();
        listeners.spawn(async move {
            gateway_for_tls
                .serve_http_tls(tls_listener, tls_acceptor, shutdown_for_tls)
                .await
                .map_err(service_error)
        });
        let gateway_for_plain = gateway.clone();
        let shutdown_for_plain = shutdown_rx.clone();
        listeners.spawn(async move {
            gateway_for_plain
                .serve_http(plain_listener, shutdown_for_plain)
                .await
                .map_err(service_error)
        });
        let gateway_for_health = gateway.clone();
        listeners.spawn(async move {
            gateway_for_health
                .serve_health(health_listener, shutdown_rx)
                .await
                .map_err(service_error)
        });
        tracing::info!(
            tls = %config.tls_bind,
            plain = %config.plain_bind,
            frontend = %config.frontend_address,
            health = %config.health_bind,
            "Data Plane Edge Frontend serving"
        );

        tokio::pin!(shutdown);
        let failure = tokio::select! {
            _ = &mut shutdown => None,
            result = listeners.join_next() => Some(listener_failure(result)),
        };
        gateway.start_drain();
        let _ = shutdown_tx.send(true);
        let deadline = tokio::time::Instant::now() + config.drain_timeout;
        while gateway.active_sessions() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        watcher_task.abort();
        route_reconciler_task.abort();
        if failure.is_some() {
            listeners.abort_all();
        }
        let mut shutdown_error = None;
        while let Some(result) = listeners.join_next().await {
            if let Ok(Err(error)) = result {
                shutdown_error.get_or_insert(error);
            }
        }
        failure.or(shutdown_error).map_or(Ok(()), Err)
    }
}

fn load_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, ServiceError> {
    adx_transport::tls::http_server_acceptor(cert_path, key_path, None, vec![b"http/1.1".to_vec()])
}

fn send_error(error: Box<dyn std::error::Error>) -> ServiceError {
    std::io::Error::other(error.to_string()).into()
}

fn service_error(error: std::io::Error) -> ServiceError {
    Box::new(error)
}

fn listener_failure(result: Option<Result<Result<(), ServiceError>, JoinError>>) -> ServiceError {
    match result {
        Some(Ok(Err(error))) => error,
        Some(Ok(Ok(()))) => "Edge listener stopped unexpectedly".into(),
        Some(Err(error)) => std::io::Error::other(error.to_string()).into(),
        None => "Edge listener set stopped unexpectedly".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::EdgeNodeSecurityMode,
        edge::{parse_static_routes, ReverseProxyConfig},
    };
    use adx_transport::tls::TlsFiles;
    use std::{collections::BTreeMap, path::PathBuf, time::Duration};

    #[tokio::test]
    async fn shared_service_binds_and_stops_cleanly() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let certificate = fixture.join("ingress-cert.pem");
        let private_key = fixture.join("ingress-key.pem");
        let config = EdgeFrontendConfig {
            etcd_endpoints: Vec::new(),
            tls_bind: "127.0.0.1:0".parse().unwrap(),
            plain_bind: "127.0.0.1:0".parse().unwrap(),
            health_bind: "127.0.0.1:0".parse().unwrap(),
            tls_cert: certificate.display().to_string(),
            tls_key: private_key.display().to_string(),
            frontend_address: "127.0.0.1:8888".into(),
            reverse_proxy: ReverseProxyConfig {
                max_idle_connections: 8,
                idle_timeout: Duration::from_secs(1),
                connect_timeout: Duration::from_secs(1),
            },
            proxy_routes: Vec::new(),
            control_plane_routes: parse_static_routes("exact:/healthz").unwrap(),
            validate_iam: false,
            iam_address: String::new(),
            auth_cache_ttl: Duration::from_secs(1),
            default_direct_port: 50_090,
            default_tunnel_port: 8_765,
            node_security_mode: EdgeNodeSecurityMode::Network,
            node_tls_ca: String::new(),
            node_tls_server_name: String::new(),
            node_tls_client_cert: String::new(),
            node_tls_client_key: String::new(),
            connections_per_node: 1,
            max_connections_per_node: 1,
            backend_http_max_connections_per_endpoint: 1,
            backend_http_max_idle_connections: 1,
            backend_http_max_idle_connections_per_endpoint: 1,
            backend_http_idle_timeout: Duration::from_secs(1),
            backend_http_acquire_timeout: Duration::from_secs(1),
            drain_timeout: Duration::from_millis(10),
            allowed_client_networks: vec!["127.0.0.1/32".parse().unwrap()],
            allow_any_client: false,
            command_watch_max_subscriptions: 1,
            command_watch_queue_capacity: 1,
            command_watch_max_frame_bytes: 1_024,
            command_watch_ping_interval: Duration::from_secs(1),
            etcd_tls_ca: String::new(),
            etcd_tls_cert: String::new(),
            etcd_tls_key: String::new(),
            etcd_tls_domain: String::new(),
            etcd_username: String::new(),
            etcd_password: String::new(),
        };
        let control = ControlConfig {
            redis_url: "redis://127.0.0.1:1/".into(),
            namespace: "edge-service-test".into(),
            tls: TlsFiles {
                ca: certificate.clone(),
                certificate: certificate.clone(),
                private_key,
                server_name: "localhost".into(),
                peers: BTreeMap::from([("master".into(), certificate)]),
            },
            rpc_timeout_seconds: 1,
            refresh_seconds: 1,
            auth_cache_seconds: 1,
            auth_cache_entries: 1,
        };

        EdgeFrontendService::bind(config, control)
            .await
            .unwrap()
            .serve(async {})
            .await
            .unwrap();
    }
}
