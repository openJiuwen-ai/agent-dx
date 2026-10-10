//! One Ingress lifecycle shared by standalone and API Server embedded deployment.
use super::{
    coordinator_routes::{ControlConfig, CoordinatorConnection},
    CommandWatchConfig, DataPlaneL4Connector, Ingress, IngressAuthenticator, IngressRouteResolver,
    RouteChange, RouteStore,
};
use crate::config::IngressConfig;
use std::{future::Future, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    task::{JoinError, JoinSet},
};
use tokio_rustls::TlsAcceptor;

#[cfg(feature = "agent-api")]
type SandboxOverride = Option<(
    Arc<dyn adx_agent_core::sandbox::Sandbox>,
    Arc<dyn super::sandbox_files::SandboxDirectory>,
)>;
#[cfg(not(feature = "agent-api"))]
type SandboxOverride = ();

pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;

pub struct IngressService {
    config: IngressConfig,
    tls_listener: TcpListener,
    plain_listener: TcpListener,
    health_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    gateway: Arc<Ingress>,
    watcher: Arc<CoordinatorConnection>,
    store: Arc<RouteStore>,
    route_changes: broadcast::Receiver<RouteChange>,
    #[cfg(feature = "agent-api")]
    ssh: Option<super::ssh::SshListener>,
}

impl IngressService {
    /// Bind all public listeners before the owning process reports readiness.
    pub async fn bind(config: IngressConfig, control: ControlConfig) -> Result<Self, ServiceError> {
        #[cfg(feature = "agent-api")]
        let injected = None;
        #[cfg(not(feature = "agent-api"))]
        let injected = ();
        Self::bind_inner(config, control, injected).await
    }

    /// Use the embedding API Server's Sandbox application service for Agent lifecycle calls.
    #[cfg(feature = "agent-api")]
    pub async fn bind_with_sandbox_service(
        config: IngressConfig,
        control: ControlConfig,
        service: Arc<dyn adx_agent_core::sandbox::Sandbox>,
        directory: Arc<dyn super::sandbox_files::SandboxDirectory>,
    ) -> Result<Self, ServiceError> {
        Self::bind_inner(config, control, Some((service, directory))).await
    }

    async fn bind_inner(
        config: IngressConfig,
        control: ControlConfig,
        injected: SandboxOverride,
    ) -> Result<Self, ServiceError> {
        #[cfg(not(feature = "agent-api"))]
        let () = injected;
        #[cfg(not(feature = "agent-api"))]
        if std::env::var_os("ADX_SANDBOX_FILES_CONFIG").is_some()
            || std::env::var_os("ADX_AGENT_CONFIG").is_some()
            || std::env::var_os("ADX_SSH_CONFIG").is_some()
            || std::env::var_os("ADX_JIUWEN_CONFIG").is_some()
        {
            return Err(
                "Sandbox, Agent, Jiuwen and SSH configuration require a Gateway built with --features agent-api".into(),
            );
        }
        let tls_listener = TcpListener::bind(config.tls_bind).await?;
        let plain_listener = TcpListener::bind(config.plain_bind).await?;
        let health_listener = TcpListener::bind(config.health_bind).await?;
        let tls_acceptor = load_tls_acceptor(&config.tls_cert, &config.tls_key)?;
        let store = Arc::new(RouteStore::new());
        let route_changes = store.subscribe();
        let watcher = Arc::new(CoordinatorConnection::new(control).map_err(send_error)?);
        let resolver = Arc::new(IngressRouteResolver::new(store.clone()).stream_only());
        let connector = DataPlaneL4Connector::new(config.h2_pool_config()?);
        let authenticator = IngressAuthenticator::with_verifier(watcher.clone());
        #[cfg(feature = "agent-api")]
        let file_access = if let Ok(path) = std::env::var("ADX_SANDBOX_FILES_CONFIG") {
            let settings: super::sandbox_files::FileAccessConfig =
                serde_json::from_slice(&std::fs::read(path)?)
                    .map_err(|_| "invalid Sandbox file configuration")?;
            if settings.port != config.default_direct_port {
                return Err("Sandbox file port must match Ingress direct port".into());
            }
            Some(settings.build(injected.as_ref().map(|(_, directory)| directory.clone()))?)
        } else {
            None
        };
        #[cfg(feature = "agent-api")]
        let agent_api = if let Ok(path) = std::env::var("ADX_AGENT_CONFIG") {
            use super::agent_api::{AgentApi, AgentConfig};
            let settings: AgentConfig = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| "invalid Agent configuration")?;
            let api = if settings.embedded.is_some() {
                AgentApi::new_embedded(
                    settings,
                    injected
                        .as_ref()
                        .map(|(service, _)| service.clone())
                        .ok_or("embedded Activator requires a Sandbox service")?,
                )
                .await
                .map_err(send_error)?
            } else {
                AgentApi::new(settings).map_err(send_error)?
            };
            Some(Arc::new(api))
        } else {
            None
        };
        #[cfg(feature = "agent-api")]
        let jiuwen_api = if let Ok(path) = std::env::var("ADX_JIUWEN_CONFIG") {
            use super::jiuwen::entrypoint::{JiuwenApi, JiuwenConfig};
            let settings: JiuwenConfig = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| "invalid Jiuwen configuration")?;
            if let Some(domain) = &config.port_host_domain {
                if settings
                    .allowed_hosts
                    .iter()
                    .any(|host| host.ends_with(&format!(".{domain}")))
                {
                    return Err("Jiuwen hosts must not use the direct-port host domain".into());
                }
            }
            let agent = agent_api
                .as_ref()
                .ok_or("Jiuwen requires ADX_AGENT_CONFIG")?;
            if file_access.is_none() {
                return Err("Jiuwen requires ADX_SANDBOX_FILES_CONFIG".into());
            }
            let account_path = std::env::var("ADX_ACCOUNT_CONFIG")
                .map_err(|_| "Jiuwen requires ADX_ACCOUNT_CONFIG")?;
            let account_config: super::accounts::AccountConfig =
                serde_json::from_slice(&std::fs::read(account_path)?)
                    .map_err(|_| "invalid account configuration")?;
            if account_config.tenant != settings.tenant {
                return Err("Jiuwen/account tenant mismatch".into());
            }
            let auth = Arc::new(super::accounts::AccountService::connect(account_config).await?);
            Some(Arc::new(JiuwenApi::new(
                settings,
                agent.managed.clone(),
                auth,
            )?))
        } else {
            None
        };
        let gateway = Ingress::new(
            resolver,
            connector,
            authenticator,
            config.default_direct_port,
            config.default_tunnel_port,
            config.frontend_address.clone(),
            config.control_plane_routes.clone(),
        )
        .with_port_host_domain(config.port_host_domain.clone())
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
        let gateway = match file_access {
            Some(access) => gateway.with_execd_access(access),
            None => gateway,
        };
        #[cfg(feature = "agent-api")]
        let gateway = if let Some(api) = &agent_api {
            gateway.with_agent_api(api.clone())
        } else {
            gateway
        };
        #[cfg(feature = "agent-api")]
        let gateway = if let Some(api) = jiuwen_api {
            gateway.with_jiuwen_api(api)
        } else {
            gateway
        };
        let gateway = Arc::new(gateway);
        #[cfg(feature = "agent-api")]
        let ssh = if let Ok(path) = std::env::var("ADX_SSH_CONFIG") {
            let settings = serde_json::from_slice(&std::fs::read(path)?)
                .map_err(|_| "invalid SSH configuration")?;
            Some(super::ssh::SshListener::bind(settings, gateway.clone()).await?)
        } else {
            None
        };
        Ok(Self {
            #[cfg(feature = "agent-api")]
            ssh,
            config,
            tls_listener,
            plain_listener,
            health_listener,
            tls_acceptor,
            gateway,
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
            #[cfg(feature = "agent-api")]
            ssh,
        } = self;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let watcher_task = tokio::spawn(watcher.run(store));
        let route_reconciler_task =
            tokio::spawn(gateway.clone().run_route_reconciler(route_changes));
        let mut listeners = JoinSet::new();
        #[cfg(feature = "agent-api")]
        if let Some(ssh) = ssh {
            let shutdown = shutdown_rx.clone();
            listeners.spawn(async move {
                ssh.serve(shutdown)
                    .await
                    .map_err(|error| Box::new(error) as ServiceError)
            });
        }
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
            "Data Plane Ingress serving"
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
        Some(Ok(Ok(()))) => "Ingress listener stopped unexpectedly".into(),
        Some(Err(error)) => std::io::Error::other(error.to_string()).into(),
        None => "Ingress listener set stopped unexpectedly".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::IngressNodeSecurityMode,
        ingress::{parse_static_routes, ReverseProxyConfig},
    };
    use adx_transport::tls::TlsFiles;
    use std::{collections::BTreeMap, path::PathBuf, time::Duration};

    #[tokio::test]
    async fn shared_service_binds_and_stops_cleanly() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        let certificate = fixture.join("ingress-cert.pem");
        let private_key = fixture.join("ingress-key.pem");
        let config = IngressConfig {
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
            port_host_domain: None,
            node_security_mode: IngressNodeSecurityMode::Network,
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
            namespace: "ingress-service-test".into(),
            tls: TlsFiles {
                mode: Default::default(),
                ca: certificate.clone(),
                certificate: certificate.clone(),
                private_key,
                server_name: "localhost".into(),
                peers: BTreeMap::from([("coordinator".into(), certificate)]),
            },
            rpc_timeout_seconds: 1,
            refresh_seconds: 1,
            auth_cache_seconds: 1,
            auth_cache_entries: 1,
        };

        IngressService::bind(config, control)
            .await
            .unwrap()
            .serve(async {})
            .await
            .unwrap();
    }
}
