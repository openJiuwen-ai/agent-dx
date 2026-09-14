#![cfg(feature = "etcd-watch")]

use data_plane_gateway::common::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::common::shutdown::shutdown_signal;
use data_plane_gateway::config::EdgeFrontendConfig;
use data_plane_gateway::edge::{
    CommandWatchConfig, DataPlaneL4Connector, EdgeAuthenticator, EdgeFrontend, EdgeRouteResolver,
    RouteStore, RouteWatcher,
};
use std::sync::Arc;
use std::{fs::File, io::BufReader};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    data_plane_gateway::common::install_crypto_provider();
    let _logging_guard = data_plane_gateway::common::logging::init("edge-frontend", true)?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Edge Frontend FD limit configured");
    run(EdgeFrontendConfig::from_env()?).await
}

async fn run(config: EdgeFrontendConfig) -> Result<(), Box<dyn std::error::Error>> {
    let tls_listener = TcpListener::bind(config.tls_bind).await?;
    let plain_listener = TcpListener::bind(config.plain_bind).await?;
    let health_listener = TcpListener::bind(config.health_bind).await?;
    let store = Arc::new(RouteStore::new());
    let route_changes = store.subscribe();
    let watcher = Arc::new(RouteWatcher::new(
        store.clone(),
        config.etcd_endpoints.clone(),
        config.etcd_connect_options()?,
    ));
    let resolver = Arc::new(EdgeRouteResolver::new(store).with_point_getter(watcher.clone()));
    let connector = DataPlaneL4Connector::new(config.h2_pool_config()?);
    let authenticator = EdgeAuthenticator::new(
        true,
        config.validate_iam,
        config.iam_address.clone(),
        config.auth_cache_ttl,
    )?;
    let gateway = Arc::new(
        EdgeFrontend::new(
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
        ),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let watcher_task = tokio::spawn(watcher.run());
    let route_reconciler_task = tokio::spawn(gateway.clone().run_route_reconciler(route_changes));
    let tls_acceptor = load_tls_acceptor(&config.tls_cert, &config.tls_key)?;
    let tls_task = tokio::spawn(gateway.clone().serve_http_tls(
        tls_listener,
        tls_acceptor,
        shutdown_rx.clone(),
    ));
    let plain_task = tokio::spawn(
        gateway
            .clone()
            .serve_http(plain_listener, shutdown_rx.clone()),
    );
    let health_task = tokio::spawn(
        gateway
            .clone()
            .serve_health(health_listener, shutdown_rx.clone()),
    );
    tracing::info!(
        tls = %config.tls_bind,
        plain = %config.plain_bind,
        frontend = %config.frontend_address,
        health = %config.health_bind,
        "Data Plane Edge Frontend serving"
    );

    shutdown_signal().await?;
    gateway.start_drain();
    let _ = shutdown_tx.send(true);
    let deadline = tokio::time::Instant::now() + config.drain_timeout;
    while gateway.active_sessions() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    watcher_task.abort();
    route_reconciler_task.abort();
    tls_task.await??;
    plain_task.await??;
    health_task.await??;
    Ok(())
}

fn load_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let mut cert_reader = BufReader::new(File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err("Edge TLS certificate file is empty".into());
    }
    let mut key_reader = BufReader::new(File::open(key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or("Edge TLS key is empty")?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}
