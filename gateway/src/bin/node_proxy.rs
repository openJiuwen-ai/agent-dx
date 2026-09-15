use data_plane_gateway::common::listener::accept_with_backoff;
use data_plane_gateway::common::protocol::GatewayPolicy;
use data_plane_gateway::common::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::common::shutdown::shutdown_signal;
use data_plane_gateway::config::{EdgeNodeSecurityMode, NodeProxyConfig};
#[cfg(feature = "activity-client")]
use data_plane_gateway::node::ActivityTracker;
use data_plane_gateway::node::{serve_health, NodeProxy};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    data_plane_gateway::common::install_crypto_provider();
    let _logging_guard = data_plane_gateway::common::logging::init("node-proxy", false)?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Node Proxy FD limit configured");
    run(NodeProxyConfig::from_env()?).await
}

async fn run(config: NodeProxyConfig) -> Result<(), Box<dyn std::error::Error>> {
    if config.activity_uds_dir.is_none() {
        return Err(
            "ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR is required for managed bindings".into(),
        );
    }
    let listener = TcpListener::bind(config.bind).await?;
    let health_listener = TcpListener::bind(config.health_bind).await?;
    let mut gateway = NodeProxy::new(GatewayPolicy::new(config.allowed_target_networks.clone()))
        .with_max_active_streams(config.max_streams);
    #[cfg(feature = "activity-client")]
    let activity_control = if let Some(uds_dir) = &config.activity_uds_dir {
        let uds_path = std::path::Path::new(uds_dir)
            .join("node-manager.sock")
            .to_string_lossy()
            .into_owned();
        let tracker = Arc::new(ActivityTracker::new(config.gateway_epoch.clone()));
        gateway = gateway
            .with_activity_tracker(tracker.clone())
            .with_route_enforcement();
        Some((tracker, uds_path, uds_dir.clone()))
    } else {
        None
    };
    let gateway = Arc::new(gateway);
    #[cfg(feature = "activity-client")]
    if activity_control.is_some() {
        gateway.set_bindings_ready(false);
    }
    #[cfg(feature = "activity-client")]
    if let Some((tracker, activity_uds_path, uds_dir)) = activity_control {
        let route_uds_path = std::path::Path::new(&uds_dir)
            .join("route.sock")
            .to_string_lossy()
            .into_owned();
        // Bind the admission-control socket before publishing the gateway
        // session. Node Manager must reconcile bindings before resumed traffic;
        // activity snapshots are observations, not route replay commands.
        let route_listener = data_plane_gateway::node::bind_route_control(&route_uds_path).await?;
        let route_gateway = gateway.clone();
        tokio::spawn(async move {
            if let Err(error) =
                data_plane_gateway::node::serve_route_control(route_gateway, route_listener).await
            {
                tracing::error!(%error, "Node Proxy route control server stopped");
            }
        });
        tokio::spawn(data_plane_gateway::node::run_activity_publisher(
            tracker,
            activity_uds_path,
            config.activity_interval,
        ));
        tracing::info!("Data Plane Gateway activity and route control enabled");
    }
    let tls_acceptor = if config.edge_security_mode == EdgeNodeSecurityMode::Mtls {
        Some(Arc::new(load_tls_acceptor(
            &config.tls_cert,
            &config.tls_key,
            &config.mtls_client_ca,
        )?))
    } else {
        None
    };
    let (health_shutdown_tx, health_shutdown_rx) = watch::channel(false);
    let health_task = tokio::spawn(serve_health(
        gateway.clone(),
        health_listener,
        health_shutdown_rx,
    ));
    let mut connections = JoinSet::new();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    tracing::info!(
        bind = %config.bind,
        health = %config.health_bind,
        security_mode = ?config.edge_security_mode,
        "Data Plane Node Proxy serving"
    );

    loop {
        tokio::select! {
            result = &mut shutdown => {
                result?;
                break;
            }
            (stream, peer) = accept_with_backoff(&listener, "node-h2") => {
                if !config.peer_allowed(peer.ip()) {
                    tracing::warn!(
                        target: "adx_audit",
                        event = "edge_acl",
                        decision = "deny",
                        peer = %peer,
                        reason = "source_outside_allowed_cidrs",
                        "Node Proxy peer denied"
                    );
                    continue;
                }
                let gateway = gateway.clone();
                let tls_acceptor = tls_acceptor.clone();
                connections.spawn(async move {
                    let result = match tls_acceptor {
                        Some(acceptor) => match acceptor.accept(stream).await {
                            Ok(stream) => gateway.serve_h2(stream).await,
                            Err(error) => {
                                tracing::debug!(%peer, %error, "Node Proxy TLS handshake rejected");
                                return;
                            }
                        },
                        None => gateway.serve_h2(stream).await,
                    };
                    if let Err(error) = result {
                        tracing::debug!(%peer, %error, "Node Proxy H2 connection closed");
                    }
                });
            }
        }
    }

    gateway.start_drain();
    let deadline = tokio::time::Instant::now() + config.drain_timeout;
    while gateway.active_streams() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let _ = health_shutdown_tx.send(true);
    health_task.await??;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

fn load_tls_acceptor(
    cert_path: &str,
    key_path: &str,
    mtls_client_ca: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let mut cert_reader = BufReader::new(File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = BufReader::new(File::open(key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or("no private key found")?;
    let builder = rustls::ServerConfig::builder();
    let mut config = if mtls_client_ca.is_empty() {
        builder.with_no_client_auth().with_single_cert(certs, key)?
    } else {
        let mut ca_reader = BufReader::new(File::open(mtls_client_ca)?);
        let ca_certificates =
            rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
        let mut roots = rustls::RootCertStore::empty();
        for certificate in ca_certificates {
            roots.add(certificate)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}
