use adx_coordinator::{
    auth::{AuthRpc, Credential},
    rpc::CoordinatorRpc,
    storage::RedisStore,
    Placement,
};
use adx_core::Error;
use adx_process::{read_config, shutdown};
use adx_protocol::control as pb;
use adx_transport::tls::TlsFiles;
use serde::Deserialize;
use std::{future::Future, net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    key_file: PathBuf,
    #[serde(flatten)]
    credential: Credential,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    storage_domains: std::collections::BTreeMap<String, adx_core::volumes::StorageDomain>,
    #[serde(default)]
    checkpoint_reclaimers: Vec<adx_checkpoint_storage::reclaim::ReclaimerConfig>,
    listen: SocketAddr,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    autoscaling_listen: Option<SocketAddr>,
    #[serde(default)]
    advertised_address: Option<String>,
    #[serde(default = "default_heartbeat")]
    heartbeat_timeout_seconds: u64,
    #[serde(default = "default_node_retention")]
    node_retention_seconds: u64,
    #[serde(default = "default_ttl")]
    discovery_ttl_seconds: u64,
    #[serde(default = "default_failed_retention")]
    failed_retention_seconds: u64,
    redis_url: String,
    namespace: String,
    scheduler_shards: usize,
    placement: String,
    rpc_timeout_seconds: u64,
    tls: TlsFiles,
    bootstrap_credentials: Vec<Bootstrap>,
}
fn default_node_retention() -> u64 {
    1800
}
fn default_heartbeat() -> u64 {
    30
}
fn default_ttl() -> u64 {
    15
}
fn default_failed_retention() -> u64 {
    600
}

async fn supervise<S, H, B>(
    server: S,
    shutdown_signal: H,
    background: B,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: Future<Output = Result<(), Box<dyn std::error::Error>>>,
    H: Future<Output = ()>,
    B: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        _ = shutdown_signal => server.await,
        result = background => result,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _logging_guard = adx_observability::logging::init("adx-coordinator", false)?;
    let config: Config = read_config()?;
    if config.node_retention_seconds == 0
        || config.heartbeat_timeout_seconds == 0
        || config.discovery_ttl_seconds < 3
        || config.failed_retention_seconds == 0
    {
        return Err(
            "positive node/Failed retention and heartbeat timeout, and discovery TTL >= 3 required"
                .into(),
        );
    }
    let placement = match config.placement.as_str() {
        "pack" => Placement::Pack,
        "spread" => Placement::Spread,
        _ => return Err("placement must be pack or spread".into()),
    };
    let (server_tls, client_tls, peers) = config
        .tls
        .load_rpc(adx_protocol::auth::Principal::Coordinator)?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let session = loop {
        let result = match RedisStore::connect(&config.redis_url, &config.namespace, timeout).await
        {
            Ok(store) => store.begin(config.scheduler_shards).await,
            Err(error) => Err(error),
        };
        match result {
            Ok(session) => break session,
            Err(Error::Unavailable(message)) => {
                adx_observability::warn!("Coordinator storage recovery unavailable: {message}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    _ = shutdown() => return Ok(()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    };
    let mut administrators = Vec::new();
    let mut tenants = Vec::new();
    for credential in config.bootstrap_credentials {
        let key = std::fs::read_to_string(credential.key_file)?;
        let entry = (key.trim().to_owned(), credential.credential);
        if entry.1.administrator {
            administrators.push(entry);
        } else {
            tenants.push(entry);
        }
    }
    session.reconcile_administrators(&administrators).await?;
    for (key, credential) in tenants {
        session.bootstrap_credential(&key, &credential).await?;
    }
    let auth = AuthRpc::new(session.clone(), peers.clone());
    let routes = adx_coordinator::routes::RoutePublisher::new(session.clone(), peers.clone());
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        placement,
        peers,
        client_tls,
        timeout,
        Duration::from_secs(config.heartbeat_timeout_seconds),
    )
    .await?;
    let mut reclaimers = adx_checkpoint_storage::reclaim::Reclaimers::default();
    for store in config.checkpoint_reclaimers {
        reclaimers.insert(store.build()?)?;
    }
    let rpc = rpc
        .with_volume_domains(config.storage_domains)?
        .with_reclaimers(reclaimers);
    let metrics_listener = match config.metrics_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let metrics_rpc = rpc.clone();
    let autoscaling_listener = match config.autoscaling_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let autoscaling_rpc = rpc.clone();
    let autoscaling = async move {
        if let Some(listener) = autoscaling_listener {
            adx_observability::info!(address=%listener.local_addr()?, "Coordinator autoscaling HTTP listening");
            adx_coordinator::autoscaling::serve(listener, autoscaling_rpc).await?;
        }
        std::future::pending::<std::io::Result<()>>().await
    };
    let metrics = async move {
        if let Some(listener) = metrics_listener {
            adx_coordinator::metrics::serve(listener, metrics_rpc).await?;
        }
        std::future::pending::<std::io::Result<()>>().await
    };
    routes.refresh().await?;
    let route_task = Box::pin(routes.clone().run(Duration::from_millis(200)));
    let ttl = Duration::from_secs(config.discovery_ttl_seconds);
    if let Some(address) = &config.advertised_address {
        session.advertise(&config.namespace, address, ttl).await?;
    }
    let maintenance_rpc = rpc.clone();
    // Lifecycle workers carry large typed records; keep their futures off the main stack.
    let maintenance = Box::pin(async {
        let mut tick = tokio::time::interval(Duration::from_secs(
            (config
                .heartbeat_timeout_seconds
                .min(config.discovery_ttl_seconds)
                / 3)
            .max(1),
        ));
        loop {
            tick.tick().await;
            if let Err(error) = maintenance_rpc.expire_nodes().await {
                adx_observability::warn!("node health publication unavailable: {error}");
            }
            if let Err(error) = maintenance_rpc
                .collect_retired_nodes(Duration::from_secs(config.node_retention_seconds))
                .await
            {
                adx_observability::warn!(%error, "offline node retirement deferred");
            }
            if let Some(address) = &config.advertised_address {
                if let Err(error) = session.advertise(&config.namespace, address, ttl).await {
                    adx_observability::warn!("Coordinator discovery renewal unavailable: {error}");
                }
            }
        }
    });
    let recovery_rpc = rpc.clone();
    let recovery = Box::pin(async {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if let Err(error) = recovery_rpc.recover_environments().await {
                adx_observability::warn!("environment recovery incomplete: {error}");
            }
            if let Err(error) = recovery_rpc.resume_pending().await {
                adx_observability::warn!("pending resume reconciliation unavailable: {error}");
            }
        }
    });
    let preparation_rpc = rpc.clone();
    let preparation = Box::pin(async {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if let Err(error) = preparation_rpc.reconcile_snapshot_preparations().await {
                adx_observability::warn!("snapshot preparation reconciliation incomplete: {error}");
            }
        }
    });
    let collection_rpc = rpc.clone();
    let collection = Box::pin(async {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            if let Err(error) = collection_rpc.collect_shared_artifacts().await {
                adx_observability::warn!("shared artifact collection incomplete: {error}");
            }
            if let Err(error) = collection_rpc.collect_snapshots().await {
                adx_observability::warn!("snapshot collection incomplete: {error}");
            }
            if let Err(error) = collection_rpc.expire_dormant().await {
                adx_observability::warn!("dormant expiration incomplete: {error}");
            }
            if let Err(error) = collection_rpc.collect_checkpoint_cleanups().await {
                adx_observability::warn!("checkpoint cleanup incomplete: {error}");
            }
            if let Err(error) = collection_rpc
                .collect_failed(Duration::from_secs(config.failed_retention_seconds))
                .await
            {
                adx_observability::warn!("failed environment collection incomplete: {error}");
            }
        }
    });
    adx_observability::info!("adx-coordinator listening on {}", listener.local_addr()?);
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = server_tls {
        builder = builder.tls_config(tls)?;
    }
    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel();
    let closing_routes = routes.clone();
    let closing_nodes = rpc.clone();
    let server = builder
        .add_service(
            pb::volume_execution_service_server::VolumeExecutionServiceServer::new(rpc.clone()),
        )
        .add_service(pb::volume_service_server::VolumeServiceServer::new(
            rpc.clone(),
        ))
        .add_service(
            pb::snapshot_preparation_service_server::SnapshotPreparationServiceServer::new(
                rpc.clone(),
            ),
        )
        .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
            rpc.clone(),
        ))
        .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
        .add_service(pb::credential_service_server::CredentialServiceServer::new(
            auth.clone(),
        ))
        .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
        .add_service(
            pb::route_service_server::RouteServiceServer::new(routes.clone())
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .add_service(
            pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                routes,
            )
            .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                let _ = server_shutdown_rx.await;
                closing_routes.close_subscriptions();
                closing_nodes.close_subscriptions();
            },
        );
    let server = Box::pin(async move {
        server
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
    });
    let background = Box::pin(async {
        tokio::select! {
            result = autoscaling => result.map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            _ = maintenance => Ok(()),
            _ = route_task => Ok(()),
            _ = collection => Ok(()),
            _ = preparation => Ok(()),
            _ = recovery => Ok(()),
            result = metrics => result.map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
        }
    });
    supervise(
        server,
        async move {
            shutdown().await;
            let _ = server_shutdown_tx.send(());
        },
        background,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::supervise;
    use std::{
        error::Error,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::{oneshot, Notify};

    #[tokio::test]
    async fn shutdown_stops_background_polling_while_server_drains() {
        let (signal_tx, signal_rx) = oneshot::channel();
        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let draining = Arc::new(Notify::new());
        let server_draining = draining.clone();
        let server = async move {
            let _ = server_shutdown_rx.await;
            server_draining.notify_one();
            let _ = release_rx.await;
            Ok::<(), Box<dyn Error>>(())
        };
        let ticks = Arc::new(AtomicUsize::new(0));
        let background_ticks = ticks.clone();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        let background = std::future::poll_fn(move |cx| {
            while interval.poll_tick(cx).is_ready() {
                background_ticks.fetch_add(1, Ordering::SeqCst);
            }
            std::task::Poll::<Result<(), Box<dyn Error>>>::Pending
        });
        let controller = async {
            signal_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), draining.notified())
                .await
                .expect("server did not enter drain");
            let before = ticks.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(
                ticks.load(Ordering::SeqCst),
                before,
                "background polled during drain"
            );
            release_tx.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(
            supervise(
                server,
                async move {
                    let _ = signal_rx.await;
                    let _ = server_shutdown_tx.send(());
                },
                background,
            ),
            controller,
        );
        result.unwrap();
    }
}
