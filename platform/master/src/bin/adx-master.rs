use adx_master::{
    auth::{AuthRpc, Credential},
    rpc::MasterRpc,
    storage::RedisStore,
    Placement,
};
use adx_process::{read_config, shutdown};
use adx_protocol::control as pb;
use adx_transport::tls::TlsFiles;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
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
    listen: SocketAddr,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    advertised_address: Option<String>,
    #[serde(default = "default_heartbeat")]
    heartbeat_timeout_seconds: u64,
    #[serde(default = "default_ttl")]
    discovery_ttl_seconds: u64,
    redis_url: String,
    namespace: String,
    scheduler_shards: usize,
    placement: String,
    rpc_timeout_seconds: u64,
    tls: TlsFiles,
    bootstrap_credentials: Vec<Bootstrap>,
}
fn default_heartbeat() -> u64 {
    30
}
fn default_ttl() -> u64 {
    15
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _logging_guard = adx_observability::logging::init("adx-master", false)?;
    let config: Config = read_config()?;
    if config.heartbeat_timeout_seconds == 0 || config.discovery_ttl_seconds < 3 {
        return Err("positive heartbeat timeout and discovery TTL >= 3 required".into());
    }
    let placement = match config.placement.as_str() {
        "pack" => Placement::Pack,
        "spread" => Placement::Spread,
        _ => return Err("placement must be pack or spread".into()),
    };
    let (server_tls, client_tls, peers) = config.tls.load()?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let store = RedisStore::connect(&config.redis_url, &config.namespace, timeout).await?;
    let session = store.begin(config.scheduler_shards).await?;
    for credential in config.bootstrap_credentials {
        let key = std::fs::read_to_string(credential.key_file)?;
        session
            .bootstrap_credential(key.trim(), &credential.credential)
            .await?;
    }
    let auth = AuthRpc::new(session.clone(), peers.clone());
    let routes = adx_master::routes::RoutePublisher::new(session.clone(), peers.clone());
    let rpc = MasterRpc::with_heartbeat_timeout(
        session.clone(),
        placement,
        peers,
        client_tls,
        timeout,
        Duration::from_secs(config.heartbeat_timeout_seconds),
    )
    .await?;
    let metrics_listener = match config.metrics_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let metrics_rpc = rpc.clone();
    let metrics = async move {
        if let Some(listener) = metrics_listener {
            adx_master::metrics::serve(listener, metrics_rpc).await?;
        }
        std::future::pending::<std::io::Result<()>>().await
    };
    routes.refresh().await?;
    let route_task = routes.clone().run(Duration::from_millis(200));
    let ttl = Duration::from_secs(config.discovery_ttl_seconds);
    if let Some(address) = &config.advertised_address {
        session.advertise(&config.namespace, address, ttl).await?;
    }
    let maintenance_rpc = rpc.clone();
    let maintenance = async {
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
            if let Some(address) = &config.advertised_address {
                if let Err(error) = session.advertise(&config.namespace, address, ttl).await {
                    adx_observability::warn!("Master discovery renewal unavailable: {error}");
                }
            }
        }
    };
    let recovery_rpc = rpc.clone();
    let recovery = async {
        let mut tick = tokio::time::interval(Duration::from_secs(2));
        loop {
            tick.tick().await;
            if let Err(error) = recovery_rpc.recover_capsules().await {
                adx_observability::warn!("capsule recovery incomplete: {error}");
            }
        }
    };
    let collection_rpc = rpc.clone();
    let collection = async {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            if let Err(error) = collection_rpc.collect_snapshots().await {
                adx_observability::warn!("snapshot collection incomplete: {error}");
            }
        }
    };
    adx_observability::info!("adx-master listening on {}", listener.local_addr()?);
    let server = tonic::transport::Server::builder()
        .tls_config(server_tls)?
        .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
            rpc.clone(),
        ))
        .add_service(pb::master_service_server::MasterServiceServer::new(rpc))
        .add_service(pb::credential_service_server::CredentialServiceServer::new(
            auth.clone(),
        ))
        .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
        .add_service(
            pb::route_service_server::RouteServiceServer::new(routes.clone())
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .add_service(
            pb::capsule_directory_service_server::CapsuleDirectoryServiceServer::new(routes)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutdown(),
        );
    tokio::select! {
        result = server => result?,
        _ = maintenance => {}
        _ = route_task => {}
        _ = collection => {}
        _ = recovery => {}
        result = metrics => result?,
    }
    Ok(())
}
