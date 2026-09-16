use adx_node_manager::resources::{PressureGate, PressurePolicy, ResourceSource};
use adx_node_manager::{
    readiness::RrtReadiness,
    routes::UdsRoutes,
    rpc::{MasterStateSink, NodeRpc},
    sandboxd::{Config as RuntimeConfig, Sandboxd},
    NodeManager,
};
use adx_protocol::{
    control as pb,
    tls::{read_config, shutdown, TlsFiles},
};
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_id: String,
    #[serde(default)]
    labels: HashMap<String, String>,
    listen: SocketAddr,
    advertised_address: String,
    proxy_address: String,
    #[serde(default)]
    master_address: Option<String>,
    #[serde(default)]
    discovery: Option<Discovery>,
    tls: TlsFiles,
    #[serde(default)]
    admin_socket: Option<PathBuf>,
    sandboxd_socket: PathBuf,
    proxy_socket: PathBuf,
    #[serde(default)]
    proxy_mode: adx_node_manager::proxy::ProxyMode,
    #[serde(default)]
    capacity_file: Option<PathBuf>,
    #[serde(default)]
    resource_source: Option<ResourceSource>,
    #[serde(default)]
    pressure: Option<PressureConfig>,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    rrt_health_failure_threshold: Option<u32>,
    #[serde(default)]
    checkpoint_dir: Option<PathBuf>,
    #[serde(default)]
    checkpoint_storage: Option<adx_node_manager::checkpoint::StorageConfig>,
    #[serde(default)]
    degradation_journal: Option<PathBuf>,
    #[serde(default)]
    checkpoint_gc: adx_node_manager::checkpoint::RemoteGcConfig,
    report_interval_seconds: u64,
    rpc_timeout_seconds: u64,
    #[serde(default = "runtime_ready_timeout")]
    runtime_ready_timeout_seconds: u64,
    rrt_port: u16,
    rrt_command: Vec<String>,
    rrt_env: HashMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Discovery {
    redis_url: String,
    namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PressureConfig {
    disk_path: PathBuf,
    #[serde(default)]
    thresholds: PressurePolicy,
}
fn runtime_ready_timeout() -> u64 {
    120
}
async fn sample(
    source: &ResourceSource,
    runtime: &Sandboxd,
    timeout: Duration,
) -> adx_core::Result<(adx_node_manager::resources::Observation, Duration)> {
    if matches!(source, ResourceSource::Sandboxd { .. }) {
        runtime.check_health().await?;
    }
    source.sample(timeout).await
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let c: Config = read_config()?;
    c.checkpoint_gc.validate()?;
    let _logging_guard = if c.proxy_mode == adx_node_manager::proxy::ProxyMode::Embedded {
        Some(data_plane_gateway::common::logging::init(
            "node-manager",
            false,
        )?)
    } else {
        None
    };
    if c.node_id.is_empty()
        || c.report_interval_seconds == 0
        || c.rpc_timeout_seconds == 0
        || c.runtime_ready_timeout_seconds == 0
    {
        return Err("node identity and positive intervals required".into());
    }
    let source = match (c.capacity_file.clone(), c.resource_source.clone()) {
        (Some(path), None) => ResourceSource::File { path },
        (None, Some(source)) => source,
        _ => return Err("configure exactly one of capacity_file and resource_source".into()),
    };
    let mut pressure_gate = c
        .pressure
        .as_ref()
        .map(|p| PressureGate::new(p.thresholds.clone()))
        .transpose()?;
    let (server_tls, client_tls, peers) = c.tls.load()?;
    let timeout = Duration::from_secs(c.rpc_timeout_seconds);
    let listener = tokio::net::TcpListener::bind(c.listen).await?;
    let discovery = match (&c.master_address, &c.discovery) {
        (Some(_), None) => None,
        (None, Some(d)) => Some(adx_discovery::RedisDiscovery::new(
            &d.redis_url,
            &d.namespace,
            timeout,
        )?),
        _ => return Err("configure exactly one of master_address and discovery".into()),
    };
    let mut endpoint = loop {
        if let Some(address) = &c.master_address {
            break address.clone();
        }
        match discovery.as_ref().unwrap().lookup().await {
            Ok(found) => break found.address,
            Err(_) => {
                tokio::select! { _ = tokio::time::sleep(Duration::from_secs(c.report_interval_seconds)) => (), _ = shutdown() => return Ok(()) }
            }
        }
    };
    let channel_for =
        |address: String| -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
            Ok(tonic::transport::Endpoint::from_shared(address)?
                .tls_config(client_tls.clone())?
                .connect_timeout(timeout)
                .timeout(timeout)
                .connect_lazy())
        };
    let channel = channel_for(endpoint.clone())?;
    let session_id = uuid::Uuid::new_v4().to_string();
    // Leave part of the operation deadline for a durable fallback after RPC timeout.
    let commit_timeout = if c.degradation_journal.is_some() {
        timeout / 2
    } else {
        timeout
    };
    let sink = Arc::new(
        MasterStateSink::new(channel.clone(), commit_timeout)?.with_session(session_id.clone()),
    );
    let journal = c.degradation_journal.map(|path| {
        Arc::new(adx_node_manager::journal::JournalSink::new(
            path,
            sink.clone(),
        ))
    });
    let state_sink: Arc<dyn adx_node_manager::StateSink> = match &journal {
        Some(journal) => journal.clone(),
        None => sink.clone(),
    };
    let mut master = pb::master_service_client::MasterServiceClient::new(channel);
    let mut env = c.rrt_env;
    env.insert("RRT_HTTP_ONLY".into(), "1".into());
    env.insert("RRT_HTTP_PORT".into(), c.rrt_port.to_string());
    let token = env.get("RRT_HTTP_TOKEN").cloned();
    let runtime = Arc::new(
        Sandboxd::connect(
            c.sandboxd_socket,
            RuntimeConfig {
                command: c.rrt_command,
                env,
                cwd: "/".into(),
                rpc_timeout: timeout,
            },
        )
        .await?,
    );
    runtime
        .wait_ready_with_timeout(Duration::from_secs(c.runtime_ready_timeout_seconds))
        .await?;
    let mut readiness = RrtReadiness::new(
        runtime.clone(),
        c.rrt_port,
        Duration::from_millis(100),
        timeout,
        timeout,
    )?;
    if let Some(token) = &token {
        readiness = readiness.with_token(token)?;
    }
    let mut manager = NodeManager::new(
        c.node_id.clone(),
        runtime.clone(),
        Arc::new(readiness),
        Arc::new(UdsRoutes::new(c.proxy_socket.clone(), timeout)?),
        state_sink,
    )
    .with_operation_timeout(timeout)?
    .with_health_check(c.rrt_health_failure_threshold)?;
    let checkpoint_storage = match (c.checkpoint_dir, c.checkpoint_storage) {
        (Some(root), None) => Some(adx_node_manager::checkpoint::StorageConfig::Local { root }),
        (None, storage) => storage,
        (Some(_), Some(_)) => {
            return Err("configure only one of checkpoint_dir and checkpoint_storage".into())
        }
    };
    if let Some(storage) = checkpoint_storage {
        let mut control =
            adx_node_manager::runtime_control::RuntimeControlClient::new(c.rrt_port, timeout)?;
        if let Some(token) = &token {
            control = control.with_token(token)?;
        }
        manager = manager
            .with_checkpointing(
                storage.build_for_node(
                    c.node_id.clone(),
                    session_id.clone(),
                    c.checkpoint_gc.clone(),
                )?,
                Arc::new(control),
            )?
            .with_snapshot_catalog(sink.clone())?;
    }
    let manager = Arc::new(manager);
    manager.pause_lifecycle().await;
    let (o, valid) = loop {
        match sample(&source, &runtime, timeout).await {
            Ok(sample) => break sample,
            Err(error) => {
                eprintln!("waiting for first valid capacity observation: {error}");
                tokio::select! { _ = tokio::time::sleep(Duration::from_secs(c.report_interval_seconds)) => (), _ = shutdown() => return Ok(()) }
            }
        }
    };
    manager.update_capacity(o.capacity, valid)?;
    manager.update_devices(o.devices.clone(), valid)?;
    let mut embedded = if c.proxy_mode == adx_node_manager::proxy::ProxyMode::Embedded {
        data_plane_gateway::common::resource::raise_nofile_soft_limit_from_env()?;
        Some(
            adx_node_manager::proxy::EmbeddedProxy::start(
                data_plane_gateway::config::NodeProxyConfig::from_env()?,
                &c.proxy_socket,
            )
            .await?,
        )
    } else {
        None
    };
    let registration = pb::RegisterNodeRequest {
        node_id: c.node_id.clone(),
        node_address: c.advertised_address,
        proxy_address: c.proxy_address,
        capacity: Some(o.capacity.into()),
        devices: o.devices.into_iter().map(Into::into).collect(),
        labels: c.labels,
        accepting_allocations: false,
        session_id: session_id.clone(),
        heartbeat_sequence: 0,
        reconciling: true,
    };
    // Bind before registration so dispatch cannot race an unbound address.
    let server = tonic::transport::Server::builder()
        .tls_config(server_tls)?
        .add_service(pb::node_service_server::NodeServiceServer::new(
            NodeRpc::new(manager.clone(), peers, session_id.clone()),
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::pin!(server);
    let admin = async {
        if let Some(path) = &c.admin_socket {
            adx_node_manager::admin::serve(manager.clone(), path).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    let report = async {
        let mut last = registration.clone();
        let mut valid_until = tokio::time::Instant::now() + valid;
        let mut interval = tokio::time::interval(Duration::from_secs(c.report_interval_seconds));
        let mut reconciled = false;
        let mut gc_jobs = tokio::task::JoinSet::new();
        let gc_interval = Duration::from_secs(c.checkpoint_gc.interval_seconds);
        let mut next_gc = tokio::time::Instant::now();
        loop {
            interval.tick().await;
            while let Some(result) = gc_jobs.try_join_next() {
                match result {
                    Ok(Ok(Ok(count))) => eprintln!("remote orphan GC completed: removed={count}"),
                    other => eprintln!("remote orphan GC incomplete: {other:?}"),
                }
            }
            if let Some(discovery) = &discovery {
                match discovery.lookup().await {
                    Ok(found) if found.address != endpoint => {
                        let channel = channel_for(found.address.clone())?;
                        sink.reconnect(channel.clone());
                        master = pb::master_service_client::MasterServiceClient::new(channel);
                        endpoint = found.address;
                        reconciled = false;
                    }
                    // Existing channel may remain useful while Redis is unavailable.
                    _ => (),
                }
            }
            if reconciled && manager.sync_proxy().await.is_err() {
                reconciled = false;
            }
            let mut r = last.clone();
            if let Ok((o, valid)) = sample(&source, &runtime, timeout).await {
                valid_until = tokio::time::Instant::now() + valid;
                manager.update_capacity(o.capacity, valid)?;
                manager.update_devices(o.devices.clone(), valid)?;
                r.capacity = Some(o.capacity.into());
                r.devices = o.devices.into_iter().map(Into::into).collect();
            }
            if let (Some(config), Some(gate)) = (&c.pressure, &mut pressure_gate) {
                let path = config.disk_path.clone();
                let accepting = match tokio::task::spawn_blocking(move || {
                    adx_node_manager::resources::pressure(&path)
                })
                .await
                {
                    Ok(Ok(pressure)) => gate.update(pressure),
                    _ => false,
                };
                manager.set_pressure(!accepting);
            }
            r.heartbeat_sequence = r
                .heartbeat_sequence
                .checked_add(1)
                .ok_or("heartbeat sequence exhausted")?;
            r.reconciling = !reconciled;
            r.accepting_allocations = !manager.is_draining()
                && manager.accepting_allocations()
                && reconciled
                && tokio::time::Instant::now() < valid_until;
            if !reconciled {
                manager.pause_lifecycle().await;
            }
            last = r.clone();
            match master.register_node(r).await {
                Ok(_) if !reconciled => {
                    let catalog = master
                        .inspect_node(pb::InspectNodeRequest {
                            node_id: c.node_id.clone(),
                            session_id: session_id.clone(),
                        })
                        .await;
                    match catalog {
                        Ok(snapshot) => {
                            let mut catalog = snapshot.into_inner();
                            if let Some(journal) = &journal {
                                let records = catalog
                                    .records
                                    .iter()
                                    .cloned()
                                    .map(TryInto::try_into)
                                    .collect::<adx_core::Result<Vec<_>>>()?;
                                if let Err(error) = journal.recover(&records).await {
                                    eprintln!("degradation journal recovery incomplete: {error}");
                                    continue;
                                }
                                // Replay may change executions and checkpoint ownership. Fetch
                                // both Instance and snapshot records from the same fresh catalog.
                                catalog = match master
                                    .inspect_node(pb::InspectNodeRequest {
                                        node_id: c.node_id.clone(),
                                        session_id: session_id.clone(),
                                    })
                                    .await
                                {
                                    Ok(snapshot) => snapshot.into_inner(),
                                    Err(_) => continue,
                                };
                            }
                            let records = catalog
                                .records
                                .into_iter()
                                .map(TryInto::try_into)
                                .collect::<adx_core::Result<Vec<_>>>()?;
                            let snapshots = catalog
                                .snapshots
                                .into_iter()
                                .map(TryInto::try_into)
                                .collect::<adx_core::Result<Vec<_>>>()?;
                            let retained = catalog
                                .retained_checkpoints
                                .into_iter()
                                .map(|cp| {
                                    adx_core::RestorePoint::try_from(cp).map(|cp| cp.artifact)
                                })
                                .collect::<adx_core::Result<Vec<_>>>()?;
                            let recovery = manager.reconcile_retained(records, snapshots, retained);
                            tokio::pin!(recovery);
                            // Large catalogs must not expire a healthy node while
                            // the serial controllers restore bindings and commit results.
                            loop {
                                tokio::select! {
                                    result = &mut recovery => {
                                        match result {
                                            Ok(()) => reconciled = true,
                                            Err(error) => eprintln!("node reconciliation incomplete: {error}"),
                                        }
                                        break;
                                    }
                                    _ = interval.tick() => {
                                        last.heartbeat_sequence = last.heartbeat_sequence.checked_add(1).ok_or("heartbeat sequence exhausted")?;
                                        if master.register_node(last.clone()).await.is_err() {
                                            // Cancellation leaves the gate closed. Accepted controller
                                            // work completes independently and retries on the next catalog.
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => eprintln!(
                            "authoritative node catalog unavailable; lifecycle remains closed"
                        ),
                    }
                }
                Ok(_) => {
                    if let Some(journal) = &journal {
                        if let Err(error) = journal.flush().await {
                            if matches!(
                                error,
                                adx_core::Error::Conflict | adx_core::Error::NotFound
                            ) {
                                reconciled = false;
                                manager.pause_lifecycle().await;
                            }
                            eprintln!("degradation journal publication incomplete: {error}");
                            continue;
                        }
                    }
                    if c.checkpoint_gc.enabled
                        && gc_jobs.is_empty()
                        && tokio::time::Instant::now() >= next_gc
                    {
                        next_gc = tokio::time::Instant::now() + gc_interval;
                        let manager = manager.clone();
                        gc_jobs.spawn(async move {
                            tokio::time::timeout(timeout, manager.collect_remote_orphans()).await
                        });
                    }
                }
                Err(status) => {
                    if status.code() == tonic::Code::FailedPrecondition {
                        reconciled = false;
                    }
                    eprintln!("node heartbeat unavailable; waiting to register or reconcile");
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let metrics = async {
        if let Some(bind) = c.metrics_listen {
            adx_node_manager::metrics::serve(manager.clone(), bind).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), std::io::Error>(())
    };
    let monitoring = async {
        let mut interval = tokio::time::interval(Duration::from_secs(c.report_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = manager.monitor_instances().await {
                eprintln!("instance observation incomplete: {error}");
            }
        }
    };
    let expiry = async {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(error) = manager.expire_checkpoints().await {
                eprintln!("checkpoint expiry incomplete: {error}");
            }
        }
    };
    eprintln!("adx-node-manager RPC listener ready");
    let proxy_failure = async {
        match &mut embedded {
            Some(proxy) => proxy.failed().await,
            None => std::future::pending::<adx_core::Result<()>>().await,
        }
    };
    tokio::select! {r=proxy_failure=>r?,r=&mut server=>r?,r=report=>r?,r=metrics=>r?,_=monitoring=>{},_=expiry=>{},r=admin=>r.map_err(|e| -> Box<dyn std::error::Error> { e })?,_=shutdown()=>{}}
    if let Some(proxy) = embedded {
        proxy.shutdown().await?;
    }
    Ok(())
}
