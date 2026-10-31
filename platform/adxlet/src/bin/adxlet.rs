use adx_process::{read_config, shutdown};
use adx_protocol::control as pb;
use adx_transport::rpc::{RpcChannel, RpcClient};
use adx_transport::tls::TlsFiles;
use adxlet::resources::{PressureGate, PressurePolicy, ResourceSource};
use adxlet::{
    jiuwenbox::Jiuwenbox,
    readiness::ExecdReadiness,
    routes::UdsRoutes,
    rpc::{CoordinatorStateSink, NodeRpc},
    runtime_logs::{RuntimeLogPolicy, RuntimeLogs},
    runtime_router::RuntimeRouter,
    sandboxd::{connect_when_ready, Config as RuntimeConfig},
    Adxlet,
};
use serde::Deserialize;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_id: String,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    storage_mounts: std::collections::BTreeMap<String, adx_core::volumes::StorageMount>,
    listen: SocketAddr,
    advertised_address: String,
    proxy_address: String,
    #[serde(default)]
    coordinator_address: Option<String>,
    #[serde(default)]
    discovery: Option<Discovery>,
    tls: TlsFiles,
    #[serde(default)]
    admin_socket: Option<PathBuf>,
    #[serde(default)]
    sandboxd_socket: Option<PathBuf>,
    #[serde(default)]
    jiuwenbox: Option<adxlet::jiuwenbox::Config>,
    #[serde(default)]
    runtime_logs: RuntimeLogPolicy,
    proxy_socket: PathBuf,
    #[serde(default)]
    proxy_mode: adxlet::proxy::ProxyMode,
    #[serde(default)]
    capacity_file: Option<PathBuf>,
    #[serde(default)]
    resource_source: Option<ResourceSource>,
    #[serde(default)]
    pressure: Option<PressureConfig>,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    execd_health_failure_threshold: Option<u32>,
    #[serde(default)]
    checkpoint_dir: Option<PathBuf>,
    #[serde(default)]
    checkpoint_storage: Option<adxlet::checkpoint::StorageConfig>,
    #[serde(default)]
    degradation_journal: Option<PathBuf>,
    report_interval_seconds: u64,
    #[serde(default = "metrics_interval")]
    metrics_interval_seconds: u64,
    rpc_timeout_seconds: u64,
    #[serde(default = "runtime_ready_timeout")]
    runtime_ready_timeout_seconds: u64,
    execd_port: u16,
    #[serde(default)]
    runtime_control: Option<adxlet::control_stream::StreamConfig>,
    #[serde(default)]
    runtime_profile: Option<adx_core::runtime_profile::RuntimeProfile>,
    #[serde(default)]
    execd_command: Vec<String>,
    #[serde(default)]
    execd_env: HashMap<String, String>,
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
fn metrics_interval() -> u64 {
    15
}
fn runtime_ready_timeout() -> u64 {
    120
}
fn execd_ready_poll_interval() -> Duration {
    Duration::from_millis(10)
}
async fn sample(
    source: &ResourceSource,
    runtime: &RuntimeRouter,
    timeout: Duration,
) -> adx_core::Result<(adxlet::resources::Observation, Duration)> {
    if matches!(source, ResourceSource::Sandboxd { .. }) {
        tokio::time::timeout(timeout, runtime.check_health())
            .await
            .map_err(|_| {
                adx_core::Error::Unavailable("sandboxd health check timed out".into())
            })??;
    }
    source.sample(timeout).await
}

fn coordinator_channel(
    transport: &RpcClient,
    address: &str,
    timeout: Duration,
) -> Result<RpcChannel, std::io::Error> {
    // ForwardCreate can wait for new capacity beyond the ordinary RPC budget.
    // Response deadlines belong to each call; keep connection setup bounded.
    Ok(transport.wrap(
        transport
            .endpoint(address)?
            .connect_timeout(timeout)
            .connect_lazy(),
    ))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = read_config()?;
    if config.sandboxd_socket.is_none() && config.jiuwenbox.is_none() {
        return Err("configure sandboxd_socket and/or jiuwenbox".into());
    }
    if config.runtime_control.is_none() {
        return Err("runtime_control is required for event-driven lifecycle observations".into());
    }
    for mount in config.storage_mounts.values() {
        mount.validate()?;
    }
    let runtime_logs = RuntimeLogs::new(config.runtime_logs.clone())?;
    // Embedded Proxy and Adxlet share this process-wide subscriber and
    // tracer provider; standalone Proxy initializes the same library itself.
    let _logging_guard = adx_observability::logging::init("adxlet", false)?;
    if config.node_id.is_empty()
        || config.report_interval_seconds == 0
        || config.metrics_interval_seconds == 0
        || config.rpc_timeout_seconds == 0
        || config.runtime_ready_timeout_seconds == 0
    {
        return Err("node identity and positive intervals required".into());
    }
    let source = match (config.capacity_file.clone(), config.resource_source.clone()) {
        (Some(path), None) => ResourceSource::File { path },
        (None, Some(source)) => source,
        _ => return Err("configure exactly one of capacity_file and resource_source".into()),
    };
    let mut pressure_gate = config
        .pressure
        .as_ref()
        .map(|p| PressureGate::new(p.thresholds.clone()))
        .transpose()?;
    let (server_tls, client_tls, peers) = config
        .tls
        .load_rpc(adx_protocol::auth::Principal::Node(config.node_id.clone()))?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let discovery = match (&config.coordinator_address, &config.discovery) {
        (Some(_), None) => None,
        (None, Some(d)) => Some(adx_discovery::RedisDiscovery::new(
            &d.redis_url,
            &d.namespace,
            timeout,
        )?),
        _ => return Err("configure exactly one of coordinator_address and discovery".into()),
    };
    let mut endpoint = loop {
        if let Some(address) = &config.coordinator_address {
            break address.clone();
        }
        match discovery
            .as_ref()
            .expect("missing Coordinator address implies configured discovery")
            .lookup()
            .await
        {
            Ok(found) => break found.address,
            Err(_) => {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.report_interval_seconds)) => {}
                    _ = shutdown() => return Ok(()),
                }
            }
        }
    };
    let channel = coordinator_channel(&client_tls, &endpoint, timeout)?;
    let session_id = uuid::Uuid::new_v4().to_string();
    // Leave part of the operation deadline for a durable fallback after RPC timeout.
    let commit_timeout = if config.degradation_journal.is_some() {
        timeout / 2
    } else {
        timeout
    };
    let sink = Arc::new(
        CoordinatorStateSink::with_rpc_channel(channel.clone(), commit_timeout)?
            .with_session(session_id.clone()),
    );
    let journal = config
        .degradation_journal
        .map(|path| Arc::new(adxlet::journal::JournalSink::new(path, sink.clone())));
    let state_sink: Arc<dyn adxlet::StateSink> = match &journal {
        Some(journal) => journal.clone(),
        None => sink.clone(),
    };
    let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
    let mut env = config.execd_env;
    if config.checkpoint_dir.is_some() || config.checkpoint_storage.is_some() {
        env.entry("ADX_EXECD_CONTROL_SOCKET_PATH".into())
            .or_insert_with(|| "/run/adx".into());
    } else if env
        .get("ADX_EXECD_CONTROL_SOCKET_PATH")
        .is_some_and(|v| !v.is_empty())
    {
        return Err("checkpoint socket requires checkpoint storage".into());
    }
    env.insert("EXECD_HTTP_PORT".into(), config.execd_port.to_string());
    let token = env.get("EXECD_HTTP_TOKEN").cloned();
    let stream_setup = config
        .runtime_control
        .as_ref()
        .map(|config| -> adx_core::Result<_> {
            let secret = config.load_key()?;
            env.insert(
                "ADX_RUNTIME_CONTROL_ADDRESS".into(),
                config.advertised_address.clone(),
            );
            // Host-only input to the Start adapter; removed before runtime injection.
            env.insert(
                "ADX_RUNTIME_CONTROL_SECRET".into(),
                String::from_utf8(secret.clone()).map_err(|_| {
                    adx_core::Error::Invalid("runtime control key must be UTF-8".into())
                })?,
            );
            Ok((
                config.listen,
                adxlet::control_stream::RuntimeControlHub::new(secret, timeout)?,
            ))
        })
        .transpose()?;
    let runtime_config = RuntimeConfig {
        runtime_profile: config.runtime_profile,
        command: config.execd_command,
        env,
        cwd: "/".into(),
        rpc_timeout: timeout,
        runtime_logs: Some(config.runtime_logs.clone()),
    };
    let jiuwenbox = match config.jiuwenbox {
        Some(backend) => Some(Jiuwenbox::connect(backend, runtime_config.clone()).await?),
        None => None,
    };
    let sandboxd = match config.sandboxd_socket {
        Some(socket) => Some(tokio::select! {
            result = connect_when_ready(socket, runtime_config,
                Duration::from_secs(config.runtime_ready_timeout_seconds),
                Duration::from_secs(config.report_interval_seconds)) => result?,
            _ = shutdown() => return Ok(()),
        }),
        None => None,
    };
    let runtime = Arc::new(RuntimeRouter::new(sandboxd, jiuwenbox)?);
    // The sandboxd network plugin may create the configured bridge during
    // initialization. Bind only after backend readiness, before admitting work.
    let stream_listener = if let Some((listen, _)) = &stream_setup {
        Some(tokio::net::TcpListener::bind(listen).await?)
    } else {
        None
    };
    let mut control =
        adxlet::runtime_control::RuntimeControlClient::new(config.execd_port, timeout)?;
    if let Some(token) = &token {
        control = control.with_token(token)?;
    }
    if let Some((_, hub)) = &stream_setup {
        control = control.with_stream(hub.clone());
    }
    let readiness = ExecdReadiness::with_client(
        runtime.clone(),
        control.clone(),
        execd_ready_poll_interval(),
        timeout,
        timeout,
    )?;
    let mut manager = Adxlet::new(
        config.node_id.clone(),
        runtime.clone(),
        Arc::new(readiness),
        Arc::new(UdsRoutes::new(config.proxy_socket.clone(), timeout)?),
        state_sink,
    )
    .with_runtime_control(control.clone())?
    .with_operation_timeout(timeout)?
    .with_health_check(config.execd_health_failure_threshold)?;
    let checkpoint_storage = match (config.checkpoint_dir, config.checkpoint_storage) {
        (Some(root), None) => Some(adxlet::checkpoint::StorageConfig::Local { root }),
        (None, storage) => storage,
        (Some(_), Some(_)) => {
            return Err("configure only one of checkpoint_dir and checkpoint_storage".into())
        }
    };
    let checkpoint_scopes = checkpoint_storage
        .as_ref()
        .and_then(adxlet::checkpoint::StorageConfig::shared_scope)
        .into_iter()
        .collect();
    if let Some(storage) = checkpoint_storage {
        manager = manager
            .with_checkpointing(
                storage.build_for_node(config.node_id.clone(), session_id.clone())?,
                Arc::new(control.clone()),
            )?
            .with_snapshot_catalog(sink.clone())?;
    }
    let manager = Arc::new(manager);
    manager.pause_lifecycle().await;
    let (o, valid) = loop {
        match sample(&source, &runtime, timeout).await {
            Ok(sample) => break sample,
            Err(error) => {
                adx_observability::warn!("waiting for first valid capacity observation: {error}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.report_interval_seconds)) => {}
                    _ = shutdown() => return Ok(()),
                }
            }
        }
    };
    manager.update_capacity(o.capacity, valid)?;
    manager.update_devices(o.devices.clone(), valid)?;
    let mut embedded = if config.proxy_mode == adxlet::proxy::ProxyMode::Embedded {
        adx_process::resource::raise_nofile_soft_limit_from_env()?;
        Some(
            adxlet::proxy::EmbeddedProxy::start(
                data_plane_gateway::config::RelayConfig::from_env()?,
                &config.proxy_socket,
            )
            .await?,
        )
    } else {
        None
    };
    let mut labels = config.labels;
    if let Some(alias) = manager.checkpoint_storage_alias() {
        labels.insert("ADX_CHECKPOINT_STORAGE".into(), alias.into());
    }
    let registration = pb::RegisterNodeRequest {
        storage_mounts: Default::default(),
        node_id: config.node_id.clone(),
        node_address: config.advertised_address,
        proxy_address: config.proxy_address,
        capacity: Some(o.capacity.into()),
        devices: o.devices.into_iter().map(Into::into).collect(),
        labels,
        accepting_allocations: false,
        session_id: session_id.clone(),
        heartbeat_sequence: 0,
        reconciling: true,
        runtime_classes: vec![],
        checkpoint_scopes,
    };
    // Bind before registration so dispatch cannot race an unbound address.
    let node_rpc = NodeRpc::new(manager.clone(), peers, session_id.clone())
        .with_local_creation((*sink).clone());
    let claim_retries = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.report_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            node_rpc.retry_local_claims().await;
        }
    };
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = server_tls {
        builder = builder.tls_config(tls)?;
    }
    let server = builder
        .add_service(pb::node_service_server::NodeServiceServer::new(
            node_rpc.clone(),
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::pin!(server);
    let admin = async {
        if let Some(path) = &config.admin_socket {
            adxlet::admin::serve(manager.clone(), path).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    let report = async {
        let mut last = registration.clone();
        let mut valid_until = tokio::time::Instant::now() + valid;
        // Resource/backend observation is not on the heartbeat critical path.
        // Bound each sample to one reporting period and keep the last successful
        // capacity during outages. Heartbeats continue independently.
        let sample_timeout = timeout.min(Duration::from_secs(config.report_interval_seconds));
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.report_interval_seconds));
        loop {
            interval.tick().await;
            if let Some(discovery) = &discovery {
                match discovery.lookup().await {
                    Ok(found) if found.address != endpoint => {
                        let channel = coordinator_channel(&client_tls, &found.address, timeout)?;
                        sink.reconnect(channel.clone())?;
                        coordinator =
                            pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
                        endpoint = found.address;
                    }
                    // Existing channel may remain useful while Redis is unavailable.
                    _ => (),
                }
            }
            let mut r = last.clone();
            // Observe storage and runtime capabilities concurrently so storage
            // does not add another timeout period to the heartbeat path.
            let (mounts, classes) = tokio::join!(
                tokio::time::timeout(
                    sample_timeout,
                    adxlet::managed_mounts::mounted_storage(config.storage_mounts.clone()),
                ),
                tokio::time::timeout(sample_timeout, runtime.available_runtime_classes()),
            );
            // A failed storage observation withdraws only volume eligibility.
            r.storage_mounts = match mounts {
                Ok(Ok(mounts)) => mounts
                    .into_iter()
                    .map(|(id, mount)| (id, mount.into()))
                    .collect(),
                _ => Default::default(),
            };
            // A failed runtime observation closes new admission, while accepted
            // executions and heartbeats continue through reconciliation.
            r.runtime_classes = match classes {
                Ok(Ok(classes)) => classes,
                _ => vec![],
            };
            manager.update_runtime_classes(r.runtime_classes.clone())?;
            if let Ok((o, valid)) = sample(&source, &runtime, sample_timeout).await {
                valid_until = tokio::time::Instant::now() + valid;
                manager.update_capacity(o.capacity, valid)?;
                manager.update_devices(o.devices.clone(), valid)?;
                r.capacity = Some(o.capacity.into());
                r.devices = o.devices.into_iter().map(Into::into).collect();
            }
            if let (Some(config), Some(gate)) = (&config.pressure, &mut pressure_gate) {
                let path = config.disk_path.clone();
                let accepting =
                    match tokio::task::spawn_blocking(move || adxlet::resources::pressure(&path))
                        .await
                    {
                        Ok(Ok(pressure)) => gate.update(pressure),
                        _ => false,
                    };
                manager.set_pressure(!accepting);
            }
            last = r;
            // Startup and every reporting period use the same complete authority
            // cycle for every configured runtime. The cycle publishes readiness
            // immediately after success, before waiting for the next report tick.
            match adxlet::node_reconciliation::reconcile_node(
                &mut coordinator,
                &mut last,
                &manager,
                journal.as_deref(),
                timeout,
                Duration::from_secs(config.report_interval_seconds),
                valid_until,
            )
            .await
            {
                Ok(()) => (),
                Err(error) => {
                    adx_observability::warn!(accepting_allocations=manager.accepting_allocations(), %error, "authoritative node reconciliation incomplete")
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let metrics = async {
        if let Some(bind) = config.metrics_listen {
            adxlet::metrics::serve(manager.clone(), bind).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), std::io::Error>(())
    };
    let monitoring = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.metrics_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = manager.sample_metrics().await {
                adx_observability::warn!("environment observation incomplete: {error}");
            }
        }
    };
    let log_gc = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(runtime_logs.interval_seconds()));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(active) = manager.retained_runtime_ids() else {
                continue;
            };
            let logs = runtime_logs.clone();
            match tokio::task::spawn_blocking(move || {
                logs.collect(&active, std::time::SystemTime::now())
            })
            .await
            {
                Ok(Ok(_)) => (),
                Ok(Err(error)) => adx_observability::warn!("runtime log GC failed: {error}"),
                Err(error) => adx_observability::warn!("runtime log GC task failed: {error}"),
            }
        }
    };
    adx_observability::info!("adxlet RPC listener ready");
    let control_server = async {
        if let (Some(listener), Some((_, hub))) = (stream_listener, &stream_setup) {
            hub.clone().serve(listener).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), tonic::transport::Error>(())
    };
    let proxy_failure = async {
        match &mut embedded {
            Some(proxy) => proxy.failed().await,
            None => std::future::pending::<adx_core::Result<()>>().await,
        }
    };
    tokio::select! {
        result = proxy_failure => result?,
        result = control_server => result?,
        result = &mut server => result?,
        result = report => result?,
        result = metrics => result?,
        _ = claim_retries => {}
        _ = monitoring => {}
        _ = log_gc => {}
        result = admin => result.map_err(|error| -> Box<dyn std::error::Error> { error })?,
        _ = shutdown() => {}
    }
    manager.set_maintenance(true);
    if let Err(error) = adxlet::node_reconciliation::unregister_node(
        &mut coordinator,
        &registration,
        timeout.min(Duration::from_secs(5)),
    )
    .await
    {
        adx_observability::warn!(%error, "planned node departure unconfirmed; heartbeat expiry remains active");
    }
    if let Some(proxy) = embedded {
        proxy.shutdown().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A single-method service probes the production Coordinator channel without
    // requiring Redis or reproducing Coordinator scheduling in this fixture.
    struct DelayedResponse;

    #[tonic::async_trait]
    impl pb::auth_service_server::AuthService for DelayedResponse {
        async fn verify_api_key(
            &self,
            _: tonic::Request<pb::VerifyApiKeyRequest>,
        ) -> Result<tonic::Response<pb::VerifyApiKeyResponse>, tonic::Status> {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(tonic::Response::new(pb::VerifyApiKeyResponse::default()))
        }
    }

    #[tokio::test]
    async fn coordinator_channel_honors_long_and_short_request_deadlines() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(pb::auth_service_server::AuthServiceServer::new(
                    DelayedResponse,
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let transport = RpcClient::network(adx_protocol::auth::Principal::Node("n".into()));
        let channel =
            coordinator_channel(&transport, &address.to_string(), Duration::from_millis(100))
                .unwrap();
        let mut client = pb::auth_service_client::AuthServiceClient::new(channel);

        // ForwardCreate already supplies its full remaining create budget.
        // The channel must allow a response later than the ordinary RPC cap.
        let mut long = tonic::Request::new(pb::VerifyApiKeyRequest::default());
        long.set_timeout(Duration::from_secs(2));
        let long_result = client.verify_api_key(long).await;

        // Removing the channel cap must still respect each short RPC deadline.
        let mut short = tonic::Request::new(pb::VerifyApiKeyRequest::default());
        short.set_timeout(Duration::from_millis(50));
        let short_result = client.verify_api_key(short).await;
        let _ = stop.send(());
        server.await.unwrap();

        assert!(
            long_result.is_ok(),
            "long request was cut short: {long_result:?}"
        );
        let error = short_result.unwrap_err();
        assert!(
            matches!(
                error.code(),
                tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
            ),
            "{error}"
        );
    }

    #[test]
    fn jiuwenbox_example_configures_a_node_without_sandboxd() {
        let config: Config = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../build/config/examples/node-jiuwenbox.json"
        )))
        .unwrap();
        assert!(config.sandboxd_socket.is_none());
        config.jiuwenbox.as_ref().unwrap().validate().unwrap();
        assert!(matches!(
            config.resource_source,
            Some(ResourceSource::Auto { .. })
        ));
        assert!(config.checkpoint_dir.is_none());
    }

    #[test]
    fn execd_readiness_poll_keeps_startup_jitter_below_twenty_milliseconds() {
        assert!(execd_ready_poll_interval() <= Duration::from_millis(20));
    }
}
