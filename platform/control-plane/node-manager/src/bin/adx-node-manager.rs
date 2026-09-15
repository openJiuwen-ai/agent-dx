use adx_core::{scheduling::Device, Resources};
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
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_id: String,
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
    capacity_file: PathBuf,
    report_interval_seconds: u64,
    rpc_timeout_seconds: u64,
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
struct Observation {
    capacity: Resources,
    devices: Vec<Device>,
    valid_until_unix_seconds: u64,
}
fn observation(
    path: &std::path::Path,
) -> Result<(Observation, Duration), Box<dyn std::error::Error>> {
    let o: Observation = serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|_| "invalid capacity observation")?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let valid = o
        .valid_until_unix_seconds
        .checked_sub(now)
        .filter(|v| *v > 0)
        .ok_or("capacity observation expired")?;
    o.capacity.validate()?;
    Ok((o, Duration::from_secs(valid)))
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let c: Config = read_config()?;
    if c.node_id.is_empty() || c.report_interval_seconds == 0 || c.rpc_timeout_seconds == 0 {
        return Err("node identity and positive intervals required".into());
    }
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
    let sink =
        Arc::new(MasterStateSink::new(channel.clone(), timeout)?.with_session(session_id.clone()));
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
    let mut readiness = RrtReadiness::new(
        runtime.clone(),
        c.rrt_port,
        Duration::from_millis(100),
        timeout,
        timeout,
    )?;
    if let Some(token) = token {
        readiness = readiness.with_token(&token)?;
    }
    let manager = Arc::new(
        NodeManager::new(
            c.node_id.clone(),
            runtime,
            Arc::new(readiness),
            Arc::new(UdsRoutes::new(c.proxy_socket, timeout)?),
            sink.clone(),
        )
        .with_operation_timeout(timeout)?,
    );
    manager.pause_lifecycle().await;
    let (o, valid) = observation(&c.capacity_file)?;
    manager.update_capacity(o.capacity, valid)?;
    manager.update_devices(o.devices.clone(), valid)?;
    let registration = pb::RegisterNodeRequest {
        node_id: c.node_id.clone(),
        node_address: c.advertised_address,
        proxy_address: c.proxy_address,
        capacity: Some(o.capacity.into()),
        devices: o.devices.into_iter().map(Into::into).collect(),
        labels: Default::default(),
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
        loop {
            interval.tick().await;
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
            if let Ok((o, valid)) = observation(&c.capacity_file) {
                valid_until = tokio::time::Instant::now() + valid;
                manager.update_capacity(o.capacity, valid)?;
                manager.update_devices(o.devices.clone(), valid)?;
                r.capacity = Some(o.capacity.into());
                r.devices = o.devices.into_iter().map(Into::into).collect();
            }
            r.heartbeat_sequence = r
                .heartbeat_sequence
                .checked_add(1)
                .ok_or("heartbeat sequence exhausted")?;
            r.reconciling = !reconciled;
            r.accepting_allocations =
                !manager.is_draining() && reconciled && tokio::time::Instant::now() < valid_until;
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
                            let records = snapshot
                                .into_inner()
                                .records
                                .into_iter()
                                .map(TryInto::try_into)
                                .collect::<adx_core::Result<Vec<_>>>()?;
                            let recovery = manager.reconcile(records);
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
                Ok(_) => (),
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
    eprintln!("adx-node-manager RPC listener ready");
    tokio::select! {r=&mut server=>r?,r=report=>r?,r=admin=>r.map_err(|e| -> Box<dyn std::error::Error> { e })?,_=shutdown()=>{}}
    Ok(())
}
