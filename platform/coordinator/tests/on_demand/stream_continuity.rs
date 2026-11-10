use super::*;
use adx_core::scheduling::{LabelRequirement, LabelSelector, SelectorOp};
use data_plane_gateway::{
    common::protocol::GatewayPolicy,
    ingress::{
        coordinator_routes::CoordinatorConnection, server::IngressStream, AccessKind,
        DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator, IngressRouteResolver,
        RouteStore,
    },
    node::Relay,
};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{broadcast::error::TryRecvError, watch},
    task::JoinSet,
    time::{interval_at, sleep, timeout, Instant},
};

const KEY: &str = "valid-isolated-stream-probe-test-key-32-bytes";
const CONTROLS: [(&str, &str); 2] = [("stay-on-a", "a"), ("stay-on-b", "b")];

async fn attach_relay(coordinator: &str, worker: &mut Worker, tasks: &mut JoinSet<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    worker.report.proxy_address = listener.local_addr().unwrap().to_string();
    *worker.backend.relay.lock().unwrap() = Some(relay.clone());
    let capacity = Resources {
        cpu_millis: 400,
        memory_bytes: 512,
        disk_bytes: 0,
    };
    worker
        .manager
        .update_capacity(capacity, Duration::from_secs(60))
        .unwrap();
    worker.report.capacity = Some(capacity.into());
    worker.report.heartbeat_sequence += 1;
    register(coordinator, worker).await;
    tasks.spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let relay = relay.clone();
            connections.spawn(async move { relay.serve_h2(socket).await });
        }
    });
}

async fn create_control(rig: &Rig, id: &str, node: &str) -> adx_core::Assignment {
    let mut specification = spec();
    specification.id = id.into();
    specification.scheduling.required_node.push(LabelSelector {
        expressions: vec![LabelRequirement {
            key: "NODE_ID".into(),
            op: SelectorOp::In,
            values: vec![node.into()],
        }],
        ..Default::default()
    });
    let record = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::ApiServer).await,
    )
    .create_environment(pb::CreateEnvironmentRequest {
        spec: Some(specification.into()),
        caller: caller(),
        schedule_timeout_seconds: 3,
        create_timeout_seconds: 5,
        ..Default::default()
    })
    .await
    .unwrap()
    .into_inner()
    .record
    .unwrap();
    let assignment: adx_core::Assignment = record.assignment.unwrap().try_into().unwrap();
    assert_eq!(assignment.node_id, node);
    assignment
}

async fn exchange(stream: &mut IngressStream, id: &str, sequence: u64) {
    let payload = sequence.to_be_bytes();
    timeout(Duration::from_secs(1), async {
        stream.write_all(&payload).await?;
        let mut received = [0; 8];
        stream.read_exact(&mut received).await?;
        assert_eq!(received, payload);
        std::io::Result::Ok(())
    })
    .await
    .unwrap_or_else(|error| panic!("original stream {id} timed out: {error}"))
    .unwrap_or_else(|error| panic!("original stream {id} was interrupted: {error}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated real Redis"]
async fn planned_migration_keeps_other_streams_alive() {
    migration_with_residual_execution(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated real Redis"]
async fn planned_migration_waits_for_stale_target_execution() {
    migration_with_residual_execution(true).await;
}

async fn migration_with_residual_execution(residual_on_target: bool) {
    let mut tasks = JoinSet::new();
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    attach_relay(&rig.address, &mut rig.source, &mut tasks).await;
    attach_relay(&rig.address, &mut rig.target, &mut tasks).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let residual = rig
        .source
        .backend
        .running
        .lock()
        .unwrap()
        .values()
        .find(|runtime| runtime.environment_id == "sleep")
        .unwrap()
        .clone();
    let mut assignments = Vec::new();
    for (id, node) in CONTROLS {
        assignments.push(create_control(&rig, id, node).await);
    }
    rig.session
        .bootstrap_credential(
            KEY,
            &adx_coordinator::auth::Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
                metadata: Default::default(),
            },
        )
        .await
        .unwrap();
    rig.session
        .advertise(
            "test",
            &format!("http://{}", rig.address),
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend.local_addr().unwrap().port();
    tasks.spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let (mut socket, _) = backend.accept().await.unwrap();
            connections.spawn(async move {
                let (mut read, mut write) = socket.split();
                tokio::io::copy(&mut read, &mut write).await
            });
        }
    });
    let routes = rig.routes.clone();
    tasks.spawn(async move { routes.run(Duration::from_millis(10)).await });
    let store = Arc::new(RouteStore::new());
    let connection = Arc::new(
        CoordinatorConnection::new(
            serde_json::from_value(json!({
                "redis_url":rig._redis.url, "namespace":"test", "tls":{"mode":"network"},
                "rpc_timeout_seconds":2, "refresh_seconds":1, "auth_cache_seconds":1,
                "auth_cache_entries":10
            }))
            .unwrap(),
        )
        .unwrap(),
    );
    let watcher = connection.clone();
    let watcher_store = store.clone();
    tasks.spawn(async move { watcher.run(watcher_store).await });
    let ingress = Arc::new(Ingress::new(
        Arc::new(IngressRouteResolver::new(store.clone()).stream_only()),
        DataPlaneL4Connector::new(H2PoolConfig {
            connections_per_node: 1,
            max_connections_per_node: 1,
            ..Default::default()
        }),
        IngressAuthenticator::with_verifier(connection),
        port,
        8765,
        "127.0.0.1:1",
        vec![],
    ));
    tasks.spawn(ingress.clone().run_route_reconciler(store.subscribe()));
    timeout(Duration::from_secs(3), async {
        while !store.ready() || CONTROLS.iter().any(|(id, _)| store.get(id).is_none()) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("both control routes must be present");
    let mut changes = store.subscribe();
    let (done, stop) = watch::channel(false);
    let mut traffic = JoinSet::new();
    for ((id, _), worker) in CONTROLS.into_iter().zip([&rig.source, &rig.target]) {
        assert_eq!(
            store.get(id).unwrap().relay_address,
            worker.report.proxy_address
        );
        let mut stream = timeout(
            Duration::from_secs(3),
            ingress.open_l4_stream(id, port, AccessKind::Ssh, KEY, id.into()),
        )
        .await
        .unwrap()
        .unwrap();
        exchange(&mut stream, id, 0).await;
        let mut stop = stop.clone();
        traffic.spawn(async move {
            let mut sequence = 1_u64;
            while !*stop.borrow_and_update() {
                exchange(&mut stream, id, sequence).await;
                sequence += 1;
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = sleep(Duration::from_millis(5)) => {}
                }
            }
            exchange(&mut stream, id, sequence).await;
            stream
        });
    }
    assert_eq!(ingress.active_sessions(), CONTROLS.len());

    let (address, server, migrate) = prepare_migration(&rig, running).await;
    let mut migration = tokio::spawn(post_upgrade(address, MIGRATE_PATH, migrate));
    let period = Duration::from_secs(60);
    let next_tick = Instant::now() + period;
    let mut source_tick = interval_at(next_tick, period);
    let mut target_tick = interval_at(next_tick, period);
    timeout(Duration::from_secs(3), async {
        tokio::join!(
            rig.source.manager.wait_for_reconciliation(&mut source_tick),
            rig.target.manager.wait_for_reconciliation(&mut target_tick),
        )
    })
    .await
    .expect("planned handoff wakes both nodes");
    // Keep both live comparisons in flight long enough for route deltas to be
    // consumed by Ingress if either node is incorrectly made unroutable.
    let hold = Duration::from_millis(200);
    for worker in [&rig.source, &rig.target] {
        worker
            .backend
            .inventory_hold_ms
            .store(200, Ordering::SeqCst);
    }
    let (first, second) = if residual_on_target {
        (&mut rig.target, &mut rig.source)
    } else {
        (&mut rig.source, &mut rig.target)
    };
    let comparison = Instant::now();
    report(first, &rig.address, period)
        .await
        .expect("first handoff uses the existing live node session");
    assert!(comparison.elapsed() >= hold);
    // Model an old backend appearing after comparison, before the peer is ready.
    let residual_id = residual.runtime_id.clone();
    let backend = first.backend.clone();
    backend
        .running
        .lock()
        .unwrap()
        .insert(residual_id.clone(), residual);
    let comparison = Instant::now();
    report(second, &rig.address, period)
        .await
        .expect("second handoff uses the existing live node session");
    assert!(comparison.elapsed() >= hold);
    let inspection = first.manager.inspect_upgrade().await.unwrap();
    assert_eq!(
        inspection.controllers,
        if residual_on_target { 2 } else { 1 }
    );
    assert_eq!(inspection.backends, 2, "the residual execution is visible");
    assert!(timeout(Duration::from_millis(650), &mut migration)
        .await
        .is_err());
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    backend.running.lock().unwrap().remove(&residual_id);
    let receipt = timeout(Duration::from_secs(3), migration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt["node_id"], "b");
    assert_eq!(receipt["state"], "Running");
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
    let restored = rig.session.get("sleep").await.unwrap();
    assert_eq!(
        Some(restored.assignment.generation),
        receipt["generation"].as_u64()
    );
    let runtime_id = restored.result.unwrap().runtime.id;
    timeout(Duration::from_secs(3), async {
        while !store.get("sleep").is_some_and(|route| {
            route.relay_address == rig.target.report.proxy_address
                && route.environment_id == runtime_id
        }) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the target runtime route must reach Ingress before checking original streams");

    done.send(true).unwrap();
    let mut original_streams = Vec::new();
    while let Some(result) = traffic.join_next().await {
        original_streams.push(result.unwrap());
    }
    assert_eq!(original_streams.len(), CONTROLS.len());
    assert_eq!(ingress.active_sessions(), CONTROLS.len());
    loop {
        let change = match changes.try_recv() {
            Ok(change) => change,
            Err(TryRecvError::Empty) => break,
            Err(error) => panic!("could not observe every route change: {error}"),
        };
        if CONTROLS.iter().any(|(id, _)| *id == change.instance_id) {
            assert!(
                change.current.is_some(),
                "control route {} was withdrawn",
                change.instance_id
            );
        }
    }
    for ((id, _), assignment) in CONTROLS.into_iter().zip(assignments) {
        let current = rig.session.get(id).await.unwrap();
        assert_eq!(current.assignment, assignment);
        assert_eq!(
            current.result.unwrap().state,
            adx_core::EnvironmentState::Running
        );
    }
    server.abort();
}
