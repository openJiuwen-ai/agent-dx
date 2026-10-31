use super::*;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn live_inspection_accepts_open_admission_but_requires_a_current_session() {
    let mut rig = Rig::new().await;
    let inspection = pb::InspectNodeRequest {
        node_id: "a".into(),
        session_id: "boot-a".into(),
    };
    // Rig's registration handshake has completed and allocation is open.
    rig.claimants[0]
        .inspect_node(inspection.clone())
        .await
        .unwrap();
    let mut stale = inspection.clone();
    stale.session_id = "old-boot".into();
    assert_eq!(
        rig.claimants[0]
            .inspect_node(stale)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        rig.claimants[1]
            .inspect_node(inspection.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    // Inspection cannot revive an expired session before the expiry worker runs.
    tokio::time::sleep(Duration::from_millis(10050)).await;
    assert_eq!(
        rig.claimants[0]
            .inspect_node(inspection)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn live_catalog_keeps_a_durable_claim_pending_until_the_create_rpc() {
    let mut rig = Rig::new().await;
    rig.managers[0].reconcile(vec![]).await.unwrap();
    let spec = spec("claim-before-create");
    rig.claimants[0]
        .claim_environment(pb::ClaimEnvironmentRequest {
            create_deadline_unix_millis: 0,
            request_id: String::new(),
            spec: Some(spec.clone().into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
        })
        .await
        .unwrap();
    assert!(rig.session.get(&spec.id).await.unwrap().result.is_none());
    let snapshot = rig.session.snapshot().await.unwrap();
    let node = &snapshot.nodes["a"];
    let mut registration = pb::RegisterNodeRequest {
        checkpoint_scopes: vec![],
        storage_mounts: Default::default(),
        node_id: "a".into(),
        session_id: "boot-a".into(),
        node_address: node.address.clone(),
        proxy_address: node.proxy_address.clone(),
        capacity: Some(node.node.capacity.into()),
        heartbeat_sequence: 3,
        reconciling: false,
        accepting_allocations: true,
        runtime_classes: vec!["r".into(), "runc".into(), "runsc".into()],
        labels: Default::default(),
        devices: vec![],
    };
    let mut authority = pb::coordinator_service_client::CoordinatorServiceClient::new(
        adx_transport::rpc::authenticated_channel(channel(rig.address, "node").await),
    );
    adxlet::node_reconciliation::reconcile_node(
        &mut authority,
        &mut registration,
        &rig.managers[0],
        None,
        Duration::from_secs(2),
        Duration::from_millis(10),
        tokio::time::Instant::now() + Duration::from_secs(60),
    )
    .await
    .unwrap();
    let reserved = rig.session.get(&spec.id).await.unwrap();
    assert!(
        reserved.result.is_none(),
        "live comparison turned a durable unstarted claim into a terminal result"
    );
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 0);
    let created = rig.nodes[0]
        .create_local_environment(Rig::request("claim-before-create", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(created.state, pb::EnvironmentState::Running as i32);
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    rig.delete(&created).await;
}

fn local_peers() -> Peers {
    Peers::new(
        [
            ("coordinator", Principal::Coordinator),
            ("apiserver", Principal::ApiServer),
            ("node", Principal::Node("a".into())),
            ("ingress", Principal::Node("b".into())),
            ("unknown", Principal::Node("c".into())),
        ]
        .map(|(cert, role)| (file(&format!("{cert}.der")), role)),
    )
}

async fn redis_command_calls(redis: &common::Redis, command: &str) -> u64 {
    let client = redis::Client::open(redis.url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let info: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut connection)
        .await
        .unwrap();
    let prefix = format!("cmdstat_{command}:calls=");
    info.lines()
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .and_then(|value| value.split(',').next())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

struct Rig {
    _redis: common::Redis,
    _servers: Servers,
    session: Session,
    rpc: CoordinatorRpc,
    coordinator: pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    nodes: Vec<pb::node_service_client::NodeServiceClient<Channel>>,
    coordinator_nodes: Vec<pb::node_service_client::NodeServiceClient<Channel>>,
    claimants: Vec<pb::coordinator_service_client::CoordinatorServiceClient<Channel>>,
    managers: Vec<Arc<Adxlet>>,
    backends: Vec<Arc<Backend>>,
    node_rpcs: Vec<NodeRpc>,
    sinks: Vec<Arc<CoordinatorStateSink>>,
    address: std::net::SocketAddr,
    registrations: Vec<pb::RegisterNodeRequest>,
}
impl Rig {
    async fn new() -> Self {
        Self::new_with_shards(1).await
    }
    async fn new_with_shards(shards: usize) -> Self {
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(shards).await.unwrap();
        let rpc = CoordinatorRpc::with_heartbeat_timeout(
            session.clone(),
            Placement::Pack,
            local_peers(),
            client_tls("coordinator"),
            Duration::from_secs(3),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ma = ml.local_addr().unwrap();
        let auth = adx_coordinator::auth::AuthRpc::new(session.clone(), local_peers());
        let publication =
            adx_coordinator::routes::RoutePublisher::new(session.clone(), local_peers());
        publication.refresh().await.unwrap();
        let updater = publication.clone();
        let service = rpc.clone();
        let servers = Servers(vec![
            tokio::spawn(updater.run(Duration::from_millis(20))),
            tokio::spawn(async move {
                Server::builder()
                    .tls_config(server_tls("coordinator"))
                    .unwrap()
                    .add_service(pb::volume_service_server::VolumeServiceServer::new(service.clone()))
                    .add_service(pb::volume_execution_service_server::VolumeExecutionServiceServer::new(service.clone()))
                    .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(service))
                    .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                    .add_service(
                        pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                            publication,
                        ),
                    )
                    .serve_with_incoming(TcpListenerStream::new(ml))
                    .await
                    .unwrap();
            }),
        ]);
        let mut rig = Self {
            _redis: redis,
            _servers: servers,
            session,
            rpc,
            coordinator: pb::coordinator_service_client::CoordinatorServiceClient::new(
                channel(ma, "apiserver").await,
            ),
            nodes: vec![],
            coordinator_nodes: vec![],
            claimants: vec![],
            managers: vec![],
            backends: vec![],
            node_rpcs: vec![],
            sinks: vec![],
            address: ma,
            registrations: vec![],
        };
        for (id, cert) in [("a", "node"), ("b", "ingress")] {
            rig.add_node(id, cert).await;
        }
        rig
    }
    async fn add_node(&mut self, id: &str, cert: &'static str) {
        let client = channel(self.address, cert).await;
        let sink = Arc::new(
            CoordinatorStateSink::new(client.clone(), Duration::from_secs(2))
                .unwrap()
                .with_session(format!("boot-{id}")),
        );
        let backend = Arc::new(Backend {
            pause_writes_after_remove: Mutex::new(None),
            session: self.session.clone(),
            started: AtomicUsize::new(0),
            removed: AtomicUsize::new(0),
            running: Mutex::default(),
        });
        let manager = Arc::new(Adxlet::new(
            id.into(),
            backend.clone(),
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            sink.clone(),
        ));
        manager
            .update_capacity(spec("x").resources, Duration::from_secs(60))
            .unwrap();
        let node = NodeRpc::new(manager.clone(), local_peers(), format!("boot-{id}"))
            .with_local_creation((*sink).clone());
        self.sinks.push(sink.clone());
        self.node_rpcs.push(node.clone());
        let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let na = nl.local_addr().unwrap();
        self._servers.0.push(tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls(cert))
                .unwrap()
                .add_service(pb::node_service_server::NodeServiceServer::new(node))
                .serve_with_incoming(TcpListenerStream::new(nl))
                .await
                .unwrap();
        }));
        let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(client);
        let mut register = pb::RegisterNodeRequest {
            checkpoint_scopes: vec![],
            storage_mounts: Default::default(),
            runtime_classes: vec![
                "runsc".into(),
                "runc".into(),
                "firecracker".into(),
                "r".into(),
                "test-runtime".into(),
            ],
            node_id: id.into(),
            node_address: na.to_string(),
            proxy_address: "127.0.0.1:9999".into(),
            capacity: Some(spec("x").resources.into()),
            accepting_allocations: false,
            labels: Default::default(),
            devices: vec![],
            session_id: format!("boot-{id}"),
            heartbeat_sequence: 1,
            reconciling: true,
        };
        coordinator.register_node(register.clone()).await.unwrap();
        coordinator
            .inspect_node(pb::InspectNodeRequest {
                node_id: id.into(),
                session_id: format!("boot-{id}"),
            })
            .await
            .unwrap();
        register.heartbeat_sequence = 2;
        register.reconciling = false;
        register.accepting_allocations = true;
        coordinator.register_node(register.clone()).await.unwrap();
        self.registrations.push(register);
        self.managers.push(manager);
        self.backends.push(backend);
        self.claimants.push(coordinator);
        self.nodes
            .push(pb::node_service_client::NodeServiceClient::new(
                channel(na, "apiserver").await,
            ));
        self.coordinator_nodes
            .push(pb::node_service_client::NodeServiceClient::new(
                channel(na, "coordinator").await,
            ));
    }
    async fn report_mounts(
        &mut self,
        index: usize,
        mounts: std::collections::BTreeMap<String, adx_core::volumes::StorageMount>,
    ) {
        let report = &mut self.registrations[index];
        report.heartbeat_sequence += 1;
        report.storage_mounts = mounts
            .into_iter()
            .map(|(id, mount)| (id, mount.into()))
            .collect();
        self.claimants[index]
            .register_node(report.clone())
            .await
            .unwrap();
    }
    fn request(id: &str, index: usize) -> pb::LocalEnvironmentCreateRequest {
        pb::LocalEnvironmentCreateRequest {
            persist_claim: true,
            create: Some(create(id)),
            node_session_id: format!("boot-{}", if index == 0 { "a" } else { "b" }),
        }
    }
    async fn delete(&mut self, record: &pb::EnvironmentRecord) {
        let owner = record.assignment.as_ref().unwrap();
        let index = self
            .registrations
            .iter()
            .position(|r| r.node_id == owner.node_id)
            .unwrap();
        self.nodes[index]
            .delete_environment(pb::DeleteEnvironmentRequest {
                expected_revision: None,

                failed_revision: None,
                node_session_id: String::new(),
                assignment: Some(owner.clone()),
                caller: caller(),
            })
            .await
            .unwrap();
    }
}

async fn volume_rig() -> (Rig, tempfile::TempDir, adx_core::volumes::Volume) {
    volume_rig_with_mode(adx_core::volumes::AccessMode::Exclusive, 1).await
}

async fn volume_rig_with_mode(
    access_mode: adx_core::volumes::AccessMode,
    shards: usize,
) -> (Rig, tempfile::TempDir, adx_core::volumes::Volume) {
    use adx_core::{
        execution::CreateOptions,
        volumes::{StorageDomain, VolumeSource, VolumeSpec},
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let rig = Rig::new_with_shards(shards).await;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("workspaces/tenant/user");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o711)).unwrap();
    std::fs::write(path.join("state"), "existing").unwrap();
    let before = std::fs::metadata(&path).unwrap();
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    let backend = mounts
        .lines()
        .find_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            (fields[4] == "/")
                .then(|| fields[fields.iter().position(|f| *f == "-").unwrap() + 2].to_owned())
        })
        .unwrap();
    let domains = std::collections::BTreeMap::from([(
        "test".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: backend,
            node_roots: [
                ("a".into(), root.path().to_str().unwrap().into()),
                ("b".into(), root.path().to_str().unwrap().into()),
            ]
            .into(),
            tenant_paths: [("tenant".into(), "workspaces/tenant".into())].into(),
        },
    )]);
    rig.rpc
        .clone()
        .with_volume_domains(domains.clone())
        .unwrap();
    let volume = rig
        .session
        .ensure_volume(
            VolumeSpec {
                tenant_id: "tenant".into(),
                id: "workspace".into(),
                source: VolumeSource::Directory {
                    storage_domain: "test".into(),
                    path: "workspaces/tenant/user".into(),
                },
                access_mode,
                create_if_missing: true,
                create_options: Some(CreateOptions {
                    uid: before.uid(),
                    gid: before.gid(),
                    mode: "0700".into(),
                }),
            },
            &domains,
        )
        .await
        .unwrap();
    (rig, root, volume)
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn volume_execution_preserves_existing_data_and_releases_after_cleanup() {
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::VolumeRef,
    };
    use std::os::unix::fs::MetadataExt;
    let (mut rig, root, volume) = volume_rig().await;
    let path = root.path().join("workspaces/tenant/user");
    let before = std::fs::metadata(&path).unwrap();
    let mut raw = spec("volume-execution");
    raw.execution.mounts.push(Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: MountSource::Volume(VolumeRef {
            id: "workspace".into(),
            uid: Some(volume.uid.clone()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let mut request = Rig::request(&raw.id, 0);
    request.create.as_mut().unwrap().spec = Some(raw.clone().into());
    let created = rig.nodes[0]
        .create_local_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let stored = rig.session.get(&raw.id).await.unwrap();
    assert_eq!(stored.spec, raw);
    let held = rig.session.get_volume("tenant", "workspace").await.unwrap();
    assert_eq!(
        held.occupancy.unwrap().phase,
        adx_core::volumes::OccupancyPhase::Held
    );
    assert!(matches!(
        held.initialization,
        adx_coordinator::storage::Initialization::Ready { .. }
    ));
    let after = std::fs::metadata(&path).unwrap();
    assert_eq!(
        (before.uid(), before.gid(), before.mode()),
        (after.uid(), after.gid(), after.mode())
    );
    assert_eq!(
        std::fs::read_to_string(path.join("state")).unwrap(),
        "existing"
    );
    rig.delete(&created).await;
    assert_eq!(rig.backends[0].removed.load(Ordering::SeqCst), 1);
    let released = rig.session.get_volume("tenant", "workspace").await.unwrap();
    assert!(released.occupancy.is_none());
    assert!(released.references.is_empty());
    assert_eq!(released.volume.uid, volume.uid);
    let mut failed = raw;
    failed.id = "volume-start-failure".into();
    failed.env.insert("FAIL_START".into(), "true".into());
    let mut request = Rig::request(&failed.id, 0);
    request.create.as_mut().unwrap().spec = Some(failed.into());
    assert!(rig.nodes[0]
        .create_local_environment(request)
        .await
        .is_err());
    assert!(rig
        .session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_none());

    assert_eq!(
        std::fs::read_to_string(path.join("state")).unwrap(),
        "existing"
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn heartbeat_expiry_wakes_only_the_volume_wait_and_allows_takeover_before_old_runtime_stops()
{
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::VolumeRef,
    };
    let (mut rig, _root, volume) = volume_rig().await;
    let request = |id: &str, index: usize| {
        let mut input = spec(id);
        input.execution.mounts.push(Mount {
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["rw".into()],
            source: MountSource::Volume(VolumeRef {
                id: "workspace".into(),
                uid: Some(volume.uid.clone()),
            }),
            create_if_missing: false,
            create_options: None,
        });
        let mut request = Rig::request(id, index);
        request.create.as_mut().unwrap().spec = Some(input.into());
        request
    };
    let old = rig.nodes[0]
        .create_local_environment(request("old", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let mut node = rig.nodes[1].clone();
    let next_request = request("next", 1);
    let waiting = tokio::spawn(async move { node.create_local_environment(next_request).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let queue = rig
                .coordinator
                .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
                .await
                .unwrap()
                .into_inner();
            if queue
                .environments
                .iter()
                .any(|pending| pending.spec.as_ref().is_some_and(|spec| spec.id == "next"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let snapshot = rig.session.snapshot().await.unwrap();
    let b = &snapshot.nodes["b"];
    rig.claimants[1]
        .register_node(pb::RegisterNodeRequest {
            checkpoint_scopes: vec![],
            storage_mounts: Default::default(),
            runtime_classes: b.node.runtime_classes.clone(),
            node_id: "b".into(),
            node_address: b.address.clone(),
            proxy_address: b.proxy_address.clone(),
            capacity: Some(b.node.capacity.into()),
            accepting_allocations: true,
            labels: Default::default(),
            devices: vec![],
            session_id: "boot-b".into(),
            heartbeat_sequence: 3,
            reconciling: false,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5_100)).await;
    assert_eq!(rig.rpc.expire_nodes().await.unwrap(), 1);
    let current = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(current.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(
        rig.session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment
            .environment_id,
        "next"
    );
    assert!(rig.session.get("old").await.unwrap().invalidated);
    // The accepted failure policy does not wait for a physical stop on node a.
    assert_eq!(rig.backends[0].removed.load(Ordering::SeqCst), 0);
    assert!(rig.backends[0]
        .running
        .lock()
        .unwrap()
        .contains(&old.runtime.as_ref().unwrap().id));
    let error = rig.claimants[0]
        .commit_environment(pb::CommitEnvironmentRequest {
            cleanup_artifacts: vec![],
            record: Some(old.clone()),
            node_session_id: "boot-a".into(),
            stopped_runtime_ids: vec![old.runtime.unwrap().id],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        rig.session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment
            .environment_id,
        "next"
    );
    rig.delete(&current).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn volume_wait_releases_local_capacity_and_expiry_removes_only_the_waiting_reference() {
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::{AccessMode, StorageDomain, VolumeRef, VolumeSource, VolumeSpec},
    };
    let mut rig = Rig::new().await;
    let domains = std::collections::BTreeMap::from([(
        "sfs".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: "server:/export".into(),
            node_roots: [
                ("a".into(), "/mnt/sfs".into()),
                ("b".into(), "/mnt/sfs".into()),
            ]
            .into(),
            tenant_paths: [("tenant".into(), "workspaces/tenant".into())].into(),
        },
    )]);
    rig.rpc
        .clone()
        .with_volume_domains(domains.clone())
        .unwrap();
    rig.session
        .ensure_volume(
            VolumeSpec {
                tenant_id: "tenant".into(),
                id: "workspace".into(),
                source: VolumeSource::Directory {
                    storage_domain: "sfs".into(),
                    path: "workspaces/tenant/user".into(),
                },
                access_mode: AccessMode::Exclusive,
                create_if_missing: false,
                create_options: None,
            },
            &domains,
        )
        .await
        .unwrap();
    let with_volume = |id: &str| {
        let mut spec = spec(id);
        spec.execution.mounts.push(Mount {
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["rw".into()],
            source: MountSource::Volume(VolumeRef {
                id: "workspace".into(),
                uid: None,
            }),
            create_if_missing: false,
            create_options: None,
        });
        spec
    };
    rig.claimants[0]
        .claim_environment(pb::ClaimEnvironmentRequest {
            create_deadline_unix_millis: 0,
            request_id: String::new(),
            spec: Some(with_volume("owner").into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
        })
        .await
        .unwrap();
    let mut request = Rig::request("waiter", 1);
    let create = request.create.as_mut().unwrap();
    create.spec = Some(with_volume("waiter").into());
    create.schedule_timeout_seconds = 1;
    let mut entry = rig.nodes[1].clone();
    let waiting = tokio::spawn(async move { entry.create_local_environment(request).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let queue = rig
                .coordinator
                .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
                .await
                .unwrap()
                .into_inner();
            if queue.environments.iter().any(|pending| {
                pending
                    .spec
                    .as_ref()
                    .is_some_and(|spec| spec.id == "waiter")
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let unrelated = rig.nodes[1]
        .create_local_environment(Rig::request("other", 1))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(unrelated.assignment.as_ref().unwrap().node_id, "b");
    let error = waiting.await.unwrap().unwrap_err();
    let failure =
        adx_protocol::scheduling_failure(&error).expect("confirmed unassigned cancellation");
    assert_eq!(failure.rejections.get("waiting_for_volume"), Some(&2));
    let volume = rig.session.get_volume("tenant", "workspace").await.unwrap();
    assert_eq!(volume.references, ["owner".into()].into());
    assert_eq!(volume.occupancy.unwrap().assignment.environment_id, "owner");
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 0);
    rig.delete(&unrelated).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn failed_gc_cleans_node_controller_redis_and_allows_same_name() {
    let mut rig = Rig::new().await;
    let created = rig.nodes[0]
        .create_local_environment(Rig::request("failed-gc", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(rig.rpc.collect_failed(Duration::ZERO).await.unwrap(), 0);
    rig.backends[0].running.lock().unwrap().clear();
    rig.managers[0].monitor_environments().await.unwrap();
    let failed = rig.session.get("failed-gc").await.unwrap();
    assert_eq!(
        failed.result.as_ref().unwrap().state,
        adx_core::EnvironmentState::Failed
    );
    assert!(failed.failed_since_unix_seconds.is_some());
    assert_eq!(
        rig.rpc
            .collect_failed(Duration::from_secs(600))
            .await
            .unwrap(),
        0
    );
    rig.managers[0].pause_lifecycle().await;
    assert!(rig.rpc.collect_failed(Duration::ZERO).await.is_err());
    assert!(rig.session.get("failed-gc").await.is_ok());
    let failed_record = failed.result.unwrap();
    rig.managers[0]
        .reconcile(vec![failed_record.clone()])
        .await
        .unwrap();
    let collection_record = rig.session.get("failed-gc").await.unwrap().result.unwrap();
    assert_eq!(rig.rpc.collect_failed(Duration::ZERO).await.unwrap(), 1);
    let failed_fence = collection_record.revision;
    let assignment = pb::Assignment {
        environment_id: collection_record.assignment.environment_id.clone(),
        node_id: collection_record.assignment.node_id.clone(),
        shard_id: collection_record
            .assignment
            .shard_id
            .try_into()
            .expect("test shard id fits protocol"),
        generation: collection_record.assignment.generation,
        devices: vec![],
    };
    let replay = pb::DeleteEnvironmentRequest {
        expected_revision: None,
        failed_revision: Some(failed_fence),

        node_session_id: "boot-a".into(),
        assignment: Some(assignment.clone()),
        caller: None,
    };
    let replayed = rig.coordinator_nodes[0]
        .delete_environment(replay.clone())
        .await
        .expect("Coordinator mTLS retry with the original failed fence replays deletion")
        .into_inner();
    assert_eq!(
        replayed.record.unwrap().state,
        pb::EnvironmentState::Deleted as i32
    );
    for invalid in [
        pb::DeleteEnvironmentRequest {
            failed_revision: Some(failed_fence + 1),
            ..replay.clone()
        },
        pb::DeleteEnvironmentRequest {
            failed_revision: None,
            expected_revision: Some(failed_fence),
            ..replay.clone()
        },
        {
            let mut changed_assignment = assignment.clone();
            changed_assignment.generation += 1;
            pb::DeleteEnvironmentRequest {
                assignment: Some(changed_assignment),
                ..replay.clone()
            }
        },
    ] {
        assert_eq!(
            rig.coordinator_nodes[0]
                .delete_environment(invalid)
                .await
                .expect_err("only the original Coordinator fence may replay deletion")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(
        rig.session.get("failed-gc").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    assert!(rig.managers[0]
        .metrics()
        .contains("adx_node_managed_environments 0\n"));
    assert!(rig.backends[0].running.lock().unwrap().is_empty());
    let replacement = rig.nodes[0]
        .create_local_environment(Rig::request("failed-gc", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(
        replacement.assignment.as_ref().unwrap().generation
            > created.assignment.as_ref().unwrap().generation
    );
    rig.delete(&replacement).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn failed_gc_cleans_invalidated_owner_and_fences_delayed_running() {
    let mut rig = Rig::new().await;
    let created = rig.nodes[0]
        .create_local_environment(Rig::request("dead-gc", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(rig.rpc.expire_nodes().await.unwrap(), 2);
    assert_eq!(rig.rpc.collect_failed(Duration::ZERO).await.unwrap(), 1);
    assert_eq!(
        rig.session.get("dead-gc").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    assert!(rig.claimants[0]
        .commit_environment(pb::CommitEnvironmentRequest {
            cleanup_artifacts: vec![],
            stopped_runtime_ids: vec![],
            record: Some(created),
            node_session_id: "boot-a".into()
        })
        .await
        .is_err());
    rig.managers[0].reconcile(vec![]).await.unwrap();
    assert!(rig.backends[0].running.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn redis_restart_recovers_scheduler_after_failed_node_heartbeat() {
    let mut rig = Rig::new().await;
    let created = rig.nodes[0]
        .create_local_environment(Rig::request("redis-recovery", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let stored = rig.session.snapshot().await.unwrap();
    let node = &stored.nodes["a"];
    let heartbeat = pb::RegisterNodeRequest {
        checkpoint_scopes: vec![],
        storage_mounts: Default::default(),
        runtime_classes: vec!["runsc".into(), "runc".into()],
        node_id: "a".into(),
        node_address: node.address.clone(),
        proxy_address: node.proxy_address.clone(),
        capacity: Some(node.node.capacity.into()),
        accepting_allocations: true,
        labels: Default::default(),
        devices: vec![],
        session_id: "boot-a".into(),
        heartbeat_sequence: 3,
        reconciling: false,
    };
    rig._redis.crash();
    // An ordinary heartbeat remains live without contacting Redis.
    rig.claimants[0]
        .register_node(heartbeat.clone())
        .await
        .unwrap();
    rig._redis.start().await;

    let recovered = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if rig.rpc.expire_nodes().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        recovered.is_ok(),
        "scheduler did not recover after Redis restarted"
    );
    rig.claimants[0].register_node(heartbeat).await.unwrap();
    rig.delete(&created).await;
    assert_eq!(rig.backends[0].removed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn operator_pause_keeps_node_visible_but_removes_it_from_admission() {
    let mut rig = Rig::new().await;
    assert!(rig.claimants[0]
        .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
        .await
        .is_ok());
    let paused = rig.claimants[0]
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!paused.accepting_allocations);
    let mut directory = rig
        .coordinator
        .watch_nodes(pb::WatchNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    let frame = directory.message().await.unwrap().unwrap();
    assert_eq!(frame.nodes.len(), 2);
    assert!(frame
        .nodes
        .iter()
        .all(|node| !node.relay_address.is_empty()));
    assert!(
        !frame
            .nodes
            .iter()
            .find(|node| node.node_id == "a")
            .unwrap()
            .accepting_allocations
    );
    assert!(
        frame
            .nodes
            .iter()
            .find(|node| node.node_id == "b")
            .unwrap()
            .accepting_allocations
    );

    let created = rig.nodes[0]
        .create_local_environment(Rig::request("paused-local-fallback", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(created.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 0);
    assert_eq!(rig.backends[1].started.load(Ordering::SeqCst), 1);
    rig.delete(&created).await;

    let resumed = rig.claimants[0]
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "a".into(),
            accepting_allocations: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resumed.accepting_allocations);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let next = directory.message().await.unwrap().unwrap();
            if next
                .nodes
                .iter()
                .any(|node| node.node_id == "a" && node.accepting_allocations)
            {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn environment_directory_streams_full_then_incremental_ownership() {
    let mut rig = Rig::new().await;
    let mut directory =
        pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
            channel(rig.address, "apiserver").await,
        )
        .watch_environments(pb::WatchEnvironmentsRequest {})
        .await
        .unwrap()
        .into_inner();
    let full = directory.message().await.unwrap().unwrap();
    assert!(full.reset);
    assert_eq!(full.base_revision, 0);
    assert!(full.upserts.is_empty());
    let mut revision = full.revision;

    let created = rig.nodes[0]
        .create_local_environment(Rig::request("directory-case", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let upsert = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = directory.message().await.unwrap().unwrap();
            assert!(!frame.reset);
            assert_eq!(frame.base_revision, revision);
            revision = frame.revision;
            if let Some(entry) = frame.upserts.iter().find(|entry| {
                entry
                    .record
                    .as_ref()
                    .and_then(|record| record.spec.as_ref())
                    .is_some_and(|spec| spec.id == "directory-case")
                    && entry.record.as_ref().is_some_and(|r| {
                        r.state == pb::EnvironmentState::Running as i32
                            && r.revision >= created.revision
                    })
            }) {
                break entry.clone();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        upsert
            .record
            .as_ref()
            .unwrap()
            .assignment
            .as_ref()
            .unwrap()
            .generation,
        created.assignment.as_ref().unwrap().generation
    );
    assert!(!upsert.node_address.is_empty());
    assert!(!upsert.relay_address.is_empty());

    rig.delete(&created).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = directory.message().await.unwrap().unwrap();
            assert_eq!(frame.base_revision, revision);
            revision = frame.revision;
            for entry in &frame.upserts {
                if let Some(record) = entry
                    .record
                    .as_ref()
                    .filter(|r| r.spec.as_ref().is_some_and(|s| s.id == "directory-case"))
                {
                    assert_eq!(record.state, pb::EnvironmentState::Running as i32);
                    assert_eq!(record.observation.as_ref().unwrap().intent, "user");
                }
            }
            if frame.deleted.iter().any(|id| id == "directory-case") {
                break;
            }
        }
    })
    .await
    .unwrap();

    let mut resubscribed =
        pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
            channel(rig.address, "apiserver").await,
        )
        .watch_environments(pb::WatchEnvironmentsRequest {})
        .await
        .unwrap()
        .into_inner();
    let full = resubscribed.message().await.unwrap().unwrap();
    assert!(full.reset);
    assert!(full.upserts.iter().all(|entry| {
        entry
            .record
            .as_ref()
            .and_then(|record| record.spec.as_ref())
            .is_none_or(|spec| spec.id != "directory-case")
    }));

    let restored = RoutePublisher::new(rig.session.clone(), local_peers());
    restored.refresh().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    rig._servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(
                pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                    restored,
                ),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    let mut restored_directory =
        pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
            channel(address, "apiserver").await,
        )
        .watch_environments(pb::WatchEnvironmentsRequest {})
        .await
        .unwrap()
        .into_inner();
    let restored_full = restored_directory.message().await.unwrap().unwrap();
    assert!(restored_full.reset);
    assert!(restored_full.upserts.iter().all(|entry| {
        entry
            .record
            .as_ref()
            .and_then(|record| record.spec.as_ref())
            .is_none_or(|spec| spec.id != "directory-case")
    }));

    let error = pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
        channel(rig.address, "node").await,
    )
    .watch_environments(pb::WatchEnvironmentsRequest {})
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn environment_directory_increment_does_not_rescan_control_history() {
    let mut rig = Rig::new().await;
    let mut directory =
        pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
            channel(rig.address, "apiserver").await,
        )
        .watch_environments(pb::WatchEnvironmentsRequest {})
        .await
        .unwrap()
        .into_inner();
    let full = directory.message().await.unwrap().unwrap();
    assert!(full.reset);
    let target_revision = rig.session.revision().await.unwrap();
    let mut published_revision = full.revision;
    while published_revision < target_revision {
        published_revision = directory.message().await.unwrap().unwrap().revision;
    }
    let before = redis_command_calls(&rig._redis, "hgetall").await;

    let created = rig.nodes[0]
        .create_local_environment(Rig::request("incremental-publication", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = directory.message().await.unwrap().unwrap();
            if frame.upserts.iter().any(|entry| {
                entry.record.as_ref().is_some_and(|record| {
                    record.state == pb::EnvironmentState::Running as i32
                        && record
                            .spec
                            .as_ref()
                            .is_some_and(|spec| spec.id == "incremental-publication")
                })
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(
        redis_command_calls(&rig._redis, "hgetall").await,
        before,
        "ordinary lifecycle commits must not reload the full control hash"
    );
    rig.delete(&created).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = directory.message().await.unwrap().unwrap();
            for record in frame
                .upserts
                .iter()
                .filter_map(|entry| entry.record.as_ref())
            {
                if record
                    .spec
                    .as_ref()
                    .is_some_and(|spec| spec.id == "incremental-publication")
                {
                    // The confirmed normal-operation intent is visible before deletion.
                    assert_eq!(record.state, pb::EnvironmentState::Running as i32);
                    assert_eq!(record.observation.as_ref().unwrap().intent, "user");
                }
            }
            if frame
                .deleted
                .iter()
                .any(|id| id == "incremental-publication")
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        redis_command_calls(&rig._redis, "hgetall").await,
        before,
        "terminal lifecycle commits must remain incremental"
    );

    let stored = rig.session.snapshot().await.unwrap().nodes["a"].clone();
    let heartbeat_before = redis_command_calls(&rig._redis, "hgetall").await;
    rig.claimants[0]
        .register_node(pb::RegisterNodeRequest {
            checkpoint_scopes: vec![],
            storage_mounts: Default::default(),
            runtime_classes: stored.node.runtime_classes,
            node_id: stored.node.id,
            node_address: stored.address,
            proxy_address: stored.proxy_address,
            capacity: Some(stored.node.capacity.into()),
            accepting_allocations: stored.node.available,
            labels: stored.node.labels.into_iter().collect(),
            devices: stored.node.devices.into_iter().map(Into::into).collect(),
            session_id: "boot-a".into(),
            heartbeat_sequence: stored.session.unwrap().sequence + 1,
            reconciling: false,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), directory.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        redis_command_calls(&rig._redis, "hgetall").await,
        heartbeat_before,
        "node heartbeats must not reload the full control hash"
    );
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn periodic_recovery_does_not_rescan_a_healthy_view_with_a_pending_commit() {
    let rig = Rig::new().await;
    let publisher = RoutePublisher::new(rig.session.clone(), local_peers());
    publisher.refresh().await.unwrap();
    let before = redis_command_calls(&rig._redis, "hgetall").await;

    // The Redis revision can become visible just before the in-process commit
    // event is delivered. A periodic recovery tick in that window must leave
    // the healthy view alone; the queued event remains the incremental source
    // of truth for the normal publication path.
    rig.session
        .set_node_scheduling("a", true, false)
        .await
        .unwrap();
    publisher.recover().await.unwrap();

    assert_eq!(
        redis_command_calls(&rig._redis, "hgetall").await,
        before,
        "a periodic tick must not turn a pending incremental commit into a full scan"
    );
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn concurrent_local_entries_converge_and_fallback_preserves_both_ledgers() {
    let mut rig = Rig::new().await;
    let mut a = rig.nodes[0].clone();
    let mut b = rig.nodes[1].clone();
    let (one, two) = tokio::join!(
        a.create_local_environment(Rig::request("same", 0)),
        b.create_local_environment(Rig::request("same", 1))
    );
    let one = one.unwrap().into_inner();
    let two = two.unwrap().into_inner();
    assert_eq!(u8::from(one.newly_created) + u8::from(two.newly_created), 1);
    assert_eq!(one.record, two.record);
    assert_eq!(one.durability, two.durability);
    let record = one.record.unwrap();
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    assert_eq!(rig.session.snapshot().await.unwrap().environments.len(), 1);
    let replay = a
        .create_local_environment(Rig::request("same", 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.record.as_ref(), Some(&record));
    assert!(rig
        .rpc
        .metrics()
        .await
        .unwrap()
        .contains("adx_environment_created_total 1\n"));
    assert!(rig
        .rpc
        .environment_metrics()
        .await
        .unwrap()
        .contains("adx_environment_info{environment_id=\"same\",node_id="));
    let mut changed = Rig::request("same", 1);
    changed
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .image = "different".into();
    assert_eq!(
        b.create_local_environment(changed)
            .await
            .unwrap()
            .into_inner()
            .record,
        Some(record.clone())
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    // Full local node forwards to the center; center sees the confirmed first hold.
    let index = usize::from(record.assignment.as_ref().unwrap().node_id == "b");
    let second = rig.nodes[index]
        .create_local_environment(Rig::request("fallback", index))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_ne!(
        second.assignment.as_ref().unwrap().node_id,
        record.assignment.as_ref().unwrap().node_id
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        200
    );
    let metrics = rig.rpc.metrics().await.unwrap();
    assert!(metrics.contains("adx_environment_created_total 2\n"));
    assert!(metrics
        .contains("adx_coordinator_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"a\"} 100"));
    assert!(metrics
        .contains("adx_coordinator_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"b\"} 100"));
    rig.delete(&record).await;
    rig.delete(&second).await;
    assert!(rig
        .managers
        .iter()
        .all(|n| n.used() == Resources::default()));
    // Hard placement rejects local a and falls back to b, regardless of Pack scoring.
    let mut placed = Rig::request("placed", 0);
    placed
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .scheduling = Some(pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            match_labels: [("NODE_ID".into(), "b".into())].into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let third = a
        .create_local_environment(placed)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(third.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(rig.managers[0].used(), Resources::default());
    rig.delete(&third).await;
    let mut center = rig.coordinator.clone();
    let (central, local) = tokio::join!(
        center.create_environment(create("mixed")),
        a.create_local_environment(Rig::request("mixed", 0))
    );
    let central = central.unwrap().into_inner();
    let local = local.unwrap().into_inner();
    assert_eq!(central.record, local.record);
    assert_eq!(central.durability, local.durability);
    assert_eq!(
        usize::from(central.newly_created) + usize::from(local.newly_created),
        1
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    rig.delete(&central.record.unwrap()).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn deleted_named_environment_can_be_created_again_with_a_new_generation() {
    let mut rig = Rig::new().await;
    let first = rig.nodes[0]
        .create_local_environment(Rig::request("named-recreate", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let first_generation = first.assignment.as_ref().unwrap().generation;
    rig.delete(&first).await;

    let second = rig.nodes[0]
        .create_local_environment(Rig::request("named-recreate", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(second.assignment.as_ref().unwrap().generation > first_generation);
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 2);
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    rig.delete(&second).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn new_local_claim_avoids_redundant_preclaim_storage_read() {
    let mut rig = Rig::new().await;
    // Count only the claim: the periodic route publisher has independent reads.
    let publisher = rig._servers.0.remove(0);
    publisher.abort();
    let _ = publisher.await;
    let before = redis_command_calls(&rig._redis, "hmget").await;

    rig.claimants[0]
        .claim_environment(pb::ClaimEnvironmentRequest {
            create_deadline_unix_millis: 0,
            request_id: String::new(),
            spec: Some(spec("one-storage-read").into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
        })
        .await
        .unwrap();

    let reads = redis_command_calls(&rig._redis, "hmget").await - before;
    assert_eq!(reads, 1, "claim should use exactly one storage read");
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn concurrent_local_claims_share_bounded_redis_commits() {
    let rig = Rig::new().await;
    let before = redis_command_calls(&rig._redis, "eval").await;
    let mut claims = tokio::task::JoinSet::new();
    for index in 0..8 {
        let mut claimant = rig.claimants[0].clone();
        claims.spawn(async move {
            claimant
                .claim_environment(pb::ClaimEnvironmentRequest {
                    create_deadline_unix_millis: 0,
                    request_id: String::new(),
                    spec: Some(spec(&format!("batched-{index}")).into()),
                    caller: caller(),
                    node_session_id: "boot-a".into(),
                    devices: vec![],
                })
                .await
                .unwrap()
                .into_inner()
        });
    }
    while let Some(result) = claims.join_next().await {
        assert!(matches!(
            result.unwrap().outcome,
            Some(pb::claim_environment_response::Outcome::Owned(_))
        ));
    }

    let commits = redis_command_calls(&rig._redis, "eval").await - before;
    assert!(
        commits < 8,
        "concurrent claims must be coalesced instead of issuing one Redis CAS each; got {commits}"
    );
    assert_eq!(rig.session.snapshot().await.unwrap().environments.len(), 8);
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn claim_rpc_auth_session_directory_and_real_delayed_redis_write() {
    let mut rig = Rig::new().await;
    let claim = pb::ClaimEnvironmentRequest {
        create_deadline_unix_millis: 0,
        request_id: String::new(),
        spec: Some(spec("delayed").into()),
        caller: caller(),
        node_session_id: "boot-a".into(),
        devices: vec![],
    };
    assert_eq!(
        rig.coordinator
            .claim_environment(claim.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut stale = claim.clone();
    stale.node_session_id = "old".into();
    assert!(rig.claimants[0].claim_environment(stale).await.is_err());
    let mut forged = claim.clone();
    forged.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        rig.claimants[0]
            .claim_environment(forged)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        rig.claimants[0]
            .watch_nodes(pb::WatchNodesRequest {})
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut directory = rig
        .coordinator
        .watch_nodes(pb::WatchNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    let frame = directory.message().await.unwrap().unwrap();
    assert_eq!(frame.nodes.len(), 2);
    // Delay actual Redis writes beyond the store timeout. The first claim has an
    // unknown outcome; the retry must fence/reload, not create another owner.
    let before = rig.session.snapshot().await.unwrap().revision;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let result = rig.nodes[0]
        .create_local_environment(Rig::request("delayed", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(rig.session.snapshot().await.unwrap().revision >= before + 2); // claim + result; barrier is not a directory event
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.session
            .get("delayed")
            .await
            .unwrap()
            .assignment
            .generation
            >> 32,
        rig.session.epoch()
    );
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert!(rig
        .rpc
        .metrics()
        .await
        .unwrap()
        .contains("node_id=\"a\"} 100"));
    rig.delete(&result).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn abandoned_unknown_claim_is_retried_and_shared_hold_converges() {
    let mut rig = Rig::new().await;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1500)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let error = rig.nodes[0]
        .create_local_environment(Rig::request("abandoned", 0))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    // The periodic Coordinator worker can heal storage even without another create.
    assert_eq!(rig.rpc.expire_nodes().await.unwrap(), 0);
    // No HTTP or Node RPC retry: the production background mechanism converges.
    rig.node_rpcs[0].retry_local_claims().await;
    let stored = rig.session.get("abandoned").await.unwrap();
    assert_eq!(stored.assignment.generation >> 32, rig.session.epoch());
    assert_eq!(
        stored.result.as_ref().unwrap().state,
        adx_core::EnvironmentState::Running
    );
    let mut one = rig.nodes[0].clone();
    let mut two = rig.nodes[0].clone();
    let (one, two) = tokio::join!(
        one.create_local_environment(Rig::request("abandoned", 0)),
        two.create_local_environment(Rig::request("abandoned", 0))
    );
    assert_eq!(one.unwrap().into_inner(), two.unwrap().into_inner());
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    rig.delete(&stored.result.unwrap().try_into().unwrap())
        .await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn central_assignment_waits_for_unconfirmed_local_capacity_without_reassignment() {
    let mut rig = Rig::new().await;
    rig.managers[0].reserve_local(&spec("tentative")).unwrap();
    let mut request = create("central");
    request.spec.as_mut().unwrap().scheduling = Some(pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            match_labels: [("NODE_ID".into(), "a".into())].into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let mut coordinator = rig.coordinator.clone();
    let task = tokio::spawn(async move { coordinator.create_environment(request).await });
    let assignment = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let catalog = rig.claimants[0]
                .inspect_node(pb::InspectNodeRequest {
                    node_id: "a".into(),
                    session_id: "boot-a".into(),
                })
                .await
                .unwrap()
                .into_inner();
            if let Some(record) = catalog
                .records
                .into_iter()
                .find(|r| r.spec.as_ref().is_some_and(|s| s.id == "central"))
            {
                break adx_core::Assignment::try_from(record.assignment.unwrap()).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Claim cannot fit Coordinator's committed allocation; it releases only the local
    // token and forwards to b. The center retries a with the original generation.
    let local = rig.nodes[0]
        .create_local_environment(Rig::request("tentative", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let center = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(
        center.assignment.as_ref().unwrap().generation,
        assignment.generation
    );
    assert_eq!(center.assignment.as_ref().unwrap().node_id, "a");
    assert_eq!(local.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert_eq!(rig.managers[1].used().cpu_millis, 100);
    rig.delete(&local).await;
    rig.delete(&center).await;
}

async fn prepare_http(rig: &mut Rig) {
    for (key, tenant) in [("a", "tenant"), ("b", "other")] {
        rig.session
            .bootstrap_credential(
                &key.repeat(40),
                &adx_coordinator::auth::Credential {
                    metadata: None,
                    tenant_id: tenant.into(),
                    administrator: false,
                    expires_at_unix_seconds: 0,
                },
            )
            .await
            .unwrap();
    }
    for i in 0..2 {
        let capacity = Resources {
            cpu_millis: 1000,
            memory_bytes: 8 * 1024 * 1024,
            disk_bytes: 0,
        };
        rig.managers[i]
            .update_capacity(capacity, Duration::from_secs(60))
            .unwrap();
        let node = &rig.session.snapshot().await.unwrap().nodes[if i == 0 { "a" } else { "b" }];
        rig.claimants[i]
            .register_node(pb::RegisterNodeRequest {
                checkpoint_scopes: vec![],
                storage_mounts: Default::default(),
                runtime_classes: vec![
                    "runsc".into(),
                    "runc".into(),
                    "firecracker".into(),
                    "r".into(),
                    "test-runtime".into(),
                ],
                node_id: node.node.id.clone(),
                node_address: node.address.clone(),
                proxy_address: node.proxy_address.clone(),
                capacity: Some(capacity.into()),
                session_id: node.session.as_ref().unwrap().id.clone(),
                heartbeat_sequence: 3,
                accepting_allocations: true,
                reconciling: false,
                labels: Default::default(),
                devices: vec![],
            })
            .await
            .unwrap();
    }
}

async fn start_api(
    rig: &Rig,
    api_binary: &str,
    prefix: &str,
) -> (
    tempfile::TempDir,
    tokio::process::Child,
    std::net::SocketAddr,
) {
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
    let config = serde_json::json!({
        "listen": address.to_string(), "coordinator_address": format!("https://{}", rig.address),
        "create_mode": "local_first", "ca": tls.join("ca.pem"),
        "certificate": tls.join("apiserver.pem"), "private_key": tls.join("apiserver.key"),
        "server_name": "localhost", "rpc_timeout_seconds": 5,
        "cache_entries": 128, "auth_cache_ttl_seconds": 1,
        "ingress_mode": "standalone"
    });
    let path = directory.path().join("api.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let evidence = PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap());
    let log = std::fs::File::create(evidence.join(format!("{prefix}-api.log"))).unwrap();
    let process = tokio::process::Command::new(api_binary)
        .args(["--config", path.to_str().unwrap()])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    (directory, process, address)
}

#[tokio::test]
#[ignore = "requires real Redis, mTLS certificates and API Server binary for HTTPS"]
async fn local_first_https_directory_round_robin_and_concurrent_creation() {
    let Ok(api_binary) = std::env::var("ADX_TEST_APISERVER") else {
        return;
    };
    let mut rig = Rig::new().await;
    prepare_http(&mut rig).await;
    let (_directory, mut process, address) = start_api(&rig, &api_binary, "local-first").await;
    let evidence = PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap());
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../build/ci/local_first_http.py");
    let result = tokio::process::Command::new("python3")
        .arg(script)
        .env("ADX_TEST_API_ENDPOINT", format!("https://{address}"))
        .output()
        .await
        .unwrap();
    std::fs::write(
        evidence.join("local-first-http.log"),
        [result.stdout, result.stderr.clone()].concat(),
    )
    .unwrap();
    process.kill().await.unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        rig.session
            .get("lf-first")
            .await
            .unwrap()
            .assignment
            .node_id,
        "a"
    );
    assert_eq!(
        rig.session
            .get("lf-second")
            .await
            .unwrap()
            .assignment
            .node_id,
        "b"
    );
    // Check placement while the records exist, then release the executions.
    // A successful delete retires each record from the authoritative catalog.
    for id in ["lf-first", "lf-second", "lf-race"] {
        let record = rig.session.get(id).await.unwrap().result.unwrap();
        rig.delete(&record.try_into().unwrap()).await;
        assert!(matches!(
            rig.session.get(id).await,
            Err(adx_core::Error::NotFound)
        ));
    }
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        3
    );
    assert!(rig
        .managers
        .iter()
        .all(|m| m.used() == Resources::default()));
    assert!(rig
        .session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn cancelled_local_rpc_finishes_once_and_retry_reuses_the_owner() {
    let mut rig = Rig::new().await;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(150)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let mut request = Request::new(Rig::request("cancelled", 0));
    request.set_timeout(Duration::from_millis(40));
    assert!(rig.nodes[0]
        .create_local_environment(request)
        .await
        .is_err());
    let record = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(stored) = rig.session.get("cancelled").await {
                if let Some(record) = stored.result {
                    break record;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let retry = rig.nodes[1]
        .create_local_environment(Rig::request("cancelled", 1))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let original: pb::EnvironmentRecord = record.try_into().unwrap();
    assert_eq!(retry, original);
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    assert_eq!(rig.managers[1].used(), Resources::default());
    rig.delete(&retry).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn existing_terminal_claim_reconciles_a_late_commit_into_scheduler_accounting() {
    let mut rig = Rig::new().await;
    let running = rig.nodes[0]
        .create_local_environment(Rig::request("late-delete", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let mut terminal: EnvironmentRecord = running.clone().try_into().unwrap();
    terminal.state = adx_core::EnvironmentState::Deleted;
    terminal.resources_held = false;
    terminal.runtime.ip = None;
    terminal.revision += 3;
    terminal
        .observation
        .get_or_insert_with(Default::default)
        .intent = Some(adx_core::observation::OperationIntent::User);
    // Pause only the final result write, after the intent was acknowledged and
    // the old runtime was removed.
    *rig.backends[0].pause_writes_after_remove.lock().unwrap() =
        Some((rig._redis.url.clone(), 500));
    assert!(rig.nodes[0]
        .delete_environment(pb::DeleteEnvironmentRequest {
            expected_revision: None,

            failed_revision: None,
            node_session_id: String::new(),
            assignment: running.assignment,
            caller: caller(),
        })
        .await
        .is_err());
    assert_eq!(rig.backends[0].removed.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Fault injection: complete the already-cleaned result directly in storage,
    // without the coordinator observing it. CLIENT PAUSE alone may discard a
    // command when its connection closes, so it does not prove late application.
    rig.session.commit(terminal.clone()).await.unwrap();
    assert_eq!(
        rig.session.get("late-delete").await,
        Err(adx_core::Error::NotFound)
    );
    let recreated = rig.nodes[1]
        .create_local_environment(Rig::request("late-delete", 1))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(recreated.assignment.as_ref().unwrap().generation > terminal.assignment.generation);
    assert_eq!(
        rig.managers
            .iter()
            .map(|manager| manager.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    let metrics = rig.rpc.metrics().await.unwrap();
    assert!(
        metrics
            .contains("adx_coordinator_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"a\"} 0"),
        "{metrics}"
    );
    rig.delete(&recreated).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn ordinary_heartbeats_do_not_write_redis() {
    let mut rig = Rig::new().await;
    let saved = rig.session.snapshot().await.unwrap().nodes["a"].clone();
    let before = redis_command_calls(&rig._redis, "eval").await;
    for sequence in 3..23 {
        rig.claimants[0]
            .register_node(pb::RegisterNodeRequest {
                checkpoint_scopes: vec![],
                storage_mounts: Default::default(),
                node_id: "a".into(),
                session_id: "boot-a".into(),
                node_address: saved.address.clone(),
                proxy_address: saved.proxy_address.clone(),
                capacity: Some(saved.node.capacity.into()),
                accepting_allocations: true,
                heartbeat_sequence: sequence,
                reconciling: false,
                runtime_classes: saved.node.runtime_classes.clone(),
                labels: Default::default(),
                devices: vec![],
            })
            .await
            .unwrap();
    }
    assert_eq!(redis_command_calls(&rig._redis, "eval").await, before);
    assert_eq!(
        rig.session.snapshot().await.unwrap().nodes["a"]
            .session
            .as_ref()
            .unwrap()
            .sequence,
        22
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn central_create_has_no_durable_pending_and_one_success_publication() {
    let rig = Rig::new().await;
    let mut client = rig.coordinator.clone();
    let before = redis_command_calls(&rig._redis, "eval").await;
    let task = tokio::spawn(async move {
        let mut request = create("unpublished-central");
        request.request_id = "central-request".into();
        client.create_environment(request).await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while rig
            .backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>()
            == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rig.session.get("unpublished-central").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    let created = task.await.unwrap().unwrap().into_inner().record.unwrap();
    assert_eq!(created.state, pb::EnvironmentState::Running as i32);
    assert_eq!(redis_command_calls(&rig._redis, "eval").await - before, 1);
    assert_eq!(
        rig.session.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    let stored = rig.session.get("unpublished-central").await.unwrap();
    assert_eq!(
        stored.result.unwrap().runtime.id,
        created.runtime.unwrap().id
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn central_requests_converge_by_request_or_environment_identity() {
    let rig = Rig::new().await;
    let mut tasks = vec![];
    for index in 0..12 {
        let mut client = rig.coordinator.clone();
        tasks.push(tokio::spawn(async move {
            let mut request = create(&format!("candidate-{index}"));
            request.request_id = "one-public-operation".into();
            client
                .create_environment(request)
                .await
                .unwrap()
                .into_inner()
                .record
                .unwrap()
        }));
    }
    let mut records = vec![];
    for task in tasks {
        records.push(task.await.unwrap());
    }
    assert!(records.iter().all(|record| record == &records[0]));
    let mut retry = create(&records[0].spec.as_ref().unwrap().id);
    retry.request_id = "another-public-operation".into();
    retry.spec.as_mut().unwrap().image = "different-image".into();
    let replay = rig
        .coordinator
        .clone()
        .create_environment(retry)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(replay, records[0]);
    assert_eq!(
        rig.backends
            .iter()
            .map(|backend| backend.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    assert_eq!(rig.session.snapshot().await.unwrap().environments.len(), 1);
    assert_eq!(
        rig.session
            .published_request("tenant", "another-public-operation")
            .await
            .unwrap()
            .unwrap()
            .spec
            .id,
        records[0].spec.as_ref().unwrap().id
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn coordinator_restart_reclaims_unconfirmed_runtime_before_reassignment() {
    let mut rig = Rig::new().await;
    let old_epoch = rig.session.epoch();
    let task = tokio::spawn({
        let mut client = rig.coordinator.clone();
        async move {
            let mut request = create("unpublished-central");
            request.request_id = "lost-before-confirmation".into();
            request.create_timeout_seconds = 2;
            client.create_environment(request).await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while rig
            .backends
            .iter()
            .all(|b| b.started.load(Ordering::SeqCst) == 0)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rig.session.get("unpublished-central").await,
        Err(adx_core::Error::NotFound)
    );
    // A new Coordinator instance obtains a new epoch while the old runtime is
    // starting. The old instance is still alive, so its delayed commit is tested.
    let session = rig._redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        local_peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    rig._servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    assert!(tokio::time::timeout(Duration::from_secs(6), task)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert!(session.snapshot().await.unwrap().environments.is_empty());
    assert!(
        rig.backends
            .iter()
            .map(|b| b.running.lock().unwrap().len())
            .sum::<usize>()
            >= 1,
        "unconfirmed executions must remain observable before recovery"
    );
    let saved = session.snapshot().await.unwrap();
    for (index, id, cert) in [(0, "a", "node"), (1, "b", "ingress")] {
        let channel = channel(address, cert).await;
        rig.sinks[index]
            .reconnect(adx_transport::rpc::authenticated_channel(channel.clone()))
            .unwrap();
        let mut authority = pb::coordinator_service_client::CoordinatorServiceClient::new(
            adx_transport::rpc::authenticated_channel(channel),
        );
        let node = &saved.nodes[id];
        let mut report = pb::RegisterNodeRequest {
            checkpoint_scopes: vec![],
            storage_mounts: Default::default(),
            node_id: id.into(),
            node_address: node.address.clone(),
            proxy_address: node.proxy_address.clone(),
            session_id: format!("boot-{id}"),
            heartbeat_sequence: 20,
            reconciling: true,
            accepting_allocations: false,
            capacity: Some(node.node.capacity.into()),
            runtime_classes: node.node.runtime_classes.clone(),
            labels: Default::default(),
            devices: vec![],
        };
        adxlet::node_reconciliation::reconcile_node(
            &mut authority,
            &mut report,
            &rig.managers[index],
            None,
            Duration::from_secs(2),
            Duration::from_millis(20),
            tokio::time::Instant::now() + Duration::from_secs(60),
        )
        .await
        .unwrap();
        assert!(rig.managers[index].accepting_allocations());
        assert_eq!(rig.managers[index].used(), Resources::default());
        assert!(rig.backends[index].running.lock().unwrap().is_empty());
    }
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let mut request = create("unpublished-central");
    request.request_id = "lost-before-confirmation".into();
    let record = client
        .create_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(record.assignment.as_ref().unwrap().generation >> 32 > old_epoch);
    assert_eq!(session.snapshot().await.unwrap().environments.len(), 1);
    rig.delete(&record).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn central_recreation_retires_a_deleted_result_missing_from_its_cache() {
    let mut rig = Rig::new().await;
    let mut first = create("reuse-deleted");
    first.request_id = "old-success".into();
    let record: EnvironmentRecord = rig
        .coordinator
        .create_environment(first)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap()
        .try_into()
        .unwrap();
    // Simulate an applied deletion whose acknowledgement did not reach the
    // Coordinator. Clean the real fixture runtime, then publish directly.
    let index = usize::from(record.assignment.node_id == "b");
    rig.backends[index]
        .remove(&record.runtime.id)
        .await
        .unwrap();
    let mut deleted = record.clone();
    deleted.state = adx_core::EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 2;
    rig.session.commit(deleted).await.unwrap();
    rig.managers[index].reconcile(vec![]).await.unwrap();
    let mut next = create("reuse-deleted");
    next.request_id = "new-success".into();
    let recreated = rig
        .coordinator
        .create_environment(next)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(recreated.assignment.as_ref().unwrap().generation > record.assignment.generation);
    let old = rig
        .session
        .published_request("tenant", "old-success")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.state, adx_core::EnvironmentState::Deleted);
    assert_eq!(
        rig.session
            .get("reuse-deleted")
            .await
            .unwrap()
            .assignment
            .generation,
        recreated.assignment.as_ref().unwrap().generation
    );
    rig.delete(&recreated).await;
}

#[tokio::test]
#[ignore = "review reproduction with isolated Redis and mTLS"]
async fn optional_claim_can_retry_on_ready_node() {
    let mut rig = Rig::new().await;
    let id = "review-pending-retry";
    rig.claimants[0]
        .claim_environment(pb::ClaimEnvironmentRequest {
            create_deadline_unix_millis: 0,
            request_id: "review-pending-request".into(),
            spec: Some(spec(id).into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
        })
        .await
        .unwrap();
    rig.managers[0].pause_lifecycle().await;
    let mut request = create(id);
    request.request_id = "review-pending-request".into();
    request.create_timeout_seconds = 3;
    request.schedule_timeout_seconds = 2;
    let result = rig.coordinator.create_environment(request).await;
    let stored = rig.session.get(id).await;
    eprintln!(
        "review optional retry result={result:?}; stored={stored:?}; backend_b_starts={}",
        rig.backends[1].started.load(Ordering::SeqCst)
    );
    assert!(
        result.is_ok(),
        "unstarted claim on unavailable A must allow ready B to finish: {result:?}"
    );
    let record = result.unwrap().into_inner().record.unwrap();
    assert_eq!(record.assignment.as_ref().unwrap().node_id, "b");
    rig.delete(&record).await;
}

#[tokio::test]
#[ignore = "review reproduction with isolated Redis and mTLS"]
async fn local_replay_precedes_snapshot_normalization() {
    let mut rig = Rig::new().await;
    let id = "review-local-snapshot";
    let mut original = Rig::request(id, 0);
    original.persist_claim = false;
    original.create.as_mut().unwrap().request_id = "review-local-request".into();
    let first = rig.nodes[0]
        .create_local_environment(original)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    for persist_claim in [false, true] {
        for (environment_id, request_id) in [
            (id, "another-local-request"),
            ("another-local-environment", "review-local-request"),
        ] {
            let mut replay = Rig::request(environment_id, 0);
            replay.persist_claim = persist_claim;
            let create = replay.create.as_mut().unwrap();
            create.request_id = request_id.into();
            create.spec.as_mut().unwrap().snapshot_id = Some("missing-review-snapshot".into());
            let got = rig.nodes[0].create_local_environment(replay).await;
            assert!(
                got.is_ok(),
                "either successful identity must replay before reading new snapshot: {got:?}"
            );
            assert_eq!(got.unwrap().into_inner().record, Some(first.clone()));
        }
    }
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    rig.delete(&first).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn default_local_create_persists_only_success() {
    let mut rig = Rig::new().await;
    let mut node = rig.nodes[0].clone();
    let before = redis_command_calls(&rig._redis, "eval").await;
    let task = tokio::spawn(async move {
        let mut request = Rig::request("unpublished-central", 0);
        request.persist_claim = false;
        request.create.as_mut().unwrap().request_id = "local-memory-request".into();
        node.create_local_environment(request).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while rig.backends[0].started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rig.session.get("unpublished-central").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    let record = task.await.unwrap().unwrap().into_inner().record.unwrap();
    assert_eq!(redis_command_calls(&rig._redis, "eval").await - before, 1);
    assert_eq!(
        rig.session.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    assert_eq!(record.assignment.as_ref().unwrap().node_id, "a");
    assert_eq!(
        rig.session
            .published_request("tenant", "local-memory-request")
            .await
            .unwrap()
            .unwrap()
            .spec
            .id,
        "unpublished-central"
    );
    rig.delete(&record).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn memory_local_reservation_expires_and_late_result_is_fenced() {
    let mut rig = Rig::new().await;
    let id = "memory-local-expiry";
    let deadline = adx_transport::deadline::Deadline::after(Duration::from_millis(150)).unwrap();
    let owned = rig.claimants[0]
        .reserve_local_environment(pb::ClaimEnvironmentRequest {
            spec: Some(spec(id).into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
            request_id: "expired-local-request".into(),
            create_deadline_unix_millis: deadline.unix_millis(),
        })
        .await
        .unwrap()
        .into_inner()
        .outcome
        .unwrap();
    let pb::claim_environment_response::Outcome::Owned(owned) = owned else {
        panic!("local reservation required")
    };
    assert_eq!(
        rig.session.get(id).await.unwrap_err(),
        adx_core::Error::NotFound
    );
    tokio::time::sleep_until(deadline.instant() + Duration::from_millis(30)).await;
    rig.rpc.expire_nodes().await.unwrap();
    let catalog = rig.claimants[0]
        .inspect_node(pb::InspectNodeRequest {
            node_id: "a".into(),
            session_id: "boot-a".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!catalog
        .records
        .iter()
        .any(|r| r.spec.as_ref().is_some_and(|s| s.id == id)));
    let mut request = Rig::request(id, 0);
    request.persist_claim = false;
    request.create.as_mut().unwrap().request_id = "replacement-local-request".into();
    let created = rig.nodes[0]
        .create_local_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_ne!(created.assignment, owned.assignment);
    let mut late = created.clone();
    late.assignment = owned.assignment;
    late.runtime.as_mut().unwrap().id =
        format!("{id}-{}", late.assignment.as_ref().unwrap().generation);
    assert_eq!(
        rig.claimants[0]
            .commit_environment(pb::CommitEnvironmentRequest {
                cleanup_artifacts: vec![],
                stopped_runtime_ids: vec![],
                record: Some(late),
                node_session_id: "boot-a".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        rig.session
            .get(id)
            .await
            .unwrap()
            .result
            .unwrap()
            .assignment
            .generation,
        created.assignment.as_ref().unwrap().generation
    );
    rig.delete(&created).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn memory_local_reservation_is_lost_on_coordinator_restart() {
    let mut rig = Rig::new().await;
    let owned = rig.claimants[0]
        .reserve_local_environment(pb::ClaimEnvironmentRequest {
            spec: Some(spec("local-before-restart").into()),
            caller: caller(),
            node_session_id: "boot-a".into(),
            devices: vec![],
            request_id: "local-restart-request".into(),
            create_deadline_unix_millis: adx_transport::deadline::Deadline::after(
                Duration::from_secs(30),
            )
            .unwrap()
            .unix_millis(),
        })
        .await
        .unwrap()
        .into_inner()
        .outcome
        .unwrap();
    assert!(matches!(
        owned,
        pb::claim_environment_response::Outcome::Owned(_)
    ));
    let next = rig._redis.store().await.begin(1).await.unwrap();
    let restarted = CoordinatorRpc::new(
        next.clone(),
        Placement::Pack,
        local_peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert!(next.snapshot().await.unwrap().environments.is_empty());
    assert!(next
        .published_request("tenant", "local-restart-request")
        .await
        .unwrap()
        .is_none());
    assert!(restarted
        .metrics()
        .await
        .unwrap()
        .contains("adx_environment_created_total 0"));
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn memory_local_concurrent_entries_reuse_one_success() {
    let mut rig = Rig::new().await;
    let mut tasks = Vec::new();
    for index in 0..8 {
        let mut node = rig.nodes[index % 2].clone();
        tasks.push(tokio::spawn(async move {
            let mut request = Rig::request("memory-local-concurrent", index % 2);
            request.persist_claim = false;
            request.create.as_mut().unwrap().request_id = format!("local-concurrent-{index}");
            node.create_local_environment(request)
                .await
                .unwrap()
                .into_inner()
                .record
                .unwrap()
        }));
    }
    let mut records = Vec::new();
    for task in tasks {
        records.push(task.await.unwrap());
    }
    assert!(records.iter().all(|record| record == &records[0]));
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    for index in 0..8 {
        assert_eq!(
            rig.session
                .published_request("tenant", &format!("local-concurrent-{index}"))
                .await
                .unwrap()
                .unwrap()
                .spec
                .id,
            "memory-local-concurrent"
        );
    }
    rig.delete(&records[0]).await;
    assert!(rig
        .managers
        .iter()
        .all(|m| m.used() == Resources::default()));
}

fn managed_request(id: &str, volume: &adx_core::volumes::Volume) -> pb::CreateEnvironmentRequest {
    let mut request = create(id);
    let mut raw = spec(id);
    raw.execution.mounts.push(adx_core::execution::Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: adx_core::execution::MountSource::Volume(adx_core::volumes::VolumeRef {
            id: volume.spec.id.clone(),
            uid: Some(volume.uid.clone()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    request.spec = Some(raw.into());
    request
}

async fn wait_for_queued(rig: &mut Rig, id: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let queue = rig
                .coordinator
                .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
                .await
                .unwrap()
                .into_inner();
            if queue
                .environments
                .iter()
                .any(|pending| pending.spec.as_ref().is_some_and(|spec| spec.id == id))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn dynamic_storage_refreshes_waiting_topology_and_withdraws_without_restarting() {
    use adx_core::volumes::{AccessMode, StorageDomain, StorageMount};
    use std::collections::BTreeMap;
    for dynamic_nodes in [false, true] {
        let (mut rig, root, volume) = volume_rig_with_mode(AccessMode::Shared, 2).await;
        let backend = rig
            .session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .backend_id;
        let mounts = adxlet::managed_mounts::mounted_storage(BTreeMap::from([(
            "test".into(),
            StorageMount {
                backend_id: backend.clone(),
                root: root.path().to_str().unwrap().into(),
            },
        )]))
        .await
        .unwrap();
        rig.rpc
            .clone()
            .with_volume_domains(BTreeMap::from([(
                "test".into(),
                StorageDomain {
                    backend_id: backend,
                    dynamic_nodes,
                    node_roots: [
                        ("a".into(), root.path().to_str().unwrap().into()),
                        ("b".into(), root.path().to_str().unwrap().into()),
                    ]
                    .into(),
                    tenant_paths: [("tenant".into(), "workspaces/tenant".into())].into(),
                },
            )]))
            .unwrap();
        let primer = rig
            .coordinator
            .create_environment(create("prime-shard-rotation"))
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap();
        rig.delete(&primer).await;
        let mut occupied = Vec::new();
        for (index, id) in ["fill-a", "fill-b"].into_iter().enumerate() {
            rig.report_mounts(index, mounts.clone()).await;
            occupied.push(
                rig.nodes[index]
                    .create_local_environment(Rig::request(id, index))
                    .await
                    .unwrap()
                    .into_inner()
                    .record
                    .unwrap(),
            );
        }
        let mut api = rig.coordinator.clone();
        let request = managed_request("waiting", &volume);
        let waiting = tokio::spawn(async move { api.create_environment(request).await });
        wait_for_queued(&mut rig, "waiting").await;
        rig.add_node("c", "unknown").await;
        assert!(
            !waiting.is_finished(),
            "unreported storage entered topology"
        );
        let mut wrong = mounts.clone();
        wrong.get_mut("test").unwrap().backend_id = "other:/export".into();
        rig.report_mounts(2, wrong).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !waiting.is_finished(),
            "mismatched backend entered topology"
        );
        rig.report_mounts(2, mounts.clone()).await;
        if !dynamic_nodes {
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert!(
                !waiting.is_finished(),
                "fixed topology expanded to a new node"
            );
            rig.delete(&occupied.remove(0)).await;
        }
        let created = tokio::time::timeout(Duration::from_secs(3), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner()
            .record
            .unwrap();
        assert_eq!(
            created.assignment.as_ref().unwrap().node_id,
            if dynamic_nodes { "c" } else { "a" }
        );
        if dynamic_nodes {
            let stale = rig.registrations[2].clone();
            rig.report_mounts(2, BTreeMap::new()).await;
            let mut grants = pb::volume_execution_service_client::VolumeExecutionServiceClient::new(
                channel(rig.address, "unknown").await,
            );
            assert_eq!(
                grants
                    .authorize_execution(pb::AuthorizeVolumeExecutionRequest {
                        record: Some(created.clone()),
                        previous_runtime_id: created.runtime.as_ref().unwrap().id.clone(),
                        node_session_id: "boot-c".into(),
                    })
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::ResourceExhausted
            );
            assert_eq!(
                rig.claimants[2]
                    .register_node(stale)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
            // A capability withdrawal does not retire an already-running backend.
            assert_eq!(rig.backends[2].removed.load(Ordering::SeqCst), 0);
            rig.delete(&created).await;
            let request = managed_request("after-withdrawal", &volume);
            let mut api = rig.coordinator.clone();
            let next = tokio::spawn(async move { api.create_environment(request).await });
            wait_for_queued(&mut rig, "after-withdrawal").await;
            let unrelated = rig.nodes[2]
                .create_local_environment(pb::LocalEnvironmentCreateRequest {
                    persist_claim: false,
                    create: Some(create("unrelated")),
                    node_session_id: "boot-c".into(),
                })
                .await
                .unwrap()
                .into_inner()
                .record
                .unwrap();
            assert!(!next.is_finished());
            rig.delete(&unrelated).await;
            rig.report_mounts(2, mounts).await;
            let next = tokio::time::timeout(Duration::from_secs(3), next)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .into_inner()
                .record
                .unwrap();
            assert_eq!(next.assignment.as_ref().unwrap().node_id, "c");
            rig.delete(&next).await;
        } else {
            rig.delete(&created).await;
        }
        for record in occupied {
            rig.delete(&record).await;
        }
        assert_eq!(
            std::fs::read_to_string(root.path().join("workspaces/tenant/user/state")).unwrap(),
            "existing"
        );
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn dynamic_storage_refresh_keeps_exclusive_wait_until_release() {
    use adx_core::volumes::{StorageDomain, StorageMount};
    use std::collections::BTreeMap;
    let (mut rig, root, volume) = volume_rig().await;
    let mut request = Rig::request("holder", 0);
    request.create = Some(managed_request("holder", &volume));
    let holder = rig.nodes[0]
        .create_local_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let backend = rig
        .session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .backend_id;
    rig.rpc
        .clone()
        .with_volume_domains(BTreeMap::from([(
            "test".into(),
            StorageDomain {
                backend_id: backend.clone(),
                dynamic_nodes: true,
                node_roots: [
                    ("a".into(), root.path().to_str().unwrap().into()),
                    ("b".into(), root.path().to_str().unwrap().into()),
                ]
                .into(),
                tenant_paths: [("tenant".into(), "workspaces/tenant".into())].into(),
            },
        )]))
        .unwrap();
    let mut request = managed_request("exclusive-wait", &volume);
    request.spec.as_mut().unwrap().scheduling = Some(pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            match_labels: [("NODE_ID".into(), "c".into())].into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let mut api = rig.coordinator.clone();
    let waiting = tokio::spawn(async move { api.create_environment(request).await });
    wait_for_queued(&mut rig, "exclusive-wait").await;
    rig.add_node("c", "unknown").await;
    rig.report_mounts(
        2,
        BTreeMap::from([(
            "test".into(),
            StorageMount {
                backend_id: backend,
                root: root.path().to_str().unwrap().into(),
            },
        )]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());
    assert_eq!(rig.backends[2].started.load(Ordering::SeqCst), 0);
    assert_eq!(
        rig.session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment
            .environment_id,
        "holder"
    );
    rig.delete(&holder).await;
    let next = tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(next.assignment.as_ref().unwrap().node_id, "c");
    rig.delete(&next).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS; run control-rpc suite"]
async fn default_local_creation_still_claims_an_exclusive_volume() {
    let (mut rig, _root, volume) = volume_rig().await;
    let mut request = Rig::request("default-exclusive", 0);
    request.persist_claim = false;
    request.create = Some(managed_request("default-exclusive", &volume));
    let owner = rig.nodes[0]
        .create_local_environment(request)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let saved = rig.session.get("default-exclusive").await.unwrap();
    assert_eq!(
        saved.assignment,
        owner.assignment.clone().unwrap().try_into().unwrap()
    );
    assert!(saved.volume_execution.is_some());
    let occupancy = rig
        .session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .unwrap();
    assert_eq!(occupancy.assignment, saved.assignment);
    assert_eq!(occupancy.phase, adx_core::volumes::OccupancyPhase::Held);
    rig.delete(&owner).await;
    assert!(rig
        .session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn local_first_replay_uses_native_published_spec_with_labels() {
    let mut rig = Rig::new().await;
    let mut request = Rig::request("native-metadata", 0);
    request
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .scheduling
        .get_or_insert_with(Default::default)
        .labels = [("note".into(), "line one\nline two".into())]
        .into_iter()
        .collect();
    let created = rig.nodes[0]
        .create_local_environment(request.clone())
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let stored = rig.session.get("native-metadata").await.unwrap();
    assert!(stored.spec.compatibility.is_none());
    assert_eq!(stored.spec.scheduling.labels["note"], "line one\nline two");
    assert!(!stored.spec.env.contains_key("note"));
    assert!(stored.spec.scheduling.labels.contains_key("note"));
    request
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .scheduling
        .get_or_insert_with(Default::default)
        .labels
        .clear();
    // Native published-result replay returns its original winner, including labels.
    let replay = rig.nodes[0]
        .create_local_environment(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        replay
            .record
            .unwrap()
            .spec
            .unwrap()
            .scheduling
            .unwrap()
            .labels["note"],
        "line one\nline two"
    );
    assert_eq!(
        rig.session
            .get("native-metadata")
            .await
            .unwrap()
            .spec
            .scheduling
            .labels,
        stored.spec.scheduling.labels
    );
    assert_eq!(
        rig.backends
            .iter()
            .map(|backend| backend.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    rig.delete(&created).await;
}
