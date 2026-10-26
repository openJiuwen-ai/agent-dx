use super::*;

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn volume_rpc_uses_tenant_auth_and_reuses_existing_definitions() {
    use adx_core::volumes::StorageDomain;
    use std::collections::BTreeMap;
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap()
    .with_volume_domains(BTreeMap::from([(
        "sfs".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: "server:/export".into(),
            node_roots: BTreeMap::from([("node".into(), "/mnt/sfs".into())]),
            tenant_paths: BTreeMap::from([("tenant".into(), "workspaces/tenant".into())]),
        },
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::volume_service_server::VolumeServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut api =
        pb::volume_service_client::VolumeServiceClient::new(channel(address, "apiserver").await);
    let mut spec = pb::VolumeSpec {
        tenant_id: "tenant".into(),
        id: "workspace".into(),
        source: Some(pb::volume_spec::Source::Directory(
            pb::DirectoryVolumeSource {
                storage_domain: "sfs".into(),
                path: "workspaces/tenant/user".into(),
            },
        )),
        ..Default::default()
    };
    let original = api
        .ensure_volume(pb::EnsureVolumeRequest {
            owner_id: String::new(),
            spec: Some(spec.clone()),
            caller: caller(),
        })
        .await
        .unwrap()
        .into_inner();
    spec.access_mode = pb::VolumeAccessMode::Exclusive as i32;
    let reused = api
        .ensure_volume(pb::EnsureVolumeRequest {
            owner_id: String::new(),
            spec: Some(spec.clone()),
            caller: caller(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(original, reused);
    let query = pb::GetVolumeRequest {
        tenant_id: "tenant".into(),
        id: "workspace".into(),
        caller: caller(),
    };
    let found = api.get_volume(query.clone()).await.unwrap().into_inner();
    assert_eq!(found.volume.unwrap(), original);
    let mut source = super::spec("source");
    source.execution.mounts.push(adx_core::execution::Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: adx_core::execution::MountSource::Volume(adx_core::volumes::VolumeRef {
            id: "workspace".into(),
            uid: None,
        }),
        create_if_missing: false,
        create_options: None,
    });
    session.pin_volumes(&source).await.unwrap();
    let snapshot = session
        .publish_snapshot(
            adx_core::snapshots::Snapshot::new(
                "snapshot".into(),
                vec![],
                source.clone(),
                "node".into(),
                "source-1".into(),
                adx_core::CheckpointArtifact {
                    storage: "shared".into(),
                    location: "artifact".into(),
                    size_bytes: 1,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    session.unpin_volumes(&source).await.unwrap();
    let found = api.get_volume(query.clone()).await.unwrap().into_inner();
    assert_eq!(found.snapshot_ids, vec![snapshot.id.clone()]);
    assert_eq!(
        api.delete_volume(query.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let mut wrong_tenant = query.clone();
    wrong_tenant.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        api.get_volume(wrong_tenant).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut node =
        pb::volume_service_client::VolumeServiceClient::new(channel(address, "node").await);
    assert_eq!(
        node.ensure_volume(pb::EnsureVolumeRequest {
            owner_id: String::new(),
            spec: Some(spec),
            caller: caller()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
    let deleting = session
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    session
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .unwrap();
    api.delete_volume(query).await.unwrap();
    assert!(session.list_volumes("tenant").await.unwrap().is_empty());
    task.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn inspection_reports_latest_authorized_volume_execution() {
    use adx_coordinator::{
        storage::{LocalClaim, NodeSession},
        Node,
    };
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::{AccessMode, StorageDomain, VolumeRef, VolumeSource, VolumeSpec},
    };
    use std::collections::BTreeMap;
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let domains = BTreeMap::from([(
        "sfs".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: "server:/export".into(),
            node_roots: BTreeMap::from([("node".into(), "/mnt/sfs".into())]),
            tenant_paths: BTreeMap::from([("tenant".into(), "workspaces".into())]),
        },
    )]);
    session
        .ensure_volume(
            VolumeSpec {
                tenant_id: "tenant".into(),
                id: "workspace".into(),
                source: VolumeSource::Directory {
                    storage_domain: "sfs".into(),
                    path: "workspaces/user".into(),
                },
                access_mode: AccessMode::Exclusive,
                create_if_missing: false,
                create_options: None,
            },
            &domains,
        )
        .await
        .unwrap();
    let report = pb::RegisterNodeRequest {
        storage_mounts: Default::default(),
        node_id: "node".into(),
        node_address: "127.0.0.1:9000".into(),
        proxy_address: "127.0.0.1:9001".into(),
        session_id: "boot".into(),
        heartbeat_sequence: 2,
        reconciling: true,
        accepting_allocations: false,
        runtime_classes: vec!["runc".into()],
        capacity: Some(
            Resources {
                cpu_millis: 1000,
                memory_bytes: 4096,
                disk_bytes: 0,
            }
            .into(),
        ),
        labels: Default::default(),
        devices: vec![],
    };
    session
        .register_session(
            Node {
                id: "node".into(),
                capacity: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 4096,
                    disk_bytes: 0,
                },
                available: true,
                labels: Default::default(),
                devices: vec![],
                runtime_classes: vec!["runc".into()],
            },
            report.node_address.clone(),
            report.proxy_address.clone(),
            Some(NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let mut source = spec("source");
    source.execution.mounts.push(Mount {
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
    let assignment = session
        .claim(
            source.clone(),
            &LocalClaim {
                request_id: String::new(),
                node_id: "node".into(),
                node_session_id: "boot".into(),
                devices: vec![],
            },
        )
        .await
        .unwrap()
        .record()
        .assignment
        .clone();
    let mut record = EnvironmentRecord {
        observation: None,
        spec: source,
        runtime: adx_core::Runtime {
            id: format!("source-{}", assignment.generation),
            ip: None,
        },
        assignment,
        state: adx_core::EnvironmentState::Starting,
        revision: 1,
        resources_held: true,
        snapshot_source: None,
        failure: None,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        execution_artifacts: vec![],
    };
    session
        .authorize_volumes(&record, &record.runtime.id, "boot")
        .await
        .unwrap();
    record.state = adx_core::EnvironmentState::Running;
    record.revision = 2;
    record.runtime.ip = Some("10.0.0.1".parse().unwrap());
    session.commit(record.clone()).await.unwrap();
    let previous = record.runtime.id.clone();
    record.state = adx_core::EnvironmentState::Starting;
    record.revision = 3;
    record.runtime.ip = None;
    record.runtime.id = format!("{previous}-r3");
    session
        .authorize_volumes(&record, &previous, "boot")
        .await
        .unwrap();
    let rpc = CoordinatorRpc::new(
        session,
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap()
    .with_volume_domains(domains)
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "node").await,
    );
    client.register_node(report).await.unwrap();
    let inspected = client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inspected.records.len(), 1);
    assert_eq!(
        inspected.records[0].runtime.as_ref().unwrap().id,
        record.runtime.id,
        "inspection sent an older result identity after a newer execution was durably authorized"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis and TLS; build/ci/run.py control-rpc"]
async fn release_volume_owner_rejects_foreign_pending_requests_before_cancellation() {
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::{AccessMode, StorageDomain, VolumeRef, VolumeSource, VolumeSpec},
    };
    use std::collections::BTreeMap;
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let domains = BTreeMap::from([(
        "sfs".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: "server:/export".into(),
            node_roots: BTreeMap::from([("node".into(), "/mnt/sfs".into())]),
            tenant_paths: BTreeMap::from([("tenant-b".into(), "workspaces/b".into())]),
        },
    )]);
    let volume = session
        .ensure_volume(
            VolumeSpec {
                tenant_id: "tenant-b".into(),
                id: "workspace".into(),
                source: VolumeSource::Directory {
                    storage_domain: "sfs".into(),
                    path: "workspaces/b/user".into(),
                },
                access_mode: AccessMode::Shared,
                create_if_missing: false,
                create_options: None,
            },
            &domains,
        )
        .await
        .unwrap();
    let rpc = CoordinatorRpc::new(
        session,
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap()
    .with_volume_domains(domains)
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc.clone()))
            .add_service(pb::volume_service_server::VolumeServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut source = spec("victim-environment");
    source.tenant_id = "tenant-b".into();
    source.execution.mounts.push(Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: MountSource::Volume(VolumeRef {
            id: "workspace".into(),
            uid: Some(volume.uid),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let mut create_client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    let pending = tokio::spawn(async move {
        create_client
            .create_environment(pb::CreateEnvironmentRequest {
                request_id: String::new(),
                spec: Some(source.into()),
                caller: Some(pb::CallerContext {
                    tenant_id: "tenant-b".into(),
                    administrator: false,
                }),
                schedule_timeout_seconds: 60,
                create_timeout_seconds: 90,
                create_deadline_unix_millis: 0,
                expected_assignment: None,
            })
            .await
    });
    let mut observer = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "apiserver").await,
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if observer
                .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
                .await
                .unwrap()
                .into_inner()
                .environments
                .len()
                == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut attacker =
        pb::volume_service_client::VolumeServiceClient::new(channel(address, "apiserver").await);
    let result = attacker
        .release_owner(pb::ReleaseVolumeOwnerRequest {
            tenant_id: "tenant-a".into(),
            owner_id: "victim-environment".into(),
            caller: Some(pb::CallerContext {
                tenant_id: "tenant-a".into(),
                administrator: false,
            }),
        })
        .await;
    assert!(result.is_err(), "foreign owner must be rejected");
    let after = observer
        .get_scheduling_queue(pb::GetSchedulingQueueRequest {})
        .await
        .unwrap()
        .into_inner();
    pending.abort();
    assert_eq!(
        after.environments.len(),
        1,
        "foreign release_owner rejected only after mutating the victim tenant's scheduling queue"
    );
}
