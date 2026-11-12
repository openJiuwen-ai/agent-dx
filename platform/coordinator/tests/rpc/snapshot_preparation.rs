use super::*;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn terminal_seed_create_failure_finishes_build_and_retains_previous_success() {
    Box::pin(terminal_seed_create_failure_scenario()).await;
}

async fn terminal_seed_create_failure_scenario() {
    use adx_core::{snapshot_preparation::PreparationState, snapshots::Snapshot};

    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let coordinator = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = ml.local_addr().unwrap();
    let publication = RoutePublisher::new(session.clone(), peers());
    let mut servers = Servers(vec![serve_lifecycle_coordinator(
        ml,
        coordinator.clone(),
        adx_coordinator::auth::AuthRpc::new(session.clone(), peers()),
        publication,
    )]);
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
        pause_writes_after_remove: Mutex::new(None),
    });
    let sink = Arc::new(
        CoordinatorStateSink::new(channel(address, "node").await, Duration::from_secs(2))
            .unwrap()
            .with_session("golden-boot".into()),
    );
    let manager = Arc::new(Adxlet::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        sink,
    ));
    manager
        .update_capacity(spec("capacity").resources, Duration::from_secs(60))
        .unwrap();
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = nl.local_addr().unwrap();
    servers.0.push(serve_lifecycle_node(
        nl,
        NodeRpc::new(manager, peers(), "golden-boot".into()),
    ));
    let mut node_client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(address, "node").await,
    );
    let mut register = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: node_address.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        session_id: "golden-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        capacity: Some(spec("capacity").resources.into()),
        runtime_classes: vec!["runc".into()],
        ..Default::default()
    };
    node_client.register_node(register.clone()).await.unwrap();
    node_client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "golden-boot".into(),
        })
        .await
        .unwrap();
    register.heartbeat_sequence += 1;
    register.reconciling = false;
    register.accepting_allocations = true;
    node_client.register_node(register).await.unwrap();
    let mut templates =
        pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
            channel(address, "apiserver").await,
        );

    for (index, failure, reason) in [
        (0, "image_missing", "manifest_unknown"),
        (1, "unavailable", "injected runtime start failure"),
    ] {
        let name = format!("golden-failure-{index}");
        let retained_snapshot = format!("snapshot-retained-{index}");
        let previous_spec = spec(&format!("previous-{index}"));
        session
            .publish_snapshot(
                Snapshot::new_with_kind(
                    adx_core::snapshots::SnapshotKind::Golden,
                    retained_snapshot.clone(),
                    vec![name.clone()],
                    previous_spec.clone(),
                    "node".into(),
                    format!("previous-{index}-1"),
                    adx_core::CheckpointArtifact {
                        storage: "shared".into(),
                        scope: String::new(),
                        location: "retained-artifact".into(),
                        size_bytes: 1,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let (family, previous) = session
            .register_snapshot_preparation("tenant", &name, previous_spec.resources)
            .await
            .unwrap();
        session
            .update_snapshot_template(&family.id, |template| {
                template.preparation.state = PreparationState::Ready;
                template.preparation.spec = Some(previous_spec.clone());
                template.preparation.snapshot_id = Some(retained_snapshot.clone());
                Ok(())
            })
            .await
            .unwrap();
        let mut bad = spec(&format!("bad-seed-{index}"));
        bad.env.insert("FAIL_START".into(), failure.into());
        let (_, build) = session
            .register_snapshot_preparation("tenant", &name, bad.resources)
            .await
            .unwrap();
        session
            .update_snapshot_template(&family.id, |template| {
                template.preparation.state = PreparationState::Creating;
                template.preparation.spec = Some(bad.clone());
                template.preparation.cleanup_pending = true;
                Ok(())
            })
            .await
            .unwrap();

        tokio::time::timeout(
            Duration::from_secs(5),
            coordinator.reconcile_snapshot_preparations(),
        )
        .await
        .unwrap()
        .unwrap();
        let failed = templates
            .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
                caller: caller(),
                template_id: family.id.clone(),
                preparation_id: build.id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(failed.phase, pb::SnapshotPreparationPhase::Failed as i32);
        assert!(failed.error.contains(reason), "{}", failed.error);
        assert!(matches!(
            session.get(&bad.id).await,
            Err(adx_core::Error::NotFound)
        ));
        let retained = templates
            .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
                caller: caller(),
                reference: name,
                registration_only: false,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(retained.preparation_id, previous.id);
        assert_eq!(retained.snapshot_id, retained_snapshot);
        assert_eq!(retained.phase, pb::SnapshotPreparationPhase::Ready as i32);
        coordinator.reconcile_snapshot_preparations().await.unwrap();
        let cleaned = session
            .get_snapshot_template("tenant", &family.id)
            .await
            .unwrap();
        assert_eq!(cleaned.preparation.state, PreparationState::Failed);
        assert!(!cleaned.preparation.cleanup_pending);
        assert!(cleaned.preparation.spec.is_none());
        assert!(session.get_snapshot(&retained_snapshot).await.is_ok());
        assert_eq!(backend.started.load(Ordering::SeqCst), index + 1);
        assert!(backend.running.lock().unwrap().is_empty());
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn preparation_rpc_keeps_registration_distinct_from_ready_and_tenant_scoped() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(
                pb::snapshot_preparation_service_server::SnapshotPreparationServiceServer::new(rpc),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        channel(address, "apiserver").await,
    );
    let input = pb::RegisterSnapshotPreparationRequest {
        caller: caller(),
        name: "image".into(),
        resources: Some(pb::Resources {
            cpu_millis: 1000,
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 0,
        }),
    };
    let first = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap()
        .into_inner();
    let busy = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap_err();
    assert!(busy.message().contains("busy"));
    let bare = pb::ResolveSnapshotTemplateRequest {
        caller: caller(),
        reference: "image".into(),
        registration_only: true,
    };
    let pending = client
        .resolve_snapshot_template(bare.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pending.template_id, first.template_id);
    assert_eq!(
        pending.phase,
        pb::SnapshotPreparationPhase::Registered as i32
    );
    assert!(pending.snapshot_id.is_empty());
    let mut create = bare.clone();
    create.registration_only = false;
    assert_eq!(
        client
            .resolve_snapshot_template(create)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut foreign = bare;
    foreign.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        client
            .resolve_snapshot_template(foreign)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    session
        .update_snapshot_template(&first.template_id, |template| {
            let build = &mut template.preparation;
            build.state = adx_core::snapshot_preparation::PreparationState::Failed;
            build.error = Some("failed input".into());
            Ok(())
        })
        .await
        .unwrap();
    let failed = client
        .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
            caller: caller(),
            template_id: first.template_id.clone(),
            preparation_id: first.preparation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(failed.phase, pb::SnapshotPreparationPhase::Failed as i32);
    let missing_capture = session
        .update_snapshot_template(&first.template_id, |template| {
            let build = &mut template.preparation;
            build.state = adx_core::snapshot_preparation::PreparationState::Ready;
            build.snapshot_id = Some("snapshot-missing".into());
            build.cleanup_pending = false;
            Ok(())
        })
        .await;
    assert_eq!(missing_capture.unwrap_err(), adx_core::Error::Conflict);
    let unavailable = client
        .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
            caller: caller(),
            reference: "image".into(),
            registration_only: false,
        })
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
    // Usage restriction is caller policy. Unlinking keeps the independent Golden.
    let golden = adx_core::snapshots::Snapshot::new_with_kind(
        adx_core::snapshots::SnapshotKind::Golden,
        "snapshot-missing".into(),
        vec!["image".into()],
        spec("source"),
        "node".into(),
        "source-1".into(),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            scope: String::new(),
            location: "artifact".into(),
            size_bytes: 1,
        },
    )
    .unwrap();
    session.publish_snapshot(golden).await.unwrap();
    session
        .update_snapshot_template(&first.template_id, |template| {
            let build = &mut template.preparation;
            build.state = adx_core::snapshot_preparation::PreparationState::Ready;
            build.spec = Some(spec("source"));
            build.snapshot_id = Some("snapshot-missing".into());
            build.error = None;
            Ok(())
        })
        .await
        .unwrap();
    let second = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.template_id, first.template_id);
    assert_ne!(second.preparation_id, first.preparation_id);
    let latest_successful = client
        .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
            caller: caller(),
            template_id: first.template_id.clone(),
            preparation_id: first.preparation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        latest_successful.phase,
        pb::SnapshotPreparationPhase::Ready as i32
    );
    assert_eq!(
        latest_successful.resources.as_ref().unwrap().cpu_millis,
        spec("source").resources.cpu_millis
    );
    session
        .update_snapshot_template(&first.template_id, |template| {
            template.preparation.state = adx_core::snapshot_preparation::PreparationState::Failed;
            Ok(())
        })
        .await
        .unwrap();
    let third = client
        .register_snapshot_preparation(input)
        .await
        .unwrap()
        .into_inner();
    let selected = client
        .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
            caller: caller(),
            reference: "image".into(),
            registration_only: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(selected.preparation_id, first.preparation_id);
    assert_eq!(selected.snapshot_id, "snapshot-missing");
    assert_eq!(
        client
            .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
                caller: caller(),
                template_id: first.template_id.clone(),
                preparation_id: second.preparation_id,
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
                caller: caller(),
                template_id: first.template_id.clone(),
                preparation_id: third.preparation_id,
            })
            .await
            .unwrap()
            .into_inner()
            .phase,
        pb::SnapshotPreparationPhase::Registered as i32
    );

    session
        .acquire_snapshot(
            "snapshot-missing",
            "tenant",
            adx_core::snapshots::Reference::Template {
                node_id: "node".into(),
                template_id: "active-clone".into(),
            },
        )
        .await
        .unwrap();
    let mut delete = pb::DeleteSnapshotTemplateRequest {
        caller: caller(),
        template_id: first.template_id,
        require_unused: true,
    };
    assert_eq!(
        client
            .delete_snapshot_template(delete.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    delete.require_unused = false;
    client
        .delete_snapshot_template(delete.clone())
        .await
        .unwrap();
    let retained = session.get_snapshot("snapshot-missing").await.unwrap();
    assert_eq!(retained.kind, adx_core::snapshots::SnapshotKind::Golden);
    assert_eq!(retained.references.len(), 1);
    assert_eq!(
        client
            .delete_snapshot_template(delete)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
                caller: caller(),
                reference: "image".into(),
                registration_only: true,
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}
