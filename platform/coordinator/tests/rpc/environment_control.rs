use super::*;
use adx_core::{Assignment, EnvironmentState};
use adx_protocol::control::coordinator_service_server::CoordinatorService;

fn api_request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    request
}

#[tokio::test]
#[ignore = "requires isolated Redis; run control-rpc suite"]
async fn paused_controls_use_native_creation_and_retention_when_owner_is_offline() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    session
        .register_session(
            adx_coordinator::Node {
                id: "offline".into(),
                capacity: spec("paused").resources,
                available: true,
                runtime_classes: vec!["runc".into()],
                labels: Default::default(),
                devices: vec![],
            },
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "old-session".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let mut expected = Vec::new();
    for (id, expiry) in [
        ("paused-finite", Some(2_000_000_000)),
        ("paused-forever", None),
    ] {
        let mut configured = spec(id);
        configured.run_policy = Some(adx_core::environment_control::RunPolicy {
            timeout_seconds: 60,
            action: adx_core::environment_control::RunExpiryAction::Pause,
        });
        let assignment = Assignment {
            environment_id: id.into(),
            node_id: "offline".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        session
            .reserve(configured.clone(), assignment.clone())
            .await
            .unwrap();
        let mut record = EnvironmentRecord {
            spec: configured,
            assignment,
            state: EnvironmentState::Running,
            revision: 2,
            runtime: adx_core::Runtime {
                id: format!("{id}-1"),
                ip: Some("10.0.0.2".parse().unwrap()),
            },
            resources_held: true,
            checkpoint: None,
            last_operation: None,
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
        };
        session.commit(record.clone()).await.unwrap();
        let creation = session.get(id).await.unwrap().creation.unwrap();
        let first_running = creation.settled_at_unix_millis.unwrap();
        assert!(first_running > 0);
        record.state = EnvironmentState::Paused;
        record.revision += 1;
        record.resources_held = false;
        record.runtime.ip = None;
        record.checkpoint = Some(adx_core::RestorePoint {
            recovery_profile: Default::default(),
            id: format!("pause-{id}"),
            origin: None,
            expires_at_unix_seconds: expiry,
            source_runtime_id: record.runtime.id.clone(),
            artifact: adx_core::CheckpointArtifact {
                scope: String::new(),
                storage: "obs".into(),
                location: format!("artifact-{id}"),
                size_bytes: 8,
            },
        });
        session.commit(record.clone()).await.unwrap();
        expected.push((
            id,
            first_running,
            expiry.unwrap_or_default() * 1000,
            record.revision,
        ));
    }
    session
        .invalidate_node("offline", "old-session")
        .await
        .unwrap();
    for (id, _, _, revision) in &mut expected {
        *revision = session.get(id).await.unwrap().result.unwrap().revision;
    }
    let rpc = CoordinatorRpc::new(
        session,
        Placement::Pack,
        Peers::network(),
        ClientTlsConfig::new(),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
    for &(id, first_running, expiry, revision) in &expected {
        let control = rpc
            .get_environment_control(api_request(pb::GetEnvironmentControlRequest {
                id: id.into(),
                caller: caller(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(control.started_at_unix_ms, first_running);
        assert_eq!(control.expires_at_unix_ms, expiry);
        assert_eq!(control.revision, revision);
        assert_eq!(control.generation, 1);
    }
    let batch = rpc
        .batch_get_environment_control(api_request(pb::BatchGetEnvironmentControlRequest {
            ids: vec![
                "paused-finite".into(),
                "deleted-between-list-and-batch".into(),
                "paused-forever".into(),
            ],
            caller: caller(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(batch.records.len(), 2);
    for (id, first_running, expiry, _) in expected {
        let control = batch
            .records
            .iter()
            .find(|control| control.environment_id == id)
            .unwrap();
        assert_eq!(control.started_at_unix_ms, first_running);
        assert_eq!(control.expires_at_unix_ms, expiry);
    }
    assert_eq!(
        rpc.update_environment_control(api_request(pb::UpdateEnvironmentControlRequest {
            id: "paused-finite".into(),
            caller: caller(),
            timeout_seconds: 120,
            extend_only: true,
        }))
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        rpc.batch_get_environment_control(api_request(pb::BatchGetEnvironmentControlRequest {
            ids: vec!["paused-finite".into()],
            caller: Some(pb::CallerContext {
                tenant_id: "other".into(),
                administrator: false
            }),
        }))
        .await
        .unwrap_err()
        .code(),
        tonic::Code::PermissionDenied
    );
}
