use super::*;
use adx_core::{CheckpointArtifact, Error};
use adx_protocol::{auth::Peers, control as pb, control::node_service_server::NodeService};
use adxlet::{journal::JournalSink, routes::UdsRoutes};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};

type Publication = (EnvironmentRecord, Vec<String>, Vec<CheckpointArtifact>);

#[derive(Default)]
struct Sink {
    fail: AtomicBool,
    lose_reply: AtomicBool,
    publications: Mutex<Vec<Publication>>,
    node: Mutex<Weak<Adxlet>>,
    expected_cpu: Mutex<Option<u64>>,
    catalog: Mutex<Option<Arc<Mutex<Vec<pb::EnvironmentRecord>>>>>,
}
impl Sink {
    fn publish(
        &self,
        record: &EnvironmentRecord,
        stopped: &[String],
        artifacts: &[CheckpointArtifact],
    ) -> Result<Durability> {
        if let Some(expected) = *self.expected_cpu.lock().unwrap() {
            assert_eq!(
                self.node
                    .lock()
                    .unwrap()
                    .upgrade()
                    .unwrap()
                    .used()
                    .cpu_millis,
                expected,
                "all execution reservations must precede any publication"
            );
        }
        if record.spec.id == "busy" && self.fail.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("publication unavailable".into()));
        }
        let publication = (record.clone(), stopped.to_vec(), artifacts.to_vec());
        let mut publications = self.publications.lock().unwrap();
        if !publications.contains(&publication) {
            publications.push(publication);
        }
        if let Some(catalog) = &*self.catalog.lock().unwrap() {
            let mut catalog = catalog.lock().unwrap();
            catalog.retain(|r| r.spec.as_ref().unwrap().id != record.spec.id);
            catalog.push(record.clone().try_into().unwrap());
        }
        if record.spec.id == "busy" && self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(Error::Unavailable("committed reply lost".into()));
        }
        Ok(Durability::Published)
    }
}
#[async_trait]
impl StateSink for Sink {
    async fn commit(&self, r: &EnvironmentRecord) -> Result<Durability> {
        self.publish(r, &[], &[])
    }
    async fn commit_releasing(
        &self,
        r: &EnvironmentRecord,
        stopped: &[String],
    ) -> Result<Durability> {
        self.publish(r, stopped, &[])
    }
    async fn commit_deleted(
        &self,
        r: &EnvironmentRecord,
        artifacts: &[CheckpointArtifact],
        stopped: &[String],
    ) -> Result<Durability> {
        self.publish(r, stopped, artifacts)
    }
}

fn running(id: &str) -> EnvironmentRecord {
    let mut r = record(id, EnvironmentState::Running);
    r.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
        timeout_seconds: 3600,
        action: adx_core::environment_control::RunExpiryAction::Delete,
    });
    r
}
fn node(backend: &Arc<Backend>, routes: Arc<dyn Routes>, sink: Arc<dyn StateSink>) -> Arc<Adxlet> {
    let node = Arc::new(Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        routes,
        sink,
    ));
    node.update_capacity(
        Resources {
            cpu_millis: 1000,
            memory_bytes: 1000,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    node
}

#[tokio::test]
async fn restarting_adxlet_keeps_failed_owned_binding_retryable_on_existing_relay() {
    use data_plane_gateway::{
        common::protocol::GatewayPolicy,
        node::{
            route_control::{proto::relay_service_server::RelayServiceServer, BindingService},
            Relay,
        },
    };
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("relay.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let relay = Arc::new(Relay::new(GatewayPolicy::new(vec![])).with_route_enforcement());
    let service = BindingService::new(relay.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RelayServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .unwrap();
    });
    let backend = Arc::new(Backend::default());
    let healthy = running("healthy");
    let busy = running("busy");
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    let build = || {
        node(
            &backend,
            Arc::new(UdsRoutes::new(socket.clone(), Duration::from_secs(1)).unwrap()),
            backend.clone(),
        )
    };
    let original = build();
    authority.cycle(&original).await.unwrap();
    drop(original);
    tokio::task::yield_now().await;
    let restored = build();
    *backend.recovery_fault.lock().unwrap() = Some("readiness");
    assert!(authority.cycle(&restored).await.is_err());
    assert!(relay.ready());
    failure_domains::control(&restored, &healthy).await.unwrap();
    *backend.recovery_fault.lock().unwrap() = None;
    for _ in 0..5 {
        let _ = authority.cycle(&restored).await;
    }
    failure_domains::control(&restored, &busy)
        .await
        .expect("same-generation binding must reactivate after readiness recovers");
    assert!(relay.ready());
    drop(restored);
    let mut current = busy.clone();
    current.assignment.generation += 1;
    current.runtime.id = "busy-8".into();
    let mut successor = observed("busy");
    successor.generation = 8;
    successor.runtime_id = current.runtime.id.clone();
    backend.actual.lock().unwrap().push(successor);
    *authority.authority.records.lock().unwrap() = vec![
        current.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    let replacement = build();
    authority.registration.reconciling = true;
    for _ in 0..3 {
        let _ = authority.cycle(&replacement).await;
    }
    failure_domains::control(&replacement, &current)
        .await
        .unwrap();
    assert!(
        !backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.runtime_id == busy.runtime.id),
        "confirmed successor binding must allow old execution cleanup"
    );
    assert_eq!(replacement.used().cpu_millis, 4);
    assert!(replacement.accepting_allocations());
    assert!(authority.registration.accepting_allocations);
    server.abort();
}

#[tokio::test]
async fn same_session_accepts_a_new_environment_after_reconciling_an_unconfirmed_delete() {
    use data_plane_gateway::{
        common::protocol::GatewayPolicy,
        node::{route_control::serve_route_control, ActivityTracker, Relay},
    };
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("relay.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let relay = Relay::new(GatewayPolicy::new(vec![]))
        .with_route_enforcement()
        .with_activity_tracker(Arc::new(ActivityTracker::new("session")));
    let server = tokio::spawn(serve_route_control(Arc::new(relay), listener));
    let routes = Arc::new(UdsRoutes::new(socket, Duration::from_secs(1)).unwrap());
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let journal = Arc::new(JournalSink::new(
        root.path().join("journal.sqlite"),
        sink.clone(),
    ));
    let node = node(&backend, routes.clone(), journal.clone());
    let mut authority = authority::Fixture::new().await;
    let busy = running("busy");
    *backend.actual.lock().unwrap() = vec![observed("busy")];
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(busy.clone().try_into().unwrap());
    authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap();
    routes.activity(&busy).await.unwrap();

    sink.fail.store(true, Ordering::SeqCst);
    let handle = node
        .environment(busy.spec.clone(), busy.assignment.clone())
        .unwrap();
    assert!(matches!(handle.delete().await, Err(Error::Unavailable(_))));
    let local = handle.sync().await.unwrap();
    assert_eq!(local.durability, Durability::Journaled);
    assert!(journal.pending().await.unwrap() > 0);

    let mut failed = busy;
    failed.revision += 1;
    failed.state = EnvironmentState::Failed;
    failed.resources_held = false;
    failed.runtime.ip = None;
    assert!(local.record.revision > failed.revision);
    *authority.authority.records.lock().unwrap() = vec![failed.clone().try_into().unwrap()];
    sink.fail.store(false, Ordering::SeqCst);
    authority
        .authority
        .requires_handshake
        .store(true, Ordering::SeqCst);
    let fenced = authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap_err();
    assert_eq!(fenced.code(), tonic::Code::FailedPrecondition);
    assert!(!node.accepting_allocations());
    authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap();

    assert!(node.accepting_allocations() && authority.registration.accepting_allocations);
    assert_eq!(journal.pending().await.unwrap(), 0);
    assert!(backend.actual.lock().unwrap().is_empty());
    assert_eq!(node.used(), Resources::default());
    assert_eq!(routes.activity(&failed).await.unwrap_err(), Error::Conflict);
    backend.allow_start.store(true, Ordering::SeqCst);
    let fresh = record("fresh", EnvironmentState::Pending);
    let created = node
        .environment(fresh.spec, fresh.assignment)
        .unwrap()
        .create()
        .await
        .unwrap();
    assert_eq!(created.record.state, EnvironmentState::Running);
    assert_eq!(created.durability, Durability::Published);
    routes.activity(&created.record).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn takeover_spec_conflict_fences_only_its_controller() {
    let backend = Arc::new(Backend::default());
    let node = manager(&backend);
    let healthy = running("healthy");
    let busy = running("busy");
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    authority.cycle(&node).await.unwrap();
    let mut conflicting = busy.clone();
    conflicting.spec.image = "different-image".into();
    *authority.authority.records.lock().unwrap() = vec![
        conflicting.try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    authority.registration.reconciling = true;
    for _ in 0..2 {
        let _ = authority.cycle(&node).await;
        failure_domains::control(&node, &healthy).await.unwrap();
        assert!(failure_domains::control(&node, &busy).await.is_err());
        assert_eq!(node.used().cpu_millis, 4);
        assert!(node.accepting_allocations());
        assert!(authority.registration.accepting_allocations);
    }
    failure_domains::delete(&node, &healthy).await;
    *authority.authority.records.lock().unwrap() = vec![busy.clone().try_into().unwrap()];
    for _ in 0..5 {
        let _ = authority.cycle(&node).await;
    }
    failure_domains::control(&node, &busy).await.unwrap();
}

#[tokio::test]
async fn terminal_journal_recovery_preserves_cleanup_details_without_repeating_runtime_deletion() {
    for lost_reply in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Backend::default());
        let sink = Arc::new(Sink::default());
        let path = root.path().join("journal.sqlite");
        let busy = running("busy");
        let healthy = running("healthy");
        let mut deleted = busy.clone();
        deleted.state = EnvironmentState::Deleted;
        deleted.resources_held = false;
        deleted.runtime.ip = None;
        deleted.revision += 1;
        let artifact = CheckpointArtifact {
            storage: "local".into(),
            scope: String::new(),
            location: root
                .path()
                .join("old-checkpoint")
                .to_string_lossy()
                .into_owned(),
            size_bytes: 1,
        };
        let stopped = vec![busy.runtime.id.clone()];
        sink.fail.store(!lost_reply, Ordering::SeqCst);
        sink.lose_reply.store(lost_reply, Ordering::SeqCst);
        let previous = JournalSink::new(path.clone(), sink.clone());
        assert_eq!(
            previous
                .commit_deleted(&deleted, std::slice::from_ref(&artifact), &stopped)
                .await
                .unwrap(),
            Durability::Journaled
        );
        drop(previous);
        sink.fail.store(true, Ordering::SeqCst);
        let journal = Arc::new(JournalSink::new(path, sink.clone()));
        let restored = node(&backend, backend.clone(), journal.clone());
        *sink.node.lock().unwrap() = Arc::downgrade(&restored);
        *backend.actual.lock().unwrap() = vec![observed("healthy")];
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![
            busy.try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
        ];
        *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
        failure_domains::control(&restored, &healthy).await.unwrap();
        assert_eq!(journal.pending().await.unwrap(), 1);
        sink.fail.store(false, Ordering::SeqCst);
        for _ in 0..5 {
            let _ = authority
                .cycle_with_journal(&restored, Some(&journal))
                .await;
        }
        assert_eq!(
            journal.pending().await.unwrap(),
            0,
            "original stopped-runtime and artifact cleanup details must replay after restart"
        );
        assert!(sink
            .publications
            .lock()
            .unwrap()
            .contains(&(deleted, stopped, vec![artifact])));
        assert_eq!(
            backend
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.as_str() == "remove:busy-7")
                .count(),
            0,
            "completed physical deletion must not execute again during journal replay"
        );
    }
}

#[tokio::test]
async fn journaled_network_update_survives_takeover_and_restart_without_fencing_healthy_env() {
    for restart in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let backend = Arc::new(Backend::default());
        let sink = Arc::new(Sink::default());
        let path = root.path().join("journal.sqlite");
        let journal = Arc::new(JournalSink::new(path.clone(), sink.clone()));
        let original = node(&backend, backend.clone(), journal.clone());
        let busy = running("busy");
        let healthy = running("healthy");
        *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![
            busy.clone().try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
        ];
        *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
        authority
            .cycle_with_journal(&original, Some(&journal))
            .await
            .unwrap();
        sink.fail.store(true, Ordering::SeqCst);
        let rpc = adxlet::rpc::NodeRpc::new(original.clone(), Peers::network(), "session".into());
        let policy = adx_core::execution::NetworkPolicy {
            dns: None,
            traffic: Some(adx_core::execution::TrafficPolicy {
                ingress_default_action: adx_core::execution::NetworkAction::Deny,
                egress_default_action: adx_core::execution::NetworkAction::Allow,
                mode: adx_core::execution::TrafficMode::Stateful,
                rules: vec![],
            }),
        };
        let mut request = tonic::Request::new(pb::UpdateNetworkPolicyRequest {
            assignment: Some(busy.assignment.clone().try_into().unwrap()),
            caller: Some(pb::CallerContext {
                tenant_id: "t".into(),
                administrator: false,
            }),
            network: Some(policy.clone().into()),
            operation_id: "change-network".into(),
            expected_revision: busy.revision,
        });
        request
            .metadata_mut()
            .insert("adx-component", "apiserver".parse().unwrap());
        rpc.update_network_policy(request).await.unwrap();
        drop(rpc);
        assert_eq!(journal.pending().await.unwrap(), 1);
        let (restored, journal) = if restart {
            drop(original);
            drop(journal);
            let journal = Arc::new(JournalSink::new(path, sink.clone()));
            (node(&backend, backend.clone(), journal.clone()), journal)
        } else {
            (original, journal)
        };
        *sink.node.lock().unwrap() = Arc::downgrade(&restored);
        *sink.expected_cpu.lock().unwrap() = Some(4);
        authority.registration.reconciling = true;
        for _ in 0..2 {
            let _ = authority
                .cycle_with_journal(&restored, Some(&journal))
                .await;
            failure_domains::control(&restored, &healthy).await.unwrap();
            assert_eq!(
                journal.pending().await.unwrap(),
                1,
                "mutable Spec difference must not acknowledge a pending update"
            );
            assert_eq!(restored.used().cpu_millis, 4);
            assert!(restored.accepting_allocations());
        }
        sink.fail.store(false, Ordering::SeqCst);
        for _ in 0..5 {
            let _ = authority
                .cycle_with_journal(&restored, Some(&journal))
                .await;
        }
        assert_eq!(journal.pending().await.unwrap(), 0);
        assert!(sink.publications.lock().unwrap().iter().any(
            |(r, _, _)| r.spec.id == "busy" && r.spec.execution.network == Some(policy.clone())
        ));
        failure_domains::control(&restored, &busy).await.unwrap();
        *sink.expected_cpu.lock().unwrap() = None;
        failure_domains::delete(&restored, &healthy).await;
    }
}

#[tokio::test]
async fn running_journal_restores_cpu_and_device_reservations_before_publication_over_capacity() {
    use adx_core::{
        scheduling::{Device, DeviceAllocation, DeviceKind, DeviceRequest},
        RestorePoint,
    };
    use adxlet::checkpoint::{CheckpointAccess, LocalCheckpointStore};
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let storage = Arc::new(LocalCheckpointStore::new(root.path().join("checkpoints")).unwrap());
    let capture = storage.begin_capture().await.unwrap();
    std::fs::write(capture.join("state"), b"checkpoint data").unwrap();
    let artifact = storage.seal(&capture).await.unwrap();
    let healthy = running("healthy");
    let mut paused = running("busy");
    paused.spec.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("test".into()),
        count: 1,
    }];
    paused.assignment.devices = vec![DeviceAllocation {
        kind: DeviceKind::Gpu,
        id: 0,
        model: "test".into(),
    }];
    paused.state = EnvironmentState::Paused;
    paused.resources_held = false;
    paused.runtime.ip = None;
    paused.checkpoint = Some(RestorePoint {
        origin: None,
        recovery_profile: Default::default(),
        id: "checkpoint".into(),
        artifact: artifact.clone(),
        expires_at_unix_seconds: None,
        source_runtime_id: paused.runtime.id.clone(),
    });
    let mut resumed = paused.clone();
    resumed.state = EnvironmentState::Running;
    resumed.resources_held = true;
    resumed.runtime.ip = Some("10.0.0.3".parse().unwrap());
    resumed.revision += 2;
    resumed.runtime.id = "busy-7-r3".into();
    resumed.execution_artifacts = vec![artifact];
    let path = root.path().join("journal.sqlite");
    sink.fail.store(true, Ordering::SeqCst);
    let previous = JournalSink::new(path.clone(), sink.clone());
    assert_eq!(
        previous.commit(&resumed).await.unwrap(),
        Durability::Journaled
    );
    drop(previous);
    let journal = Arc::new(JournalSink::new(path, sink.clone()));
    let restored = Arc::new(
        Adxlet::new(
            "n".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            journal.clone(),
        )
        .with_checkpointing(storage, backend.clone())
        .unwrap(),
    );
    restored
        .update_capacity(
            Resources {
                cpu_millis: 3,
                memory_bytes: 3,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    restored
        .update_devices(
            vec![Device {
                id: 0,
                kind: DeviceKind::Gpu,
                model: "test".into(),
                healthy: true,
            }],
            Duration::from_secs(60),
        )
        .unwrap();
    *sink.node.lock().unwrap() = Arc::downgrade(&restored);
    *sink.expected_cpu.lock().unwrap() = Some(4);
    let mut actual = observed("busy");
    actual.runtime_id = resumed.runtime.id.clone();
    *backend.actual.lock().unwrap() = vec![actual, observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        paused.try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
    for _ in 0..3 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
        failure_domains::control(&restored, &healthy).await.unwrap();
        assert_eq!(restored.used().cpu_millis, 4);
        assert!(
            !restored.accepting_allocations(),
            "over-capacity recovered usage must restrict new allocation"
        );
        assert!(!authority.registration.accepting_allocations);
    }
    sink.fail.store(false, Ordering::SeqCst);
    for _ in 0..5 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
    }
    assert_eq!(journal.pending().await.unwrap(), 0);
    assert_eq!(restored.used().cpu_millis, 4);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.starts_with("start:")));
    *sink.expected_cpu.lock().unwrap() = None;
    restored
        .update_capacity(
            Resources {
                cpu_millis: 10,
                memory_bytes: 10,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    authority
        .cycle_with_journal(&restored, Some(&journal))
        .await
        .unwrap();
    assert!(restored.accepting_allocations());
    let mut fresh = record("fresh", EnvironmentState::Pending);
    fresh.spec.scheduling.devices = resumed.spec.scheduling.devices.clone();
    fresh.assignment.devices = resumed.assignment.devices.clone();
    let handle = restored.environment(fresh.spec, fresh.assignment).unwrap();
    assert_eq!(
        handle.create().await.unwrap_err(),
        Error::NoCapacity,
        "the recovered device must still be reserved exactly once"
    );
    assert_eq!(restored.used().cpu_millis, 4);
}

#[tokio::test]
async fn revoked_journal_only_accounts_and_cleans_the_old_execution() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let old = running("busy");
    let healthy = running("healthy");
    let path = root.path().join("journal.sqlite");
    sink.fail.store(true, Ordering::SeqCst);
    let previous = JournalSink::new(path.clone(), sink.clone());
    previous.commit(&old).await.unwrap();
    drop(previous);
    sink.fail.store(false, Ordering::SeqCst);
    let mut current = old.clone();
    current.assignment.generation += 1;
    current.runtime.id = "busy-8".into();
    let mut newer = observed("busy");
    newer.generation = 8;
    newer.runtime_id = current.runtime.id.clone();
    *backend.actual.lock().unwrap() = vec![observed("busy"), newer, observed("healthy")];
    *backend.recovery_fault.lock().unwrap() = Some("remove");
    let journal = Arc::new(JournalSink::new(path, sink.clone()));
    let restored = node(&backend, backend.clone(), journal.clone());
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        current.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
    for _ in 0..3 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
        failure_domains::control(&restored, &healthy).await.unwrap();
        failure_domains::control(&restored, &current).await.unwrap();
        assert!(failure_domains::control(&restored, &old).await.is_err());
        assert_eq!(
            restored.used().cpu_millis,
            6,
            "old physical occupancy must remain accounted while cleanup fails"
        );
        assert_eq!(journal.pending().await.unwrap(), 0);
        assert!(restored.accepting_allocations());
    }
    assert!(!sink
        .publications
        .lock()
        .unwrap()
        .iter()
        .any(|(r, _, _)| r.assignment == old.assignment));
    *backend.recovery_fault.lock().unwrap() = None;
    for _ in 0..5 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
    }
    assert_eq!(restored.used().cpu_millis, 4);
    assert!(!backend
        .actual
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.runtime_id == old.runtime.id));
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.starts_with("start:")));
}

#[tokio::test]
async fn stopped_journal_cannot_release_a_still_observed_execution_before_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let busy = running("busy");
    let healthy = running("healthy");
    let mut stopped = busy.clone();
    stopped.state = EnvironmentState::Deleted;
    stopped.resources_held = false;
    stopped.runtime.ip = None;
    stopped.revision += 1;
    let path = root.path().join("journal.sqlite");
    sink.fail.store(true, Ordering::SeqCst);
    let previous = JournalSink::new(path.clone(), sink.clone());
    previous
        .commit_releasing(&stopped, std::slice::from_ref(&busy.runtime.id))
        .await
        .unwrap();
    drop(previous);
    let journal = Arc::new(JournalSink::new(path, sink.clone()));
    let restored = node(&backend, backend.clone(), journal.clone());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    *backend.recovery_fault.lock().unwrap() = Some("remove");
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
    for _ in 0..2 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
        failure_domains::control(&restored, &healthy).await.unwrap();
        assert_eq!(restored.used().cpu_millis, 4);
        assert_eq!(journal.pending().await.unwrap(), 1);
    }
    *backend.recovery_fault.lock().unwrap() = None;
    let _ = authority
        .cycle_with_journal(&restored, Some(&journal))
        .await;
    assert_eq!(restored.used().cpu_millis, 2);
    assert_eq!(
        journal.pending().await.unwrap(),
        1,
        "confirmed physical cleanup and publication are separate conditions"
    );
    sink.fail.store(false, Ordering::SeqCst);
    for _ in 0..5 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
    }
    assert_eq!(journal.pending().await.unwrap(), 0);
}

#[tokio::test]
async fn healthy_lifecycle_and_new_allocations_do_not_wait_for_a_peers_publication() {
    use adxlet::checkpoint::{LocalCheckpointStore, PauseRequest};
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let restored = Arc::new(
        Adxlet::new(
            "n".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
        )
        .with_checkpointing(
            Arc::new(LocalCheckpointStore::new(root.path().into()).unwrap()),
            backend.clone(),
        )
        .unwrap(),
    );
    restored
        .update_capacity(
            Resources {
                cpu_millis: 1000,
                memory_bytes: 1000,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    let healthy = running("healthy");
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    backend.block_commit.store(true, Ordering::SeqCst);
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        running("busy").try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    let reports = authority.authority.reports.clone();
    let recovery = tokio::spawn({
        let restored = restored.clone();
        async move { authority.cycle(&restored).await }
    });
    backend.commit_entered.notified().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if failure_domains::control(&restored, &healthy).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !reports
            .lock()
            .unwrap()
            .last()
            .is_some_and(|r| !r.reconciling && r.accepting_allocations)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("phase-one allocation trust must be reported before a peer publishes");
    assert!(restored.accepting_allocations());
    backend.allow_start.store(true, Ordering::SeqCst);
    let fresh = record("fresh", EnvironmentState::Pending);
    let fresh = restored
        .environment(fresh.spec, fresh.assignment)
        .expect("trusted reservations must permit unrelated allocations before a peer publishes");
    fresh.create().await.unwrap();
    fresh.delete().await.unwrap();
    backend.block_checkpoint.store(true, Ordering::SeqCst);
    *backend.checkpoint_gate.lock().unwrap() = Some(healthy.runtime.id.clone());
    let pausing = tokio::spawn({
        let restored = restored.clone();
        let healthy = healthy.clone();
        async move {
            restored
                .environment(healthy.spec, healthy.assignment)
                .unwrap()
                .pause(PauseRequest {
                    operation_id: "pause-during-peer-recovery".into(),
                    expected_revision: healthy.revision,
                    retention: adx_core::lifecycle::RetentionPolicy::Forever,
                    timeout_seconds: 30,
                    storage: None,
                })
                .await
        }
    });
    backend.checkpoint_entered.notified().await;
    let capture = backend.checkpoint_path.lock().unwrap().clone().unwrap();
    backend.commit_release.notify_one();
    recovery.await.unwrap().unwrap();
    assert!(
        capture.join("state").is_file(),
        "recovery GC must not scan a newly admitted capture"
    );
    backend.checkpoint_release.notify_one();
    assert_eq!(
        pausing.await.unwrap().unwrap().record.state,
        EnvironmentState::Paused
    );
}

#[tokio::test]
async fn missing_input_configuration_only_fences_its_environment_after_accounting() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let restored = node(&backend, backend.clone(), backend.clone());
    let mut busy = running("busy");
    busy.execution_artifacts.push(CheckpointArtifact {
        storage: "local".into(),
        scope: String::new(),
        location: root.path().to_string_lossy().into_owned(),
        size_bytes: 1,
    });
    let healthy = running("healthy");
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    for _ in 0..2 {
        let _ = authority.cycle(&restored).await;
        failure_domains::control(&restored, &healthy).await.unwrap();
        assert!(failure_domains::control(&restored, &busy).await.is_err());
        assert_eq!(restored.used().cpu_millis, 4);
        assert!(restored.accepting_allocations());
    }
}

#[tokio::test]
async fn slow_missing_input_download_does_not_block_healthy_admission_or_reports() {
    use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
    use object_store::{
        memory::InMemory,
        throttle::{ThrottleConfig, ThrottledStore},
    };
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let build = || {
        ObjectCheckpointStore::new_scoped(
            "shared".into(),
            "scope".into(),
            remote.clone(),
            "test".into(),
            root.path().into(),
            128 * 1024 * 1024,
        )
        .unwrap()
    };
    let previous = build();
    let staged = previous.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"running input").unwrap();
    let artifact = previous.seal(&staged).await.unwrap();
    previous.committed(&artifact).await.unwrap();
    drop(previous);
    std::fs::remove_dir_all(&staged).unwrap();
    remote.config_mut(|config| config.wait_get_per_call = Duration::from_secs(1));
    let storage = Arc::new(build());
    let backend = Arc::new(Backend::default());
    let restored = Arc::new(
        Adxlet::new(
            "n".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
        )
        .with_checkpointing(storage.clone(), backend.clone())
        .unwrap(),
    );
    restored
        .update_capacity(
            Resources {
                cpu_millis: 1000,
                memory_bytes: 1000,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    let mut busy = running("busy");
    busy.execution_artifacts.push(artifact.clone());
    let healthy = running("healthy");
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    let reports = authority.authority.reports.clone();
    let recovery = tokio::spawn({
        let restored = restored.clone();
        async move { authority.cycle(&restored).await }
    });
    tokio::time::timeout(Duration::from_millis(400), async {
        loop {
            if restored.used().cpu_millis == 4
                && failure_domains::control(&restored, &healthy).await.is_ok()
                && reports
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|r| !r.reconciling && r.accepting_allocations)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("healthy control and trusted capacity reports cannot wait for a peer's input download");
    assert!(restored.accepting_allocations());
    assert!(!recovery.is_finished());
    assert!(
        matches!(storage.release(&artifact).await, Err(Error::Conflict)),
        "the input reference must already be protected while its files are missing"
    );
    backend.allow_start.store(true, Ordering::SeqCst);
    let fresh = record("fresh", EnvironmentState::Pending);
    let handle = restored.environment(fresh.spec, fresh.assignment).unwrap();
    handle.create().await.unwrap();
    handle.delete().await.unwrap();
    assert_eq!(restored.used().cpu_millis, 4);
    recovery.await.unwrap().unwrap();
    failure_domains::control(&restored, &healthy).await.unwrap();
    failure_domains::control(&restored, &busy).await.unwrap();
    assert!(matches!(
        storage.release(&artifact).await,
        Err(Error::Conflict)
    ));
}

#[tokio::test]
async fn authoritative_terminal_cleanup_retains_new_stop_evidence_until_published() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let mut old = running("busy");
    old.spec.execution.mounts.push(adx_core::execution::Mount {
        kind: "bind".into(),
        target: "/data".into(),
        options: vec![],
        source: adx_core::execution::MountSource::Volume(adx_core::volumes::VolumeRef {
            id: "data".into(),
            uid: Some("uid".into()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let healthy = running("healthy");
    let mut authority_failed = old.clone();
    authority_failed.state = EnvironmentState::Failed;
    authority_failed.resources_held = false;
    authority_failed.runtime.ip = None;
    authority_failed.revision += 1;
    let mut pending = old.clone();
    pending.revision += 2;
    let path = root.path().join("journal.sqlite");
    sink.fail.store(true, Ordering::SeqCst);
    let previous = JournalSink::new(path.clone(), sink.clone());
    previous.commit(&pending).await.unwrap();
    drop(previous);
    let journal = Arc::new(JournalSink::new(path, sink.clone()));
    let restored = node(&backend, backend.clone(), journal.clone());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        authority_failed.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
    let _ = authority
        .cycle_with_journal(&restored, Some(&journal))
        .await;
    failure_domains::control(&restored, &healthy).await.unwrap();
    assert_eq!(restored.used().cpu_millis, 2);
    assert_eq!(
        journal.pending().await.unwrap(),
        1,
        "the revoked Running result must be replaced by the terminal cleanup proof"
    );
    sink.fail.store(false, Ordering::SeqCst);
    for _ in 0..5 {
        let _ = authority
            .cycle_with_journal(&restored, Some(&journal))
            .await;
    }
    assert_eq!(journal.pending().await.unwrap(), 0);
    assert!(sink.publications.lock().unwrap().contains(&(
        authority_failed,
        vec![old.runtime.id],
        vec![]
    )));
    assert!(!sink
        .publications
        .lock()
        .unwrap()
        .iter()
        .any(|(r, _, _)| r.spec.id == "busy" && r.state == EnvironmentState::Running));
}

#[tokio::test]
async fn invalid_global_authority_cannot_ack_journal_before_recovery_validation() {
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let sink = Arc::new(Sink::default());
    let busy = running("busy");
    let healthy = running("healthy");
    let path = root.path().join("journal.sqlite");
    sink.fail.store(true, Ordering::SeqCst);
    let journal = Arc::new(JournalSink::new(path, sink.clone()));
    let mut pending = busy.clone();
    pending.revision += 1;
    journal.commit(&pending).await.unwrap();
    let restored = node(&backend, backend.clone(), journal.clone());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let mut authority = authority::Fixture::new().await;
    let mut invalid = busy.clone();
    invalid.assignment.node_id = "another-node".into();
    *authority.authority.records.lock().unwrap() = vec![
        invalid.try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    assert!(authority
        .cycle_with_journal(&restored, Some(&journal))
        .await
        .is_err());
    assert_eq!(journal.pending().await.unwrap(), 1);
    assert!(!restored.accepting_allocations());
    assert!(backend.events.lock().unwrap().is_empty());
    assert!(sink.publications.lock().unwrap().is_empty());
    *authority.authority.records.lock().unwrap() =
        vec![busy.try_into().unwrap(), healthy.try_into().unwrap()];
    *sink.catalog.lock().unwrap() = Some(authority.authority.records.clone());
    *sink.node.lock().unwrap() = Arc::downgrade(&restored);
    *sink.expected_cpu.lock().unwrap() = Some(4);
    sink.fail.store(false, Ordering::SeqCst);
    authority
        .cycle_with_journal(&restored, Some(&journal))
        .await
        .unwrap();
    assert_eq!(journal.pending().await.unwrap(), 0);
}

#[tokio::test]
async fn invalid_global_live_snapshot_fences_new_allocation_before_adoption_and_recovers() {
    for corrupt in ["catalog", "inventory", "wire"] {
        let backend = Arc::new(Backend::default());
        let restored = node(&backend, backend.clone(), backend.clone());
        let healthy = running("healthy");
        let busy = running("busy");
        let fresh = running("fresh");
        *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![
            busy.clone().try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
        ];
        authority.cycle(&restored).await.unwrap();
        backend.events.lock().unwrap().clear();
        let mut catalog: Vec<adx_protocol::control::EnvironmentRecord> = vec![
            busy.clone().try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
            fresh.clone().try_into().unwrap(),
        ];
        backend.actual.lock().unwrap().push(observed("fresh"));
        if corrupt == "catalog" {
            let mut invalid = busy.clone();
            invalid.assignment.node_id = "another-node".into();
            catalog[0] = invalid.try_into().unwrap();
        } else if corrupt == "wire" {
            catalog[0].spec = None;
        } else {
            backend.actual.lock().unwrap().push(RuntimeObservation {
                environment_id: "invalid".into(),
                tenant_id: "t".into(),
                runtime_id: "invalid".into(),
                generation: 0,
                running: true,
            });
        }
        *authority.authority.records.lock().unwrap() = catalog;
        assert!(authority.cycle(&restored).await.is_err());
        assert!(
            !restored.accepting_allocations(),
            "{corrupt}: invalid global facts must restrict allocations"
        );
        assert!(!authority.registration.accepting_allocations);
        failure_domains::control(&restored, &healthy).await.unwrap();
        tokio::task::yield_now().await;
        assert!(
            !backend
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e == "bind:fresh-7"),
            "invalid global inventory cannot authorize a new adoption"
        );
        *authority.authority.records.lock().unwrap() = vec![
            busy.try_into().unwrap(),
            healthy.try_into().unwrap(),
            fresh.clone().try_into().unwrap(),
        ];
        backend.actual.lock().unwrap().retain(|r| r.generation != 0);
        for _ in 0..3 {
            let _ = authority.cycle(&restored).await;
        }
        assert!(restored.accepting_allocations());
        failure_domains::control(&restored, &fresh).await.unwrap();
        assert_eq!(restored.used().cpu_millis, 6);
    }
}
