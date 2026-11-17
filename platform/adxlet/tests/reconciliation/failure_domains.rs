use super::*;
use adx_protocol::{
    auth::Peers,
    control::{self as pb, node_service_server::NodeService},
};
use std::sync::atomic::Ordering;

pub(super) async fn control(
    node: &Adxlet,
    record: &EnvironmentRecord,
) -> std::result::Result<(), tonic::Status> {
    let rpc = adxlet::rpc::NodeRpc::new(Arc::new(node.clone()), Peers::network(), "session".into());
    let mut request = tonic::Request::new(pb::NodeGetEnvironmentControlRequest {
        assignment: Some(record.assignment.clone().try_into().unwrap()),
        caller: Some(pb::CallerContext {
            tenant_id: "t".into(),
            administrator: false,
        }),
        node_session_id: "session".into(),
    });
    request
        .metadata_mut()
        .insert("adx-component", "coordinator".parse().unwrap());
    rpc.get_environment_control(request).await.map(|_| ())
}

pub(super) async fn delete(node: &Adxlet, record: &EnvironmentRecord) {
    let rpc = adxlet::rpc::NodeRpc::new(Arc::new(node.clone()), Peers::network(), "session".into());
    let mut request = tonic::Request::new(pb::DeleteEnvironmentRequest {
        assignment: Some(record.assignment.clone().try_into().unwrap()),
        caller: Some(pb::CallerContext {
            tenant_id: "t".into(),
            administrator: false,
        }),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    rpc.delete_environment(request).await.unwrap();
}

async fn isolated(mode: &str, fault: &'static str) {
    let backend = Arc::new(Backend::default());
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let mut healthy = record("healthy", EnvironmentState::Running);
    let mut busy = record("busy", EnvironmentState::Running);
    for record in [&mut healthy, &mut busy] {
        record.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
            timeout_seconds: 3600,
            action: adx_core::environment_control::RunExpiryAction::Delete,
        });
    }
    let records = vec![healthy.clone(), busy.clone()];
    if mode != "startup" {
        let initial = if mode == "periodic" {
            vec![healthy.clone()]
        } else {
            records.clone()
        };
        *backend.actual.lock().unwrap() = initial.iter().map(|r| observed(&r.spec.id)).collect();
        *authority.authority.records.lock().unwrap() =
            initial.into_iter().map(|r| r.try_into().unwrap()).collect();
        authority.cycle(&node).await.unwrap();
    }
    *backend.actual.lock().unwrap() = records.iter().map(|r| observed(&r.spec.id)).collect();
    *authority.authority.records.lock().unwrap() =
        records.into_iter().map(|r| r.try_into().unwrap()).collect();
    *backend.recovery_fault.lock().unwrap() = Some(fault);
    if mode == "session" {
        authority.registration.reconciling = true;
    }
    let _ = authority.cycle(&node).await;
    assert!(
        node.accepting_allocations(),
        "{mode}/{fault}: one failed execution must not fence node admission"
    );
    control(&node, &healthy)
        .await
        .expect("healthy control RPC must remain available");
    assert!(backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|x| x == "bind:healthy-7"));
    assert_eq!(
        node.used().cpu_millis,
        healthy.spec.resources.cpu_millis + busy.spec.resources.cpu_millis
    );
    assert!(
        control(&node, &busy).await.is_err(),
        "failed recovery must remain fenced"
    );
    let _ = authority.cycle(&node).await;
    control(&node, &healthy).await.unwrap();
    delete(&node, &healthy).await;
    assert_eq!(node.used(), busy.spec.resources);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .retain(|record| record.spec.as_ref().unwrap().id != "healthy");
    assert!(node.accepting_allocations());
    backend.allow_start.store(true, Ordering::SeqCst);
    let fresh = record("new", EnvironmentState::Pending);
    let fresh = node.environment(fresh.spec, fresh.assignment).unwrap();
    assert_eq!(
        fresh.create().await.unwrap().record.state,
        EnvironmentState::Running
    );
    fresh.delete().await.unwrap();
    *backend.recovery_fault.lock().unwrap() = None;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let _ = authority.cycle(&node).await;
            if control(&node, &busy).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovery must converge after the per-execution fault is removed");
    assert_eq!(node.used(), busy.spec.resources);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn startup_isolates_each_execution_recovery_failure() {
    for fault in ["observation", "readiness", "route", "commit", "journaled"] {
        isolated("startup", fault).await;
    }
}

#[tokio::test]
async fn session_takeover_isolates_each_execution_recovery_failure() {
    for fault in ["observation", "readiness", "route", "commit", "journaled"] {
        isolated("session", fault).await;
    }
}

#[tokio::test]
async fn periodic_adoption_isolates_each_execution_recovery_failure() {
    for fault in ["observation", "readiness", "route", "commit", "journaled"] {
        isolated("periodic", fault).await;
    }
}

async fn orphan_cleanup_failure(accounted: bool) {
    let modes: &[&str] = if accounted {
        &["session", "periodic"]
    } else {
        &["startup", "session", "periodic"]
    };
    for &mode in modes {
        let backend = Arc::new(Backend::default());
        let node = manager(&backend);
        let mut authority = authority::Fixture::new().await;
        let mut healthy = record("healthy", EnvironmentState::Running);
        healthy.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
            timeout_seconds: 3600,
            action: adx_core::environment_control::RunExpiryAction::Delete,
        });
        *authority.authority.records.lock().unwrap() = vec![healthy.clone().try_into().unwrap()];
        *backend.actual.lock().unwrap() = vec![observed("healthy")];
        let busy = record("busy", EnvironmentState::Running);
        if accounted {
            authority
                .authority
                .records
                .lock()
                .unwrap()
                .push(busy.clone().try_into().unwrap());
            backend.actual.lock().unwrap().push(observed("busy"));
            authority.cycle(&node).await.unwrap();
            authority
                .authority
                .records
                .lock()
                .unwrap()
                .retain(|r| r.spec.as_ref().unwrap().id != "busy");
        } else if mode != "startup" {
            authority.cycle(&node).await.unwrap();
        }
        if mode == "session" {
            authority.registration.reconciling = true;
        }
        if !accounted {
            backend.actual.lock().unwrap().push(observed("busy"));
        }
        *backend.recovery_fault.lock().unwrap() = Some("remove");
        let _ = authority.cycle(&node).await;
        assert_eq!(
            node.accepting_allocations(),
            accounted,
            "{mode}: only an unaccounted orphan must fence new allocations"
        );
        let candidate = record("other", EnvironmentState::Pending);
        if !accounted {
            assert!(node
                .environment(candidate.spec.clone(), candidate.assignment.clone())
                .is_err());
        }
        control(&node, &healthy)
            .await
            .expect("an unaccounted orphan must not fence healthy control");
        let _ = authority.cycle(&node).await;
        control(&node, &healthy).await.unwrap();
        delete(&node, &healthy).await;
        assert_eq!(
            node.used(),
            if accounted {
                busy.spec.resources
            } else {
                Resources::default()
            }
        );
        authority.authority.records.lock().unwrap().clear();
        *backend.recovery_fault.lock().unwrap() = None;
        for _ in 0..3 {
            let _ = authority.cycle(&node).await;
        }
        assert!(
            node.accepting_allocations(),
            "{mode}: cleanup must reopen allocations without restarting"
        );
        assert!(backend.actual.lock().unwrap().is_empty());
        assert_eq!(node.used(), Resources::default());
        assert!(node
            .environment(candidate.spec, candidate.assignment)
            .is_ok());
    }
}

#[tokio::test]
async fn unknown_orphan_blocks_only_new_allocations_until_cleanup_converges() {
    orphan_cleanup_failure(false).await;
}

#[tokio::test]
async fn accounted_orphan_cleanup_failure_preserves_allocation_admission() {
    orphan_cleanup_failure(true).await;
}

#[tokio::test]
async fn one_orphan_failure_does_not_cancel_another_accepted_cleanup() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("other"), observed("busy")];
    backend.block_remove.store(true, Ordering::SeqCst);
    *backend.recovery_fault.lock().unwrap() = Some("remove");
    let node = manager(&backend);
    let recovering = tokio::spawn(async move { node.reconcile(vec![]).await });
    backend.remove_entered.notified().await;
    backend.block_remove.store(false, Ordering::SeqCst);
    backend.remove_release.notify_one();
    assert!(recovering.await.unwrap().is_err());
    assert!(
        !backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.environment_id == "other"),
        "reconciliation must await accepted cleanup before returning a peer's failure"
    );
}

#[tokio::test]
async fn idle_cache_cleanup_failure_does_not_close_real_relay_or_healthy_control() {
    use adxlet::{checkpoint::ObjectCheckpointStore, routes::UdsRoutes};
    use data_plane_gateway::{
        common::protocol::GatewayPolicy,
        node::{
            route_control::{proto::relay_service_server::RelayServiceServer, BindingService},
            Relay,
        },
    };
    for mode in ["startup", "session", "periodic"] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("route.sock");
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
        let storage = Arc::new(
            ObjectCheckpointStore::new_scoped(
                "shared".into(),
                "scope".into(),
                Arc::new(object_store::memory::InMemory::new()),
                "test".into(),
                directory.path().join("checkpoints"),
                1024,
            )
            .unwrap(),
        );
        let node = Adxlet::new(
            "n".into(),
            backend.clone(),
            backend.clone(),
            Arc::new(UdsRoutes::new(socket, Duration::from_secs(1)).unwrap()),
            backend.clone(),
        )
        .with_checkpointing(storage, backend.clone())
        .unwrap();
        node.update_capacity(
            Resources {
                cpu_millis: 1000,
                memory_bytes: 1000,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
        let mut healthy = record("healthy", EnvironmentState::Running);
        healthy.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
            timeout_seconds: 3600,
            action: adx_core::environment_control::RunExpiryAction::Delete,
        });
        *backend.actual.lock().unwrap() = vec![observed("healthy")];
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![healthy.clone().try_into().unwrap()];
        if mode != "startup" {
            authority.cycle(&node).await.unwrap();
        }
        if mode == "session" {
            authority.registration.reconciling = true;
        }
        let bad_index = directory.path().join("checkpoints/cache-index/stale.json");
        std::fs::create_dir(&bad_index).unwrap();
        let _ = authority.cycle(&node).await;
        assert!(
            node.accepting_allocations(),
            "{mode}: idle cache cleanup must not fence node readiness"
        );
        assert!(
            relay.ready(),
            "{mode}: real relay must publish healthy recovered bindings"
        );
        control(&node, &healthy).await.unwrap();
        let _ = authority.cycle(&node).await;
        std::fs::remove_dir(&bad_index).unwrap();
        authority.cycle(&node).await.unwrap();
        node.environment(healthy.spec, healthy.assignment)
            .unwrap()
            .delete()
            .await
            .unwrap();
        assert!(relay.ready());
        server.abort();
    }
}

#[tokio::test]
async fn failed_journal_resume_replay_retains_the_live_execution_on_restart_and_takeover() {
    use adxlet::{
        checkpoint::{
            CheckpointAccess, LocalCheckpointStore, ObjectCheckpointStore, PauseRequest,
            ResumeRequest,
        },
        journal::JournalSink,
    };
    for (restart, shared) in [(false, false), (true, false), (false, true), (true, true)] {
        let directory = tempfile::tempdir().unwrap();
        let backend = Arc::new(Backend::default());
        backend.allow_start.store(true, Ordering::SeqCst);
        let path = directory.path().join("journal.sqlite");
        let journal = Arc::new(JournalSink::new(path.clone(), backend.clone()));
        let remote = Arc::new(object_store::memory::InMemory::new());
        let build = |journal: Arc<JournalSink>| {
            let storage: Arc<dyn CheckpointAccess> = if shared {
                Arc::new(
                    ObjectCheckpointStore::new_scoped(
                        "shared".into(),
                        "scope".into(),
                        remote.clone(),
                        "test".into(),
                        directory.path().join("checkpoints"),
                        128 * 1024 * 1024,
                    )
                    .unwrap(),
                )
            } else {
                Arc::new(LocalCheckpointStore::new(directory.path().join("checkpoints")).unwrap())
            };
            let node = Adxlet::new(
                "n".into(),
                backend.clone(),
                backend.clone(),
                backend.clone(),
                journal,
            )
            .with_checkpointing(storage, backend.clone())
            .unwrap();
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
        };
        let original = build(journal.clone());
        let mut busy = record("busy", EnvironmentState::Running);
        let mut healthy = record("healthy", EnvironmentState::Running);
        for record in [&mut busy, &mut healthy] {
            record.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
                timeout_seconds: 3600,
                action: adx_core::environment_control::RunExpiryAction::Delete,
            });
        }
        *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![
            busy.clone().try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
        ];
        authority
            .cycle_with_journal(&original, Some(&journal))
            .await
            .unwrap();
        let handle = original
            .environment(busy.spec.clone(), busy.assignment.clone())
            .unwrap();
        let paused = handle
            .pause(PauseRequest {
                operation_id: "pause-before-outage".into(),
                expected_revision: busy.revision,
                retention: adx_core::lifecycle::RetentionPolicy::Forever,
                timeout_seconds: 30,
                storage: None,
            })
            .await
            .unwrap()
            .record;
        *authority.authority.records.lock().unwrap() = vec![
            paused.clone().try_into().unwrap(),
            healthy.clone().try_into().unwrap(),
        ];
        *backend.recovery_fault.lock().unwrap() = Some("running_commit");
        let resumed = handle
            .resume(ResumeRequest {
                operation_id: "journaled-resume".into(),
                expected_revision: paused.revision,
                run_timeout_seconds: None,
            })
            .await
            .unwrap();
        assert_eq!(resumed.durability, Durability::Journaled);
        let pending = resumed.record;
        let (node, journal) = if restart {
            drop(handle);
            drop(original);
            drop(journal);
            let journal = Arc::new(JournalSink::new(path, backend.clone()));
            (build(journal.clone()), journal)
        } else {
            (original, journal)
        };
        authority.registration.reconciling = true;
        let _ = authority.cycle_with_journal(&node, Some(&journal)).await;
        assert!(
            node.accepting_allocations(),
            "restart={restart}: pending publication must not fence healthy admission"
        );
        control(&node, &healthy).await.unwrap();
        assert!(control(&node, &pending).await.is_err());
        assert_eq!(
            node.used().cpu_millis,
            healthy.spec.resources.cpu_millis + pending.spec.resources.cpu_millis
        );
        assert!(backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.runtime_id == pending.runtime.id));
        assert!(journal.pending().await.unwrap() > 0);
        *backend.recovery_fault.lock().unwrap() = None;
        let _ = authority.cycle_with_journal(&node, Some(&journal)).await;
        control(&node, &pending).await.unwrap();
        assert_eq!(journal.pending().await.unwrap(), 0);
        let result = node
            .environment(pending.spec.clone(), pending.assignment.clone())
            .unwrap()
            .sync()
            .await
            .unwrap();
        assert_eq!(result.record.runtime.id, pending.runtime.id);
        assert_eq!(result.record.revision, pending.revision);
        assert_eq!(result.record.state, EnvironmentState::Running);
    }
}

#[tokio::test]
async fn drain_waits_for_authoritative_recovery_without_holding_the_rpc_gate() {
    let backend = Arc::new(Backend::default());
    backend.block_remove.store(true, Ordering::SeqCst);
    backend.actual.lock().unwrap().push(observed("orphan"));
    let node = Arc::new(manager(&backend));
    let recovery = tokio::spawn({
        let node = node.clone();
        async move { node.reconcile(vec![]).await }
    });
    backend.remove_entered.notified().await;
    let mut draining = tokio::spawn({
        let node = node.clone();
        async move { node.drain().await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut draining)
            .await
            .is_err(),
        "drain must wait for recovery instead of returning Unavailable"
    );
    backend.block_remove.store(false, Ordering::SeqCst);
    backend.remove_release.notify_one();
    recovery.await.unwrap().unwrap();
    assert_eq!(draining.await.unwrap().unwrap(), 0);
    assert!(node.is_draining());
}

#[tokio::test]
async fn partial_execution_input_retention_retries_every_missing_input_without_losing_pins() {
    use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
    use object_store::{memory::InMemory, path::Path as Key, ObjectStoreExt};
    let directory = tempfile::tempdir().unwrap();
    let remote = Arc::new(InMemory::new());
    let store = Arc::new(
        ObjectCheckpointStore::new_scoped(
            "shared".into(),
            "scope".into(),
            remote.clone(),
            "test".into(),
            directory.path().join("checkpoints"),
            128 * 1024 * 1024,
        )
        .unwrap(),
    );
    let mut inputs = vec![];
    let mut paths = vec![];
    for value in [b"first input", b"other input"] {
        let path = store.begin_capture().await.unwrap();
        std::fs::write(path.join("state"), value).unwrap();
        let artifact = store.seal(&path).await.unwrap();
        store.committed(&artifact).await.unwrap();
        inputs.push(artifact);
        paths.push(path);
    }
    let key = Key::from(format!("test/{}/manifest.json", inputs[1].location));
    let manifest = remote.get(&key).await.unwrap().bytes().await.unwrap();
    remote.delete(&key).await.unwrap();
    std::fs::remove_dir_all(&paths[1]).unwrap();
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("healthy")];
    let node = Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(store.clone(), backend.clone())
    .unwrap();
    node.update_capacity(
        Resources {
            cpu_millis: 1000,
            memory_bytes: 1000,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    let mut busy = record("busy", EnvironmentState::Running);
    let mut healthy = record("healthy", EnvironmentState::Running);
    for record in [&mut busy, &mut healthy] {
        record.spec.run_policy = Some(adx_core::environment_control::RunPolicy {
            timeout_seconds: 3600,
            action: adx_core::environment_control::RunExpiryAction::Delete,
        });
    }
    busy.execution_artifacts = inputs.clone();
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![
        busy.clone().try_into().unwrap(),
        healthy.clone().try_into().unwrap(),
    ];
    let _ = authority.cycle(&node).await;
    control(&node, &healthy).await.unwrap();
    assert!(control(&node, &busy).await.is_err());
    assert!(matches!(
        store.release(&inputs[0]).await,
        Err(adx_core::Error::Conflict)
    ));
    let _ = authority.cycle(&node).await;
    assert!(
        control(&node, &busy).await.is_err(),
        "one retained input must not hide the still-missing second input"
    );
    remote.put(&key, manifest.into()).await.unwrap();
    for _ in 0..3 {
        let _ = authority.cycle(&node).await;
    }
    control(&node, &busy).await.unwrap();
    for input in inputs {
        assert!(matches!(
            store.release(&input).await,
            Err(adx_core::Error::Conflict)
        ));
    }
    assert_eq!(
        node.used().cpu_millis,
        busy.spec.resources.cpu_millis + healthy.spec.resources.cpu_millis
    );
}
