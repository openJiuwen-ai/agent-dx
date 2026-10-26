mod common;
use adx_coordinator::storage::RedisStore;
use adx_core::{
    execution::{Mount, MountSource},
    volumes::{AccessMode, StorageDomain, VolumeRef, VolumeSource, VolumeSpec},
    EnvironmentSpec, Resources,
};
use std::collections::BTreeMap;

fn domains() -> BTreeMap<String, StorageDomain> {
    BTreeMap::from([(
        "sfs".into(),
        StorageDomain {
            dynamic_nodes: false,
            backend_id: "server:/export".into(),
            node_roots: BTreeMap::from([
                ("node-a".into(), "/mnt/sfs".into()),
                ("node-b".into(), "/data/sfs".into()),
            ]),
            tenant_paths: BTreeMap::from([("tenant".into(), "workspaces/tenant".into())]),
        },
    )])
}

fn volume(id: &str, path: &str) -> VolumeSpec {
    VolumeSpec {
        tenant_id: "tenant".into(),
        id: id.into(),
        source: VolumeSource::Directory {
            storage_domain: "sfs".into(),
            path: path.into(),
        },
        access_mode: AccessMode::Shared,
        create_if_missing: false,
        create_options: None,
    }
}

fn environment(id: &str, reference: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "app".into(),
        runtime_class: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        execution: adx_core::execution::ExecutionOptions {
            mounts: vec![Mount {
                kind: "bind".into(),
                target: "/workspace".into(),
                options: vec!["rw".into()],
                source: MountSource::Volume(VolumeRef {
                    id: reference.into(),
                    uid: None,
                }),
                create_if_missing: false,
                create_options: None,
            }],
            ..Default::default()
        },
    }
}

async fn store(rig: &common::Redis) -> RedisStore {
    rig.store().await
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn ensure_existing_volume_uses_original_definition_and_uid() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let first = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    let mut changed = volume("workspace", "workspaces/tenant/elsewhere");
    changed.access_mode = AccessMode::Exclusive;
    changed.create_if_missing = true;
    let reused = session.ensure_volume(changed, &domains()).await.unwrap();
    assert_eq!(first, reused);
    assert_eq!(reused.spec.access_mode, AccessMode::Shared);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_ensure_commits_one_identity_and_source() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let mut requests = Vec::new();
    for index in 0..8 {
        let session = session.clone();
        requests.push(tokio::spawn(async move {
            session
                .ensure_volume(
                    volume("workspace", &format!("workspaces/tenant/user-{index}")),
                    &domains(),
                )
                .await
                .unwrap()
        }));
    }
    let first = requests.remove(0).await.unwrap();
    for request in requests {
        assert_eq!(request.await.unwrap(), first);
    }
    assert_eq!(session.list_volumes("tenant").await.unwrap(), vec![first]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn directory_aliases_overlap_and_unauthorized_sources_are_rejected() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let mut configured = domains();
    configured.insert("alias".into(), configured["sfs"].clone());
    session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &configured)
        .await
        .unwrap();
    for path in [
        "workspaces/tenant/user",
        "workspaces/tenant/user/child",
        "workspaces/tenant",
    ] {
        let mut second = volume("other", path);
        second.source = VolumeSource::Directory {
            storage_domain: "alias".into(),
            path: path.into(),
        };
        assert!(session.ensure_volume(second, &configured).await.is_err());
    }
    assert!(session
        .ensure_volume(volume("outside", "workspaces/other/user"), &configured)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn pinned_references_survive_restart_and_protect_deletion_without_assignment() {
    let mut rig = common::Redis::new().await;
    let persistence = store(&rig).await;
    let session = persistence.begin(1).await.unwrap();
    let volume = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    let spec = environment("env", "workspace");
    assert_eq!(
        session.pin_volumes(&spec).await.unwrap(),
        vec![volume.clone()]
    );
    assert_eq!(
        session.pin_volumes(&spec).await.unwrap(),
        vec![volume.clone()]
    );
    assert!(session.delete_volume("tenant", "workspace").await.is_err());
    rig.crash();
    rig.start().await;
    let recovered = store(&rig).await.begin(1).await.unwrap();
    assert_eq!(recovered.pin_volumes(&spec).await.unwrap(), vec![volume]);
    assert!(recovered
        .delete_volume("tenant", "workspace")
        .await
        .is_err());
    let mut conflicting = spec;
    conflicting.execution.mounts[0].target = "/other".into();
    assert!(recovered.pin_volumes(&conflicting).await.is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn recreation_generates_a_new_uid_and_rejects_an_old_fixed_reference() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let original = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    session.delete_volume("tenant", "workspace").await.unwrap();
    let recreated = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    assert_ne!(original.uid, recreated.uid);
    let mut spec = environment("env", "workspace");
    spec.execution.mounts[0].source = MountSource::Volume(VolumeRef {
        id: "workspace".into(),
        uid: Some(original.uid),
    });
    assert!(session.pin_volumes(&spec).await.is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cancelled_unassigned_reference_allows_deletion_but_never_rebinds_the_same_request() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let original = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    let spec = environment("env", "workspace");
    session.pin_volumes(&spec).await.unwrap();
    session.unpin_volumes(&spec).await.unwrap();
    session.unpin_volumes(&spec).await.unwrap();
    session.delete_volume("tenant", "workspace").await.unwrap();
    let recreated = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    assert_ne!(original.uid, recreated.uid);
    assert!(session.pin_volumes(&spec).await.is_err());
}

async fn claimant(
    session: &adx_coordinator::storage::Session,
    id: &str,
) -> adx_coordinator::storage::LocalClaim {
    use adx_coordinator::{storage::NodeSession, Node};
    session
        .register_session(
            Node {
                id: id.into(),
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
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: format!("boot-{id}"),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    adx_coordinator::storage::LocalClaim {
        request_id: String::new(),
        node_id: id.into(),
        node_session_id: format!("boot-{id}"),
        devices: vec![],
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn exclusive_claim_and_central_reservation_commit_one_owner_with_the_assignment() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(2).await.unwrap();
    let a = claimant(&session, "node-a").await;
    claimant(&session, "node-b").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    let volume = session.ensure_volume(definition, &domains()).await.unwrap();
    let first = environment("first", "workspace");
    let other = environment("other", "workspace");
    session.pin_volumes(&first).await.unwrap();
    session.pin_volumes(&other).await.unwrap();
    let claimed = session.claim(first.clone(), &a).await.unwrap();
    let assignment = adx_core::Assignment {
        environment_id: other.id.clone(),
        node_id: "node-b".into(),
        shard_id: 1,
        generation: claimed.record().assignment.generation + 1,
        devices: vec![],
    };
    assert!(session.reserve(other, assignment).await.is_err());
    assert!(matches!(
        session.get("other").await,
        Err(adx_core::Error::NotFound)
    ));
    let occupancy = session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .unwrap();
    assert_eq!(occupancy.volume_uid, volume.uid);
    assert_eq!(occupancy.assignment, claimed.record().assignment);
    assert_eq!(occupancy.node_session_id, a.node_session_id);
    assert_eq!(session.claim(first, &a).await.unwrap(), claimed);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn busy_volume_does_not_discard_unrelated_claims_in_the_same_batch() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let first = environment("first", "workspace");
    let blocked = environment("blocked", "workspace");
    session.pin_volumes(&first).await.unwrap();
    session.pin_volumes(&blocked).await.unwrap();
    let mut unrelated = environment("unrelated", "workspace");
    unrelated.execution.mounts.clear();
    let results = session
        .claim_batch(vec![
            (first, candidate.clone()),
            (blocked, candidate.clone()),
            (unrelated, candidate),
        ])
        .await
        .unwrap();
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
    assert!(session.get("blocked").await.is_err());
    assert!(session.get("unrelated").await.is_ok());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn shared_claims_have_references_without_exclusive_occupancy() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    for id in ["first", "second"] {
        let spec = environment(id, "workspace");
        session.pin_volumes(&spec).await.unwrap();
        session.claim(spec, &candidate).await.unwrap();
    }
    let stored = session.get_volume("tenant", "workspace").await.unwrap();
    assert!(stored.occupancy.is_none());
    assert_eq!(stored.references.len(), 2);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn rejected_unstarted_assignment_transfers_its_matching_volume_reservation() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(2).await.unwrap();
    let a = claimant(&session, "node-a").await;
    claimant(&session, "node-b").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let spec = environment("env", "workspace");
    let initial = session.claim(spec.clone(), &a).await.unwrap();
    let replacement = adx_core::Assignment {
        node_id: "node-b".into(),
        shard_id: 1,
        generation: initial.record().assignment.generation + 1,
        ..initial.record().assignment.clone()
    };
    let transferred = session
        .replace_rejected(&initial.record().assignment, replacement.clone())
        .await
        .unwrap();
    assert_eq!(transferred.assignment, replacement);
    let occupancy = session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .unwrap();
    assert_eq!(occupancy.assignment, replacement);
    assert_eq!(occupancy.node_session_id, "boot-node-b");
    assert_eq!(
        session
            .replace_rejected(&initial.record().assignment, replacement)
            .await
            .unwrap(),
        transferred
    );
    assert!(matches!(
        session.claim(spec, &a).await.unwrap(),
        adx_coordinator::storage::ClaimOutcome::Existing(_)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_exclusive_claims_commit_only_one_environment_across_shards() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(2).await.unwrap();
    let a = claimant(&session, "node-a").await;
    let b = claimant(&session, "node-b").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let mut requests = Vec::new();
    for index in 0..8 {
        let session = session.clone();
        let candidate = if index % 2 == 0 { a.clone() } else { b.clone() };
        requests.push(tokio::spawn(async move {
            session
                .claim(
                    environment(&format!("env-{index}"), "workspace"),
                    &candidate,
                )
                .await
        }));
    }
    let mut winners = Vec::new();
    for request in requests {
        match request.await.unwrap() {
            Ok(outcome) => winners.push(outcome.record().assignment.clone()),
            Err(adx_core::Error::VolumeBusy(_)) => (),
            Err(error) => panic!("unexpected admission result: {error}"),
        }
    }
    assert_eq!(winners.len(), 1);
    let saved = session.snapshot().await.unwrap();
    assert_eq!(saved.environments.len(), 1);
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment,
        winners[0]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn failed_result_without_cleanup_proof_keeps_occupancy_and_stale_release_cannot_free_a_new_owner(
) {
    use adx_core::{EnvironmentRecord, EnvironmentState, Runtime};
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let spec = environment("first", "workspace");
    let assigned = session
        .claim(spec.clone(), &candidate)
        .await
        .unwrap()
        .record()
        .assignment
        .clone();
    let runtime_id = format!("first-{}", assigned.generation);
    let starting = EnvironmentRecord {
        observation: None,
        spec,
        assignment: assigned,
        runtime: Runtime {
            id: runtime_id.clone(),
            ip: None,
        },
        state: EnvironmentState::Starting,
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
        .authorize_volumes(&starting, &runtime_id, &candidate.node_session_id)
        .await
        .unwrap();
    let mut failed = starting.clone();
    failed.state = EnvironmentState::Failed;
    failed.revision = 2;
    failed.resources_held = false;
    session.commit(failed.clone()).await.unwrap();
    assert!(session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_some());
    session
        .commit_releasing(
            failed.clone(),
            &candidate.node_session_id,
            std::slice::from_ref(&runtime_id),
        )
        .await
        .unwrap();
    assert!(session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_none());
    assert!(session
        .authorize_volumes(&starting, &runtime_id, &candidate.node_session_id)
        .await
        .is_err());
    let next = session
        .claim(environment("next", "workspace"), &candidate)
        .await
        .unwrap();
    session
        .commit_releasing(failed, &candidate.node_session_id, &[runtime_id])
        .await
        .unwrap();
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment,
        next.record().assignment
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn shared_initialization_is_coordinated_and_failed_attributes_are_not_ready() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.create_if_missing = true;
    let volume = session.ensure_volume(definition, &domains()).await.unwrap();
    let mut records = Vec::new();
    for id in ["first", "second"] {
        let spec = environment(id, "workspace");
        let assignment = session
            .claim(spec.clone(), &candidate)
            .await
            .unwrap()
            .record()
            .assignment
            .clone();
        let record = adx_core::EnvironmentRecord {
            observation: None,
            runtime: adx_core::Runtime {
                id: format!("{id}-{}", assignment.generation),
                ip: None,
            },
            spec,
            assignment,
            state: adx_core::EnvironmentState::Starting,
            revision: 1,
            resources_held: true,
            checkpoint: None,
            snapshot_source: None,
            failure: None,
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
            execution_artifacts: vec![],
        };
        session
            .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
            .await
            .unwrap();
        records.push(record);
    }
    assert_eq!(
        session
            .begin_volume_initialization(&records[0], &candidate.node_session_id, &volume.uid)
            .await
            .unwrap(),
        adx_coordinator::storage::InitializationAction::Prepare
    );
    assert!(matches!(
        session
            .begin_volume_initialization(&records[1], &candidate.node_session_id, &volume.uid)
            .await,
        Err(adx_core::Error::VolumeBusy(_))
    ));
    session
        .complete_volume_initialization(
            &records[0],
            &candidate.node_session_id,
            &volume.uid,
            Some("attributes failed"),
        )
        .await
        .unwrap();
    assert!(session
        .begin_volume_initialization(&records[1], &candidate.node_session_id, &volume.uid)
        .await
        .is_err());
}

fn starting(
    spec: EnvironmentSpec,
    assignment: adx_core::Assignment,
) -> adx_core::EnvironmentRecord {
    adx_core::EnvironmentRecord {
        observation: None,
        runtime: adx_core::Runtime {
            id: format!("{}-{}", spec.id, assignment.generation),
            ip: None,
        },
        spec,
        assignment,
        state: adx_core::EnvironmentState::Starting,
        revision: 1,
        resources_held: true,
        checkpoint: None,
        snapshot_source: None,
        failure: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        execution_artifacts: vec![],
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_loss_releases_reserved_and_current_authorized_volumes_with_the_failed_assignment() {
    use adx_core::EnvironmentState;
    let mut rig = common::Redis::new().await;
    let session = store(&rig).await.begin(2).await.unwrap();
    let a = claimant(&session, "node-a").await;
    let b = claimant(&session, "node-b").await;
    for id in ["held", "reserved", "unrelated"] {
        let mut definition = volume(id, &format!("workspaces/tenant/{id}"));
        definition.access_mode = AccessMode::Exclusive;
        session.ensure_volume(definition, &domains()).await.unwrap();
    }
    let mut records = Vec::new();
    for (id, candidate) in [("held", &a), ("reserved", &a), ("unrelated", &b)] {
        let spec = environment(id, id);
        let assignment = session
            .claim(spec.clone(), candidate)
            .await
            .unwrap()
            .record()
            .assignment
            .clone();
        records.push(starting(spec, assignment));
    }
    let first = &records[0];
    session
        .authorize_volumes(first, &first.runtime.id, &a.node_session_id)
        .await
        .unwrap();
    let mut running = first.clone();
    running.state = EnvironmentState::Running;
    running.revision = 2;
    running.runtime.ip = Some("10.0.0.1".parse().unwrap());
    running.checkpoint = Some(adx_core::RestorePoint {
        id: "checkpoint".into(),
        origin: None,
        expires_at_unix_seconds: None,
        source_runtime_id: first.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(running.clone()).await.unwrap();
    let mut authorized = running.clone();
    authorized.state = EnvironmentState::Starting;
    authorized.revision = 3;
    authorized.runtime.id = format!("{}-r3", first.runtime.id);
    authorized.runtime.ip = None;
    session
        .authorize_volumes(&authorized, &first.runtime.id, &a.node_session_id)
        .await
        .unwrap();
    let unrelated = session.get_volume("tenant", "unrelated").await.unwrap();
    let snapshot = session
        .invalidate_node("node-a", &a.node_session_id)
        .await
        .unwrap();
    assert!(snapshot.environments["held"].invalidated);
    assert!(snapshot.environments["reserved"].invalidated);
    assert_eq!(
        snapshot.environments["held"]
            .result
            .as_ref()
            .unwrap()
            .runtime
            .id,
        authorized.runtime.id
    );
    assert!(!snapshot.environments["held"].resources_held());
    assert!(
        snapshot.environments["held"]
            .volume_execution
            .as_ref()
            .unwrap()
            .released
    );
    for id in ["held", "reserved"] {
        assert!(session
            .get_volume("tenant", id)
            .await
            .unwrap()
            .occupancy
            .is_none());
    }
    assert_eq!(
        session.get_volume("tenant", "unrelated").await.unwrap(),
        unrelated
    );
    assert!(session
        .authorize_volumes(&authorized, &first.runtime.id, &a.node_session_id)
        .await
        .is_err());
    rig.crash();
    rig.start().await;
    let restarted = store(&rig).await.begin(2).await.unwrap();
    assert_eq!(
        restarted.get("held").await.unwrap(),
        snapshot.environments["held"]
    );
    assert!(restarted
        .get_volume("tenant", "held")
        .await
        .unwrap()
        .occupancy
        .is_none());
    let replacement = adx_core::Assignment {
        node_id: b.node_id.clone(),
        shard_id: snapshot.nodes[&b.node_id].shard_id,
        generation: snapshot.generation + 1,
        ..first.assignment.clone()
    };
    restarted
        .reserve_recovery(&first.assignment, replacement.clone(), 1)
        .await
        .unwrap();
    assert!(restarted
        .commit_releasing(running, &a.node_session_id, &[authorized.runtime.id])
        .await
        .is_err());
    assert_eq!(
        restarted
            .get_volume("tenant", "held")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .assignment,
        replacement
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_loss_marks_interrupted_shared_initialization_for_repair() {
    use adx_coordinator::storage::Initialization;
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let a = claimant(&session, "node-a").await;
    let b = claimant(&session, "node-b").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.create_if_missing = true;
    let volume = session.ensure_volume(definition, &domains()).await.unwrap();
    let mut records = Vec::new();
    for (id, candidate) in [("initializing", &a), ("waiting", &b)] {
        let spec = environment(id, "workspace");
        let assignment = session
            .claim(spec.clone(), candidate)
            .await
            .unwrap()
            .record()
            .assignment
            .clone();
        let record = starting(spec, assignment);
        session
            .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
            .await
            .unwrap();
        records.push(record);
    }
    assert_eq!(
        session
            .begin_volume_initialization(&records[0], &a.node_session_id, &volume.uid)
            .await
            .unwrap(),
        adx_coordinator::storage::InitializationAction::Prepare
    );
    session
        .invalidate_node("node-a", &a.node_session_id)
        .await
        .unwrap();
    assert!(matches!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .initialization,
        Initialization::NeedsRepair { .. }
    ));
    assert!(session
        .begin_volume_initialization(&records[1], &b.node_session_id, &volume.uid)
        .await
        .is_err());
    assert!(session
        .complete_volume_initialization(&records[0], &a.node_session_id, &volume.uid, None)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn owner_associations_are_idempotent_and_only_the_last_release_deletes_the_definition() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let spec = volume("workspace", "workspaces/tenant/user");
    let first = session
        .ensure_volume_for_owner(spec.clone(), &domains(), "binding-a")
        .await
        .unwrap();
    assert_eq!(
        first,
        session
            .ensure_volume_for_owner(spec.clone(), &domains(), "binding-a")
            .await
            .unwrap()
    );
    assert_eq!(
        first,
        session
            .ensure_volume_for_owner(spec, &domains(), "binding-b")
            .await
            .unwrap()
    );
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .owner_ids
            .len(),
        2
    );
    assert!(session.delete_volume("tenant", "workspace").await.is_err());
    let pending = environment("binding-a", "workspace");
    session.pin_volumes(&pending).await.unwrap();
    session
        .release_volume_owner("tenant", "binding-a")
        .await
        .unwrap();
    let retained = session.get_volume("tenant", "workspace").await.unwrap();
    assert_eq!(retained.owner_ids, ["binding-b".into()].into());
    assert!(retained.references.is_empty());
    session
        .release_volume_owner("tenant", "binding-a")
        .await
        .unwrap();
    session
        .release_volume_owner("tenant", "binding-b")
        .await
        .unwrap();
    assert!(matches!(
        session.get_volume("tenant", "workspace").await,
        Err(adx_core::Error::NotFound)
    ));
    session
        .release_volume_owner("tenant", "binding-b")
        .await
        .unwrap();
}

fn reusable_snapshot(id: &str, spec: EnvironmentSpec) -> adx_core::snapshots::Snapshot {
    adx_core::snapshots::Snapshot::new(
        id.into(),
        vec![],
        spec.clone(),
        "node-a".into(),
        format!("{}-1", spec.id),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: format!("artifact-{id}"),
            size_bytes: 1,
        },
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn snapshots_fix_volume_uid_and_protect_definition_after_last_binding_release() {
    let mut rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    session.refresh_inspection_inventory().await.unwrap();
    let definition = session
        .ensure_volume_for_owner(
            volume("workspace", "workspaces/tenant/user"),
            &domains(),
            "binding",
        )
        .await
        .unwrap();
    let source = environment("binding", "workspace");
    session.pin_volumes(&source).await.unwrap();
    let original = reusable_snapshot("snapshot-a", source.clone());
    let snapshot = session.publish_snapshot(original.clone()).await.unwrap();
    assert_eq!(
        session.inspection_inventory().await.unwrap().0,
        vec![snapshot.clone()]
    );
    let MountSource::Volume(reference) = &snapshot.template.execution.mounts[0].source else {
        panic!("expected fixed volume reference");
    };
    assert_eq!(reference.uid.as_deref(), Some(definition.uid.as_str()));
    let second = session
        .publish_snapshot(reusable_snapshot("snapshot-b", source.clone()))
        .await
        .unwrap();
    session.unpin_volumes(&source).await.unwrap();
    session
        .release_volume_owner("tenant", "binding")
        .await
        .unwrap();
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .volume,
        definition
    );
    assert!(session.delete_volume("tenant", "workspace").await.is_err());
    // A lost publish response can be replayed after the source has been deleted.
    assert_eq!(session.publish_snapshot(original).await.unwrap(), snapshot);
    rig.crash();
    rig.start().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    session.refresh_inspection_inventory().await.unwrap();
    for (index, snapshot) in [snapshot, second].into_iter().enumerate() {
        let deleting = session
            .delete_snapshot(&snapshot.id, "tenant")
            .await
            .unwrap();
        assert!(session.get_volume("tenant", "workspace").await.is_ok());
        session
            .finish_snapshot_deletion(&snapshot.id, deleting.revision)
            .await
            .unwrap();
        session
            .finish_snapshot_deletion(&snapshot.id, deleting.revision)
            .await
            .unwrap();
        assert!(session
            .inspection_inventory()
            .await
            .unwrap()
            .0
            .iter()
            .any(|cached| cached.id == snapshot.id
                && cached.state == adx_core::snapshots::SnapshotState::Deleted));
        if index == 0 {
            assert!(session.get_volume("tenant", "workspace").await.is_ok());
        }
    }
    assert!(matches!(
        session.get_volume("tenant", "workspace").await,
        Err(adx_core::Error::NotFound)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn explicit_environment_reference_retains_volume_after_snapshot_deletion() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let definition = session
        .ensure_volume_for_owner(
            volume("workspace", "workspaces/tenant/user"),
            &domains(),
            "binding",
        )
        .await
        .unwrap();
    let source = environment("binding", "workspace");
    session.pin_volumes(&source).await.unwrap();
    let snapshot = session
        .publish_snapshot(reusable_snapshot("snapshot", source.clone()))
        .await
        .unwrap();
    // Clone does not inherit mounts; this caller explicitly requests the same VolumeRef.
    let mut restored = snapshot.template.clone();
    restored.id = "restored".into();
    session.pin_volumes(&restored).await.unwrap();
    session.unpin_volumes(&source).await.unwrap();
    session
        .release_volume_owner("tenant", "binding")
        .await
        .unwrap();
    let deleting = session
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    session
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .unwrap();
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .volume,
        definition
    );
    session.unpin_volumes(&restored).await.unwrap();
    assert!(matches!(
        session.get_volume("tenant", "workspace").await,
        Err(adx_core::Error::NotFound)
    ));
    session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    restored.id = "late-restored".into();
    assert!(matches!(
        session.pin_volumes(&restored).await,
        Err(adx_core::Error::Conflict)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn deleting_snapshot_keeps_standalone_volume_definition() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let definition = session
        .ensure_volume(volume("workspace", "workspaces/tenant/user"), &domains())
        .await
        .unwrap();
    let source = environment("source", "workspace");
    session.pin_volumes(&source).await.unwrap();
    let snapshot = session
        .publish_snapshot(reusable_snapshot("snapshot", source.clone()))
        .await
        .unwrap();
    session.unpin_volumes(&source).await.unwrap();
    assert!(session.delete_volume("tenant", "workspace").await.is_err());
    let deleting = session
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    session
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .unwrap();
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .volume,
        definition
    );
    session.delete_volume("tenant", "workspace").await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn snapshot_publication_and_binding_release_do_not_leave_unprotected_uids() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    for index in 0..16 {
        let id = format!("volume-{index}");
        let owner = format!("binding-{index}");
        session
            .ensure_volume_for_owner(
                volume(&id, &format!("workspaces/tenant/user-{index}")),
                &domains(),
                &owner,
            )
            .await
            .unwrap();
        let source = environment(&owner, &id);
        session.pin_volumes(&source).await.unwrap();
        let snapshot = reusable_snapshot(&format!("snapshot-{index}"), source.clone());
        let (published, released) =
            tokio::join!(session.publish_snapshot(snapshot.clone()), async {
                session.unpin_volumes(&source).await?;
                session.release_volume_owner("tenant", &owner).await
            });
        released.unwrap();
        match published {
            Ok(snapshot) => {
                let volume = session.get_volume("tenant", &id).await.unwrap();
                assert_eq!(volume.snapshot_ids, [snapshot.id.clone()].into());
                assert!(volume.owner_ids.is_empty());
                let deleting = session
                    .delete_snapshot(&snapshot.id, "tenant")
                    .await
                    .unwrap();
                session
                    .finish_snapshot_deletion(&snapshot.id, deleting.revision)
                    .await
                    .unwrap();
            }
            Err(adx_core::Error::Conflict | adx_core::Error::NotFound) => {
                assert!(matches!(
                    session.get_snapshot(&snapshot.id).await,
                    Err(adx_core::Error::NotFound)
                ));
            }
            Err(error) => panic!("unexpected publication error: {error}"),
        }
        assert!(matches!(
            session.get_volume("tenant", &id).await,
            Err(adx_core::Error::NotFound)
        ));
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn completed_environment_deletion_removes_deferred_volume_and_is_replayable() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    session
        .ensure_volume_for_owner(
            volume("workspace", "workspaces/tenant/user"),
            &domains(),
            "binding",
        )
        .await
        .unwrap();
    let source = environment("binding", "workspace");
    session.pin_volumes(&source).await.unwrap();
    let snapshot = session
        .publish_snapshot(reusable_snapshot("snapshot", source.clone()))
        .await
        .unwrap();
    // Clone does not inherit mounts; this caller explicitly requests the same VolumeRef.
    let mut restored = snapshot.template.clone();
    restored.id = "restored".into();
    let assignment = session
        .claim(restored.clone(), &candidate)
        .await
        .unwrap()
        .record()
        .assignment
        .clone();
    let mut record = starting(restored, assignment);
    session
        .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
        .await
        .unwrap();
    session.unpin_volumes(&source).await.unwrap();
    session
        .release_volume_owner("tenant", "binding")
        .await
        .unwrap();
    let deleting = session
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    session
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .unwrap();
    record.state = adx_core::EnvironmentState::Deleted;
    record.resources_held = false;
    record.revision = 2;
    session
        .commit_releasing(
            record.clone(),
            &candidate.node_session_id,
            std::slice::from_ref(&record.runtime.id),
        )
        .await
        .unwrap();
    assert!(matches!(
        session.get_volume("tenant", "workspace").await,
        Err(adx_core::Error::NotFound)
    ));
    session
        .commit_releasing(
            record.clone(),
            &candidate.node_session_id,
            &[record.runtime.id.clone()],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replacement_session_can_release_existing_exclusive_occupancy() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let spec = environment("env", "workspace");
    let assignment = session
        .claim(spec.clone(), &candidate)
        .await
        .unwrap()
        .record()
        .assignment
        .clone();
    let mut record = starting(spec, assignment);
    session
        .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
        .await
        .unwrap();
    record.state = adx_core::EnvironmentState::Running;
    record.revision = 2;
    record.runtime.ip = Some("10.0.0.1".parse().unwrap());
    session.commit(record.clone()).await.unwrap();
    let current = session
        .snapshot()
        .await
        .unwrap()
        .nodes
        .remove("node-a")
        .unwrap();
    // This is the storage write made by the existing live process replacement path.
    session
        .register_session(
            current.node,
            current.address,
            current.proxy_address,
            Some(adx_coordinator::storage::NodeSession {
                id: "new-session".into(),
                sequence: 2,
                routable: true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .unwrap()
            .node_session_id,
        "new-session"
    );
    assert_eq!(
        session
            .get("env")
            .await
            .unwrap()
            .volume_execution
            .unwrap()
            .node_session_id,
        "new-session"
    );
    record.state = adx_core::EnvironmentState::Failed;
    record.resources_held = false;
    record.runtime.ip = None;
    record.revision = 3;
    session
        .commit_releasing(
            record.clone(),
            "new-session",
            std::slice::from_ref(&record.runtime.id),
        )
        .await
        .unwrap();
    assert!(session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_none());
    // Even node expiry under the current session must not leave an eternal old slot.
    session
        .invalidate_node("node-a", "new-session")
        .await
        .unwrap();
    assert!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .is_none(),
        "new session cleanup + invalidation left the old session occupancy permanently held"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replacement_session_expires_reserved_occupancy_and_rejects_old_session() {
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let spec = environment("env", "workspace");
    session.claim(spec, &candidate).await.unwrap();
    let current = session
        .snapshot()
        .await
        .unwrap()
        .nodes
        .remove("node-a")
        .unwrap();
    session
        .register_session(
            current.node,
            current.address,
            current.proxy_address,
            Some(adx_coordinator::storage::NodeSession {
                id: "replacement".into(),
                sequence: 2,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let slot = session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .unwrap();
    assert_eq!(slot.node_session_id, "replacement");
    assert!(session
        .invalidate_node("node-a", &candidate.node_session_id)
        .await
        .is_err());
    assert!(session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_some());
    session
        .invalidate_node("node-a", "replacement")
        .await
        .unwrap();
    assert!(session
        .get_volume("tenant", "workspace")
        .await
        .unwrap()
        .occupancy
        .is_none());
    assert!(session.get("env").await.unwrap().invalidated);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replacement_session_does_not_mark_interrupted_initialization_ready() {
    use adx_coordinator::storage::Initialization;
    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let candidate = claimant(&session, "node-a").await;
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.create_if_missing = true;
    let volume = session.ensure_volume(definition, &domains()).await.unwrap();
    let spec = environment("env", "workspace");
    let assignment = session
        .claim(spec.clone(), &candidate)
        .await
        .unwrap()
        .record()
        .assignment
        .clone();
    let record = starting(spec, assignment);
    session
        .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
        .await
        .unwrap();
    assert_eq!(
        session
            .begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid)
            .await
            .unwrap(),
        adx_coordinator::storage::InitializationAction::Prepare
    );
    let current = session
        .snapshot()
        .await
        .unwrap()
        .nodes
        .remove("node-a")
        .unwrap();
    session
        .register_session(
            current.node,
            current.address,
            current.proxy_address,
            Some(adx_coordinator::storage::NodeSession {
                id: "replacement".into(),
                sequence: 2,
                routable: true,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .initialization,
        Initialization::NeedsRepair { .. }
    ));
    assert!(session
        .complete_volume_initialization(&record, &candidate.node_session_id, &volume.uid, None)
        .await
        .is_err());
    assert!(session
        .begin_volume_initialization(&record, "replacement", &volume.uid)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replacement_session_racing_node_expiry_keeps_occupancy_consistent() {
    for _ in 0..8 {
        let rig = common::Redis::new().await;
        let session = store(&rig).await.begin(1).await.unwrap();
        let candidate = claimant(&session, "node-a").await;
        let mut definition = volume("workspace", "workspaces/tenant/user");
        definition.access_mode = AccessMode::Exclusive;
        session.ensure_volume(definition, &domains()).await.unwrap();
        let spec = environment("env", "workspace");
        let assignment = session
            .claim(spec.clone(), &candidate)
            .await
            .unwrap()
            .record()
            .assignment
            .clone();
        let record = starting(spec, assignment);
        session
            .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
            .await
            .unwrap();
        let current = session
            .snapshot()
            .await
            .unwrap()
            .nodes
            .remove("node-a")
            .unwrap();
        let (registered, expired) = tokio::join!(
            session.register_session(
                current.node,
                current.address,
                current.proxy_address,
                Some(adx_coordinator::storage::NodeSession {
                    id: "replacement".into(),
                    sequence: 2,
                    routable: true,
                })
            ),
            session.invalidate_node("node-a", &candidate.node_session_id),
        );
        registered.unwrap();
        assert!(expired.is_ok() || matches!(expired, Err(adx_core::Error::Conflict)));
        let saved = session.get("env").await.unwrap();
        let occupancy = session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy;
        if saved.invalidated {
            assert!(
                occupancy.is_none(),
                "replacement must not revive an expired assignment"
            );
        } else {
            assert_eq!(occupancy.unwrap().node_session_id, "replacement");
            assert_eq!(
                saved.volume_execution.unwrap().node_session_id,
                "replacement"
            );
        }
        session
            .invalidate_node("node-a", "replacement")
            .await
            .unwrap();
        assert!(session
            .get_volume("tenant", "workspace")
            .await
            .unwrap()
            .occupancy
            .is_none());
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn initialization_retry_requires_attribute_verification_until_completion_is_published() {
    use adx_coordinator::storage::{Initialization, InitializationAction};
    for failure in [false, true] {
        let rig = common::Redis::new().await;
        let session = store(&rig).await.begin(1).await.unwrap();
        let candidate = claimant(&session, "node-a").await;
        let mut definition = volume("workspace", "workspaces/tenant/user");
        definition.create_if_missing = true;
        let volume = session.ensure_volume(definition, &domains()).await.unwrap();
        let spec = environment("first", "workspace");
        let assignment = session
            .claim(spec.clone(), &candidate)
            .await
            .unwrap()
            .record()
            .assignment
            .clone();
        let record = starting(spec, assignment);
        session
            .authorize_volumes(&record, &record.runtime.id, &candidate.node_session_id)
            .await
            .unwrap();
        let (first, duplicate) = tokio::join!(
            session.begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid),
            session.begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid),
        );
        let actions = [first.unwrap(), duplicate.unwrap()];
        assert_eq!(
            actions
                .iter()
                .filter(|action| **action == InitializationAction::Prepare)
                .count(),
            1
        );
        assert_eq!(
            actions
                .iter()
                .filter(|action| **action == InitializationAction::Verify)
                .count(),
            1
        );
        // The first response/completion was not delivered. Reopen the persisted view.
        let recovered = store(&rig).await.begin(1).await.unwrap();
        assert_eq!(
            recovered
                .begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid)
                .await
                .unwrap(),
            InitializationAction::Verify
        );
        if failure {
            recovered
                .complete_volume_initialization(
                    &record,
                    &candidate.node_session_id,
                    &volume.uid,
                    Some("directory attributes do not match"),
                )
                .await
                .unwrap();
            assert!(matches!(
                recovered
                    .get_volume("tenant", "workspace")
                    .await
                    .unwrap()
                    .initialization,
                Initialization::NeedsRepair { .. }
            ));
            assert!(recovered
                .begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid)
                .await
                .is_err());
        } else {
            recovered
                .complete_volume_initialization(
                    &record,
                    &candidate.node_session_id,
                    &volume.uid,
                    None,
                )
                .await
                .unwrap();
            // A committed completion with a lost reply remains Ready and is idempotent.
            recovered
                .complete_volume_initialization(
                    &record,
                    &candidate.node_session_id,
                    &volume.uid,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                recovered
                    .begin_volume_initialization(&record, &candidate.node_session_id, &volume.uid)
                    .await
                    .unwrap(),
                InitializationAction::Ready
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn volume_claim_batch_and_delete_preserve_request_identity_and_creation_summary() {
    use adx_coordinator::storage::CreationResult;
    use adx_core::{EnvironmentRecord, EnvironmentState, Runtime};

    let rig = common::Redis::new().await;
    let session = store(&rig).await.begin(1).await.unwrap();
    let mut candidate = claimant(&session, "node-a").await;
    candidate.request_id = "volume-request".into();
    let mut plain_candidate = candidate.clone();
    plain_candidate.request_id = "plain-request".into();
    let mut definition = volume("workspace", "workspaces/tenant/user");
    definition.access_mode = AccessMode::Exclusive;
    session.ensure_volume(definition, &domains()).await.unwrap();
    let mounted = environment("mounted", "workspace");
    let mut plain = environment("plain", "workspace");
    plain.execution.mounts.clear();
    let batch = session
        .claim_batch(vec![
            (mounted.clone(), candidate.clone()),
            (plain.clone(), plain_candidate),
        ])
        .await
        .unwrap();
    let client = redis::Client::open(rig.url.clone()).unwrap();
    let mut redis = client.get_multiplexed_async_connection().await.unwrap();
    let requests_key = "adx:{test}:control:v1:create-receipts";
    let pending: u64 = redis::cmd("HLEN")
        .arg(requests_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        pending, 0,
        "an unfinished claim is not a successful creation receipt"
    );

    for (index, outcome) in batch.into_iter().enumerate() {
        let stored = outcome.unwrap().record().clone();
        assert_eq!(
            stored.creation.as_ref().unwrap().request_id,
            if index == 0 {
                "volume-request"
            } else {
                "plain-request"
            }
        );
        let runtime_id = format!("{}-{}", stored.spec.id, stored.assignment.generation);
        let mut record = EnvironmentRecord {
            observation: None,
            spec: stored.spec,
            assignment: stored.assignment,
            runtime: Runtime {
                id: runtime_id.clone(),
                ip: None,
            },
            state: EnvironmentState::Starting,
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
        if index == 0 {
            session
                .authorize_volumes(&record, &runtime_id, &candidate.node_session_id)
                .await
                .unwrap();
        }
        record.runtime.ip = Some("127.0.0.1".parse().unwrap());
        record.state = EnvironmentState::Running;
        record.revision += 1;
        let published = session
            .publish_initial(
                record.clone(),
                if index == 0 {
                    "volume-request"
                } else {
                    "plain-request"
                },
                &candidate.node_session_id,
            )
            .await
            .unwrap();
        assert_eq!(published, record);
        let stored = session.get(&record.spec.id).await.unwrap();
        assert_eq!(stored.creation.unwrap().result, CreationResult::Success);
        if index == 0 {
            assert!(stored.volume_execution.is_some());
        }
        record.runtime.ip = None;
        record.state = EnvironmentState::Deleted;
        record.resources_held = false;
        record.revision += 1;
        session
            .commit_releasing(record.clone(), &candidate.node_session_id, &[runtime_id])
            .await
            .unwrap();
        let replay = session.commit_with_state(record.clone()).await.unwrap();
        assert_eq!(replay.result, record);
        assert!(replay.stored.is_none());
    }
    let bindings: u64 = redis::cmd("HLEN")
        .arg(requests_key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(bindings, 2);
    assert_eq!(
        session
            .published_request("tenant", "volume-request")
            .await
            .unwrap()
            .unwrap()
            .state,
        EnvironmentState::Deleted
    );
    let definition = session.get_volume("tenant", "workspace").await.unwrap();
    assert!(definition.occupancy.is_none());
    session.delete_volume("tenant", "workspace").await.unwrap();
    assert_eq!(session.business_counters().await.unwrap().creations[0], 2);
    plain.id = "reused-request".into();
    assert!(session.claim(plain, &candidate).await.is_ok());
}
