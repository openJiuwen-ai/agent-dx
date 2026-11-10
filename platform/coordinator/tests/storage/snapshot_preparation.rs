use super::*;
use adx_core::snapshot_preparation::PreparationState;

fn resources() -> Resources {
    Resources {
        cpu_millis: 1000,
        memory_bytes: 1024,
        disk_bytes: 0,
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn preparation_registration_is_serial_and_preserves_family_identity() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, build) = session
        .register_snapshot_preparation("tenant", " Image ", resources())
        .await
        .unwrap();
    assert!(
        matches!(session.register_snapshot_preparation("tenant", "image", resources()).await,
        Err(Error::Invalid(message)) if message.contains("busy"))
    );
    session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Failed;
            Ok(())
        })
        .await
        .unwrap();
    let (again, second) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_eq!(first.id, again.id);
    assert_ne!(build.id, second.id);
    assert_eq!(again.preparation.id, second.id);
    assert_eq!(again.revision, 3);
    assert_eq!(
        session
            .get_snapshot_template("tenant", "IMAGE")
            .await
            .unwrap(),
        again
    );
    assert_eq!(
        session.get_snapshot_template("other", &again.id).await,
        Err(Error::NotFound)
    );
    assert!(session
        .list_snapshot_templates("other")
        .await
        .unwrap()
        .is_empty());
    session
        .update_snapshot_template(&again.id, |template| {
            template.deleted = true;
            Ok(())
        })
        .await
        .unwrap();
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_ne!(replacement.id, again.id);
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap()
            .id,
        replacement.id
    );
    assert!(
        session
            .get_snapshot_template("tenant", &again.id)
            .await
            .unwrap()
            .deleted
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn concurrent_registration_admits_one_build_and_cas_updates_preserve_every_commit() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            session
                .register_snapshot_preparation("tenant", "image", resources())
                .await
        }));
    }
    let mut accepted = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok((template, _)) => accepted.push(template),
            Err(Error::Invalid(message)) => assert!(message.contains("busy")),
            other => panic!("unexpected registration result: {other:?}"),
        }
    }
    assert_eq!(accepted.len(), 1);
    let id = accepted.pop().unwrap().id;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        let id = id.clone();
        tasks.push(tokio::spawn(async move {
            session
                .update_snapshot_template(&id, |template| {
                    template
                        .preparation
                        .error
                        .get_or_insert_with(String::new)
                        .push('x');
                    template.preparation.state = PreparationState::Failed;
                    Ok(())
                })
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let template = session.get_snapshot_template("tenant", &id).await.unwrap();
    assert_eq!(template.revision, 9);
    assert_eq!(template.preparation.error.as_deref(), Some("xxxxxxxx"));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn snapshot_preparation_persists_across_restart_and_fences_the_previous_epoch() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let active = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        active
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap(),
        template
    );
    // The old connection manager reconnects asynchronously after the Redis crash.
    // Require an actual epoch rejection once transport recovers, not Unavailable.
    let stale_update = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = session
                .update_snapshot_template(&template.id, |_| Ok(()))
                .await;
            if matches!(result, Err(Error::Unavailable(_))) {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            break result;
        }
    })
    .await
    .expect("stale client reconnects within the test deadline");
    assert_eq!(stale_update, Err(Error::Conflict));
    assert_eq!(
        session
            .remove_finished_snapshot_template(&template.id)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .register_snapshot_preparation("tenant", "other", resources())
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        active.list_snapshot_templates("tenant").await.unwrap(),
        vec![template]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn finished_tombstone_removal_waits_for_work_and_preserves_a_reused_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    let spec: adx_core::EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"seed", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
        "priority":0, "resources":{"cpu_millis":1000,"memory_bytes":1024,"disk_bytes":0}
    }))
    .unwrap();
    for phase in [
        PreparationState::Creating,
        PreparationState::Snapshotting,
        PreparationState::Cleaning,
    ] {
        session
            .update_snapshot_template(&template.id, |template| {
                template.deleted = true;
                let build = &mut template.preparation;
                build.state = phase;
                build.spec = Some(spec.clone());
                build.expected_revision = Some(1);
                build.snapshot_id = Some("independent-golden".into());
                Ok(())
            })
            .await
            .unwrap();
        assert!(!session
            .remove_finished_snapshot_template(&template.id)
            .await
            .unwrap());
    }
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&template.id, |template| {
            let build = &mut template.preparation;
            build.state = PreparationState::Failed;
            build.cleanup_pending = true;
            Ok(())
        })
        .await
        .unwrap();
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    session
        .update_snapshot_template(&template.id, |template| {
            template.preparation.cleanup_pending = false;
            Ok(())
        })
        .await
        .unwrap();
    assert!(session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    assert_eq!(
        session.get_snapshot_template("tenant", &template.id).await,
        Err(Error::NotFound)
    );
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap(),
        replacement
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn deleted_preparation_cleanup_cannot_steal_a_reused_family_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&first.id, |template| {
            template.deleted = true;
            Ok(())
        })
        .await
        .unwrap();
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Failed;
            template.preparation.error = Some("cleaned".into());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap(),
        replacement
    );
    assert_eq!(
        session
            .update_snapshot_template(&first.id, |template| {
                template.deleted = false;
                Ok(())
            })
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .update_snapshot_template(&replacement.id, |template| {
                template.name = "renamed".into();
                Ok(())
            })
            .await,
        Err(Error::Conflict)
    );
    let (other, _) = session
        .register_snapshot_preparation("other", "image", resources())
        .await
        .unwrap();
    assert_ne!(other.id, replacement.id);
    assert_eq!(
        session.list_snapshot_templates("other").await.unwrap(),
        vec![other]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn template_record_limit_fails_without_claiming_an_active_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut command = redis::cmd("HSET");
    command.arg("adx:{test}:control:v1:snapshot-templates");
    for index in 0..4096 {
        command.arg(format!("reserved-{index}")).arg("{}");
    }
    command.query_async::<()>(&mut connection).await.unwrap();
    assert!(matches!(
        session
            .register_snapshot_preparation("tenant", "blocked", resources())
            .await,
        Err(Error::Invalid(_))
    ));
    let count: u64 = redis::cmd("HLEN")
        .arg("adx:{test}:control:v1:snapshot-template-names")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn invalid_preparation_state_does_not_advance_the_persisted_revision() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert!(matches!(
        session
            .update_snapshot_template(&template.id, |template| {
                template.preparation.state = PreparationState::Ready;
                Ok(())
            })
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap(),
        template
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn rebuilds_retain_only_current_and_latest_successful_without_a_generation_limit() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, successful) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    let first_snapshot = golden(&session, "native-golden").await;
    ready(&session, &first.id, &first_snapshot).await;
    for _ in 0..300 {
        let (template, build) = session
            .register_snapshot_preparation("tenant", "image", resources())
            .await
            .unwrap();
        assert_eq!(template.id, first.id);
        assert_ne!(build.id, successful.id);
        assert_eq!(
            template.latest_successful.as_ref().unwrap().preparation_id,
            successful.id
        );
        session
            .update_snapshot_template(&template.id, |template| {
                template.preparation.state = PreparationState::Failed;
                Ok(())
            })
            .await
            .unwrap();
    }
    let template = session
        .get_snapshot_template("tenant", &first.id)
        .await
        .unwrap();
    assert_eq!(
        template.latest_successful.as_ref().unwrap().snapshot_id,
        "native-golden"
    );
    let new_snapshot = golden(&session, "new-native-golden").await;
    let updated = ready(&session, &first.id, &new_snapshot).await;
    assert_eq!(
        updated.latest_successful.as_ref().unwrap().preparation_id,
        updated.preparation.id
    );
    assert_eq!(
        updated.latest_successful.as_ref().unwrap().snapshot_id,
        "new-native-golden"
    );
}

async fn golden(
    session: &adx_coordinator::storage::Session,
    id: &str,
) -> adx_core::snapshots::Snapshot {
    let snapshot = adx_core::snapshots::Snapshot::new_with_kind(
        adx_core::snapshots::SnapshotKind::Golden,
        id.into(),
        vec![],
        spec(&format!("seed-{id}")),
        "node".into(),
        format!("seed-{id}-1"),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            scope: String::new(),
            location: id.into(),
            size_bytes: 64,
        },
    )
    .unwrap();
    session.publish_snapshot(snapshot).await.unwrap()
}

async fn ready(
    session: &adx_coordinator::storage::Session,
    family: &str,
    snapshot: &adx_core::snapshots::Snapshot,
) -> adx_core::snapshot_preparation::SnapshotTemplate {
    session
        .update_snapshot_template(family, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.spec = Some(snapshot.template.clone());
            template.preparation.snapshot_id = Some(snapshot.id.clone());
            Ok(())
        })
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn ready_binds_only_matching_golden_and_preserves_real_build_timestamps() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (family, build) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_eq!(family.usage.as_ref().unwrap().spawn_count, 0);
    assert_eq!(family.usage.as_ref().unwrap().last_spawned_at_unix_ms, None);
    assert_eq!(build.updated_at_unix_ms, build.created_at_unix_ms);
    assert!(family.updated_at_unix_ms >= family.created_at_unix_ms);
    let snapshot = golden(&session, "binding").await;
    let invalid = session
        .update_snapshot_template(&family.id, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.snapshot_id = Some(snapshot.id.clone());
            template.preparation.spec = Some(spec("wrong-source"));
            Ok(())
        })
        .await;
    assert_eq!(invalid, Err(Error::Conflict));
    assert_eq!(session.get_snapshot(&snapshot.id).await.unwrap(), snapshot);
    assert_eq!(
        session
            .get_snapshot_template("tenant", &family.id)
            .await
            .unwrap(),
        family
    );
    session
        .update_snapshot_template(&family.id, |template| {
            template.preparation.state = PreparationState::Cleaning;
            template.preparation.spec = Some(snapshot.template.clone());
            template.preparation.snapshot_id = Some(snapshot.id.clone());
            template.preparation.cleanup_pending = true;
            Ok(())
        })
        .await
        .unwrap();
    let replacement = golden(&session, "replacement").await;
    let substituted = session
        .update_snapshot_template(&family.id, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.spec = Some(replacement.template.clone());
            template.preparation.snapshot_id = Some(replacement.id.clone());
            template.preparation.cleanup_pending = false;
            Ok(())
        })
        .await;
    assert_eq!(substituted, Err(Error::Conflict));
    assert_eq!(
        session.get_snapshot(&replacement.id).await.unwrap(),
        replacement
    );
    let completed = session
        .update_snapshot_template(&family.id, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.spec = None;
            template.preparation.cleanup_pending = false;
            Ok(())
        })
        .await
        .unwrap();
    let bound = session.get_snapshot(&snapshot.id).await.unwrap();
    assert_eq!(
        bound.preparation_template_id.as_deref(),
        Some(family.id.as_str())
    );
    assert_eq!(bound.revision, snapshot.revision + 1);
    assert_eq!(session.publish_snapshot(snapshot).await.unwrap(), bound);
    let successful = completed.latest_successful.as_ref().unwrap();
    assert_eq!(successful.created_at_unix_ms, build.created_at_unix_ms);
    assert_eq!(
        successful.updated_at_unix_ms,
        completed.preparation.updated_at_unix_ms
    );
    let unchanged = session
        .update_snapshot_template(&family.id, |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(unchanged.preparation, completed.preparation);
    assert_eq!(unchanged.latest_successful, completed.latest_successful);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn template_usage_counts_atomic_first_success_across_replay_rebuild_and_retirement() {
    const COUNT: usize = 8;
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let (family, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    let snapshot = golden(&session, "old-build").await;
    let completed = ready(&session, &family.id, &snapshot).await;
    let old_build = completed.latest_successful.clone();
    let mut tasks = Vec::new();
    for index in 0..COUNT {
        let mut specification = spec(&format!("spawn-{index}"));
        specification.snapshot_id = Some(snapshot.id.clone());
        let record = running(
            specification,
            Assignment {
                environment_id: format!("spawn-{index}"),
                node_id: "node".into(),
                shard_id: 0,
                generation: index as u64 + 1,
                devices: vec![],
            },
        );
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            let request = format!("spawn-request-{index}");
            let published = session
                .publish_initial(record.clone(), &request, "")
                .await
                .unwrap();
            assert_eq!(
                session.publish_initial(record, &request, "").await.unwrap(),
                published
            );
            published
        }));
    }
    // Registration and publication must compare the same old family bytes, retaining both updates.
    let (rebuilding, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_eq!(rebuilding.latest_successful, old_build);
    let mut records = Vec::new();
    for task in tasks {
        records.push(task.await.unwrap());
    }
    let counted = session
        .get_snapshot_template("tenant", &family.id)
        .await
        .unwrap();
    assert_eq!(counted.usage.as_ref().unwrap().spawn_count, COUNT as u64);
    assert_eq!(counted.preparation, rebuilding.preparation);
    let mut latest = 0;
    for record in &records {
        let stored = session.get(&record.spec.id).await.unwrap();
        latest = latest.max(stored.creation.unwrap().settled_at_unix_millis.unwrap());
    }
    assert_eq!(
        counted.usage.as_ref().unwrap().last_spawned_at_unix_ms,
        Some(latest)
    );
    // A different execution with an existing receipt cannot add another family spawn.
    let mut loser = records[0].clone();
    loser.spec.id = "receipt-loser".into();
    loser.assignment.environment_id = loser.spec.id.clone();
    loser.runtime.id = "receipt-loser-1".into();
    assert_eq!(
        session
            .publish_initial(loser, "spawn-request-0", "")
            .await
            .unwrap(),
        records[0]
    );
    let mut retired = records[0].clone();
    retired.state = EnvironmentState::Deleted;
    retired.resources_held = false;
    retired.revision += 1;
    session.commit(retired).await.unwrap();
    let mut paused = records[1].clone();
    paused.state = EnvironmentState::Paused;
    paused.resources_held = false;
    paused.runtime.ip = None;
    paused.revision += 1;
    paused.checkpoint = Some(adx_core::RestorePoint {
        id: "pause".into(),
        origin: None,
        expires_at_unix_seconds: None,
        source_runtime_id: paused.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            scope: String::new(),
            storage: "shared".into(),
            location: "paused-spawn".into(),
            size_bytes: 8,
        },
    });
    session.commit(paused.clone()).await.unwrap();
    let mut resumed = paused.clone();
    resumed.state = EnvironmentState::Running;
    resumed.resources_held = true;
    resumed.runtime.ip = records[1].runtime.ip;
    resumed.runtime.id = format!("{}-r1", paused.runtime.id);
    resumed.revision += 1;
    resumed.last_operation = Some(adx_core::CompletedOperation {
        id: "resume".into(),
        expected_revision: paused.revision,
        kind: adx_core::LifecycleKind::Resume,
    });
    session.commit(resumed.clone()).await.unwrap();
    session
        .publish_initial(resumed, "resume-request", "")
        .await
        .unwrap();
    assert_eq!(
        session
            .get_snapshot_template("tenant", &family.id)
            .await
            .unwrap(),
        counted
    );
    let replacement = golden(&session, "new-build").await;
    let rebuilt = ready(&session, &family.id, &replacement).await;
    let mut specification = spec("from-old-build");
    specification.snapshot_id = Some(snapshot.id);
    session
        .publish_initial(
            running(
                specification,
                Assignment {
                    environment_id: "from-old-build".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: 99,
                    devices: vec![],
                },
            ),
            "from-old-build",
            "",
        )
        .await
        .unwrap();
    let final_family = session
        .get_snapshot_template("tenant", &family.id)
        .await
        .unwrap();
    assert_eq!(
        final_family.usage.as_ref().unwrap().spawn_count,
        COUNT as u64 + 1
    );
    assert_eq!(final_family.preparation, rebuilt.preparation);
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [COUNT as u64 + 1, 0, 0]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn legacy_template_history_remains_unknown_and_does_not_block_creation() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let (family, _) = session
        .register_snapshot_preparation("tenant", "legacy", resources())
        .await
        .unwrap();
    let snapshot = golden(&session, "legacy-golden").await;
    let completed = ready(&session, &family.id, &snapshot).await;
    let mut legacy = serde_json::to_value(completed).unwrap();
    legacy.as_object_mut().unwrap().remove("usage");
    legacy.as_object_mut().unwrap().remove("updated_at_unix_ms");
    legacy["preparation"]
        .as_object_mut()
        .unwrap()
        .remove("updated_at_unix_ms");
    for field in ["created_at_unix_ms", "updated_at_unix_ms"] {
        legacy["latest_successful"]
            .as_object_mut()
            .unwrap()
            .remove(field);
    }
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HSET")
        .arg("adx:{test}:control:v1:snapshot-templates")
        .arg(&family.id)
        .arg(serde_json::to_string(&legacy).unwrap())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let unknown = session
        .get_snapshot_template("tenant", &family.id)
        .await
        .unwrap();
    assert_eq!(unknown.usage, None);
    assert_eq!(unknown.updated_at_unix_ms, 0);
    assert_eq!(unknown.preparation.updated_at_unix_ms, 0);
    assert_eq!(
        unknown
            .latest_successful
            .as_ref()
            .unwrap()
            .created_at_unix_ms,
        0
    );
    let mut specification = spec("legacy-spawn");
    specification.snapshot_id = Some(snapshot.id);
    session
        .publish_initial(
            running(
                specification,
                Assignment {
                    environment_id: "legacy-spawn".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: 1,
                    devices: vec![],
                },
            ),
            "legacy-spawn",
            "",
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .get_snapshot_template("tenant", &family.id)
            .await
            .unwrap(),
        unknown
    );
}
