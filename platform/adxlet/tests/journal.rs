use adx_core::{
    Assignment, CheckpointArtifact, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error,
    Resources, Result,
};
use adxlet::{journal::JournalSink, Durability, StateSink};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Coordinator {
    failure: Mutex<Option<Error>>,
    records: Mutex<Vec<EnvironmentRecord>>,
    stopped: Mutex<Vec<Vec<String>>>,
    cleanup: Mutex<Vec<Vec<CheckpointArtifact>>>,
    catalog: Mutex<Option<Vec<EnvironmentRecord>>>,
}
#[async_trait]
impl StateSink for Coordinator {
    async fn has_current_assignment(&self, record: &EnvironmentRecord) -> Result<bool> {
        let catalog = self.catalog.lock().unwrap();
        let catalog = catalog
            .as_ref()
            .ok_or_else(|| Error::Unavailable("authority unavailable".into()))?;
        Ok(catalog
            .iter()
            .any(|current| current.assignment == record.assignment))
    }

    async fn commit_deleted(
        &self,
        record: &EnvironmentRecord,
        cleanup_artifacts: &[CheckpointArtifact],
        stopped: &[String],
    ) -> Result<Durability> {
        let durability = self.commit(record).await?;
        if !stopped.is_empty() {
            self.stopped.lock().unwrap().push(stopped.to_vec());
        }
        self.cleanup
            .lock()
            .unwrap()
            .push(cleanup_artifacts.to_vec());
        Ok(durability)
    }
    async fn commit_releasing(
        &self,
        record: &EnvironmentRecord,
        stopped: &[String],
    ) -> Result<Durability> {
        let durability = self.commit(record).await?;
        if !stopped.is_empty() {
            self.stopped.lock().unwrap().push(stopped.to_vec());
        }
        Ok(durability)
    }
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        if let Some(e) = self.failure.lock().unwrap().clone() {
            return Err(e);
        }
        self.records.lock().unwrap().push(record.clone());
        Ok(Durability::Published)
    }
}
fn record(revision: u64) -> EnvironmentRecord {
    EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            envd: None,
            run_policy: None,
            compatibility: None,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runc".into(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 0,
            },
            priority: 0,
            execution: Default::default(),
        },
        assignment: Assignment {
            environment_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        runtime: adx_core::Runtime {
            id: "i-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        state: EnvironmentState::Running,
        revision,
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    }
}

fn deleted_record(revision: u64) -> EnvironmentRecord {
    let mut record = record(revision);
    record.state = EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record
}

fn artifact() -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "memory".into(),
        scope: "node".into(),
        location: "checkpoint/obsolete".into(),
        size_bytes: 1,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_outage_results_are_journaled_and_recover_after_restart() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Outage {
        concurrent: AtomicBool,
        gate: tokio::sync::Barrier,
    }

    #[async_trait]
    impl StateSink for Outage {
        async fn commit(&self, _record: &EnvironmentRecord) -> Result<Durability> {
            if self.concurrent.load(Ordering::SeqCst) {
                tokio::time::timeout(std::time::Duration::from_secs(10), self.gate.wait())
                    .await
                    .unwrap();
            }
            Err(Error::Unavailable(
                "coordinator temporarily unavailable".into(),
            ))
        }
    }

    const WRITERS: usize = 64;
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let upstream = Arc::new(Outage {
        concurrent: AtomicBool::new(false),
        gate: tokio::sync::Barrier::new(WRITERS),
    });
    let sink = Arc::new(JournalSink::new(path.clone(), upstream.clone()));
    let mut catalog = vec![record(2)];
    assert_eq!(
        sink.commit(&catalog[0]).await.unwrap(),
        Durability::Journaled
    );
    upstream.concurrent.store(true, Ordering::SeqCst);
    for round in 0..4 {
        let mut writers = tokio::task::JoinSet::new();
        for number in 0..WRITERS {
            let mut value = record(2);
            value.spec.id = format!("outage-{round}-{number}");
            value.assignment.environment_id = value.spec.id.clone();
            value.runtime.id = format!("{}-1", value.spec.id);
            catalog.push(value.clone());
            let sink = sink.clone();
            writers.spawn(async move { sink.commit(&value).await });
        }
        let mut errors = vec![];
        while let Some(answer) = writers.join_next().await {
            match answer.unwrap() {
                Ok(Durability::Journaled) => (),
                result => errors.push(result),
            }
        }
        assert!(
            errors.is_empty(),
            "Concurrent journal writes failed: {errors:?}"
        );
        assert_eq!(sink.pending().await.unwrap(), catalog.len());
    }
    drop(sink);
    let coordinator = Arc::new(Coordinator::default());
    let recovered = JournalSink::new(path, coordinator.clone());
    // The outage results are revision 2; the authoritative catalog still has
    // revision 1. Supplying revision 2 would correctly acknowledge them as
    // already published instead of replaying them to the test Coordinator.
    for value in &mut catalog {
        value.revision = 1;
    }
    recovered.recover(&catalog).await.unwrap();
    assert_eq!(recovered.pending().await.unwrap(), 0);
    let published = coordinator.records.lock().unwrap();
    assert_eq!(published.len(), catalog.len());
    assert!(published.iter().all(|value| value.revision == 2));
    assert_eq!(
        published
            .iter()
            .map(|value| &value.spec.id)
            .collect::<std::collections::BTreeSet<_>>(),
        catalog.iter().map(|value| &value.spec.id).collect()
    );
}

#[tokio::test]
async fn recovery_replays_cleanup_proof_even_when_the_failed_result_was_already_committed() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    let mut failed = record(2);
    failed.state = EnvironmentState::Failed;
    failed.resources_held = false;
    failed.runtime.ip = None;
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    sink.commit(&failed).await.unwrap();
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert_eq!(
        sink.commit_releasing(&failed, &["i-1".into()])
            .await
            .unwrap(),
        Durability::Journaled
    );
    drop(sink);
    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[failed]).await.unwrap();
    assert_eq!(
        *coordinator.stopped.lock().unwrap(),
        vec![vec!["i-1".to_owned()]]
    );
    assert_eq!(sink.pending().await.unwrap(), 0);
}

#[tokio::test]
async fn healthy_commits_do_not_create_or_require_a_local_database() {
    let coordinator = Arc::new(Coordinator::default());
    let tmp = tempfile::tempdir().unwrap();
    let not_directory = tmp.path().join("file");
    std::fs::write(&not_directory, "not a directory").unwrap();
    let sink = JournalSink::new(not_directory.join("journal.sqlite"), coordinator.clone());
    assert_eq!(
        sink.commit(&record(2)).await.unwrap(),
        Durability::Published
    );
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert!(sink.commit(&record(3)).await.is_err());
}

#[tokio::test]
async fn outage_is_durable_and_replays_in_order_before_new_results() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    assert_eq!(
        sink.commit(&record(2)).await.unwrap(),
        Durability::Published
    );
    assert!(!path.exists());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    for revision in [3, 4, 4] {
        assert_eq!(
            sink.commit(&record(revision)).await.unwrap(),
            Durability::Journaled
        );
    }
    assert_eq!(sink.pending().await.unwrap(), 2);
    assert_eq!(sink.commit(&record(3)).await, Err(Error::Conflict));
    drop(sink);
    let sink = JournalSink::new(path, coordinator.clone());
    // A restart must first obtain the authoritative catalog, even if the caller
    // knows a locally journaled identity.
    assert!(sink.commit(&record(5)).await.is_err());
    *coordinator.failure.lock().unwrap() = None;
    sink.recover(&[record(2)]).await.unwrap();
    assert_eq!(
        sink.commit(&record(5)).await.unwrap(),
        Durability::Published
    );
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert_eq!(
        coordinator
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.revision)
            .collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
}

#[tokio::test]
async fn rejected_ownership_is_not_treated_as_an_outage() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Conflict);
    let sink = JournalSink::new(path.clone(), coordinator);
    assert_eq!(sink.commit(&record(2)).await, Err(Error::Conflict));
    assert!(!path.exists());
}

#[tokio::test]
async fn recovery_discards_retired_ownership_and_already_committed_results() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    sink.commit(&record(3)).await.unwrap();
    drop(sink);
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    *coordinator.failure.lock().unwrap() = None;
    let mut replacement = record(4);
    replacement.assignment.generation = 2;
    sink.recover(&[replacement]).await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert_eq!(sink.pending().await.unwrap(), 0);

    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    sink.commit(&record(4)).await.unwrap();
    drop(sink);
    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[record(4)]).await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert_eq!(sink.pending().await.unwrap(), 0);
}

#[tokio::test]
async fn authoritative_failure_discards_newer_local_running_result() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    let journal = JournalSink::new(root.path().join("outage.sqlite"), coordinator.clone());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert_eq!(
        journal.commit(&record(100)).await.unwrap(),
        Durability::Journaled
    );
    *coordinator.failure.lock().unwrap() = None;
    let mut failed = record(3);
    failed.state = EnvironmentState::Failed;
    failed.resources_held = false;
    failed.runtime.ip = None;
    journal.recover(&[failed]).await.unwrap();
    journal.flush().await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn direct_recovery_evidence_survives_journal_restart_and_replay() {
    use adx_core::observation::{
        EvidenceSource, ExecutionObservation, InterruptionEvidence, InterruptionReason,
    };
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    let old = record(2);
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    sink.commit(&old).await.unwrap();
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("response lost".into()));
    let mut recovered = record(5);
    recovered.runtime.id = "i-1-recovered".into();
    recovered.observation = Some(ExecutionObservation {
        interruption: Some(InterruptionEvidence {
            event_id: "execution:1:i-1".into(),
            detected_at_unix_millis: 1,
            assignment_generation: 1,
            runtime_id: old.runtime.id.clone(),
            worker_id: "n".into(),
            reason: InterruptionReason::Oom,
            evidence_source: EvidenceSource::BackendExit,
            exit_code: Some(137),
            detail: "backend confirmed OOM".into(),
        }),
        ..Default::default()
    });
    assert_eq!(
        sink.commit(&recovered).await.unwrap(),
        Durability::Journaled
    );
    drop(sink);
    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[old]).await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert_eq!(coordinator.records.lock().unwrap().last(), Some(&recovered));
    assert!(coordinator
        .records
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.state == EnvironmentState::Running));
}

#[tokio::test]
async fn deleted_cleanup_survives_restart_and_replays_with_its_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("cleanup.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    let deleted = deleted_record(3);
    let artifact = artifact();
    assert_eq!(
        sink.commit_deleted(
            &deleted,
            std::slice::from_ref(&artifact),
            &["runtime-stopped".into()],
        )
        .await
        .unwrap(),
        Durability::Journaled
    );
    drop(sink);

    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[record(2)]).await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert_eq!(coordinator.records.lock().unwrap().as_slice(), &[deleted]);
    assert_eq!(
        coordinator.cleanup.lock().unwrap().as_slice(),
        &[vec![artifact]]
    );
    assert_eq!(
        coordinator.stopped.lock().unwrap().as_slice(),
        &[vec!["runtime-stopped".to_owned()]]
    );
}

#[tokio::test]
async fn cleanup_recovery_discards_a_reassigned_environment_without_blocking() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("reassigned.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    let deleted = deleted_record(3);
    sink.commit_deleted(&deleted, &[artifact()], &[])
        .await
        .unwrap();
    drop(sink);

    *coordinator.failure.lock().unwrap() = None;
    let mut replacement = record(4);
    replacement.assignment.generation = 2;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[replacement]).await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert!(coordinator.cleanup.lock().unwrap().is_empty());
}

#[tokio::test]
async fn deleted_cleanup_recovery_discards_revoked_ownership_without_claiming_publication() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("retired.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    sink.commit_deleted(&deleted_record(3), &[artifact()], &[])
        .await
        .unwrap();
    drop(sink);
    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[]).await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert!(coordinator.cleanup.lock().unwrap().is_empty());
}

#[tokio::test]
async fn expired_deleted_receipt_requires_fresh_revoked_assignment_before_journal_ack() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(root.path().join("expired.sqlite"), coordinator.clone());
    let deleted = deleted_record(3);
    sink.commit_deleted(&deleted, &[artifact()], &[])
        .await
        .unwrap();
    // Coordinator's 600-second receipt has expired. NotFound alone is not proof.
    *coordinator.failure.lock().unwrap() = Some(Error::NotFound);
    assert!(matches!(sink.flush().await, Err(Error::Unavailable(_))));
    assert_eq!(sink.pending().await.unwrap(), 1);
    *coordinator.catalog.lock().unwrap() = Some(vec![record(2)]);
    assert_eq!(sink.flush().await, Err(Error::NotFound));
    assert_eq!(sink.pending().await.unwrap(), 1);
    *coordinator.catalog.lock().unwrap() = Some(vec![]);
    sink.flush().await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    // Discarding an obsolete journal is not a successful commit receipt.
    assert_eq!(
        sink.commit_deleted(&deleted, &[artifact()], &[]).await,
        Err(Error::NotFound)
    );
    assert!(coordinator.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn conflicting_cleanup_payload_is_never_discarded_as_retired_ownership() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(root.path().join("conflict.sqlite"), coordinator.clone());
    sink.commit_deleted(&deleted_record(3), &[artifact()], &[])
        .await
        .unwrap();
    *coordinator.failure.lock().unwrap() = Some(Error::Conflict);
    *coordinator.catalog.lock().unwrap() = Some(vec![record(2)]);
    assert_eq!(sink.flush().await, Err(Error::Conflict));
    assert_eq!(sink.pending().await.unwrap(), 1);
}

#[tokio::test]
async fn live_flush_discards_a_journal_for_the_previous_generation_of_the_same_name() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(root.path().join("recreated.sqlite"), coordinator.clone());
    let deleted = deleted_record(3);
    sink.commit_deleted(&deleted, &[artifact()], &[])
        .await
        .unwrap();
    *coordinator.failure.lock().unwrap() = Some(Error::Conflict);
    let mut replacement = record(1);
    replacement.assignment.generation = 2;
    replacement.runtime.id = "i-2".into();
    assert_eq!(replacement.spec.id, deleted.spec.id);
    *coordinator.catalog.lock().unwrap() = Some(vec![replacement]);
    sink.flush().await.unwrap();
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert_eq!(
        sink.commit_deleted(&deleted, &[artifact()], &[]).await,
        Err(Error::Conflict)
    );
}
