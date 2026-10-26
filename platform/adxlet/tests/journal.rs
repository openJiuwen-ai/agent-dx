use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources, Result,
};
use adxlet::{journal::JournalSink, Durability, StateSink};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Coordinator {
    failure: Mutex<Option<Error>>,
    records: Mutex<Vec<EnvironmentRecord>>,
    stopped: Mutex<Vec<Vec<String>>>,
}
#[async_trait]
impl StateSink for Coordinator {
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
