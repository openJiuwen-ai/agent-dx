//! Exercise real Coordinator storage operations, not a mock or extracted Lua.
use super::*;
use adx_coordinator::storage::RedisStore;
use adx_core::{
    snapshots::{Reference, Snapshot},
    CheckpointArtifact,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated real Redis"]
async fn checkpoint_publication_rounds_1024() {
    const COUNT: usize = 1024;
    let rig = common::Redis::new().await;
    // The test measures metadata contention, not the default transport budget.
    let store = RedisStore::connect(&rig.url, "test", Duration::from_secs(60))
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    session.refresh_inspection_inventory().await.unwrap();
    register(&session, "node").await;
    let mut records = Vec::new();
    for index in 0..COUNT {
        let spec = spec(&format!("source-{index}"));
        let assignment = Assignment {
            environment_id: spec.id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        session
            .reserve(spec.clone(), assignment.clone())
            .await
            .unwrap();
        let record = running(spec, assignment);
        session.commit(record.clone()).await.unwrap();
        records.push(record);
    }
    for round in 0..2 {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(COUNT));
        let mut tasks = tokio::task::JoinSet::new();
        for mut record in records.drain(..) {
            let session = session.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                let started = std::time::Instant::now();
                let staged = CheckpointArtifact {
                    storage: "shared".into(),
                    scope: String::new(),
                    location: format!("staging-{}-{round}", record.spec.id),
                    size_bytes: 0,
                };
                record.execution_artifacts = vec![staged];
                record.revision += 1;
                session.commit(record.clone()).await?;
                let reserved = started.elapsed();
                // Capturing bytes is deliberately outside this metadata test.
                let sealed = CheckpointArtifact {
                    storage: "shared".into(),
                    scope: String::new(),
                    location: format!("sealed-{}-{round}", record.spec.id),
                    size_bytes: 1024,
                };
                record.execution_artifacts = vec![sealed.clone()];
                record.revision += 1;
                session.commit(record.clone()).await?;
                let held = started.elapsed();
                let snapshot = Snapshot::new(
                    format!("snapshot-{}-{round}", record.spec.id),
                    vec![],
                    record.spec.clone(),
                    "node".into(),
                    record.runtime.id.clone(),
                    sealed,
                )?;
                session.publish_snapshot(snapshot).await?;
                let published = started.elapsed();
                record.execution_artifacts.clear();
                record.revision += 1;
                session.commit(record.clone()).await?;
                Ok::<_, Error>((
                    record,
                    [
                        reserved,
                        held - reserved,
                        published - held,
                        started.elapsed() - published,
                        started.elapsed(),
                    ],
                ))
            });
        }
        let mut failures = Vec::new();
        let mut durations = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok((record, elapsed)) => {
                    records.push(record);
                    durations.push(elapsed);
                }
                Err(error) => failures.push(error),
            }
        }
        for (stage, name) in ["reserve", "sealed_hold", "catalog", "release", "total"]
            .iter()
            .enumerate()
        {
            let mut samples: Vec<_> = durations.iter().map(|v| v[stage].as_secs_f64()).collect();
            samples.sort_by(f64::total_cmp);
            if !samples.is_empty() {
                eprintln!("checkpoint_metadata round={round} stage={name} count={} p50={:.6} p95={:.6} max={:.6}", samples.len(), samples[samples.len()/2], samples[(samples.len()*95/100).min(samples.len()-1)], samples[samples.len()-1]);
            }
        }
        assert!(
            failures.is_empty(),
            "round {round}: {} independent publications failed: {:?}",
            failures.len(),
            &failures[..failures.len().min(8)]
        );
        assert_eq!(records.len(), COUNT);
    }
    // Source retirement must not reclaim artifacts still owned by saved snapshots.
    for mut record in records {
        record.state = EnvironmentState::Deleted;
        record.resources_held = false;
        record.runtime.ip = None;
        record.revision += 1;
        session.commit(record.clone()).await.unwrap();
        let id = format!("snapshot-{}-1", record.spec.id);
        let saved = session
            .acquire_snapshot(
                &id,
                "tenant",
                Reference::Restore {
                    environment_id: format!("restored-{}", record.spec.id),
                },
            )
            .await
            .unwrap();
        assert!(session
            .claim_artifact_deletion(&saved.artifact, None)
            .await
            .is_err());
    }
    assert_eq!(
        session.inspection_inventory().await.unwrap().0.len(),
        COUNT * 2
    );
}
