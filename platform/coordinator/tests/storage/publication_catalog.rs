use super::*;
use adx_coordinator::routes::RoutePublisher;
use adx_protocol::{
    auth::Peers,
    control::{self as pb, environment_directory_service_server::EnvironmentDirectoryService},
};
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::Request;

async fn directory(publisher: &RoutePublisher) -> pb::EnvironmentDirectoryFrame {
    let mut request = Request::new(pb::WatchEnvironmentsRequest {});
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    publisher
        .watch_environments(request)
        .await
        .unwrap()
        .into_inner()
        .next()
        .await
        .unwrap()
        .unwrap()
}

fn record(id: &str, generation: u64) -> EnvironmentRecord {
    running(
        spec(id),
        Assignment {
            environment_id: id.into(),
            node_id: "node".into(),
            shard_id: 0,
            generation,
            devices: vec![],
        },
    )
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn publication_reset_after_event_overflow_uses_committed_memory_without_redis() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let publisher = RoutePublisher::new(session.clone(), Peers::network());
    publisher.refresh().await.unwrap();
    // Keep the publication receiver stopped while more than its retained history commits.
    for generation in 1..=1030 {
        session
            .publish_initial(
                record(&format!("overflow-{generation}"), generation),
                "",
                "",
            )
            .await
            .unwrap();
    }
    rig.crash();
    publisher.refresh().await.unwrap();
    let frame = directory(&publisher).await;
    assert!(frame.reset);
    assert_eq!(frame.upserts.len(), 1030);
    assert!(frame.upserts.iter().any(|entry| {
        entry.record.as_ref().unwrap().spec.as_ref().unwrap().id == "overflow-1030"
    }));
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn publication_reset_preserves_delete_and_recreate_generation_without_redis() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let publisher = RoutePublisher::new(session.clone(), Peers::network());
    let original = record("recreated", 1);
    session
        .publish_initial(original.clone(), "old", "")
        .await
        .unwrap();
    publisher.refresh().await.unwrap();
    let mut deleted = original.clone();
    deleted.revision += 1;
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    session.commit(deleted).await.unwrap();
    let next = record("recreated", 2);
    session
        .publish_initial(next.clone(), "new", "")
        .await
        .unwrap();
    assert!(session.commit(original).await.is_err());
    rig.crash();
    publisher.refresh().await.unwrap();
    let frame = directory(&publisher).await;
    assert_eq!(frame.upserts.len(), 1);
    let current = frame.upserts[0].record.as_ref().unwrap();
    assert_eq!(current.assignment.as_ref().unwrap().generation, 2);
    assert_eq!(current.runtime.as_ref().unwrap().id, next.runtime.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Redis; run storage suite"]
async fn publication_overflow_recovers_during_continuous_writes_and_again_after_restart() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let mut rig = common::Redis::new().await;
    // This test checks catalog recovery under sustained durable writes, not a
    // subsecond Redis deadline. AOF fsync shares the CI worker's busy disk.
    let store =
        adx_coordinator::storage::RedisStore::connect(&rig.url, "test", Duration::from_secs(2))
            .await
            .unwrap();
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let publisher = RoutePublisher::new(session.clone(), Peers::network());
    publisher.refresh().await.unwrap();
    // An overflow must be handled by run's receiver-loss branch, not by a
    // manually forced refresh. Repeated updates bound live inventory size.
    let mut updated = record("busy", 1);
    session
        .publish_initial(updated.clone(), "busy-request", "")
        .await
        .unwrap();
    for _ in 0..1030 {
        updated.revision += 1;
        session.commit(updated.clone()).await.unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let writer_session = session.clone();
    let writer = tokio::spawn(async move {
        let mut count = 0;
        while !stopped.load(Ordering::Acquire) {
            updated.revision += 1;
            writer_session.commit(updated.clone()).await.unwrap();
            count += 1;
        }
        (updated, count)
    });
    let runner = tokio::spawn(publisher.clone().run(Duration::from_millis(10)));
    let first_visible = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let mut request = Request::new(pb::WatchEnvironmentsRequest {});
            request
                .metadata_mut()
                .insert("adx-component", "apiserver".parse().unwrap());
            if let Ok(response) = publisher.watch_environments(request).await {
                let frame = response.into_inner().next().await.unwrap().unwrap();
                if frame
                    .upserts
                    .iter()
                    .any(|e| e.record.as_ref().unwrap().revision >= 1032)
                {
                    break frame;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !stop.load(Ordering::Acquire),
        "recovery must complete while writes continue"
    );
    assert_eq!(first_visible.upserts.len(), 1);
    stop.store(true, Ordering::Release);
    let (last, count) = writer.await.unwrap();
    assert!(count > 0);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = directory(&publisher).await;
            if frame.upserts[0].record.as_ref().unwrap().revision == last.revision {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    runner.abort();
    // A new Coordinator seeds the same catalog from persisted state once.
    let restarted = store.begin(1).await.unwrap();
    let recovered = RoutePublisher::new(restarted.clone(), Peers::network());
    recovered.refresh().await.unwrap();
    assert_eq!(
        directory(&recovered).await.upserts[0]
            .record
            .as_ref()
            .unwrap()
            .revision,
        last.revision
    );
    assert!(
        session.commit(last.clone()).await.is_err(),
        "old epoch must be fenced"
    );
    let mut deleted = last;
    deleted.revision += 1;
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    restarted.commit(deleted).await.unwrap();
    rig.crash();
    recovered.refresh().await.unwrap();
    assert!(directory(&recovered).await.upserts.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Redis; run storage suite"]
async fn publication_resolves_a_timed_out_write_before_publishing_its_repair() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let publisher = RoutePublisher::new(session.clone(), Peers::network());
    publisher.refresh().await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(800)
        .arg("WRITE")
        .query_async(&mut connection)
        .await
        .unwrap();
    let result = session
        .publish_initial(record("unknown-write", 1), "unknown-request", "")
        .await;
    assert!(matches!(result, Err(Error::Unavailable(_))));
    // A fenced old client may have executed before recovery or may be removed
    // while WRITE-paused. An authoritative empty repair must stay empty after
    // the pause expires; an unfenced late command would violate that decision.
    let visible_after_repair = if publisher.refresh().await.is_ok() {
        Some(!directory(&publisher).await.upserts.is_empty())
    } else {
        None
    };
    tokio::time::sleep(Duration::from_millis(900)).await;
    if let Some(visible) = visible_after_repair {
        assert_eq!(session.get("unknown-write").await.is_ok(), visible);
    }
    let expected = record("unknown-write", 1);
    assert_eq!(
        session
            .publish_initial(expected.clone(), "unknown-request", "")
            .await
            .unwrap(),
        expected
    );
    publisher.refresh().await.unwrap();
    let frame = directory(&publisher).await;
    assert_eq!(frame.upserts.len(), 1);
    assert_eq!(
        frame.upserts[0]
            .record
            .as_ref()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .id,
        "unknown-write"
    );
    // The repair is now committed to memory and no longer needs Redis.
    rig.crash();
    publisher.refresh().await.unwrap();
    assert_eq!(directory(&publisher).await.upserts.len(), 1);
}
