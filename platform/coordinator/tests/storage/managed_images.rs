use super::{common, node, running, spec};
use adx_coordinator::storage::{LocalClaim, NodeSession, Session};
use adx_core::managed_images::{
    ImagePublisher, ImageRetention, ManagedImage, PublicationState, RegistryScope,
};
use adx_core::Assignment;
use std::collections::BTreeMap;

fn scope() -> RegistryScope {
    RegistryScope {
        authority: "registry.test".into(),
        repository_prefix: "managed".into(),
    }
}

async fn ready_image(session: &Session, request: &str) -> (String, String) {
    let image = session
        .begin_managed_image(
            ManagedImage::new(
                format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                ),
                "tenant",
                request,
                ImageRetention::Ephemeral { grace_seconds: 0 },
                scope(),
                10,
            )
            .unwrap(),
            1000,
            100,
        )
        .await
        .unwrap();
    let digest = format!("sha256:{}", "b".repeat(64));
    let repository = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{}/service-0",
        image.id
    );
    session
        .mutate_managed_image("tenant", &image.id, |image| {
            image.plan(
                BTreeMap::from([("main".into(), format!("{repository}:build"))]),
                ImagePublisher::Operator,
            )
        })
        .await
        .unwrap();
    session
        .mutate_managed_image("tenant", &image.id, |image| {
            image.publish(BTreeMap::from([("main".into(), digest.clone())]), 12)?;
            image.release("publisher")?;
            image.observe(&[], 13)
        })
        .await
        .unwrap();
    (image.id, format!("{repository}@{digest}"))
}

async fn register_worker(session: &Session, boot: &str, host: Option<&str>) {
    let mut worker = node("worker");
    if let Some(host) = host {
        worker
            .labels
            .insert("ADX_NODE_HOST_UUID".into(), host.into());
    }
    session
        .register_session(
            worker,
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: boot.into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn managed_image_deletion_fences_pending_local_claim_commit() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register_worker(&session, "boot", None).await;
    let local = LocalClaim {
        request_id: "create".into(),
        node_id: "worker".into(),
        node_session_id: "boot".into(),
        devices: vec![],
    };
    let (id, reference) = ready_image(&session, "claim-race").await;
    let mut consumer = spec("consumer");
    consumer.image = reference.clone();
    session
        .normalize_managed_image_dependencies(&mut consumer, Some(&scope()))
        .await
        .unwrap();
    // Hold the storage claim back after Coordinator admission. A deletion
    // winner must fence the eventual batch write, even after admission ran.
    session.acquire_managed_images(&consumer).await.unwrap();
    session
        .mutate_managed_image("tenant", &id, |image| {
            image.observe(&[], 14)?;
            image.claim_manifests(std::slice::from_ref(&reference), 14)
        })
        .await
        .unwrap();
    let outcome = session.claim_batch(vec![(consumer, local)]).await.unwrap();
    assert!(outcome[0].is_err(), "closed image accepted a late claim");
    assert!(session.get("consumer").await.is_err());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn managed_image_dependency_survives_failed_environment_retirement() {
    const ORIGINAL_HOST: &str = "11111111-1111-4111-8111-111111111111";
    const OTHER_HOST: &str = "22222222-2222-4222-8222-222222222222";
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register_worker(&session, "boot", Some(ORIGINAL_HOST)).await;
    let (_, reference) = ready_image(&session, "lost-rootfs").await;
    let mut consumer = spec("consumer");
    consumer.image = reference;
    session
        .normalize_managed_image_dependencies(&mut consumer, Some(&scope()))
        .await
        .unwrap();
    let assignment = Assignment {
        environment_id: consumer.id.clone(),
        node_id: "worker".into(),
        shard_id: 0,
        generation: session.epoch() << 32 | 1,
        devices: vec![],
    };
    session
        .reserve(consumer.clone(), assignment.clone())
        .await
        .unwrap();
    session
        .commit(running(consumer, assignment.clone()))
        .await
        .unwrap();
    session.invalidate_node("worker", "boot").await.unwrap();
    let failed = session.get("consumer").await.unwrap();
    session
        .retire_invalidated_failed(&failed, u64::MAX)
        .await
        .unwrap();
    assert!(session.get("consumer").await.is_err());
    let holds = session.execution_holds().await.unwrap();
    assert_eq!(holds.len(), 1, "ordinary image has no checkpoint hold");
    assert_eq!(holds[0].assignment, assignment);
    assert_eq!(holds[0].tenant_id, "tenant");
    assert_eq!(holds[0].managed_image_publications.len(), 1);
    assert_eq!(holds[0].host_uuid.as_deref(), Some(ORIGINAL_HOST));

    register_worker(&session, "replacement", Some(OTHER_HOST)).await;
    session
        .release_execution_holds("worker", "replacement")
        .await
        .unwrap();
    assert_eq!(session.execution_holds().await.unwrap().len(), 1);

    register_worker(&session, "reconciled", Some(ORIGINAL_HOST)).await;
    session
        .release_execution_holds("worker", "reconciled")
        .await
        .unwrap();
    assert!(session.execution_holds().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn managed_image_request_replay_and_budgets_survive_cancellation() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let new = |id: &str, request: &str| {
        ManagedImage::new(
            id.repeat(64),
            "tenant",
            request,
            ImageRetention::Retained,
            scope(),
            1,
        )
        .unwrap()
    };
    let first = session
        .begin_managed_image(new("a", "first"), 2, 1)
        .await
        .unwrap();
    assert_eq!(
        session
            .begin_managed_image(new("b", "first"), 2, 1)
            .await
            .unwrap()
            .id,
        first.id
    );
    assert!(session
        .begin_managed_image(new("b", "second"), 2, 1)
        .await
        .is_err());
    session
        .mutate_managed_image("tenant", &first.id, |image| image.cancel_preparation())
        .await
        .unwrap();
    let second = session
        .begin_managed_image(new("b", "second"), 2, 1)
        .await
        .unwrap();
    session
        .mutate_managed_image("tenant", &second.id, |image| image.cancel_preparation())
        .await
        .unwrap();
    assert!(session
        .begin_managed_image(new("c", "third"), 2, 1)
        .await
        .is_err());
    assert_eq!(
        session
            .begin_managed_image(new("d", "first"), 2, 1)
            .await
            .unwrap()
            .state,
        PublicationState::Abandoned
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn image_publication_release_and_deletion_survive_redis_and_coordinator_restart() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    // Unrelated runtime options must not enroll reserved-looking external images.
    let external_id = "f".repeat(64);
    let external: adx_core::EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"external", "tenant_id":"tenant", "image":format!("external.test/publications/{external_id}/image@sha256:{}", "a".repeat(64)),
        "runtime_class":"runsc", "resources":{"cpu_millis":100,"memory_bytes":1024,"disk_bytes":0}, "priority":0,
        "execution":{"failover":false,"inherit_entrypoint":false,"extra_config":serde_json::json!({"unrelated":external_id}).to_string()}
    })).unwrap();
    session.acquire_managed_images(&external).await.unwrap();
    let original = ManagedImage::new(
        "a".repeat(64),
        "tenant",
        "persisted-request",
        ImageRetention::Ephemeral { grace_seconds: 0 },
        scope(),
        10,
    )
    .unwrap();
    let id = original.id.clone();
    assert_eq!(
        session
            .begin_managed_image(original.clone(), 1000, 100)
            .await
            .unwrap(),
        session
            .begin_managed_image(original, 1000, 100)
            .await
            .unwrap()
    );
    let repo = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{id}/service-0"
    );
    let reference = format!("{repo}@sha256:{}", "b".repeat(64));
    session
        .mutate_managed_image("tenant", &id, |i| {
            i.plan(
                BTreeMap::from([("main".into(), format!("{repo}:build"))]),
                ImagePublisher::Operator,
            )
        })
        .await
        .unwrap();
    assert!(session
        .mutate_managed_image("tenant", &id, |i| i.publish(BTreeMap::new(), 12))
        .await
        .is_err());
    assert_eq!(
        session.managed_image("tenant", &id).await.unwrap().state,
        PublicationState::Publishing
    );
    session
        .mutate_managed_image("tenant", &id, |i| {
            i.publish(
                BTreeMap::from([("main".into(), format!("sha256:{}", "b".repeat(64)))]),
                13,
            )
        })
        .await
        .unwrap();
    session
        .mutate_managed_image("tenant", &id, |i| i.release("publisher"))
        .await
        .unwrap();
    session
        .mutate_managed_image("tenant", &id, |i| i.observe(&[], 20))
        .await
        .unwrap();
    session
        .mutate_managed_image("tenant", &id, |i| {
            i.claim_manifests(std::slice::from_ref(&reference), 20)
        })
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    let image = recovered.managed_image("tenant", &id).await.unwrap();
    assert_eq!(image.state, PublicationState::Deleting);
    assert!(image.deletion_manifests.contains(&reference));
    assert!(session
        .mutate_managed_image("tenant", &id, |i| i.retain("stale-process"))
        .await
        .is_err());
    assert!(recovered
        .mutate_managed_image("tenant", &id, |i| i.retain("late-client"))
        .await
        .is_err());
    assert!(recovered
        .managed_image("foreign-tenant", &id)
        .await
        .is_err());
    recovered
        .mutate_managed_image("tenant", &id, |i| i.finish_deletion())
        .await
        .unwrap();
    recovered
        .mutate_managed_image("tenant", &id, |i| i.finish_deletion())
        .await
        .unwrap();
    assert_eq!(
        recovered
            .managed_image("tenant", &id)
            .await
            .unwrap()
            .deleted_manifest_count,
        1
    );
    assert_eq!(
        recovered
            .managed_images("tenant", "", 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(recovered
        .managed_images("tenant", &id, 1)
        .await
        .unwrap()
        .is_empty());
}
