mod common;
use adx_core::managed_images::{ImageRetention, ManagedImage, PublicationState};
use std::collections::BTreeMap;

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
        "tenant",
        "persisted-request",
        ImageRetention::Ephemeral { grace_seconds: 0 },
        10,
    )
    .unwrap();
    let id = original.id.clone();
    assert_eq!(
        session.begin_managed_image(original.clone()).await.unwrap(),
        session.begin_managed_image(original).await.unwrap()
    );
    let repo = format!("registry.test/managed/publications/{id}/service-0");
    let reference = format!("{repo}@sha256:{}", "b".repeat(64));
    session
        .mutate_managed_image("tenant", &id, |i| {
            i.plan(
                BTreeMap::from([("main".into(), format!("{repo}:build"))]),
                11,
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
