use adx_core::managed_images::{
    ImagePublisher, ImageRetention, ManagedImage, PublicationState, RegistryScope,
};
use std::collections::BTreeMap;

fn image() -> ManagedImage {
    ManagedImage::new(
        "a".repeat(64),
        "tenant",
        "request-1",
        ImageRetention::Ephemeral { grace_seconds: 60 },
        RegistryScope {
            authority: "registry.test".into(),
            repository_prefix: "managed".into(),
        },
        100,
    )
    .unwrap()
}

#[test]
fn release_does_not_expire_pending_publication_and_is_idempotent() {
    let mut image = image();
    image.release("publisher").unwrap();
    image.release("publisher").unwrap();
    image.observe(&[], 200).unwrap();
    assert_eq!(image.unreferenced_since, None);
    assert!(!image.reclaimable(1000));
}

#[test]
fn retain_retry_remains_idempotent_when_lease_history_is_full() {
    let mut image = image();
    for index in 0..4095 {
        image.retain(&format!("reader-{index}")).unwrap();
    }
    // A lost acquisition response can be replayed even at the capacity boundary.
    image.retain("reader-0").unwrap();
    image.release("reader-1").unwrap();
    image.retain("reader-0").unwrap();
    assert!(image.retain("reader-1").is_err());
    assert!(image.retain("new-reader").is_err());
}

#[test]
fn grace_starts_after_last_reference_and_new_use_restarts_it() {
    let mut image = image();
    let tagged = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{}/service-0:build",
        image.id
    );
    image
        .plan(
            BTreeMap::from([("single".into(), tagged)]),
            ImagePublisher::Operator,
        )
        .unwrap();
    image
        .publish(
            BTreeMap::from([("single".into(), format!("sha256:{}", "a".repeat(64)))]),
            102,
        )
        .unwrap();
    image.retain("other-client").unwrap();
    image.release("publisher").unwrap();
    image.observe(&[], 200).unwrap();
    assert!(!image.reclaimable(1000));
    image.release("other-client").unwrap();
    image.observe(&[], 300).unwrap();
    assert!(!image.reclaimable(359));
    assert!(image.reclaimable(360));
    image.observe(&["snapshot:saved".into()], 361).unwrap();
    assert!(!image.reclaimable(1000));
    image.observe(&[], 400).unwrap();
    assert!(!image.reclaimable(459));
    image.claim_deletion(460).unwrap();
    assert_eq!(image.state, PublicationState::Deleting);
    assert!(image.retain("late-consumer").is_err());
    assert!(image.observe(&["environment:late".into()], 461).is_err());
}

#[test]
fn legacy_repositories_and_changed_completion_are_rejected() {
    let mut image = image();
    assert!(image
        .plan(
            BTreeMap::from([("single".into(), "registry.test/history:old".into())]),
            ImagePublisher::Operator
        )
        .is_err());
    let tagged = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{}/service-0:build",
        image.id
    );
    image
        .plan(
            BTreeMap::from([("single".into(), tagged)]),
            ImagePublisher::Operator,
        )
        .unwrap();
    let outputs = BTreeMap::from([("single".into(), format!("sha256:{}", "b".repeat(64)))]);
    image.publish(outputs.clone(), 102).unwrap();
    image.publish(outputs, 103).unwrap();
    assert!(image
        .publish(
            BTreeMap::from([("single".into(), format!("sha256:{}", "c".repeat(64)))]),
            104
        )
        .is_err());
}

#[test]
fn managed_scope_and_attempt_identity_bound_the_publication() {
    let mut image = image();
    let target = |authority: &str, collection: &str, output: &str| {
        format!(
            "{authority}/managed/{collection}/publications/{}/{}:build",
            "a".repeat(64),
            output
        )
    };
    let collection = "0123456789abcdef0123456789abcdef";
    let valid = target("registry.test", collection, "service-0");
    assert!(image
        .plan(
            BTreeMap::from([(
                "single".into(),
                target("foreign.test", collection, "service-0")
            )]),
            ImagePublisher::Operator
        )
        .is_err());
    assert!(image
        .plan(
            BTreeMap::from([
                ("first".into(), valid.clone()),
                (
                    "second".into(),
                    target("registry.test", "0".repeat(32).as_str(), "service-1")
                ),
            ]),
            ImagePublisher::Operator
        )
        .is_err());
    let outputs = BTreeMap::from([("single".into(), valid)]);
    let owner = ImagePublisher::Environment {
        id: "builder".into(),
        generation: 7,
    };
    image.plan(outputs.clone(), owner.clone()).unwrap();
    image.plan(outputs.clone(), owner).unwrap();
    assert!(image
        .plan(
            outputs,
            ImagePublisher::Environment {
                id: "builder".into(),
                generation: 8
            }
        )
        .is_err());
}

#[test]
fn confirmed_abandonment_keeps_partial_bytes_and_independent_holds() {
    let mut abandoned = image();
    abandoned.cancel_preparation().unwrap();
    abandoned.cancel_preparation().unwrap();
    assert!(abandoned.admission_open());
    assert!(abandoned
        .plan(BTreeMap::new(), ImagePublisher::Operator)
        .is_err());
    assert!(!abandoned.reclaimable(u64::MAX));

    let mut image = image();
    let base = format!(
        "registry.test/managed/{}/publications/{}",
        "0".repeat(32),
        image.id
    );
    let first = format!("{base}/service-0:build");
    image
        .plan(
            BTreeMap::from([
                ("first".into(), first.clone()),
                ("second".into(), format!("{base}/service-1:build")),
            ]),
            ImagePublisher::Operator,
        )
        .unwrap();
    image.retain("reader").unwrap();
    assert!(image.cancel_preparation().is_err());
    let result = BTreeMap::from([("first".into(), format!("sha256:{}", "a".repeat(64)))]);
    assert!(image
        .reconcile_abandoned(result.clone(), "writer-done", 0)
        .is_err());
    image
        .reconcile_abandoned(result.clone(), "writer-done", image.revision)
        .unwrap();
    image.reconcile_abandoned(result, "writer-done", 0).unwrap();
    assert_eq!(image.outputs.len(), 1);
    assert!(!image.leases.contains("publisher"));
    assert!(image.leases.contains("reader"));
    assert_eq!(image.state, PublicationState::Abandoned);
    image.release("reader").unwrap();
    image.observe_after_admission(&[], 200, 1).unwrap();
    image.observe_after_admission(&[], 220, 2).unwrap();
    assert!(!image.reclaimable(279));
    assert!(image.reclaimable(280));
}

#[test]
fn pin_policy_revision_and_released_lease_identity_prevent_stale_shortening() {
    let mut image = image();
    image.retain("client-hold").unwrap();
    image.release("client-hold").unwrap();
    assert!(image.retain("client-hold").is_err());
    image.extend(500).unwrap();
    image.extend(400).unwrap();
    assert_eq!(image.retain_until, 500);
    image.pin().unwrap();
    assert!(!image.reclaimable(u64::MAX));
    assert!(image
        .set_retention(ImageRetention::Ephemeral { grace_seconds: 1 }, 0)
        .is_err());
    image
        .set_retention(ImageRetention::Ephemeral { grace_seconds: 1 }, 1)
        .unwrap();
    assert_eq!(image.unreferenced_since, None);
}
