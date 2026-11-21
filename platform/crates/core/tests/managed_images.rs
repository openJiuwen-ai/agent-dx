use adx_core::managed_images::{ImageRetention, ManagedImage, PublicationState, RegistryScope};
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
        .register_outputs(BTreeMap::from([("single".into(), tagged)]), "test-attempt")
        .unwrap();
    image
        .complete(
            BTreeMap::from([("single".into(), format!("sha256:{}", "a".repeat(64)))]),
            "test-attempt",
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
    let worklist = image.outputs.values().cloned().collect::<Vec<_>>();
    image.claim_manifests(&worklist, 460).unwrap();
    assert_eq!(image.state, PublicationState::Deleting);
    assert!(image.retain("late-consumer").is_err());
    assert!(image.observe(&["environment:late".into()], 461).is_err());
}

#[test]
fn legacy_repositories_and_changed_completion_are_rejected() {
    let mut image = image();
    assert!(image
        .register_outputs(
            BTreeMap::from([("single".into(), "registry.test/history:old".into())]),
            "test-attempt"
        )
        .is_err());
    let tagged = format!(
        "registry.test/managed/0123456789abcdef0123456789abcdef/publications/{}/service-0:build",
        image.id
    );
    image
        .register_outputs(BTreeMap::from([("single".into(), tagged)]), "test-attempt")
        .unwrap();
    let outputs = BTreeMap::from([("single".into(), format!("sha256:{}", "b".repeat(64)))]);
    image.complete(outputs.clone(), "test-attempt").unwrap();
    image.complete(outputs, "test-attempt").unwrap();
    assert!(image
        .complete(
            BTreeMap::from([("single".into(), format!("sha256:{}", "c".repeat(64)))]),
            "test-attempt"
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
        .register_outputs(
            BTreeMap::from([(
                "single".into(),
                target("foreign.test", collection, "service-0")
            )]),
            "test-attempt"
        )
        .is_err());
    assert!(image
        .register_outputs(
            BTreeMap::from([
                ("first".into(), valid.clone()),
                (
                    "second".into(),
                    target("registry.test", &"0".repeat(64), "service-1")
                ),
            ]),
            "test-attempt"
        )
        .is_err());
    let outputs = BTreeMap::from([("single".into(), valid)]);
    image
        .register_outputs(outputs.clone(), "writer-one")
        .unwrap();
    image
        .register_outputs(outputs.clone(), "writer-one")
        .unwrap();
    assert!(image.register_outputs(outputs, "writer-two").is_err());
    let digests = BTreeMap::from([("single".into(), format!("sha256:{}", "b".repeat(64)))]);
    assert!(image.complete(digests.clone(), "writer-two").is_err());
    image.complete(digests, "writer-one").unwrap();
}

#[test]
fn publications_do_not_require_a_builder_repository_layout() {
    let mut image = image();
    let outputs = BTreeMap::from([
        (
            "root".into(),
            format!("registry.test/managed/team/{}/root:build", image.id),
        ),
        (
            "data".into(),
            format!("registry.test/managed/{}/layers/data:result", image.id),
        ),
    ]);
    image.register_outputs(outputs, "test-attempt").unwrap();
    assert!(image.matches_repository(&format!(
        "http://REGISTRY.test/managed/team/{}/root:build@sha256:{}",
        image.id,
        "b".repeat(64)
    )));
}

#[test]
fn confirmed_abandonment_keeps_partial_bytes_and_independent_holds() {
    let mut abandoned = image();
    abandoned.cancel_preparation().unwrap();
    abandoned.cancel_preparation().unwrap();
    assert!(abandoned.admission_open());
    assert!(abandoned
        .register_outputs(BTreeMap::new(), "test-attempt")
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
        .register_outputs(
            BTreeMap::from([
                ("first".into(), first.clone()),
                ("second".into(), format!("{base}/service-1:build")),
            ]),
            "test-attempt",
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
