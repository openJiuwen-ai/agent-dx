use super::*;
use std::os::unix::fs::PermissionsExt;

async fn verification_finished(store: &ObjectCheckpointStore) {
    let verification = store.cache.lock().await.verification.clone();
    let _finished = verification.lock().await;
}

#[tokio::test]
async fn interrupted_idle_verification_is_retried_without_rechecking_successes() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(object_store::memory::InMemory::new()),
        "test".into(),
        root.path().into(),
        1024 * 1024,
    )
    .unwrap();
    let capture = store.begin_capture().await.unwrap();
    let memory = capture.join("memory");
    std::fs::write(&memory, b"idle").unwrap();
    let artifact = store.seal(&capture).await.unwrap();
    store.committed(&artifact).await.unwrap();
    std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o0)).unwrap();
    // Privileged test runners may read mode 000. An execution pin exercises
    // the same deferred retry boundary there; ordinary runners get an I/O error.
    let privileged_pin = if tokio::fs::File::open(&memory).await.is_ok() {
        Some(store.retain_execution(&artifact).await.unwrap())
    } else {
        None
    };
    store.verify_idle(vec![artifact.location.clone()]).await;
    tokio::task::yield_now().await;
    verification_finished(&store).await;
    assert!(store
        .cache
        .lock()
        .await
        .unverified
        .contains(&artifact.location));
    std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(privileged_pin);
    store.maintain().await.unwrap();
    verification_finished(&store).await;
    assert!(!store
        .cache
        .lock()
        .await
        .unverified
        .contains(&artifact.location));
    std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o0)).unwrap();
    store.maintain().await.unwrap();
    verification_finished(&store).await;
    assert!(store.cache.lock().await.unverified.is_empty());
    std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
async fn rejected_idle_copy_is_not_reused_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(object_store::memory::InMemory::new());
    let store = || {
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            root.path().into(),
            1024 * 1024,
        )
        .unwrap()
    };
    let original = store();
    let capture = original.begin_capture().await.unwrap();
    std::fs::write(capture.join("memory"), b"good").unwrap();
    let artifact = original.seal(&capture).await.unwrap();
    original.committed(&artifact).await.unwrap();
    std::fs::write(capture.join("memory"), b"junk").unwrap();
    original.verify_idle(vec![artifact.location.clone()]).await;
    verification_finished(&original).await;
    let pin = original.protect_execution(&artifact).await.unwrap();
    assert!(matches!(
        original.prepare_restore(&artifact).await,
        Err(adx_core::Error::Conflict)
    ));
    assert_eq!(std::fs::read(capture.join("memory")).unwrap(), b"junk");
    drop(pin);
    drop(original);

    let restarted = store();
    let restored = restarted.prepare_restore(&artifact).await.unwrap();
    assert_eq!(std::fs::read(restored.join("memory")).unwrap(), b"good");
}

#[tokio::test]
async fn unindexed_copy_is_verified_before_reuse() {
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(object_store::memory::InMemory::new());
    let store = || {
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            root.path().into(),
            1024 * 1024,
        )
        .unwrap()
    };
    let original = store();
    let capture = original.begin_capture().await.unwrap();
    std::fs::write(capture.join("memory"), b"good").unwrap();
    let artifact = original.seal(&capture).await.unwrap();
    original.remove_index(&artifact.location).await.unwrap();
    std::fs::write(capture.join("memory"), b"junk").unwrap();
    drop(original);

    let restarted = store();
    let restored = restarted.prepare_restore(&artifact).await.unwrap();
    assert_eq!(std::fs::read(restored.join("memory")).unwrap(), b"good");
}
