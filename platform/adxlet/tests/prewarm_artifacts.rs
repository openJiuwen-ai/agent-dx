use adx_core::{prewarm::*, Resources};
use adxlet::prewarm::*;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs};

fn fixture() -> (tempfile::TempDir, ArtifactCache, PreparationProfile) {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = vec![0; 4096];
    bytes[1024..1028].copy_from_slice(&[0xe2, 0xe1, 0xf5, 0xe0]);
    fs::write(dir.path().join("source.img"), &bytes).unwrap();
    let image = format!("app@sha256:{}", "a".repeat(64));
    let profile = PreparationProfile {
        source_ref: SourceRef {
            tenant: "tenant".into(),
            template: "app".into(),
            version: "1".into(),
            template_digest: format!("sha256:{}", "b".repeat(64)),
        },
        image: image.clone(),
        runtime_class: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        architecture: if std::env::consts::ARCH == "aarch64" {
            "arm64"
        } else {
            "amd64"
        }
        .into(),
        runtime_recipe_digest: fingerprint(
            &Option::<adx_core::runtime_profile::RuntimeProfile>::None,
        )
        .unwrap(),
        security_digest: format!("sha256:{}", "c".repeat(64)),
        late_binding_contract_digest: format!("sha256:{}", "d".repeat(64)),
        contract_version: 1,
    };
    let config = ArtifactCacheConfig {
        root: dir.path().join("cache"),
        max_disk_bytes: 16384,
        sources: vec![ArtifactSource {
            image,
            path: dir.path().join("source.img"),
            sha256: format!("sha256:{:x}", Sha256::digest(bytes)),
            architecture: if std::env::consts::ARCH == "aarch64" {
                "arm64"
            } else {
                "amd64"
            }
            .into(),
            runtime_classes: vec!["runc".into()],
            process: ImageProcess {
                version: 1,
                args: vec!["/app/start".into()],
                cwd: "/".into(),
                user: "1000:1000".into(),
            },
            env: BTreeMap::from([("PATH".into(), "/bin".into())]),
        }],
    };
    let cache = ArtifactCache::new(config, None).unwrap();
    (dir, cache, profile)
}

#[tokio::test]
async fn immutable_artifact_preparation_is_idempotent_and_pinned_until_physical_stop() {
    let (dir, cache, profile) = fixture();
    let first = cache.prepare("artifact-a", &profile, 8192).await.unwrap();
    assert!(first.ready);
    assert_eq!(
        first.size_bytes,
        cache
            .prepare("artifact-a", &profile, 8192)
            .await
            .unwrap()
            .size_bytes
    );
    let runtime = adx_core::runtime::RuntimeIdentity {
        environment_id: "adx-user-a".into(),
        runtime_id: "adx-user-a-7".into(),
        ownership_generation: 7,
    };
    cache
        .pin("artifact-a", &profile.image, "tenant", &runtime)
        .await
        .unwrap();
    assert!(cache
        .pin("artifact-a", &profile.image, "other-tenant", &runtime)
        .await
        .is_err());
    assert!(cache.destroy("artifact-a").await.is_err());
    // Restart retains the lease; a missing in-memory runtime inventory is no release proof.
    let restarted = ArtifactCache::new(cache.config().clone(), None).unwrap();
    assert!(restarted.observe("artifact-a").await.unwrap().in_use);
    assert!(restarted.destroy("artifact-a").await.is_err());
    restarted.release(&runtime.runtime_id).await.unwrap();
    restarted.destroy("artifact-a").await.unwrap();
    restarted.destroy("artifact-a").await.unwrap();
    assert!(!dir.path().join("cache/artifact-a/rootfs.img").exists());
    assert!(
        restarted
            .prepare("artifact-a", &profile, 8192)
            .await
            .is_err(),
        "destroy receipt fences late prepare"
    );
}

#[tokio::test]
async fn prepare_rejects_unknown_images_budget_overflow_corruption_and_identity_reuse() {
    let (dir, cache, mut profile) = fixture();
    assert!(cache.prepare("../escape", &profile, 8192).await.is_err());
    assert!(cache.prepare("artifact-a", &profile, 1).await.is_err());
    let image = profile.image.clone();
    profile.image = format!("other@sha256:{}", "a".repeat(64));
    assert!(cache.qualify(&profile).await.is_err());
    profile.image = image;
    cache.prepare("artifact-a", &profile, 8192).await.unwrap();
    profile.resources.cpu_millis += 1;
    assert!(cache.prepare("artifact-a", &profile, 8192).await.is_err());
    fs::write(dir.path().join("cache/artifact-a/rootfs.img"), b"corrupt").unwrap();
    assert!(cache.observe("artifact-a").await.is_err());
}
