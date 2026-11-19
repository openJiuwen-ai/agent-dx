use adx_activator::{Activator, Error};
use adx_agent_core::{environment::*, prewarm::*, TemplateDraft};
use adx_agent_store::{AgentState, MemoryRepository, RedisRepository, Repository};
use async_trait::async_trait;
use serde_json::json;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Mutex;

type PreparationCall = (String, String, SeedExecution);

#[derive(Default)]
struct Platform {
    calls: Mutex<Vec<PreparationCall>>,
    replies: Mutex<VecDeque<Result<SeedResult, EnvironmentError>>>,
}

#[async_trait]
impl Environment for Platform {
    async fn prepare_snapshot(
        &self,
        tenant: &str,
        id: &str,
        seed: &SeedExecution,
    ) -> Result<SeedResult, EnvironmentError> {
        self.calls
            .lock()
            .await
            .push((tenant.into(), id.into(), seed.clone()));
        self.replies
            .lock()
            .await
            .pop_front()
            .unwrap_or(Ok(SeedResult::Pending))
    }
    async fn golden_available(
        &self,
        tenant: &str,
        id: &str,
        image: &str,
        runtime: &str,
    ) -> Result<bool, EnvironmentError> {
        Ok(tenant == "tenant"
            && matches!(id, "snapshot-ready" | "snapshot-canary" | "snapshot-stable")
            && image == fixed_image()
            && runtime == "runsc")
    }
    async fn create(
        &self,
        _: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        panic!("publication must not create a user Environment")
    }
    async fn get(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(None)
    }
    async fn delete(&self, _: &str, _: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        panic!("publication must not delete a user Environment")
    }
}

fn fixed_image() -> String {
    format!("product@sha256:{}", "e".repeat(64))
}

fn draft(automatic: bool) -> TemplateDraft {
    serde_json::from_value(json!({
        "name":"digitalmate", "image":fixed_image(), "isolation_runtime":"runsc",
        "auto_prepare_snapshot":automatic, "golden_seed_profile":automatic.then_some("golden-test"),
        "resources":{"cpu_millis":2000,"memory_mib":4096},
        "service":[{"protocol":"ws","port":18092}], "readiness_probe":{"http_get":{"path":"/health/ready","port":18092}},
        "env":{"DIGITALMATE_WORKSPACE":"/data/state", "DIGITALMATE_CRYPTO_URL":"https://privacy.example.test", "DIGITALMATE_CRYPTO_TENANT":"tenant",
            "DIGITALMATE_MODEL_CONFIG_NAME":"approved", "DIGITALMATE_CRYPTO_WORKSPACE":"${workspace}"},
        "mounts":[{"type":"bind","volume_id":"user-${workspace}","target":"/data/state"}]
    })).unwrap()
}

fn configured(store: AgentState, platform: Arc<Platform>) -> Activator {
    let profiles = serde_json::from_value(json!([{"tenant":"tenant","directory_root":"tenant/workspaces","profile":{
        "schema_version":2,"profile":"golden-test","kind":"digitalmate",
        "inherit_from_published_template":["image","isolation_runtime","resources","readiness_probe"],
        "requires":{"isolation_runtime":"runsc","readiness_http_path":"/health/ready","seed_readiness_http_path":"/health/ready/golden-seed"},
        "seed":{"inherit_entrypoint":true,"env_from_template":["DIGITALMATE_CRYPTO_URL","DIGITALMATE_CRYPTO_TENANT","DIGITALMATE_MODEL_CONFIG_NAME"],
            "env":{"DIGITALMATE_GOLDEN_PHASE":"seed","DIGITALMATE_WORKSPACE":"/data/state"},
            "workspace":{"identity_env":"DIGITALMATE_CRYPTO_WORKSPACE","directory_prefix":"golden-seeds","storage_domain":"sfs",
                "mount_target":"/data/state","access_mode":"exclusive","uid":10001,"gid":10001,"mode":"0700"}},
        "restore":{"handoff_contract_version":"1","env":{"DIGITALMATE_GOLDEN_PHASE":"restore"}}
    }}])).unwrap();
    Activator::new(store, platform)
        .with_seed_profiles(profiles)
        .unwrap()
}

#[tokio::test]
async fn publication_is_immediate_and_one_fixed_job_fills_the_same_version_after_restart() {
    publication_and_restart(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
async fn two_template_names_can_select_one_profile_and_receive_separate_results() {
    let platform = Arc::new(Platform::default());
    let author = configured(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    );
    let mut input = serde_json::to_value(draft(true)).unwrap();
    input["golden_seed_profile"] = json!("golden-test");
    input["name"] = json!("digitalmate-canary");
    let canary: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    input["name"] = json!("digitalmate-stable");
    let stable: TemplateDraft = serde_json::from_value(input).unwrap();

    for draft in [&canary, &stable] {
        let mut incompatible = draft.clone();
        incompatible
            .env
            .insert("DIGITALMATE_MEMORY_BACKEND".into(), "mem2".into());
        assert!(matches!(
            author.publish("tenant", &incompatible).await,
            Err(Error::Invalid(reason)) if reason.contains("DIGITALMATE_MEMORY_BACKEND")
        ));
        let published = author.publish("tenant", draft).await.unwrap();
        assert_eq!(published.name, draft.name);
        assert_eq!(published.version, "1");
        assert_eq!(
            published.golden_seed_profile.as_deref(),
            Some("golden-test")
        );
        assert_eq!(
            published.snapshot_preparation,
            Some(SnapshotPreparation::Pending)
        );
    }
    assert!(platform.calls.lock().await.is_empty());
    platform.replies.lock().await.extend([
        Ok(SeedResult::Ready {
            snapshot_id: "snapshot-canary".into(),
        }),
        Ok(SeedResult::Ready {
            snapshot_id: "snapshot-stable".into(),
        }),
    ]);
    author.reconcile_snapshot_preparations().await.unwrap();
    let mut recipes = Vec::new();
    for name in [&canary.name, &stable.name] {
        let template = author.template("tenant", name, "1").await.unwrap();
        assert_eq!(
            template.snapshot_preparation,
            Some(SnapshotPreparation::Ready)
        );
        recipes.push(template.prewarm_recipe.unwrap());
    }
    assert_ne!(recipes[0].snapshot_id, recipes[1].snapshot_id);
    assert_eq!(
        recipes[0].restore_profile_digest,
        recipes[1].restore_profile_digest
    );
    let calls = platform.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].1, calls[1].1);
    assert_ne!(calls[0].2.volume.id, calls[1].2.volume.id);
}

#[tokio::test]
#[ignore = "Requires dedicated local Redis; set ADX_TEST_REDIS_URL"]
async fn redis_durably_links_publication_job_and_result() {
    let url = std::env::var("ADX_TEST_REDIS_URL").expect("set URL to dedicated local Redis");
    let namespace = format!("golden-job-{}", uuid::Uuid::new_v4());
    let repo = RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    publication_and_restart(Arc::new(repo)).await;
}

#[tokio::test]
#[ignore = "Requires a Helm-rendered profile from agent-dx-iac; set ADX_TEST_GOLDEN_PROFILE"]
async fn helm_delivered_iac_profile_builds_an_anonymous_seed() {
    let path = std::env::var("ADX_TEST_GOLDEN_PROFILE").expect("set path to Helm-rendered profile");
    let raw = std::fs::read(path).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let tenant = value[0]["tenant"].as_str().unwrap();
    let directory = value[0]["directory_root"].as_str().unwrap();
    let profiles = serde_json::from_slice(&raw).unwrap();
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    )
    .with_seed_profiles(profiles)
    .unwrap();
    let path =
        std::env::var("ADX_TEST_GOLDEN_DRAFT").expect("set path to IaC-generated TemplateDraft");
    let input: TemplateDraft = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let published = activator.publish(tenant, &input).await.unwrap();
    assert_eq!(
        published.snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    );
    let path = std::env::var("ADX_TEST_GOLDEN_CANARY_DRAFT")
        .expect("set path to second IaC-generated TemplateDraft");
    let canary: TemplateDraft = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_ne!(input.name, canary.name);
    assert_eq!(input.golden_seed_profile, canary.golden_seed_profile);
    let published_canary = activator.publish(tenant, &canary).await.unwrap();
    assert_eq!(
        published_canary.snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    );
    activator.reconcile_snapshot_preparations().await.unwrap();
    let calls = platform.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].1, calls[1].1);
    assert_ne!(calls[0].2.volume.id, calls[1].2.volume.id);
    let (actual_tenant, _, seed) = &calls[0];
    assert_eq!(actual_tenant, tenant);
    assert_eq!(seed.execution.env["DIGITALMATE_GOLDEN_PHASE"], "seed");
    assert!(!seed.execution.env.contains_key("API_KEY"));
    assert_eq!(
        seed.execution
            .readiness_probe
            .as_ref()
            .unwrap()
            .http_get
            .as_ref()
            .unwrap()
            .path,
        "/health/ready/golden-seed"
    );
    let adx_agent_core::volumes::VolumeSource::Directory { path, .. } = &seed.volume.source;
    assert!(path.starts_with(&format!("{directory}/golden-seeds/")));
}

async fn publication_and_restart(repo: Arc<dyn Repository>) {
    let platform = Arc::new(Platform::default());
    let author = configured(AgentState::new(repo.clone()), platform.clone());
    let input = draft(true);
    let (a, b) = tokio::join!(
        author.publish_with_id("tenant", &input, Some("version-a")),
        author.publish_with_id("tenant", &input, Some("version-a"))
    );
    let first = a.unwrap();
    assert_eq!(first, b.unwrap());
    assert_eq!(first.version, "1");
    assert_eq!(
        first.snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    );
    assert!(first.prewarm_recipe.is_none());
    assert!(platform.calls.lock().await.is_empty());
    // This Activator cached the template before the result was written by its peer.
    assert_eq!(
        author.template("tenant", "digitalmate", "1").await.unwrap(),
        first
    );
    let worker = Activator::new(AgentState::new(repo), platform.clone());
    platform.replies.lock().await.extend([
        Err(EnvironmentError::OutcomeUnknown("lost response".into())),
        Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }),
    ]);
    worker.reconcile_snapshot_preparations().await.unwrap();
    assert_eq!(
        author.template("tenant", "digitalmate", "1").await.unwrap(),
        first
    );
    worker.reconcile_snapshot_preparations().await.unwrap();
    let completed = author.template("tenant", "digitalmate", "1").await.unwrap();
    assert_eq!(completed.version, first.version);
    assert_eq!(completed.env, first.env);
    assert_eq!(completed.mounts, first.mounts);
    assert_eq!(
        completed.snapshot_preparation,
        Some(SnapshotPreparation::Ready)
    );
    let recipe = completed.prewarm_recipe.as_ref().unwrap();
    assert_eq!(recipe.snapshot_id, "snapshot-ready");
    assert_eq!(recipe.restore_env["DIGITALMATE_GOLDEN_PHASE"], "restore");
    assert_eq!(
        author
            .publish_with_id("tenant", &input, Some("version-a"))
            .await
            .unwrap(),
        completed
    );
    worker.reconcile_snapshot_preparations().await.unwrap();
    let calls = platform.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    let seed = &calls[0].2;
    assert!(!seed.execution.env.contains_key("API_KEY"));
    assert_eq!(seed.execution.env["DIGITALMATE_GOLDEN_PHASE"], "seed");
    assert_eq!(
        seed.execution.env["DIGITALMATE_CRYPTO_WORKSPACE"],
        seed.volume.id
    );
    assert_eq!(
        seed.execution.mounts[0].volume_id.as_deref(),
        Some(seed.volume.id.as_str())
    );
}

#[tokio::test]
async fn unsupported_target_settings_are_rejected_before_publication() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let author = configured(state, platform.clone());
    for (key, value) in [
        ("API_KEY", "private"),
        ("DIGITALMATE_MEMORY_BACKEND", "mem2"),
        ("DIGITALMATE_WORKSWARM_WS", "ws://other.example"),
        ("DIGITALMATE_WORKSPACE", "/mnt/other"),
    ] {
        let mut input = draft(true);
        input.env.insert(key.into(), value.into());
        assert!(matches!(
            author.publish("tenant", &input).await,
            Err(Error::Invalid(_))
        ));
    }
    assert_eq!(
        author
            .publish("tenant", &draft(true))
            .await
            .unwrap()
            .version,
        "1"
    );
}

#[tokio::test]
async fn disabled_missing_profile_conflict_and_terminal_failure_do_not_select_a_snapshot() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let author = configured(state.clone(), platform.clone());
    let mut disabled = draft(false);
    disabled.golden_seed_profile = Some("unconfigured".into());
    let plain = author
        .publish_with_id("tenant", &disabled, Some("plain"))
        .await
        .unwrap();
    assert!(plain.snapshot_preparation.is_none());
    author.reconcile_snapshot_preparations().await.unwrap();
    assert!(platform.calls.lock().await.is_empty());
    assert!(matches!(
        author
            .publish_with_id("tenant", &draft(true), Some("plain"))
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(matches!(
        author.publish("foreign", &draft(true)).await,
        Err(Error::Invalid(_))
    ));
    for selector in [None, Some("unconfigured".into())] {
        let mut input = draft(true);
        input.golden_seed_profile = selector;
        assert!(matches!(
            author.publish("tenant", &input).await,
            Err(Error::Invalid(_))
        ));
    }
    let pending = author.publish("tenant", &draft(true)).await.unwrap();
    assert_eq!(pending.version, "2");
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Failed));
    let restarted = configured(state, platform.clone());
    restarted.reconcile_snapshot_preparations().await.unwrap();
    restarted.reconcile_snapshot_preparations().await.unwrap();
    let failed = author
        .template("tenant", "digitalmate", &pending.version)
        .await
        .unwrap();
    assert!(matches!(
        failed.snapshot_preparation,
        Some(SnapshotPreparation::Failed { .. })
    ));
    assert!(failed.prewarm_recipe.is_none());
    assert!(author
        .update_prewarm_policy(&PolicyUpdate {
            scope: PolicyScope {
                tenant: "tenant".into(),
                product: "digitalmate".into()
            },
            enabled: true,
            expected_revision: None,
            template: "digitalmate".into(),
            version: failed.version,
            validation_ref: "fixture".into(),
        })
        .await
        .is_err());
    assert_eq!(platform.calls.lock().await.len(), 1);
}
