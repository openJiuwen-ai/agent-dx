use adx_activator::{Activator, Error};
use adx_agent_core::activator::{ActivationRequest, ResolveRequest, ServiceSelector};
use adx_agent_core::alias::{AliasConfig, VersionWeight};
use adx_agent_core::{environment::*, prewarm::*, Scope, TemplateDraft, TemplateRef};
use adx_agent_store::{
    AgentState, Check, Index, Key, MemoryRepository, Record, RedisRepository, Repository,
    Transaction,
};
use async_trait::async_trait;
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{Barrier, Mutex};

type PreparationCall = (String, String, SeedExecution);

struct DigitalMateRepository {
    inner: Arc<dyn Repository>,
    wait_for_concurrent_commits: AtomicUsize,
    commits: Barrier,
}

#[async_trait]
impl Repository for DigitalMateRepository {
    async fn get(&self, key: &Key) -> adx_agent_store::Result<Option<Record>> {
        assert!(
            !key.as_str().starts_with("prewarm-policy:")
                && !key.as_str().starts_with("first-assignment:"),
            "DigitalMate accessed a legacy eligibility record"
        );
        self.inner.get(key).await
    }

    async fn commit(&self, transaction: &Transaction) -> adx_agent_store::Result<bool> {
        if self
            .wait_for_concurrent_commits
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .is_ok()
        {
            self.commits.wait().await;
        }
        self.inner.commit(transaction).await
    }

    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> adx_agent_store::Result<Vec<(String, Record)>> {
        self.inner.page(index, after, limit).await
    }
}

#[derive(Default)]
struct Platform {
    calls: Mutex<Vec<PreparationCall>>,
    replies: Mutex<VecDeque<Result<SeedResult, EnvironmentError>>>,
    creations: Mutex<Vec<CreateEnvironment>>,
    protections: Mutex<Vec<(String, String, bool)>>,
    unavailable: AtomicBool,
    pending_protection: AtomicBool,
    block_creation: AtomicBool,
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
    async fn agent_snapshot_protection(
        &self,
        tenant: &str,
        id: &str,
        proof: &str,
        retire: bool,
    ) -> Result<(), EnvironmentError> {
        assert_eq!(tenant, "tenant");
        uuid::Uuid::parse_str(proof).unwrap();
        self.protections
            .lock()
            .await
            .push((id.into(), proof.into(), retire));
        if self.pending_protection.load(Ordering::Relaxed) {
            return Err(EnvironmentError::Unavailable(
                "lost protection response".into(),
            ));
        }
        Ok(())
    }
    async fn ensure_volume(
        &self,
        _: &str,
        _: &str,
        volume: &adx_agent_core::volumes::VolumeSpec,
    ) -> Result<adx_agent_core::volumes::VolumeRef, EnvironmentError> {
        Ok(adx_agent_core::volumes::VolumeRef {
            id: volume.id.clone(),
            uid: format!("uid-{}", volume.id),
        })
    }
    async fn golden_available(
        &self,
        tenant: &str,
        id: &str,
        image: &str,
        runtime: &str,
    ) -> Result<bool, EnvironmentError> {
        Ok(!self.unavailable.load(Ordering::Relaxed)
            && tenant == "tenant"
            && matches!(id, "snapshot-ready" | "snapshot-canary" | "snapshot-stable")
            && image == fixed_image()
            && runtime == "runsc")
    }
    async fn create(
        &self,
        request: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        self.creations.lock().await.push(request.clone());
        if self.block_creation.load(Ordering::Relaxed) {
            return Err(EnvironmentError::Conflict(
                "creation blocked by fixture".into(),
            ));
        }
        Ok(EnvironmentObservation {
            id: request.id.clone(),
            tenant: request.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        })
    }
    async fn pause(
        &self,
        tenant: &str,
        id: &str,
        _: u64,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        Ok(EnvironmentObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: EnvironmentPhase::Paused,
            ready: false,
            runtime_id: None,
            message: None,
        })
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
        "auto_prepare_snapshot":automatic,
        "resources":{"cpu_millis":2000,"memory_mib":4096},
        "service":[{"protocol":"ws","port":18092}], "readiness_probe":{"http_get":{"path":"/health/ready","port":18092}},
        "env":{"DIGITALMATE_WORKSPACE":"/data/state", "DIGITALMATE_CRYPTO_URL":"https://privacy.example.test", "DIGITALMATE_CRYPTO_TENANT":"tenant",
            "DIGITALMATE_MODEL_CONFIG_NAME":"approved", "DIGITALMATE_CRYPTO_WORKSPACE":"${workspace}"},
        "mounts":[{"type":"bind","volume_id":"user-${workspace}","target":"/data/state","options":["rw"]}],
        "volumes":[{"id":"user-${workspace}", "source":{"type":"directory","storage_domain":"sfs","path":"tenant/users/${workspace}"},
            "access_mode":"exclusive","create_if_missing":true,"create_options":{"uid":10001,"gid":10001,"mode":"0700"}}]
    })).unwrap()
}

#[tokio::test]
async fn publication_is_immediate_and_one_fixed_job_fills_the_same_version_after_restart() {
    publication_and_restart(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
async fn two_template_names_can_prepare_and_receive_separate_results() {
    let platform = Arc::new(Platform::default());
    let author = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    );
    let mut input = serde_json::to_value(draft(true)).unwrap();
    input["name"] = json!("digitalmate-canary");
    let canary: TemplateDraft = serde_json::from_value(input.clone()).unwrap();
    input["name"] = json!("digitalmate-stable");
    let stable: TemplateDraft = serde_json::from_value(input).unwrap();

    for draft in [&canary, &stable] {
        let published = author.publish("tenant", draft).await.unwrap();
        assert_eq!(published.name, draft.name);
        assert_eq!(published.version, "1");
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
#[ignore = "Requires IaC-generated drafts; set ADX_TEST_GOLDEN_DRAFT and ADX_TEST_GOLDEN_CANARY_DRAFT"]
async fn iac_drafts_prepare_from_the_published_template_without_deployment_seed_config() {
    let tenant = std::env::var("ADX_TEST_GOLDEN_TENANT").unwrap();
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(
        AgentState::new(Arc::new(MemoryRepository::default())),
        platform.clone(),
    );
    let path =
        std::env::var("ADX_TEST_GOLDEN_DRAFT").expect("set path to IaC-generated TemplateDraft");
    let input: TemplateDraft = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let published = activator.publish(&tenant, &input).await.unwrap();
    assert_eq!(
        published.snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    );
    let path = std::env::var("ADX_TEST_GOLDEN_CANARY_DRAFT")
        .expect("set path to second IaC-generated TemplateDraft");
    let canary: TemplateDraft = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_ne!(input.name, canary.name);
    let published_canary = activator.publish(&tenant, &canary).await.unwrap();
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
    assert_eq!(actual_tenant, &tenant);
    assert_eq!(seed.execution.env[GOLDEN_PHASE], "seed");
    assert!(!seed.execution.env.contains_key("API_KEY"));
    assert_eq!(seed.execution.readiness_probe, input.readiness_probe);
    assert_eq!(seed.execution.service, input.service);
    assert_eq!(seed.execution.resources, input.resources);
    let adx_agent_core::volumes::VolumeSource::Directory { path, .. } = &seed.volume.source;
    assert!(path.ends_with(&format!("/{}", seed.volume.id)));
    assert_eq!(seed.volume.create_options, input.volumes[0].create_options);
}

async fn publication_and_restart(repo: Arc<dyn Repository>) {
    let platform = Arc::new(Platform::default());
    let author = Activator::new(AgentState::new(repo.clone()), platform.clone());
    let input = draft(true);
    let (a, b) = tokio::join!(
        author.publish_with_id("tenant", &input, Some("version-a")),
        author.publish_with_id("tenant", &input, Some("version-a"))
    );
    let first = a.unwrap();
    assert_eq!(first, b.unwrap());
    assert!(platform.creations.lock().await.is_empty());
    assert!(platform.protections.lock().await.is_empty());
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
    assert_eq!(recipe.restore_env[GOLDEN_PHASE], "restore");
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
    assert_eq!(seed.execution.env[GOLDEN_PHASE], "seed");
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
    let author = Activator::new(state, platform.clone());
    for (key, value) in [
        ("API_KEY", "private"),
        ("MODEL_ENDPOINT", "${model_url}"),
        ("MODEL_ENDPOINT", "https://user:password@example.test"),
        ("DIGITALMATE_GOLDEN_PHASE", "seed"),
        ("GOLDEN_PHASE", "seed"),
    ] {
        let mut input = draft(true);
        input.env.insert(key.into(), value.into());
        assert!(matches!(
            author.publish("tenant", &input).await,
            Err(Error::Invalid(_))
        ));
    }
    let mut shared = draft(true);
    shared.volumes[0].access_mode = adx_agent_core::volumes::AccessMode::Shared;
    assert!(matches!(
        author.publish("tenant", &shared).await,
        Err(Error::Invalid(_))
    ));
    let mut fixed_user = draft(true);
    let adx_agent_core::volumes::VolumeSource::Directory { path, .. } =
        &mut fixed_user.volumes[0].source;
    *path = "tenant/users/real-user".into();
    assert!(matches!(
        author.publish("tenant", &fixed_user).await,
        Err(Error::Invalid(_))
    ));
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
async fn disabled_conflict_and_terminal_failure_do_not_select_a_snapshot() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let platform = Arc::new(Platform::default());
    let author = Activator::new(state.clone(), platform.clone());
    let disabled = draft(false);
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
    let pending = author.publish("tenant", &draft(true)).await.unwrap();
    assert_eq!(pending.version, "2");
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Failed));
    let restarted = Activator::new(state, platform.clone());
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

fn scope(id: &str, reference: TemplateRef) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        binding_id: id.into(),
        template_ref: reference,
    }
}

fn digitalmate_request(scope: Scope) -> ResolveRequest {
    ResolveRequest {
        activation: ActivationRequest {
            scope,
            expected_generation: None,
            bypass_cache: true,
            parameters: Some(b"workspace=w-actor".to_vec()),
            product_start: Some(EligibleStart {
                product: "digitalmate".into(),
                registered_at_ms: None,
            }),
        },
        service: ServiceSelector {
            protocol: adx_agent_core::Protocol::Ws,
            port: Some(18092),
        },
    }
}

#[tokio::test]
async fn digitalmate_concurrent_first_binding_and_retry_ignore_legacy_eligibility() {
    digitalmate_first_binding(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
#[ignore = "Requires dedicated local Redis; set ADX_TEST_REDIS_URL"]
async fn redis_digitalmate_concurrent_first_binding_and_retry_ignore_legacy_eligibility() {
    let url = std::env::var("ADX_TEST_REDIS_URL").expect("set URL to dedicated local Redis");
    let namespace = format!("digitalmate-binding-{}", uuid::Uuid::new_v4());
    let repo = RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    digitalmate_first_binding(Arc::new(repo)).await;
}

async fn digitalmate_first_binding(inner: Arc<dyn Repository>) {
    let repo = Arc::new(DigitalMateRepository {
        inner: inner.clone(),
        wait_for_concurrent_commits: AtomicUsize::new(0),
        commits: Barrier::new(2),
    });
    let state = AgentState::new(repo.clone());
    let platform = Arc::new(Platform::default());
    let a = Activator::new(state.clone(), platform.clone());
    let b = Activator::new(AgentState::new(repo.clone()), platform.clone());
    a.publish("tenant", &draft(true)).await.unwrap();
    a.publish("tenant", &draft(false)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    a.reconcile_snapshot_preparations().await.unwrap();

    let scopes = ["stable", "canary"].map(|name| scope("actor", TemplateRef::Alias(name.into())));
    let mut aliases = Vec::new();
    for binding in &scopes {
        let alias = AliasConfig {
            tenant: binding.tenant.clone(),
            template: binding.template.clone(),
            alias: binding.template_ref.name().into(),
            versions: vec![VersionWeight {
                version: "1".into(),
                weight: 100,
            }],
            seed: "digitalmate".into(),
        };
        let revision = state.put_alias(&alias, None).await.unwrap();
        aliases.push((alias, revision));
    }
    let [left, right] = scopes.clone().map(digitalmate_request);
    let deadline = adx_agent_core::unix_time_millis() + 30_000;
    platform.block_creation.store(true, Ordering::Relaxed);
    repo.wait_for_concurrent_commits.store(2, Ordering::Relaxed);
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(a.resolve(&left, deadline), b.resolve(&right, deadline))
    })
    .await
    .expect("both first bindings must reach the commit barrier");
    assert!(matches!(first, Err(Error::Conflict(_))));
    assert!(matches!(second, Err(Error::Conflict(_))));

    let left_binding = state.binding(&scopes[0]).await.unwrap();
    let right_binding = state.binding(&scopes[1]).await.unwrap();
    assert_ne!(left_binding.is_some(), right_binding.is_some());
    let committed = left_binding.or(right_binding).unwrap();
    assert_eq!(
        committed.current.snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
    for (mut alias, revision) in aliases {
        alias.versions[0].version = "2".into();
        state.put_alias(&alias, Some(&revision)).await.unwrap();
    }
    platform.block_creation.store(false, Ordering::Relaxed);
    platform.unavailable.store(true, Ordering::Relaxed);
    for request in [&left, &right] {
        let result = b.resolve(request, deadline).await.unwrap().target.binding;
        assert_eq!(result.generation, committed.generation);
        assert_eq!(result.current, committed.current);
    }
    let creations = platform.creations.lock().await;
    assert!(!creations.is_empty());
    assert!(creations
        .iter()
        .all(|request| request.id == committed.current.environment_id
            && request.execution.snapshot_id == committed.current.snapshot_id));
    let first_assignment =
        Key::new("first-assignment", &["tenant", "digitalmate", "actor"]).unwrap();
    let policy = Key::new("prewarm-policy", &["tenant", "digitalmate"]).unwrap();
    assert!(inner.get(&first_assignment).await.unwrap().is_none());
    assert!(inner.get(&policy).await.unwrap().is_none());
}

#[tokio::test]
async fn digitalmate_reuses_live_binding_with_stale_or_missing_history() {
    digitalmate_binding_history(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
#[ignore = "Requires dedicated local Redis; set ADX_TEST_REDIS_URL"]
async fn redis_digitalmate_reuses_live_binding_with_stale_or_missing_history() {
    let url = std::env::var("ADX_TEST_REDIS_URL").expect("set URL to dedicated local Redis");
    let namespace = format!("digitalmate-history-{}", uuid::Uuid::new_v4());
    let repo = RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
        .await
        .unwrap();
    digitalmate_binding_history(Arc::new(repo)).await;
}

async fn digitalmate_binding_history(inner: Arc<dyn Repository>) {
    let repo = Arc::new(DigitalMateRepository {
        inner,
        wait_for_concurrent_commits: AtomicUsize::new(0),
        commits: Barrier::new(2),
    });
    let state = AgentState::new(repo.clone());
    let activator = Activator::new(state.clone(), Arc::new(Platform::default()));
    for _ in 0..3 {
        activator.publish("tenant", &draft(false)).await.unwrap();
    }
    let first = scope("actor", TemplateRef::Version("1".into()));
    let second = scope("actor", TemplateRef::Version("2".into()));
    let third = scope("actor", TemplateRef::Version("3".into()));
    let params = [("workspace".into(), "w-actor".into())].into();
    activator.publish("other", &draft(false)).await.unwrap();
    state
        .create_binding_with_parameters(
            Scope {
                tenant: "other".into(),
                ..first.clone()
            },
            &params,
        )
        .await
        .unwrap();
    state
        .create_binding_with_parameters(first.clone(), &params)
        .await
        .unwrap();
    let deleted = state.begin_delete(&first).await.unwrap();
    state.finish_delete(&deleted).await.unwrap();
    let existing = state
        .create_binding_with_parameters(second, &params)
        .await
        .unwrap();

    let target = activator
        .resolve(
            &digitalmate_request(third.clone()),
            adx_agent_core::unix_time_millis() + 30_000,
        )
        .await
        .unwrap()
        .target;
    assert_eq!(target.binding.current, existing.current);
    assert_eq!(target.binding.generation, existing.generation);
    assert!(state.binding(&third).await.unwrap().is_none());

    // The first implementation wrote live Bindings before the scope pointer existed.
    let history = Key::new("binding-history", &["tenant", "actor"]).unwrap();
    let record = repo.inner.get(&history).await.unwrap().unwrap();
    let deletion = Transaction::with_deletes(
        vec![Check {
            key: history.clone(),
            expected: Some(record.revision),
        }],
        Vec::new(),
        vec![history],
    )
    .unwrap();
    assert!(repo.inner.commit(&deletion).await.unwrap());
    let target = activator
        .resolve(
            &digitalmate_request(third.clone()),
            adx_agent_core::unix_time_millis() + 30_000,
        )
        .await
        .unwrap()
        .target;
    assert_eq!(target.binding.current, existing.current);
    assert_eq!(target.binding.generation, existing.generation);
    assert!(state.binding(&third).await.unwrap().is_none());
}

async fn activate(
    user: &Activator,
    scope: &Scope,
    workspace: &str,
) -> adx_agent_core::AgentBinding {
    user.activate_with_parameters(
        scope,
        None,
        adx_agent_core::unix_time_millis() + 30_000,
        true,
        Some(format!("workspace={workspace}").as_bytes()),
    )
    .await
    .unwrap()
    .binding
}

#[tokio::test]
async fn automatic_golden_is_protected_before_backfill_and_used_without_product_eligibility() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Activator::new(state, platform.clone());
    let mut input = draft(true);
    input
        .env
        .insert("DIGITALMATE_MEMORY_BACKEND".into(), "mem2".into());
    activator.publish("tenant", &input).await.unwrap();
    let reference = TemplateRef::Version("1".into());
    let first = scope("first", reference.clone());
    let second = scope("second", reference.clone());
    let third = scope("third", reference.clone());

    platform.pending_protection.store(true, Ordering::Relaxed);
    for _ in 0..2 {
        platform
            .replies
            .lock()
            .await
            .push_back(Ok(SeedResult::Ready {
                snapshot_id: "snapshot-ready".into(),
            }));
    }
    activator.reconcile_snapshot_preparations().await.unwrap();
    let template = activator
        .template("tenant", "digitalmate", "1")
        .await
        .unwrap();
    assert_eq!(
        template.snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    );
    let cold = activate(&activator, &first, "w-first").await;
    assert!(cold.current.snapshot_id.is_none());

    platform.pending_protection.store(false, Ordering::Relaxed);
    activator.reconcile_snapshot_preparations().await.unwrap();
    let restored = activate(&activator, &second, "w-second").await;
    assert_eq!(
        restored.current.snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
    let another = activate(&activator, &third, "w-third").await;
    assert_eq!(another.current.snapshot_id, restored.current.snapshot_id);
    assert_eq!(activate(&activator, &first, "w-first").await, cold);
    assert!(matches!(
        activator.retire_golden("tenant", "snapshot-ready").await,
        Err(Error::Conflict(_))
    ));
    let retired = scope("fourth", reference);
    assert!(activate(&activator, &retired, "w-fourth")
        .await
        .current
        .snapshot_id
        .is_none());
    assert_eq!(activate(&activator, &second, "w-second").await, restored);

    let prepared = platform.calls.lock().await;
    assert_eq!(prepared[0], prepared[1]);
    let seed = &prepared[0].2;
    let proofs = platform.protections.lock().await;
    assert_eq!(proofs.len(), 2);
    assert_eq!(proofs[0], proofs[1]);
    let created = platform.creations.lock().await;
    for (binding, workspace) in [(&restored, "w-second"), (&another, "w-third")] {
        let request = created
            .iter()
            .find(|request| request.id == binding.current.environment_id)
            .unwrap();
        assert_eq!(request.execution.snapshot_id, binding.current.snapshot_id);
        assert_eq!(request.execution.env[GOLDEN_PHASE], "restore");
        assert_eq!(
            request.execution.env["DIGITALMATE_CRYPTO_WORKSPACE"],
            workspace
        );
        assert_eq!(
            request.execution.env["DIGITALMATE_MEMORY_BACKEND"],
            seed.execution.env["DIGITALMATE_MEMORY_BACKEND"]
        );
        assert_eq!(
            request.execution.mounts[0].volume_uid.as_deref(),
            Some(format!("uid-user-{workspace}").as_str())
        );
        assert_ne!(
            request.execution.mounts[0].volume_id.as_deref(),
            Some(seed.volume.id.as_str())
        );
    }
}

#[tokio::test]
async fn alias_upgrade_uses_the_selected_versions_golden_and_keeps_the_workspace() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Activator::new(state.clone(), platform.clone());
    activator.publish("tenant", &draft(false)).await.unwrap();
    activator.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    activator.reconcile_snapshot_preparations().await.unwrap();
    let mut alias = AliasConfig {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        alias: "production".into(),
        seed: "seed".into(),
        versions: vec![VersionWeight {
            version: "1".into(),
            weight: 1,
        }],
    };
    let revision = state.put_alias(&alias, None).await.unwrap();
    let scope = scope("upgraded", TemplateRef::Alias("production".into()));
    let before = activate(&activator, &scope, "w-upgrade").await;
    alias.versions[0].version = "2".into();
    state.put_alias(&alias, Some(&revision)).await.unwrap();
    platform.block_creation.store(true, Ordering::Relaxed);
    assert!(matches!(
        activator
            .reconcile_binding(&scope, adx_agent_core::unix_time_millis() + 30_000)
            .await,
        Err(Error::Conflict(_))
    ));
    let pending = state.binding(&scope).await.unwrap().unwrap();
    let candidate = pending.secondary.unwrap().environment;
    assert_eq!(candidate.snapshot_id.as_deref(), Some("snapshot-ready"));
    platform.unavailable.store(true, Ordering::Relaxed);
    platform.block_creation.store(false, Ordering::Relaxed);
    activator
        .reconcile_binding(&scope, adx_agent_core::unix_time_millis() + 30_000)
        .await
        .unwrap();
    let after = state.binding(&scope).await.unwrap().unwrap();
    assert_ne!(after.current.environment_id, before.current.environment_id);
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.volumes, before.volumes);
    assert_eq!(after.current.snapshot_id.as_deref(), Some("snapshot-ready"));
    assert_eq!(after.current.version, "2");
    assert_eq!(after.current, candidate);
    let creations = platform.creations.lock().await;
    assert_eq!(creations[1].execution, creations[2].execution);
    assert_eq!(creations[1].id, creations[2].id);
    assert_eq!(
        creations[2].execution.snapshot_id,
        after.current.snapshot_id
    );
}
