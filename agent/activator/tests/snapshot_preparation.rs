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
use sha2::{Digest, Sha256};
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
    forbid_store_reads: AtomicBool,
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
        assert!(
            !self.forbid_store_reads.load(Ordering::Relaxed),
            "cached subject activation reached storage"
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
        assert_ne!(
            index,
            &Index::all_bindings(),
            "subject binding must not scan Binding records"
        );
        self.inner.page(index, after, limit).await
    }
}

#[derive(Default)]
struct Platform {
    calls: Mutex<Vec<PreparationCall>>,
    replies: Mutex<VecDeque<Result<SeedResult, EnvironmentError>>>,
    creations: Mutex<Vec<CreateEnvironment>>,
    references: Mutex<Vec<SnapshotChange>>,
    allow_delete: AtomicBool,
    acquire_gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    catalog_revision: std::sync::atomic::AtomicU64,
    revision_gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    retired: AtomicBool,
    unavailable: AtomicBool,
    unknown_reference_response: AtomicBool,
    block_creation: AtomicBool,
}

#[async_trait]
impl Environment for Platform {
    async fn prepare_snapshot(
        &self,
        tenant: &str,
        id: &str,
        seed: &SeedExecution,
        _deadline_unix_ms: u64,
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
    async fn snapshot_revision(&self, _: &str, _: &str) -> Result<u64, EnvironmentError> {
        let gate = self.revision_gate.lock().await.take();
        if let Some((entered, released)) = gate {
            entered.send(()).unwrap();
            released.await.unwrap();
        }
        Ok(self.catalog_revision.load(Ordering::Relaxed).max(1))
    }
    async fn update_snapshot_reference(
        &self,
        tenant: &str,
        _: &str,
        change: &SnapshotChange,
    ) -> Result<(), EnvironmentError> {
        assert_eq!(tenant, "tenant");
        self.references.lock().await.push(change.clone());
        if matches!(change, SnapshotChange::AcquireBinding { .. }) {
            let gate = self.acquire_gate.lock().await.take();
            if let Some((entered, released)) = gate {
                entered.send(()).unwrap();
                released.await.unwrap();
            }
        }
        match change {
            SnapshotChange::AcquireBinding { revision, .. }
                if *revision < self.catalog_revision.load(Ordering::Relaxed).max(1) =>
            {
                return Err(EnvironmentError::SnapshotAdmissionStale);
            }
            SnapshotChange::Release { .. } | SnapshotChange::CancelPending { .. } => {
                self.catalog_revision.store(
                    self.catalog_revision.load(Ordering::Relaxed).max(1) + 1,
                    Ordering::Relaxed,
                );
            }
            _ => {}
        }
        if self.unknown_reference_response.load(Ordering::Relaxed) {
            return Err(EnvironmentError::OutcomeUnknown(
                "lost reference response".into(),
            ));
        }
        if matches!(change, SnapshotChange::Retire) {
            self.retired.store(true, Ordering::Relaxed);
            if !self
                .references
                .lock()
                .await
                .iter()
                .any(|change| matches!(change, SnapshotChange::AcquireBinding { .. }))
            {
                return Ok(());
            }
            return Err(EnvironmentError::Conflict(
                "snapshot retained by Binding".into(),
            ));
        }
        if self.retired.load(Ordering::Relaxed)
            && matches!(
                change,
                SnapshotChange::Acquire { .. } | SnapshotChange::AcquireBinding { .. }
            )
        {
            return Err(EnvironmentError::Conflict("snapshot retiring".into()));
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
        assert!(
            self.allow_delete.load(Ordering::Relaxed),
            "publication must not delete a user Environment"
        );
        Err(EnvironmentError::NotFound)
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
    assert!(!seed.execution.env.contains_key(GOLDEN_PHASE));
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
    assert!(platform.references.lock().await.is_empty());
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
    assert_eq!(recipe.handoff_contract_version, "3");
    assert!(recipe.restore_env.is_empty());
    assert_ne!(
        recipe.restore_profile_digest,
        version_two_digest(&completed)
    );
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
    assert!(!seed.execution.env.contains_key(GOLDEN_PHASE));
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
    for (key, value) in [("MODEL_ENDPOINT", "${model_url}"), ("GOLDEN_PHASE", "seed")] {
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

    let mut business = draft(true);
    business.env.insert(
        "DIGITALMATE_CRYPTO_CA_PEM".into(),
        "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----\n".into(),
    );
    business
        .env
        .insert("API_KEY".into(), "publisher-owned-shared-key".into());
    business.env.insert(
        "MODEL_ENDPOINT".into(),
        "https://example.test/v1?api-version=1".into(),
    );
    business.env.insert(
        "DIGITALMATE_KEENABLE_API_KEY".into(),
        "fixture-search-key".into(),
    );
    let published = author.publish("tenant", &business).await.unwrap();
    assert_eq!(published.env, business.env);
    author.reconcile_snapshot_preparations().await.unwrap();
    let calls = platform.calls.lock().await;
    let seed = &calls.last().unwrap().2;
    assert_eq!(
        seed.execution.env["DIGITALMATE_CRYPTO_CA_PEM"],
        business.env["DIGITALMATE_CRYPTO_CA_PEM"]
    );
    assert_eq!(
        seed.execution.env["DIGITALMATE_KEENABLE_API_KEY"],
        "fixture-search-key"
    );
    for (key, value) in &business.env {
        if key != "DIGITALMATE_CRYPTO_WORKSPACE" {
            assert_eq!(seed.execution.env.get(key), Some(value));
        }
    }
    assert!(!seed.execution.env.contains_key(GOLDEN_PHASE));
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
    assert_eq!(platform.calls.lock().await.len(), 1);
}

#[tokio::test]
async fn transient_preparation_error_retries_until_its_persisted_deadline() {
    let repo = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repo.clone());
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    let pending = activator.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Err(EnvironmentError::Unavailable(
            "temporary Platform error".into(),
        )));
    activator.reconcile_snapshot_preparations().await.unwrap();
    assert!(matches!(
        activator
            .template("tenant", "digitalmate", &pending.version)
            .await
            .unwrap()
            .snapshot_preparation,
        Some(SnapshotPreparation::Pending)
    ));

    let mut jobs = state.preparation_jobs(None).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let (_, mut job) = jobs.pop().unwrap();
    let key = Key::new(
        "snapshot-preparation",
        &["tenant", "digitalmate", &pending.version],
    )
    .unwrap();
    let record = repo.get(&key).await.unwrap().unwrap();
    job.deadline_unix_ms = adx_agent_core::unix_time_millis();
    let write = adx_agent_store::Put {
        key: key.clone(),
        record: Record::new(&job).unwrap(),
    };
    assert!(repo
        .commit(
            &Transaction::new(
                vec![Check {
                    key,
                    expected: Some(record.revision),
                }],
                vec![write]
            )
            .unwrap()
        )
        .await
        .unwrap());

    let restarted = Activator::new(state.clone(), platform.clone());
    restarted.reconcile_snapshot_preparations().await.unwrap();
    assert_eq!(state.preparation_jobs(None).await.unwrap().len(), 1);
    assert!(matches!(
        restarted.template("tenant", "digitalmate", &pending.version).await.unwrap().snapshot_preparation,
        Some(SnapshotPreparation::Failed { reason }) if reason.contains("deadline")
    ));
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    platform
        .unknown_reference_response
        .store(true, Ordering::Relaxed);
    restarted.reconcile_snapshot_preparations().await.unwrap();
    platform
        .unknown_reference_response
        .store(false, Ordering::Relaxed);
    assert_eq!(state.preparation_jobs(None).await.unwrap().len(), 1);
    restarted.reconcile_snapshot_preparations().await.unwrap();
    let jobs = platform.calls.lock().await;
    assert_eq!(jobs.len(), 3);
    assert!(jobs.iter().all(|call| call.1 == job.id));
    assert!(platform
        .references
        .lock()
        .await
        .contains(&SnapshotChange::Retire));
    assert!(state.preparation_jobs(None).await.unwrap().is_empty());
    assert!(matches!(
        restarted.template("tenant", "digitalmate", &pending.version).await.unwrap().snapshot_preparation,
        Some(SnapshotPreparation::Failed { reason }) if reason.contains("deadline")
    ));
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
            reuse_subject_binding: true,
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
        forbid_store_reads: AtomicBool::new(false),
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
}

#[tokio::test]
async fn digitalmate_reuses_live_binding_with_subject_pointer() {
    digitalmate_binding_history(Arc::new(MemoryRepository::default())).await;
}

#[tokio::test]
#[ignore = "Requires dedicated local Redis; set ADX_TEST_REDIS_URL"]
async fn redis_digitalmate_reuses_live_binding_with_subject_pointer() {
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
        forbid_store_reads: AtomicBool::new(false),
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

    let mut request = digitalmate_request(third.clone());
    request.activation.bypass_cache = false;
    let deadline = adx_agent_core::unix_time_millis() + 30_000;
    let target = activator.resolve(&request, deadline).await.unwrap().target;
    assert_eq!(target.binding.current, existing.current);
    assert_eq!(target.binding.scope, existing.scope);
    assert!(state.binding(&third).await.unwrap().is_none());

    repo.forbid_store_reads.store(true, Ordering::Relaxed);
    request.activation.scope = first;
    let cached = activator.resolve(&request, deadline).await.unwrap().target;
    assert_eq!(cached.binding, target.binding);
    repo.forbid_store_reads.store(false, Ordering::Relaxed);
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

fn version_two_digest(template: &adx_agent_core::TemplateVersion) -> String {
    let normalized =
        serde_json::to_vec(&("2", ExecutionSpec::from(template), &template.volumes)).unwrap();
    format!("sha256:{:x}", Sha256::digest(normalized))
}

async fn replace_fixture_record(repo: &MemoryRepository, key: Key, value: serde_json::Value) {
    let previous = repo.get(&key).await.unwrap().unwrap();
    let write = adx_agent_store::Put {
        key: key.clone(),
        record: Record::new(&value).unwrap(),
    };
    assert!(repo
        .commit(
            &Transaction::new(
                vec![Check {
                    key,
                    expected: Some(previous.revision)
                }],
                vec![write]
            )
            .unwrap()
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn version_two_preparation_keeps_protection_without_new_golden_allocations() {
    let repo = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repo.clone());
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    let published = activator.publish("tenant", &draft(true)).await.unwrap();
    let (_, mut job) = state.preparation_jobs(None).await.unwrap().pop().unwrap();
    // Reproduce the durable job written before the phase-free contract upgrade.
    job.recipe.handoff_contract_version = "2".into();
    job.recipe.restore_profile_digest = version_two_digest(&published);
    job.recipe
        .restore_env
        .insert(GOLDEN_PHASE.into(), "restore".into());
    job.seed
        .execution
        .env
        .insert(GOLDEN_PHASE.into(), "seed".into());
    let key = Key::new(
        "snapshot-preparation",
        &["tenant", "digitalmate", &published.version],
    )
    .unwrap();
    replace_fixture_record(&repo, key, serde_json::to_value(&job).unwrap()).await;
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    let restarted = Activator::new(AgentState::new(repo), platform.clone());
    restarted.reconcile_snapshot_preparations().await.unwrap();
    let template = restarted
        .template("tenant", "digitalmate", &published.version)
        .await
        .unwrap();
    assert_eq!(
        template.snapshot_preparation,
        Some(SnapshotPreparation::Ready)
    );
    assert_eq!(
        template
            .prewarm_recipe
            .as_ref()
            .unwrap()
            .handoff_contract_version,
        "2"
    );
    assert!(platform
        .references
        .lock()
        .await
        .iter()
        .any(|change| matches!(
            change,
            SnapshotChange::Acquire {
                owner: SnapshotOwner::Template { .. }
            }
        )));
    let binding = activate(
        &restarted,
        &scope("new-user", TemplateRef::Version(published.version)),
        "w-new",
    )
    .await;
    assert!(binding.current.snapshot_id.is_none());
    assert!(!platform
        .creations
        .lock()
        .await
        .last()
        .unwrap()
        .execution
        .env
        .contains_key(GOLDEN_PHASE));
}

#[tokio::test]
async fn committed_version_two_binding_keeps_its_snapshot_and_restore_environment() {
    let repo = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repo.clone());
    let platform = Arc::new(Platform::default());
    let activator = Activator::new(state.clone(), platform.clone());
    let published = activator.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    activator.reconcile_snapshot_preparations().await.unwrap();
    let scope = scope(
        "existing-user",
        TemplateRef::Version(published.version.clone()),
    );
    platform.block_creation.store(true, Ordering::Relaxed);
    assert!(matches!(
        activator
            .activate_with_parameters(
                &scope,
                None,
                adx_agent_core::unix_time_millis() + 30_000,
                true,
                Some(b"workspace=w-existing")
            )
            .await,
        Err(Error::Conflict(_))
    ));
    let committed = state.binding(&scope).await.unwrap().unwrap();
    assert_eq!(
        committed.current.snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
    // Restore the old durable recipe beside its already committed Binding decision.
    let key = Key::new(
        "snapshot-preparation-result",
        &["tenant", "digitalmate", &published.version],
    )
    .unwrap();
    let mut result = repo.get(&key).await.unwrap().unwrap().value;
    result["recipe"]["handoff_contract_version"] = json!("2");
    result["recipe"]["restore_profile_digest"] = json!(version_two_digest(&published));
    result["recipe"]["restore_env"] = json!({"GOLDEN_PHASE":"restore"});
    replace_fixture_record(&repo, key, result).await;
    platform.block_creation.store(false, Ordering::Relaxed);
    platform.unavailable.store(true, Ordering::Relaxed);
    let restarted = Activator::new(AgentState::new(repo), platform.clone());
    let retried = activate(&restarted, &scope, "w-existing").await;
    assert_eq!(retried.current, committed.current);
    let creations = platform.creations.lock().await;
    let request = creations.last().unwrap();
    assert_eq!(request.id, committed.current.environment_id);
    assert_eq!(request.execution.snapshot_id, committed.current.snapshot_id);
    assert_eq!(request.execution.env[GOLDEN_PHASE], "restore");
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

    platform
        .unknown_reference_response
        .store(true, Ordering::Relaxed);
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

    platform
        .unknown_reference_response
        .store(false, Ordering::Relaxed);
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
    assert_eq!(
        prepared.len(),
        1,
        "Catalog retry uses the persisted preparation result"
    );
    let seed = &prepared[0].2;
    let proofs = platform.references.lock().await;
    assert!(
        matches!(&proofs[0], SnapshotChange::Acquire { owner:SnapshotOwner::Template {name, version} } if name == "digitalmate" && version == "1")
    );
    assert_eq!(proofs[0], proofs[1]);
    let created = platform.creations.lock().await;
    for (binding, workspace) in [(&restored, "w-second"), (&another, "w-third")] {
        let request = created
            .iter()
            .find(|request| request.id == binding.current.environment_id)
            .unwrap();
        assert_eq!(request.execution.snapshot_id, binding.current.snapshot_id);
        assert!(!request.execution.env.contains_key(GOLDEN_PHASE));
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
    assert!(platform
        .references
        .lock()
        .await
        .contains(&SnapshotChange::AcquireBinding {
            revision: 1,
            owner: SnapshotOwner::Binding {
                binding_id: scope.binding_id.clone(),
                environment_id: candidate.environment_id.clone()
            }
        }));
    let creations = platform.creations.lock().await;
    assert_eq!(creations[1].execution, creations[2].execution);
    assert_eq!(creations[1].id, creations[2].id);
    assert_eq!(
        creations[2].execution.snapshot_id,
        after.current.snapshot_id
    );
}

#[tokio::test]
async fn unknown_owner_response_replays_the_persisted_allocation_before_environment_create() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let worker = Activator::new(state.clone(), platform.clone());
    worker.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    worker.reconcile_snapshot_preparations().await.unwrap();
    let subject = scope("retry-owner", TemplateRef::Version("1".into()));
    platform
        .unknown_reference_response
        .store(true, Ordering::Relaxed);
    assert!(worker
        .activate_with_parameters(
            &subject,
            None,
            adx_agent_core::unix_time_millis() + 30000,
            true,
            Some(b"workspace=w-retry")
        )
        .await
        .is_err());
    let pending = state.binding(&subject).await.unwrap().unwrap();
    assert_eq!(
        pending.current.pending_snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
    assert!(pending.current.snapshot_id.is_none());
    assert!(platform.creations.lock().await.is_empty());
    drop(worker);
    let worker = Activator::new(state.clone(), platform.clone());
    platform
        .unknown_reference_response
        .store(false, Ordering::Relaxed);
    platform.block_creation.store(true, Ordering::Relaxed);
    assert!(worker
        .activate_with_parameters(
            &subject,
            None,
            adx_agent_core::unix_time_millis() + 30000,
            true,
            Some(b"workspace=w-retry")
        )
        .await
        .is_err());
    let fixed = state.binding(&subject).await.unwrap().unwrap();
    assert_eq!(fixed.current.snapshot_id.as_deref(), Some("snapshot-ready"));
    assert!(fixed.current.pending_snapshot_id.is_none());
    assert_eq!(fixed.current.environment_id, pending.current.environment_id);
    let changes = platform.references.lock().await;
    assert_eq!(changes[1], changes[2]);
}

#[tokio::test]
async fn same_subject_reuses_winning_snapshot_decision_after_revision_read() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let first = Arc::new(Activator::new(state.clone(), platform.clone()));
    let second = Activator::new(state, platform.clone());
    first.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    first.reconcile_snapshot_preparations().await.unwrap();
    let subject = scope("concurrent", TemplateRef::Version("1".into()));
    let deadline = adx_agent_core::unix_time_millis() + 30000;
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *platform.revision_gate.lock().await = Some((entered, released));
    let other = subject.clone();
    let stalled = tokio::spawn(async move {
        first
            .activate_with_parameters(
                &other,
                None,
                deadline,
                true,
                Some(b"workspace=w-concurrent"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    let winning = second
        .activate_with_parameters(
            &subject,
            None,
            deadline,
            true,
            Some(b"workspace=w-concurrent"),
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let replay = stalled.await.unwrap().unwrap();
    assert_eq!(winning.binding.current, replay.binding.current);
    assert_eq!(
        replay.binding.current.snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
}

#[tokio::test]
async fn new_binding_retries_admission_after_another_binding_releases() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let worker = Arc::new(Activator::new(state, platform.clone()));
    worker.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    worker.reconcile_snapshot_preparations().await.unwrap();
    let deadline = adx_agent_core::unix_time_millis() + 30000;
    let old = scope("old-user", TemplateRef::Version("1".into()));
    worker
        .activate_with_parameters(&old, None, deadline, true, Some(b"workspace=w-old"))
        .await
        .unwrap();
    let subject = scope("next-user", TemplateRef::Version("1".into()));
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *platform.acquire_gate.lock().await = Some((entered, released));
    let other = subject.clone();
    let request_worker = worker.clone();
    let stalled = tokio::spawn(async move {
        request_worker
            .activate_with_parameters(&other, None, deadline, true, Some(b"workspace=w-next"))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    platform.allow_delete.store(true, Ordering::Relaxed);
    worker.delete_binding(&old).await.unwrap();
    release.send(()).unwrap();
    let target = stalled.await.unwrap().unwrap();
    let changes = platform.references.lock().await;
    let attempts = changes
        .iter()
        .filter_map(|change| match change {
            SnapshotChange::AcquireBinding {
                revision,
                owner: owner @ SnapshotOwner::Binding { binding_id, .. },
            } if binding_id == &subject.binding_id => Some((*revision, owner)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(attempts.len(), 2);
    assert_eq!((attempts[0].0, attempts[1].0), (1, 2));
    assert_eq!(attempts[0].1, attempts[1].1);
    assert_eq!(
        target.binding.current.snapshot_id.as_deref(),
        Some("snapshot-ready")
    );
    let creations = platform.creations.lock().await;
    assert_eq!(creations.len(), 2);
    assert_eq!(creations[1].id, target.binding.current.environment_id);
}

#[tokio::test]
async fn deleted_binding_cannot_retry_a_fenced_delayed_admission() {
    let platform = Arc::new(Platform::default());
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let worker = Arc::new(Activator::new(state.clone(), platform.clone()));
    worker.publish("tenant", &draft(true)).await.unwrap();
    platform
        .replies
        .lock()
        .await
        .push_back(Ok(SeedResult::Ready {
            snapshot_id: "snapshot-ready".into(),
        }));
    worker.reconcile_snapshot_preparations().await.unwrap();
    let subject = scope("deleting", TemplateRef::Version("1".into()));
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *platform.acquire_gate.lock().await = Some((entered, released));
    let other = subject.clone();
    let request_worker = worker.clone();
    let stalled = tokio::spawn(async move {
        request_worker
            .activate_with_parameters(
                &other,
                None,
                adx_agent_core::unix_time_millis() + 30000,
                true,
                Some(b"workspace=w-deleting"),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    platform.allow_delete.store(true, Ordering::Relaxed);
    worker.delete_binding(&subject).await.unwrap();
    assert!(state.binding(&subject).await.unwrap().is_none());
    assert!(platform
        .references
        .lock()
        .await
        .iter()
        .any(|change| matches!(change, SnapshotChange::CancelPending { .. })));
    release.send(()).unwrap();
    assert!(matches!(
        stalled.await.unwrap(),
        Err(adx_agent_core::error::Error::Conflict(_))
    ));
    assert_eq!(
        platform
            .references
            .lock()
            .await
            .iter()
            .filter(|c| matches!(c, SnapshotChange::AcquireBinding { .. }))
            .count(),
        1
    );
    assert!(platform.creations.lock().await.is_empty());
}
