mod common;
use adx_coordinator::storage::prewarm::{Command, Condition, Mutation, PoolError, Selection};
use adx_core::{prewarm::*, Resources};

fn request(id: &str) -> Mutation {
    let digest = format!("sha256:{}", "a".repeat(64));
    Mutation {
        request_path: "/api/environment/v1/prewarm-pools/pool".into(),
        pool_id: "pool".into(),
        request_id: id.into(),
        condition: Condition::Absent,
        command: Command::Put {
            profile: Box::new(PreparationProfile {
                source_ref: SourceRef {
                    tenant: "tenant".into(),
                    template: "agent".into(),
                    version: "1".into(),
                    template_digest: digest.clone(),
                },
                image: format!("image@{digest}"),
                runtime_class: "runc".into(),
                resources: Resources {
                    cpu_millis: 100,
                    memory_bytes: 1_048_576,
                    disk_bytes: 1024,
                },
                architecture: "amd64".into(),
                runtime_recipe_digest: digest.clone(),
                security_digest: digest.clone(),
                late_binding_contract_digest: digest,
                contract_version: 1,
            }),
            policy: PoolPolicy {
                mode: PoolMode::RuntimeSlot,
                enabled: false,
                capacity: PoolCapacity {
                    unit: CapacityUnit::Slots,
                    min_ready: 1,
                    target_ready: 2,
                    max_live: 3,
                },
                refill: RefillPolicy {
                    prepare_concurrency: 2,
                    initial_backoff_seconds: 1,
                    max_backoff_seconds: 30,
                },
                health: HealthPolicy {
                    interval_seconds: 10,
                    max_staleness_seconds: 20,
                },
                ready_ttl_seconds: 60,
                acquire_wait_ms: 0,
                idle_budget: IdleBudget {
                    cpu_millis: Some(300),
                    memory_mib: Some(3),
                    disk_bytes: 4096,
                },
            },
        },
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replay_precedes_etag_check_and_survives_aof_restart() {
    let mut rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let original = request("write");
    let accepted = session.mutate_pool("tenant", &original, 100).await.unwrap();
    let mut update = request("update");
    update.condition = Condition::Revision(accepted.pool.revision);
    if let Command::Put { policy, .. } = &mut update.command {
        policy.ready_ttl_seconds = 120;
    }
    session.mutate_pool("tenant", &update, 101).await.unwrap();
    assert_eq!(
        session.mutate_pool("tenant", &original, 102).await.unwrap(),
        accepted
    );
    let mut changed = original.clone();
    changed.condition = Condition::Revision(1);
    assert_eq!(
        session
            .mutate_pool("tenant", &changed, 102)
            .await
            .unwrap_err(),
        PoolError::IdempotencyConflict
    );
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted
            .mutate_pool("tenant", &original, 103)
            .await
            .unwrap(),
        accepted
    );
    assert!(session
        .mutate_pool("tenant", &request("stale"), 103)
        .await
        .is_err());
    assert_eq!(
        restarted.get_pool("other", "pool").await.unwrap_err(),
        PoolError::NotFound
    );
    assert!(
        restarted.snapshot().await.is_ok(),
        "pool records must not pollute the Environment catalog"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_management_cas_has_one_winner_and_backend_gate_is_fail_closed() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let first = request("a");
    let second = request("b");
    let (a, b) = tokio::join!(
        session.mutate_pool("tenant", &first, 100),
        session.mutate_pool("tenant", &second, 100)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let mut enabled = request("enable");
    enabled.condition = Condition::Revision(1);
    if let Command::Put { policy, .. } = &mut enabled.command {
        policy.enabled = true;
    }
    assert_eq!(
        session
            .mutate_pool("tenant", &enabled, 101)
            .await
            .unwrap_err(),
        PoolError::Unsupported
    );
    assert_eq!(
        session.get_pool("tenant", "pool").await.unwrap().revision,
        1
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn cold_attempt_is_globally_unique_and_never_reselected_for_another_request_id() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let context = PreparationContext {
        source_ref: match request("a").command {
            Command::Put { profile, .. } => profile.source_ref,
            _ => unreachable!(),
        },
        preference: PreparationPreference::Prefer,
        contract_version: 1,
    };
    let fingerprint = format!("sha256:{}", "b".repeat(64));
    let (a, b) = tokio::join!(
        session.select_preparation("tenant", "adx-a", &context, &fingerprint, 100),
        session.select_preparation("tenant", "adx-a", &context, &fingerprint, 100)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    assert_eq!(
        session
            .preparation_attempt("tenant", "adx-a")
            .await
            .unwrap()
            .selection,
        Selection::Cold
    );
    assert!(session
        .select_preparation("other", "adx-a", &context, &fingerprint, 101)
        .await
        .is_err());
    let different = format!("sha256:{}", "c".repeat(64));
    assert!(session
        .select_preparation("tenant", "adx-a", &context, &different, 101)
        .await
        .is_err());
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        restarted
            .select_preparation("tenant", "adx-a", &context, &fingerprint, 102)
            .await
            .unwrap()
            .created_at,
        100
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn artifact_pages_bind_opaque_cursors_to_tenant_pool_and_filter() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut mutation = request("pages");
    if let Command::Put { policy, .. } = &mut mutation.command {
        policy.mode = PoolMode::ImageRootfs;
        policy.capacity.unit = CapacityUnit::ArtifactNodeReplicas;
        policy.idle_budget.cpu_millis = None;
        policy.idle_budget.memory_mib = None;
    }
    let pool = session
        .mutate_pool("tenant", &mutation, 100)
        .await
        .unwrap()
        .pool;
    let mut inventory = pool.clone();
    for id in ["artifact-a", "artifact-b"] {
        inventory.artifacts.push(PreparationArtifact {
            id: id.into(),
            node_id: id.into(),
            node_session: "session".into(),
            profile_digest: pool.profile_digest.clone(),
            state: SlotState::Ready,
            size_bytes: 512,
            created_at: 100,
            checked_at: 100,
            expires_at: 200,
            retry_at: 0,
            failures: 0,
        });
    }
    assert!(session
        .update_pool_inventory(&pool, &inventory)
        .await
        .unwrap());
    let first = session
        .prewarm_artifact_page("tenant", "pool", 1, None, "")
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    let token = first.next_page_token.unwrap();
    let second = session
        .prewarm_artifact_page("tenant", "pool", 1, None, &token)
        .await
        .unwrap();
    assert_eq!(second.items[0].id, "artifact-b");
    assert!(second.next_page_token.is_none());
    assert!(session
        .prewarm_artifact_page("tenant", "pool", 1, Some(SlotState::Ready), &token)
        .await
        .is_err());
    assert!(session
        .prewarm_artifact_page("other", "pool", 1, None, &token)
        .await
        .is_err());
    let mut other = mutation;
    other.pool_id = "pool2".into();
    other.request_id = "pages-other".into();
    session.mutate_pool("tenant", &other, 100).await.unwrap();
    assert!(session
        .prewarm_artifact_page("tenant", "pool2", 1, None, &token)
        .await
        .is_err());
    assert!(session
        .prewarm_artifact_page("tenant", "pool", 1, None, &uuid::Uuid::new_v4().to_string())
        .await
        .is_err());
    let mut over_budget = inventory.clone();
    over_budget.artifacts[0].size_bytes = u64::MAX;
    assert!(session
        .update_pool_inventory(&inventory, &over_budget)
        .await
        .is_err());
}
