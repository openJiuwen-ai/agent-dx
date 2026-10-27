use adx_core::{prewarm::*, Error, Resources};

fn policy() -> PoolPolicy {
    PoolPolicy {
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
    }
}

#[test]
fn pool_capacity_units_limits_and_integer_overflow_are_rejected() {
    let resources = Resources {
        cpu_millis: 100,
        memory_bytes: 1_048_576,
        disk_bytes: 1024,
    };
    let p = policy();
    p.validate(resources).unwrap();
    let mut bad = p.clone();
    bad.capacity.unit = CapacityUnit::ArtifactNodeReplicas;
    assert!(bad.validate(resources).is_err());
    bad = p.clone();
    bad.capacity.target_ready = 4;
    assert!(bad.validate(resources).is_err());
    bad = p.clone();
    bad.idle_budget.cpu_millis = Some(1);
    assert!(bad.validate(resources).is_err());
    bad = p.clone();
    bad.idle_budget.memory_mib = Some(u64::MAX);
    assert!(bad.validate(resources).is_err());
    bad = p.clone();
    bad.health.max_staleness_seconds = 9;
    assert!(bad.validate(resources).is_err());
    bad = p;
    bad.acquire_wait_ms = 1;
    assert!(
        bad.validate(resources).is_err(),
        "only immediate fallback is supported initially"
    );
}

#[test]
fn preparation_context_is_stable_tenant_scoped_and_digest_pinned() {
    let mut context = PreparationContext {
        source_ref: SourceRef {
            tenant: "tenant".into(),
            template: "agent".into(),
            version: "1".into(),
            template_digest: format!("sha256:{}", "a".repeat(64)),
        },
        preference: PreparationPreference::Prefer,
        contract_version: 1,
    };
    context.validate("tenant").unwrap();
    assert!(context.validate("other").is_err());
    context.contract_version = 2;
    assert!(context.validate("tenant").is_err());
    context.contract_version = 1;
    context.source_ref.template_digest = "latest".into();
    assert!(context.validate("tenant").is_err());
}

#[test]
fn reserved_and_unknown_slots_never_become_public_inventory_again() {
    let mut slot = PreparedSlot::preparing(
        "slot".into(),
        "pool".into(),
        "node".into(),
        "session".into(),
        7,
        100,
    )
    .unwrap();
    assert!(!slot.claimable(100, &policy()));
    slot.mark_ready(101, 60).unwrap();
    assert!(slot.claimable(102, &policy()));
    assert!(!slot.claimable(122, &policy()), "stale health must miss");
    assert!(!slot.claimable(161, &policy()), "expired slot must miss");
    let owner = SlotOwner {
        tenant: "tenant".into(),
        environment_id: "adx-a".into(),
        ownership_generation: 8,
    };
    slot.reserve(owner.clone(), 102, &policy()).unwrap();
    slot.quarantine("consume outcome unknown".into()).unwrap();
    assert!(slot.mark_ready(103, 60).is_err());
    assert_eq!(
        slot.reserve(
            SlotOwner {
                environment_id: "adx-b".into(),
                ..owner.clone()
            },
            103,
            &policy()
        ),
        Err(Error::Conflict)
    );
    slot.attach(&owner).unwrap();
    slot.consume(&owner).unwrap();
    assert!(!slot.counts_as_idle());
    assert!(slot.mark_ready(104, 60).is_err());
    assert_eq!(slot.owner, Some(owner));
}

#[test]
fn unknown_and_retiring_resources_remain_in_the_capacity_ledger() {
    let mut slot = PreparedSlot::preparing(
        "slot".into(),
        "pool".into(),
        "node".into(),
        "session".into(),
        1,
        100,
    )
    .unwrap();
    slot.quarantine("prepare reply lost".into()).unwrap();
    assert!(slot.counts_as_idle());
    slot.retire().unwrap();
    assert!(slot.counts_as_idle());
    slot.confirm_deleted().unwrap();
    assert!(!slot.counts_as_idle());
    assert!(slot.mark_ready(101, 60).is_err());
}

#[test]
fn serialized_management_contract_rejects_unknown_fields() {
    let mut value = serde_json::to_value(policy()).unwrap();
    value["claim_user"] = "someone".into();
    assert!(serde_json::from_value::<PoolPolicy>(value).is_err());
}
