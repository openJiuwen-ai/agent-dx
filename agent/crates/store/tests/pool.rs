#![cfg(feature = "test-memory")]

use adx_agent_core::{pool::*, Scope, TemplateRef};
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::Arc;

fn state() -> AgentState {
    AgentState::new(Arc::new(MemoryRepository::default()))
}
fn config(min_running: u64, min_ready: u64, max_provisioning: u64) -> PoolConfig {
    PoolConfig {
        pool_id: "digitalmate-v17".into(),
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        version: "1".into(),
        enabled: true,
        min_running,
        min_ready,
        max_slots: 8,
        max_provisioning,
        key_confirm_timeout_seconds: 60,
        provision_timeout_seconds: 300,
        claim_timeout_seconds: 30,
        max_ready_age_seconds: 86_400,
    }
}
fn scope(user: &str, version: &str) -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        template_ref: TemplateRef::Version(version.into()),
        binding_id: format!("digitalmate-{user}"),
    }
}
async fn ready_slot(state: &AgentState, phase: SlotPhase) -> PoolSlot {
    let slot = state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model.example/v1",
            1_000,
        )
        .await
        .unwrap()
        .expect("slot reserved");
    state
        .confirm_pool_key(
            "tenant",
            "digitalmate-v17",
            &slot.slot_id,
            &slot.key_digest,
            2_000,
        )
        .await
        .unwrap();
    state
        .mark_pool_slot_ready("tenant", "digitalmate-v17", &slot.slot_id, phase)
        .await
        .unwrap();
    state
        .pool_slot("tenant", "digitalmate-v17", &slot.slot_id)
        .await
        .unwrap()
        .unwrap()
        .1
}

#[tokio::test]
async fn reserve_persists_the_key_before_remote_confirmation() {
    let state = state();
    state.put_pool(&config(1, 1, 1), None).await.unwrap();
    let slot = state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model.example/v1",
            1_000,
        )
        .await
        .unwrap()
        .expect("first slot");
    assert_eq!(slot.state, SlotState::KeyPending);
    assert!(slot.parameters["model_api_key"].starts_with("sk-"));
    assert_ne!(slot.parameters["model_api_key"], slot.key_digest);
    assert_eq!(slot.key_digest.len(), 64);
    assert_eq!(
        slot.parameters["model_api_base"],
        "https://model.example/v1"
    );
    // The provisioning budget is already consumed, so a second replica observes no deficit.
    assert!(state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model.example/v1",
            1_000
        )
        .await
        .unwrap()
        .is_none());
    let pending = state
        .pending_pool_keys("tenant", "digitalmate-v17", 20)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].model_api_key, slot.parameters["model_api_key"]);
    let confirmed = state
        .confirm_pool_key(
            "tenant",
            "digitalmate-v17",
            &slot.slot_id,
            &slot.key_digest,
            2_000,
        )
        .await
        .unwrap();
    assert_eq!(confirmed.state, SlotState::Provisioning);
}

#[tokio::test]
async fn claim_and_commit_are_idempotent_and_deliver_once() {
    let state = state();
    state.put_pool(&config(1, 1, 2), None).await.unwrap();
    let slot = ready_slot(&state, SlotPhase::Running).await;
    let alice = scope("alice", "1");
    let first = state
        .claim_pool_slot(
            "tenant",
            "digitalmate-v17",
            &alice,
            &alice.binding_id,
            3_000,
        )
        .await
        .unwrap()
        .expect("claim granted");
    let ClaimOutcome::Granted {
        slot_id,
        parameters,
        ..
    } = first
    else {
        panic!("expected grant");
    };
    assert_eq!(slot_id, slot.slot_id);
    assert_eq!(
        parameters["model_api_key"],
        slot.parameters["model_api_key"]
    );
    let replay = state
        .claim_pool_slot(
            "tenant",
            "digitalmate-v17",
            &alice,
            &alice.binding_id,
            3_010,
        )
        .await
        .unwrap()
        .expect("same claim");
    assert!(matches!(replay, ClaimOutcome::Granted { .. }));
    // A Claiming slot never returns to Ready, so another user cannot take it.
    let bob = scope("bob", "1");
    assert!(state
        .claim_pool_slot("tenant", "digitalmate-v17", &bob, &bob.binding_id, 3_020,)
        .await
        .unwrap()
        .is_none());
    let committed = state
        .commit_pool_claim("tenant", "digitalmate-v17", &alice.binding_id)
        .await
        .unwrap();
    let ClaimOutcome::Committed { binding, .. } = committed else {
        panic!("expected committed");
    };
    assert_eq!(binding.scope, alice);
    assert_eq!(binding.current.environment_id, slot.environment_id);
    let replay = state
        .commit_pool_claim("tenant", "digitalmate-v17", &alice.binding_id)
        .await
        .unwrap();
    let ClaimOutcome::Committed {
        binding: replayed, ..
    } = replay
    else {
        panic!("expected committed replay");
    };
    assert_eq!(replayed.generation, binding.generation);
    assert!(state
        .pool_slot("tenant", "digitalmate-v17", &slot.slot_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn global_claim_lookup_keeps_original_pool_and_workspace_identity_after_delivery() {
    let repository = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repository.clone());
    state.put_pool(&config(1, 1, 1), None).await.unwrap();
    let slot = ready_slot(&state, SlotPhase::Running).await;
    uuid::Uuid::parse_str(slot.slot_id.strip_prefix("s-").unwrap()).unwrap();
    let alice = scope("alice", "1");
    state
        .claim_pool_slot("tenant", "digitalmate-v17", &alice, &alice.binding_id, 3000)
        .await
        .unwrap()
        .unwrap();
    let saved = state
        .lookup_pool_claim("tenant", &alice.binding_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.pool_id, "digitalmate-v17");
    state
        .commit_pool_claim("tenant", &saved.pool_id, &alice.binding_id)
        .await
        .unwrap();
    use adx_agent_store::Repository;
    assert!(repository
        .get(&adx_agent_store::Key::new("pool-workspace", &[&slot.slot_id]).unwrap())
        .await
        .unwrap()
        .is_some());
    let mut other = config(1, 1, 1);
    other.pool_id = "other-pool".into();
    state.put_pool(&other, None).await.unwrap();
    assert!(matches!(
        state
            .claim_pool_slot("tenant", "other-pool", &alice, &alice.binding_id, 4000)
            .await
            .unwrap(),
        Some(ClaimOutcome::Committed { .. })
    ));
    assert!(state
        .pool_slots("tenant", "other-pool")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn concurrent_claims_for_one_slot_produce_one_grant() {
    let state = Arc::new(state());
    state.put_pool(&config(0, 1, 1), None).await.unwrap();
    ready_slot(&state, SlotPhase::Running).await;
    let first = {
        let state = state.clone();
        tokio::spawn(async move {
            let scope = scope("alice", "1");
            state
                .claim_pool_slot(
                    "tenant",
                    "digitalmate-v17",
                    &scope,
                    &scope.binding_id,
                    3_000,
                )
                .await
                .unwrap()
        })
    };
    let second = {
        let state = state.clone();
        tokio::spawn(async move {
            let scope = scope("bob", "1");
            state
                .claim_pool_slot(
                    "tenant",
                    "digitalmate-v17",
                    &scope,
                    &scope.binding_id,
                    3_000,
                )
                .await
                .unwrap()
        })
    };
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        usize::from(first.is_some()) + usize::from(second.is_some()),
        1
    );
}

#[tokio::test]
async fn abort_quarantines_the_slot_and_marks_release_after_retirement() {
    let state = state();
    state.put_pool(&config(0, 1, 1), None).await.unwrap();
    let slot = ready_slot(&state, SlotPhase::Paused).await;
    let alice = scope("alice", "1");
    state
        .claim_pool_slot(
            "tenant",
            "digitalmate-v17",
            &alice,
            &alice.binding_id,
            3_000,
        )
        .await
        .unwrap()
        .expect("grant");
    let aborted = state
        .abort_pool_claim(
            "tenant",
            "digitalmate-v17",
            &alice.binding_id,
            Some("account_conflict"),
        )
        .await
        .unwrap();
    assert_eq!(
        aborted,
        ClaimOutcome::Aborted {
            claim_id: alice.binding_id.clone(),
            released: false
        }
    );
    assert_eq!(
        state
            .pool_slot("tenant", "digitalmate-v17", &slot.slot_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .state,
        SlotState::Quarantined
    );
    state
        .retire_pool_slot("tenant", "digitalmate-v17", &slot.slot_id)
        .await
        .unwrap();
    let claim = state
        .pool_claim("tenant", "digitalmate-v17", &alice.binding_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert!(claim.released);
    let retired = state
        .retired_pool_keys("tenant", "digitalmate-v17", 20)
        .await
        .unwrap();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].model_api_key, slot.parameters["model_api_key"]);
}

#[tokio::test]
async fn claim_requires_the_exact_pool_version() {
    let state = state();
    state.put_pool(&config(0, 1, 1), None).await.unwrap();
    ready_slot(&state, SlotPhase::Running).await;
    let future = scope("alice", "2");
    assert!(state
        .claim_pool_slot(
            "tenant",
            "digitalmate-v17",
            &future,
            &future.binding_id,
            3_000,
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn reservation_keeps_a_running_slot_even_when_min_ready_is_met() {
    let state = state();
    state.put_pool(&config(1, 1, 2), None).await.unwrap();
    ready_slot(&state, SlotPhase::Paused).await;
    assert!(state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model.example/v1",
            4_000
        )
        .await
        .unwrap()
        .is_some());

    assert!(state
        .reserve_pool_slot(
            "tenant",
            "digitalmate-v17",
            "https://model.example/v1",
            4_000
        )
        .await
        .unwrap()
        .is_none());
}
