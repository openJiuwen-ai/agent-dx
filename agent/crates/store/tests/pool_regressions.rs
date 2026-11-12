#![cfg(feature = "test-memory")]

use adx_agent_core::pool::PoolConfig;
use adx_agent_store::{AgentState, Index, Key, MemoryRepository, Record, Repository, Transaction};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct BarrierRepository {
    memory: MemoryRepository,
    reservations: AtomicUsize,
    barrier: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl Repository for BarrierRepository {
    async fn get(&self, key: &Key) -> adx_agent_store::Result<Option<Record>> {
        self.memory.get(key).await
    }
    async fn page(
        &self,
        index: &Index,
        after: Option<&str>,
        limit: usize,
    ) -> adx_agent_store::Result<Vec<(String, Record)>> {
        self.memory.page(index, after, limit).await
    }
    async fn commit(&self, transaction: &Transaction) -> adx_agent_store::Result<bool> {
        if self.reservations.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        self.memory.commit(transaction).await
    }
}

fn config() -> PoolConfig {
    serde_json::from_value(serde_json::json!({
        "tenant":"tenant", "pool_id":"pool", "template":"app", "version":"1",
        "enabled":true,"min_running":1,"min_ready":1,"max_slots":1,"max_provisioning":1,
        "key_confirm_timeout_seconds":60,"provision_timeout_seconds":300,
        "claim_timeout_seconds":30,"max_ready_age_seconds":86400
    }))
    .unwrap()
}

#[tokio::test]
async fn reserve_serializes_replicas_that_both_read_empty_inventory() {
    let repository = Arc::new(BarrierRepository {
        memory: MemoryRepository::default(),
        reservations: AtomicUsize::new(2),
        barrier: tokio::sync::Barrier::new(2),
    });
    let state = AgentState::new(repository.clone());
    state.put_pool(&config(), None).await.unwrap();
    repository.reservations.store(0, Ordering::SeqCst);
    let (left, right) = tokio::join!(
        state.reserve_pool_slot("tenant", "pool", "https://model/v1", 1000),
        state.reserve_pool_slot("tenant", "pool", "https://model/v1", 1000)
    );
    assert_eq!(
        usize::from(left.unwrap().is_some()) + usize::from(right.unwrap().is_some()),
        1
    );
    assert_eq!(state.pool_slots("tenant", "pool").await.unwrap().len(), 1);
}

#[tokio::test]
async fn pending_capacity_satisfies_both_inventory_targets() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let mut config = config();
    config.max_slots = 8;
    config.max_provisioning = 3;
    state.put_pool(&config, None).await.unwrap();
    let mut reserved = Vec::new();
    for _ in 0..3 {
        reserved.push(
            state
                .reserve_pool_slot("tenant", "pool", "https://model/v1", 1000)
                .await
                .unwrap()
                .is_some(),
        );
    }
    assert_eq!(reserved, [true, false, false]);
}
