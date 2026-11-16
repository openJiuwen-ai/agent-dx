use super::*;
use crate::{Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
fn pending_record() -> EnvironmentRecord {
    serde_json::from_value(serde_json::json!({
        "spec":{"id":"pending","tenant_id":"t","image":"image","runtime_class":"runc","priority":0,
            "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}},
        "assignment":{"environment_id":"pending","node_id":"n","shard_id":0,"generation":1,"devices":[]},
        "state":"Pending","revision":0,"runtime":{"id":"pending-1","ip":null},"resources_held":true,
        "checkpoint":null,"last_operation":null,"restart_attempts":0,"restart_pending":false,
        "execution_artifacts":[],"snapshot_source":null,"failure":null
    })).unwrap()
}

#[tokio::test]
async fn orphan_scan_does_not_retire_a_generation_waiting_for_its_controller() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    node.update_capacity(
        Resources {
            cpu_millis: 10,
            memory_bytes: 10,
            disk_bytes: 0,
        },
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    let record = pending_record();
    for _ in 0..2 {
        let basis = ComparisonBasis {
            controllers: Default::default(),
            inventory: Ok(vec![crate::RuntimeObservation {
                environment_id: record.spec.id.clone(),
                runtime_id: "pending-1-r2".into(),
                generation: 1,
                tenant_id: "t".into(),
                running: true,
            }]),
        };
        let outcome = node
            .compare_retained(
                basis,
                vec![record.clone()],
                vec![],
                vec![],
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ReconciliationOutcome::Complete));
        assert!(node.retired_environment("pending").is_none());
        assert!(node.accepting_allocations());
    }
    // The optional Claim's Create RPC can still register its controller.
    node.environment(record.spec, record.assignment).unwrap();
}

#[tokio::test]
async fn orphan_claim_in_controller_replacement_gap_preserves_authority_adoption() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    let mut record = pending_record();
    record.state = EnvironmentState::Running;
    record.revision = 2;
    record.runtime.id = "pending-1-r3".into();
    record.runtime.ip = Some("127.0.0.1".parse().unwrap());
    let runtime = crate::RuntimeObservation {
        environment_id: record.spec.id.clone(),
        runtime_id: "pending-1-r2".into(),
        generation: 1,
        tenant_id: "t".into(),
        running: true,
    };
    assert!(matches!(
        node.claim_orphan(&runtime, true),
        OrphanTarget::AwaitingController
    ));
    assert!(node.retired_environment("pending").is_none());
    assert!(node.adopt_comparison_record(&record).unwrap().is_some());
    assert!(matches!(
        node.claim_orphan(&runtime, true),
        OrphanTarget::Controller(_)
    ));
}

#[tokio::test]
async fn live_catalog_defers_a_claim_before_its_create_rpc_arrives() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    let record = pending_record();
    let adopted = node.adopt_comparison_record(&record).unwrap();
    assert!(
        adopted.is_none(),
        "an unstarted claim must remain available for its create RPC"
    );
    assert!(!node.environments.lock().unwrap().contains_key("pending"));
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn paused_live_adoption_does_not_leak_a_tentative_create_hold() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    let mut record = pending_record();
    record.state = EnvironmentState::Paused;
    record.revision = 1;
    record.resources_held = false;
    node.update_capacity(record.spec.resources, std::time::Duration::from_secs(60))
        .unwrap();

    let reservation = node.reserve_local(&record.spec).unwrap();
    assert_eq!(node.used(), record.spec.resources);
    assert!(node.adopt_comparison_record(&record).unwrap().is_some());
    let retry = node.reserve_local(&record.spec).unwrap();
    assert_eq!(retry.token, reservation.token);
    node.release_local(&record.spec.id, &retry).unwrap();
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn live_comparison_preserves_an_already_registered_unstarted_create() {
    for locally_reserved in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        node.update_capacity(
            adx_core::Resources {
                cpu_millis: 10,
                memory_bytes: 10,
                disk_bytes: 0,
            },
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let record = pending_record();
        if locally_reserved {
            node.reserve_local(&record.spec).unwrap();
        }
        let handle = node
            .environment(record.spec.clone(), record.assignment.clone())
            .unwrap();
        handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert_eq!(handle.view().0.state, EnvironmentState::Pending);
        assert_eq!(
            hooks.0.load(Ordering::SeqCst),
            0,
            "comparison cannot publish a failure for this create"
        );
    }
}

#[derive(Default)]
struct Hooks(AtomicUsize);
#[async_trait]
impl RuntimeDriver for Hooks {
    async fn inventory(&self) -> Result<Vec<crate::RuntimeObservation>> {
        Ok(vec![])
    }
    async fn start(
        &self,
        _: &adx_core::EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        panic!("adopting authority must not cold start");
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn startup_still_retires_an_uncertain_old_pending_execution() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    let record = pending_record();
    node.reconcile(vec![record.clone()]).await.unwrap();
    let handle = node.environment(record.spec, record.assignment).unwrap();
    assert_eq!(handle.view().0.state, EnvironmentState::Failed);
    assert!(hooks.0.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn live_comparison_does_not_exempt_pending_records_with_execution_evidence() {
    for revision in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        let mut record = pending_record();
        if revision {
            record.revision = 1;
        } else {
            record.runtime.ip = Some("127.0.0.1".parse().unwrap());
        }
        let handle = node.adopt_comparison_record(&record).unwrap().unwrap();
        handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert_eq!(handle.view().0.state, EnvironmentState::Failed);
    }
}

#[async_trait]
impl Readiness for Hooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Hooks {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl StateSink for Hooks {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Durability::Published)
    }
}

#[tokio::test]
async fn adopted_controller_requires_authority_check_before_exposing_lifecycle() {
    for terminal in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        let record: EnvironmentRecord = serde_json::from_value(serde_json::json!({
            "spec": {"id":"adopted", "tenant_id":"t", "image":"image", "runtime_class":"runc", "priority":0,
                "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}},
            "assignment":{"environment_id":"adopted","node_id":"n","shard_id":0,"generation":1,"devices":[]},
            "state": if terminal { "Failed" } else { "Running" }, "revision":2,
            "runtime":{"id":"adopted-1","ip":"127.0.0.1"}, "resources_held":!terminal,
            "checkpoint":null,"last_operation":null,"restart_attempts":0,"restart_pending":false,
            "execution_artifacts":[],"snapshot_source":null,"failure":null
        })).unwrap();
        let handle = node.adopt_comparison_record(&record).unwrap().unwrap();
        // This models a request racing the background task before its command
        // reaches the newly installed controller's queue.
        let early = if terminal {
            handle.delete().await
        } else {
            handle.ensure_running("early".into()).await
        };
        assert!(
            matches!(early, Err(Error::Unavailable(_))),
            "unverified adoption must reject lifecycle admission"
        );
        assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
        let result = handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert!(result == AuthorityResult::Current);
        if terminal {
            assert_eq!(
                handle.delete().await.unwrap().record.state,
                EnvironmentState::Deleted
            );
        } else {
            assert_eq!(
                handle
                    .ensure_running("ready".into())
                    .await
                    .unwrap()
                    .record
                    .state,
                EnvironmentState::Running
            );
        }
    }
}

#[tokio::test]
async fn newer_orphan_generation_does_not_inherit_a_completed_delete_fence() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    let record = pending_record();
    node.remember_retired_with_fence(
        &record.assignment,
        None,
        Some(crate::RetiredDeleteFence::Revision(7)),
    );
    let runtime = crate::RuntimeObservation {
        environment_id: record.spec.id,
        runtime_id: "pending-2".into(),
        generation: 2,
        tenant_id: "t".into(),
        running: true,
    };
    assert!(matches!(
        node.claim_orphan(&runtime, false),
        OrphanTarget::Unowned
    ));
    let retired = node.retired_environment("pending").unwrap();
    assert_eq!(retired.generation, 2);
    assert_eq!(retired.coordinator_delete_fence, None);
}
