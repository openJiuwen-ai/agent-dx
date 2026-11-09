use super::*;
use adx_core::{scheduling::LabelSelector, Resources};

fn placed_recovery() -> (Coordinator, EnvironmentSpec, Assignment) {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let resources = Resources {
        cpu_millis: 100,
        memory_bytes: 1024,
        disk_bytes: 1024,
    };
    coordinator
        .register(Node {
            id: "target".into(),
            capacity: resources,
            available: true,
            labels: [("ADX_CHECKPOINT_STORAGE".into(), "shared".into())].into(),
            runtime_classes: vec!["test-runtime".into()],
            devices: vec![],
        })
        .unwrap();
    let spec = EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        id: "recovering".into(),
        tenant_id: "tenant".into(),
        image: "test-image".into(),
        runtime_class: "test-runtime".into(),
        resources,
        priority: 0,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        execution: Default::default(),
    };
    let mut placement = spec.clone();
    placement.scheduling.required_node.push(LabelSelector {
        match_labels: [("ADX_CHECKPOINT_STORAGE".into(), "shared".into())].into(),
        ..Default::default()
    });
    coordinator.submit_recovery(placement).unwrap();
    let assignment = coordinator.schedule(0).unwrap().unwrap();
    (coordinator, spec, assignment)
}

#[test]
fn durable_recovery_spec_accepts_claim_without_changing_assignment_or_reservation() {
    let (mut coordinator, spec, assignment) = placed_recovery();
    let reserved = coordinator.node_resources("target").unwrap();
    let placement_snapshot = coordinator.snapshot();
    assert_ne!(placement_snapshot.environments()[&spec.id].spec, spec);
    coordinator
        .finish_recovery_placement(&spec, &assignment)
        .unwrap();
    coordinator.accept_claim(&spec, &assignment).unwrap();
    coordinator
        .finish_recovery_placement(&spec, &assignment)
        .unwrap();
    assert_eq!(coordinator.snapshot().environments()[&spec.id].spec, spec);
    assert_ne!(placement_snapshot.environments()[&spec.id].spec, spec);
    assert_eq!(coordinator.node_resources("target").unwrap(), reserved);
    assert_eq!(
        coordinator.shards[0].assignment(&spec.id),
        Some(&assignment)
    );
    coordinator.release(&assignment).unwrap();
    assert_eq!(
        coordinator.node_resources("target").unwrap().1,
        spec.resources
    );
}

#[test]
fn recovery_placement_rejects_changed_identity_without_mutating_reservations() {
    let (mut coordinator, spec, assignment) = placed_recovery();
    let reserved = coordinator.node_resources("target").unwrap();
    let snapshot = coordinator.snapshot();
    let mut changed_spec = spec.clone();
    changed_spec.resources.cpu_millis += 1;
    assert_eq!(
        coordinator.finish_recovery_placement(&changed_spec, &assignment),
        Err(Error::Conflict)
    );
    changed_spec = spec.clone();
    changed_spec.image = "different-image".into();
    assert_eq!(
        coordinator.finish_recovery_placement(&changed_spec, &assignment),
        Err(Error::Conflict)
    );
    let mut changed_assignment = assignment.clone();
    changed_assignment.generation += 1;
    assert_eq!(
        coordinator.finish_recovery_placement(&spec, &changed_assignment),
        Err(Error::Conflict)
    );
    assert_eq!(coordinator.snapshot().revision, snapshot.revision);
    assert_eq!(coordinator.node_resources("target").unwrap(), reserved);
    assert_eq!(
        coordinator.shards[0].assignment(&spec.id),
        Some(&assignment)
    );
    // Generic claims still require exact spec equality; only a committed
    // recovery placement can remove its temporary selection constraints.
    assert_eq!(
        coordinator.accept_claim(&spec, &assignment),
        Err(Error::Conflict)
    );
}
