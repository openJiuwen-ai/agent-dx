use adxlet::upgrade::{ExecutionGrant, NodeControl, UpgradeGate};

fn control(session: &str, revision: u64, phase: &str) -> NodeControl {
    NodeControl {
        task_id: "task".into(),
        epoch: 1,
        node_session_id: session.into(),
        revision,
        phase: phase.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn upgrade_authority_fences_scope_revision_session_and_accepted_work() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("upgrade.json");
    let gate = UpgradeGate::open(path.clone(), "first".into()).unwrap();
    let mut granted = control("first", 1, "canary");
    granted.grants.push(ExecutionGrant {
        environment_id: "default-upgrade-canary-a".into(),
        assignment_generation: 2,
        actions: vec!["create".into()],
    });
    gate.apply(granted.clone()).await.unwrap();
    for (id, generation, action) in [
        ("business", 2, "create"),
        ("default-upgrade-canary-a", 1, "create"),
        ("default-upgrade-canary-a", 2, "pause"),
    ] {
        assert!(gate.check(id, generation, action).is_err());
    }
    let accepted = gate
        .enter("default-upgrade-canary-a", 2, "create")
        .await
        .unwrap();
    granted.grants.clear();
    assert!(gate.apply(granted).await.is_err());
    let mut closing = Box::pin(gate.apply(control("first", 2, "quiescing")));
    assert!(futures_util::poll!(closing.as_mut()).is_pending());
    assert!(gate.check("default-upgrade-canary-a", 2, "create").is_err());
    drop(accepted);
    closing.await.unwrap();
    let restarted = UpgradeGate::open(path, "second".into()).unwrap();
    assert!(restarted.may_have_executed().unwrap());
    assert_eq!(restarted.control().unwrap().unwrap().phase, "isolated");
    assert!(restarted.control().unwrap().unwrap().grants.is_empty());
    assert!(restarted
        .apply(control("first", 3, "verified"))
        .await
        .is_err());
}
