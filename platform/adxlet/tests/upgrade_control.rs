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
    assert!(gate.apply(control("first", 1, "canary")).await.is_err());
    assert!(gate
        .check("default-upgrade-canary-user", 2, "create")
        .is_ok());
    let mut granted = control("first", 1, "evacuating");
    granted.grants.push(ExecutionGrant {
        environment_id: "business".into(),
        assignment_generation: 2,
        actions: vec!["pause".into()],
    });
    let mut unversioned = granted.clone();
    unversioned.grants[0].assignment_generation = 0;
    assert!(gate.apply(unversioned).await.is_err());
    gate.apply(granted.clone()).await.unwrap();
    for (id, generation, action) in [
        ("business", 2, "create"),
        ("business", 1, "pause"),
        ("other", 2, "pause"),
    ] {
        assert!(gate.check(id, generation, action).is_err());
    }
    let accepted = gate.enter("business", 2, "pause").await.unwrap();
    granted.grants.clear();
    assert!(gate.apply(granted).await.is_err());
    let mut closing = Box::pin(gate.apply(control("first", 2, "quiescing")));
    assert!(futures_util::poll!(closing.as_mut()).is_pending());
    assert!(gate.check("business", 2, "pause").is_err());
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
