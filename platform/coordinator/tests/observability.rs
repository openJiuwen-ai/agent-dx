mod common;
#[path = "../../../build/observability/tests/prometheus.rs"]
mod prometheus;
use adx_coordinator::{
    rpc::CoordinatorRpc,
    storage::{NodeSession, RedisStore},
    Node, Placement,
};
use adx_core::observation::{
    EvidenceSource, ExecutionObservation, InterruptionEvidence, InterruptionReason,
};
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources};
use std::time::Duration;

fn spec(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        envd: None,
        compatibility: None,
        run_policy: None,
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runc".into(),
        runtime_profile: None,
        snapshot_id: None,
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        lifecycle: Default::default(),
        env: Default::default(),
        priority: 0,
        scheduling: Default::default(),
        execution: Default::default(),
    }
}
fn record(id: &str, generation: u64) -> EnvironmentRecord {
    let mut spec = spec(id);
    spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    EnvironmentRecord {
        observation: None,
        spec,
        assignment: Assignment {
            environment_id: id.into(),
            node_id: "node".into(),
            shard_id: 0,
            generation,
            devices: vec![],
        },
        state: EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: format!("{id}-{generation}"),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        resources_held: true,
        checkpoint: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        last_operation: None,
    }
}
async fn coordinator(store: &RedisStore) -> CoordinatorRpc {
    CoordinatorRpc::new(
        store.begin(1).await.unwrap(),
        Placement::Pack,
        adx_protocol::auth::Peers::new(
            std::iter::empty::<(Vec<u8>, adx_protocol::auth::Principal)>(),
        ),
        tonic::transport::ClientTlsConfig::new(),
        Duration::from_secs(2),
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires native Redis, Prometheus and evidence directory"]
async fn private_metrics_and_latest_impact_are_collected_by_prometheus() {
    let mut redis = common::Redis::new().await;
    let store = redis.store().await;
    let session = store.begin(1).await.unwrap();
    session
        .register_session(
            Node {
                id: "node".into(),
                capacity: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 4096,
                    disk_bytes: 0,
                },
                available: true,
                labels: Default::default(),
                devices: vec![],
                runtime_classes: vec!["runc".into()],
            },
            "node:9000".into(),
            "node:9001".into(),
            Some(NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let mut first = record("a", 1);
    session
        .reserve_with_request(first.spec.clone(), first.assignment.clone(), "request-a")
        .await
        .unwrap();
    session.commit(first.clone()).await.unwrap();
    let evidence = InterruptionEvidence {
        event_id: "execution:1:a-1".into(),
        detected_at_unix_millis: adx_core::observation::now_millis().unwrap(),
        assignment_generation: 1,
        runtime_id: "a-1".into(),
        worker_id: "node".into(),
        reason: InterruptionReason::Oom,
        evidence_source: EvidenceSource::BackendExit,
        exit_code: Some(137),
        detail: "backend OOM evidence".into(),
    };
    first.observation = Some(ExecutionObservation {
        interruption: Some(evidence),
        ..Default::default()
    });
    first.state = EnvironmentState::Failed;
    first.revision = 3;
    first.restart_pending = true;
    first.runtime.ip = None;
    session.commit(first.clone()).await.unwrap();
    first.state = EnvironmentState::Running;
    first.revision = 5;
    first.restart_pending = false;
    first.restart_attempts = 1;
    first.runtime.id = "a-1-r1".into();
    first.runtime.ip = Some("10.0.0.2".parse().unwrap());
    session.commit(first).await.unwrap();
    let second = record("b", 2);
    session
        .reserve_with_request(second.spec.clone(), second.assignment.clone(), "request-b")
        .await
        .unwrap();
    session.commit(second).await.unwrap();
    let mut operating = record("operation", 3);
    session
        .reserve(operating.spec.clone(), operating.assignment.clone())
        .await
        .unwrap();
    session.commit(operating.clone()).await.unwrap();
    operating.revision += 1;
    operating.observation = Some(ExecutionObservation {
        intent: Some(adx_core::observation::OperationIntent::User),
        ..Default::default()
    });
    session.commit(operating).await.unwrap();
    session.invalidate_node("node", "boot").await.unwrap();
    session.invalidate_node("node", "boot").await.unwrap();
    let rpc = coordinator(&store).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let lease = adx_coordinator::lease::CoordinatorLease::new(
        &redis.url,
        "observability-test",
        std::time::Duration::from_secs(3),
    )
    .unwrap();
    let server = tokio::spawn(adx_coordinator::metrics::serve(listener, rpc.clone(), lease));
    let mut collector = prometheus::Collector::start("coordinator", address).await;
    collector
        .wait_value(
            "adx_environment_owned_creation_results_total{outcome=\"success\"}",
            3.0,
        )
        .await;
    collector
        .wait_value("sum(adx_environment_interruptions_total)", 3.0)
        .await;
    collector
        .wait_value("sum(adx_worker_incidents_total)", 1.0)
        .await;
    collector
        .wait_value(
            "adx_environment_recovery_results_total{result=\"recovered\"}",
            1.0,
        )
        .await;
    collector
        .wait_value("sum(adx_environment_unrecovered)", 2.0)
        .await;
    collector
        .wait_value("adx_environment_operation_unknown", 1.0)
        .await;
    let unknown = rpc
        .interruption_details("operation_unknown=true")
        .await
        .unwrap();
    assert_eq!(unknown["total_environments"], 1);
    assert_eq!(unknown["items"][0]["environment_id"], "operation");
    assert_eq!(unknown["coverage"], "current_env_operation_unknown");
    let impact = rpc.interruption_details("limit=1").await.unwrap();
    assert_eq!(impact["coverage"], "current_env_latest_interruption");
    assert_eq!(impact["total_environments"], 2);
    assert_eq!(impact["total_workers"], 1);
    assert_eq!(impact["items"].as_array().unwrap().len(), 1);
    let cursor = impact["next_cursor"].as_str().unwrap();
    let next = rpc
        .interruption_details(&format!("cursor={cursor}&limit=1"))
        .await
        .unwrap();
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_ne!(
        impact["items"][0]["environment_id"],
        next["items"][0]["environment_id"]
    );
    assert_eq!(
        rpc.interruption_details("reason=oom&include_resolved=true")
            .await
            .unwrap()["total_environments"],
        0
    );
    assert!(rpc
        .interruption_details("reason=arbitrary-high-cardinality")
        .await
        .is_err());
    assert!(rpc.interruption_details("limit=1001").await.is_err());
    let output = tokio::process::Command::new("curl")
        .args(["--silent", "--show-error", "--fail"])
        .arg(format!(
            "http://{address}/observability/environments?limit=1"
        ))
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let queried: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(queried["total_environments"], 2);
    let evidence =
        std::path::PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap()).join("coordinator");
    std::fs::write(
        evidence.join("impact.json"),
        serde_json::to_vec_pretty(&queried).unwrap(),
    )
    .unwrap();
    redis.crash();
    collector
        .wait_value("adx_observability_snapshot_age_seconds > bool 2", 1.0)
        .await;
    collector
        .wait_value("sum(adx_environment_interruptions_total)", 3.0)
        .await;
    redis.start().await;
    collector
        .wait_value("adx_observability_snapshot_age_seconds < bool 2", 1.0)
        .await;
    collector.capture().await;
    let replacement = coordinator(&store).await;
    assert!(replacement
        .metrics()
        .await
        .unwrap()
        .contains("adx_environment_owned_creation_results_total{outcome=\"success\"} 3\n"));
    collector.wait_value("up{job=\"coordinator\"}", 0.0).await;
    assert!(rpc.metrics().await.is_err());
    server.abort();
}
