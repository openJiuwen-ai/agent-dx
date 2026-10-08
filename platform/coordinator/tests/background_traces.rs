mod common;

use adx_coordinator::{rpc::CoordinatorRpc, Placement};
use adx_core::{EnvironmentSpec, Resources};
use adx_protocol::{
    auth::{Peers, Principal},
    control::{self as pb, coordinator_service_server::CoordinatorService},
};
use adx_transport::rpc::RpcClient;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::time::Duration;
use tonic::{metadata::MetadataValue, Request};

fn node_request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    request
        .metadata_mut()
        .insert_bin("adx-node-id-bin", MetadataValue::from_bytes(b"node"));
    request
}

#[tokio::test]
#[ignore = "requires isolated Redis; run control-rpc suite"]
async fn heartbeats_and_scheduler_rounds_do_not_export_spans_but_create_keeps_its_trace() {
    let mut redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session,
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());

    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: "127.0.0.1:9001".into(),
        proxy_address: "127.0.0.1:9002".into(),
        capacity: Some(
            Resources {
                cpu_millis: 100,
                memory_bytes: 1024,
                disk_bytes: 0,
            }
            .into(),
        ),
        session_id: "boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        ..Default::default()
    };
    rpc.register_node(node_request(report.clone()))
        .await
        .unwrap();
    rpc.inspect_node(node_request(pb::InspectNodeRequest {
        node_id: "node".into(),
        session_id: "boot".into(),
    }))
    .await
    .unwrap();
    report.reconciling = false;
    report.accepting_allocations = true;
    for sequence in 2..=4 {
        report.heartbeat_sequence = sequence;
        assert_eq!(
            rpc.register_node(node_request(report.clone()))
                .await
                .unwrap()
                .into_inner()
                .shard_id,
            0
        );
    }

    // Insufficient resources force real queue/round execution and queue expiry.
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id": "queued", "tenant_id": "tenant", "image": "image",
        "runtime_class": "runsc", "priority": 0,
        "resources": {"cpu_millis": 200, "memory_bytes": 128, "disk_bytes": 0}
    }))
    .unwrap();
    let mut request = Request::new(pb::CreateEnvironmentRequest {
        spec: Some(spec.into()),
        caller: Some(pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        }),
        schedule_timeout_seconds: 1,
        create_timeout_seconds: 0,
    });
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    request.metadata_mut().insert(
        "traceparent",
        "00-11111111111111111111111111111111-2222222222222222-01"
            .parse()
            .unwrap(),
    );
    let result = rpc.create_environment(request).await.unwrap_err();
    assert_eq!(result.code(), tonic::Code::DeadlineExceeded);
    assert!(rpc
        .metrics()
        .await
        .unwrap()
        .contains("adx_coordinator_queued_requests{shard_id=\"0\"} 0\n"));
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert!(
        !spans.iter().any(|s| s.name == "coordinator.register_node"),
        "heartbeats must not produce trace traffic"
    );
    assert!(
        !spans.iter().any(|s| s.name == "shard.schedule_round"),
        "internal rounds must not produce trace traffic"
    );
    let create = spans
        .iter()
        .find(|s| s.name == "coordinator.create_environment")
        .expect("user request span retained");
    assert_eq!(
        create.span_context.trace_id().to_string(),
        "11111111111111111111111111111111"
    );
    assert_eq!(create.parent_span_id.to_string(), "2222222222222222");
    assert!(spans.iter().any(|s| s.name == "coordinator.create"));
    provider.shutdown().unwrap();
    redis.crash();
}
