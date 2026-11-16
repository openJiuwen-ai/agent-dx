//! Replay capacity is a cache budget, never a create admission limit.
#[path = "support/e2b_create_errors.rs"]
mod e2b_create_errors;
#[path = "support/http.rs"]
mod http;

use adx_apiserver::{
    clients::Clients, config::Config, contract, environment_service::EnvironmentService, http::Api,
};
use adx_protocol::control as pb;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
use tonic::{Request, Response, Status};

include!("support/create_fixture.rs");

#[tokio::test]
async fn snapshot_create_uses_coordinator_even_with_a_live_local_first_entry() {
    let fixture = Fixture::with_mode(600, true).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.clients.nodes().await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        fixture.clients.nodes().await.unwrap()[0].node_id,
        "arbitrary-entry"
    );
    let mut spec = spec("snapshot-clone");
    spec.snapshot_id = Some("snapshot".into());
    let request = pb::CreateEnvironmentRequest {
        request_id: String::new(),
        spec: Some(spec),
        caller: Some(caller()),
        ..Default::default()
    };
    let (first, replay) = tokio::join!(
        fixture
            .clients
            .create_environment(request.clone(), Duration::from_secs(2)),
        fixture
            .clients
            .create_environment(request, Duration::from_secs(2)),
    );
    let ((first, _), (replay, _)) = (first.unwrap(), replay.unwrap());
    assert_eq!(first.record, replay.record);
    assert_eq!(first.durability, replay.durability);
    assert_ne!(first.newly_created, replay.newly_created);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cancelled_schedule_is_cached_without_resubmission_or_runtime_start() {
    let mut fixture = Fixture::new().await;
    // Keep all three request results resident; other fixture tests intentionally use capacity 1.
    let mut config = fixture.clients.config.clone();
    config.cache_entries = 8;
    fixture.clients = Clients::new(config).unwrap();
    fixture.service = EnvironmentService::new(fixture.clients.clone());
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture
            .service
            .inspect("tenant", "not-created")
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    fixture
        .state
        .scheduling_expired
        .store(true, Ordering::SeqCst);
    let error = fixture
        .create("too-large", "create-too-large")
        .await
        .unwrap_err();
    assert!(adx_protocol::scheduling_failure(&error).is_some());
    let replay = fixture
        .create("too-large", "create-too-large")
        .await
        .unwrap_err();
    assert_eq!(replay.details(), error.details());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert!(fixture.state.owners.lock().unwrap().is_empty());
    // A deliberate new logical request may wait for placement again.
    let error = fixture
        .create("too-large", "create-new-budget")
        .await
        .unwrap_err();
    assert!(adx_protocol::scheduling_failure(&error).is_some());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
    fixture
        .state
        .scheduling_expired
        .store(false, Ordering::SeqCst);
    let created = fixture
        .create("too-large", "placement-returned")
        .await
        .unwrap();
    assert_eq!(created["environmentId"], "too-large");
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    // The old request's result remains immutable even after a new attempt succeeds.
    let old = fixture
        .create("too-large", "create-too-large")
        .await
        .unwrap_err();
    assert!(adx_protocol::scheduling_failure(&old).is_some());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
}

#[tokio::test]
async fn create_response_preserves_authoritative_creation_ownership() {
    let fixture = Fixture::new().await;
    let first = fixture.create("owned", "owner-request").await.unwrap();
    assert_eq!(first["newlyCreated"], true);
    let retry = fixture.create("owned", "owner-request").await.unwrap();
    assert_eq!(retry["newlyCreated"], true);
    let reuse = fixture.create("owned", "another-request").await.unwrap();
    assert_eq!(reuse["newlyCreated"], false);
    assert_eq!(reuse["requestId"], "owner-request");
    assert_eq!(first["environmentId"], reuse["environmentId"]);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cached_validation_failure_recovers_after_service_replacement_with_original_identity() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .response_error
        .store(tonic::Code::FailedPrecondition as usize, Ordering::SeqCst);
    let first = fixture.create("started", "original").await.unwrap_err();
    assert_eq!(first.code(), tonic::Code::FailedPrecondition);
    fixture.state.response_error.store(0, Ordering::SeqCst);
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    let cached = fixture.create("started", "original").await.unwrap_err();
    assert_eq!(cached.message(), first.message());
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
    let replacement = EnvironmentService::new(fixture.clients.clone());
    let recovered = replacement
        .create(
            spec("started"),
            json!({"name":"started"}),
            "original",
            &caller(),
        )
        .await
        .unwrap();
    assert_eq!(recovered["environmentId"], "started");
    assert_eq!(recovered["requestId"], "original");
    assert_eq!(recovered["newlyCreated"], false);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cached_create_tracks_current_state_and_receipt_target_without_extra_calls() {
    let fixture = Fixture::new().await;
    fixture.create("reused", "original").await.unwrap();
    let mut owner = fixture.state.owners.lock().unwrap()["reused"].clone();
    owner
        .record
        .as_mut()
        .unwrap()
        .assignment
        .as_mut()
        .unwrap()
        .generation += 1;
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    let lookups = fixture.state.lookups.load(Ordering::SeqCst);
    for (state, restart_pending, error) in [
        (
            pb::EnvironmentState::Paused,
            false,
            Some(tonic::Code::Unavailable),
        ),
        (
            pb::EnvironmentState::Failed,
            true,
            Some(tonic::Code::Unavailable),
        ),
        (pb::EnvironmentState::Running, false, None),
        (
            pb::EnvironmentState::Failed,
            false,
            Some(tonic::Code::FailedPrecondition),
        ),
        (
            pb::EnvironmentState::Deleted,
            false,
            Some(tonic::Code::FailedPrecondition),
        ),
    ] {
        let record = owner.record.as_mut().unwrap();
        record.state = state as i32;
        record.restart_pending = restart_pending;
        record.resources_held = state != pb::EnvironmentState::Deleted;
        record.revision += 1;
        fixture.clients.put_owner(owner.clone()).await.unwrap();
        let result = fixture.create("reused", "original").await;
        if let Some(code) = error {
            assert_eq!(result.unwrap_err().code(), code);
        } else {
            let response = result.unwrap();
            assert_eq!(response["requestId"], "original");
            assert_eq!(response["newlyCreated"], true);
        }
    }
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), lookups);
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .insert("reused".into(), owner);
    let peer = EnvironmentService::new(fixture.clients.clone());
    assert_eq!(
        peer.create(
            spec("other-name"),
            json!({"name":"other-name"}),
            "original",
            &caller()
        )
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    fixture.state.owners.lock().unwrap().remove("reused");
    let creator = EnvironmentService::new(fixture.clients.clone());
    creator
        .create(
            spec("reused"),
            json!({"name":"reused"}),
            "replacement",
            &caller(),
        )
        .await
        .unwrap();
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    let lookups = fixture.state.lookups.load(Ordering::SeqCst);
    for (service, name) in [(&fixture.service, "reused"), (&peer, "other-name")] {
        let response = service
            .create(spec(name), json!({"name":name}), "original", &caller())
            .await
            .unwrap();
        assert_eq!(response["environmentId"], "reused");
        assert_eq!(response["requestId"], "replacement");
        assert_eq!(response["newlyCreated"], false);
    }
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), lookups);
}

#[tokio::test]
async fn concurrent_names_and_api_restart_do_not_transfer_creation_ownership() {
    for local_first in [false, true] {
        let fixture = Fixture::with_mode(600, local_first).await;
        if local_first {
            tokio::time::timeout(Duration::from_secs(2), async {
                while fixture.clients.nodes().await.is_err() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        let (a, b) = tokio::join!(
            fixture.create("shared-owner", "request-a"),
            fixture.create("shared-owner", "request-b"),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_ne!(a["newlyCreated"], b["newlyCreated"]);
        assert_eq!(a["environmentId"], b["environmentId"]);
        let replica = EnvironmentService::new(fixture.clients.clone());
        let recovered = replica
            .create(
                spec("shared-owner"),
                json!({"name":"shared-owner"}),
                "request-a",
                &caller(),
            )
            .await
            .unwrap();
        assert_eq!(recovered["newlyCreated"], false);
        assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn creation_metrics_wait_for_result_and_count_replay_as_a_response() {
    let fixture = Fixture::new().await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let pending = tokio::spawn(async move {
        service
            .create(
                spec("observed"),
                json!({"name":"observed"}),
                "observed",
                &caller(),
            )
            .await
    });
    fixture.wait_starts(1).await;
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 1\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"success\"} 0\n"));
    fixture.state.block_response.store(false, Ordering::SeqCst);
    fixture.state.release.notify_one();
    pending.await.unwrap().unwrap();
    fixture.create("observed", "observed").await.unwrap();
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 0\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"success\"} 2\n"));
    assert!(
        metrics.contains("adx_environment_create_success_call_duration_seconds_count 1\n"),
        "{metrics}"
    );
    fixture.create("observed", "different-call").await.unwrap();
    assert!(fixture
        .service
        .metrics()
        .contains("adx_environment_create_success_call_duration_seconds_count 1\n"));
    assert!(!metrics.contains("environment_id="));
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn private_metrics_include_rejected_http_arrivals_without_reporting_success() {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::new().await;
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api_service = api.clone();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = api_service.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| api.clone().serve(request));
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await
                    .unwrap();
            });
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let monitoring = listener.local_addr().unwrap();
    let metrics = tokio::spawn(adx_apiserver::metrics::serve(
        listener,
        api.environment_service.clone(),
    ));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for path in [
        "/api/environment/v1/environments",
        "/api/environment/create/",
        "/api/%65nvironment/v1/environments",
    ] {
        let response = client
            .post(format!("http://{address}{path}"))
            .body("{}")
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success());
    }
    let text = client
        .get(format!("http://{monitoring}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(text.contains("adx_environment_create_requests_total{source=\"http\"} 3\n"));
    assert!(
        text.contains("adx_environment_create_rejections_total{reason=\"unauthenticated\"} 3\n"),
        "{text}"
    );
    assert!(text.contains("adx_environment_create_responses_total{outcome=\"success\"} 0\n"));
    assert!(text.contains("adx_environment_create_in_flight 0\n"));
    assert_eq!(
        client
            .post(format!("http://{monitoring}/metrics"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    http.abort();
    metrics.abort();
}

#[tokio::test]
async fn confirmed_timeout_survives_rpc_timers_and_replay() {
    let phase = "backend preparation/runtime start";
    let fixture = Fixture::new().await;
    *fixture.state.timeout_phase.lock().unwrap() = Some(phase.into());
    let input = json!({"name":"phase-timeout", "createTimeoutSeconds":35, "scheduleTimeoutSeconds":1, "initCallTimeoutSeconds":1});
    let start = tokio::time::Instant::now();
    let error = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input.clone(),
        "create-phase",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{error:?}");
    assert!(error.message().contains(phase), "{error:?}");
    assert!(adx_protocol::deadline_failure(&error).is_some());
    assert!(start.elapsed() >= Duration::from_secs(35));
    assert!(start.elapsed() < Duration::from_secs(38));
    let replay = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input.clone(),
        "create-phase",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(replay.message(), error.message());
    // A different request ID must recover the same persisted failure too.
    let published = fixture.state.owners.lock().unwrap()["phase-timeout"].clone();
    fixture.clients.put_owner(published).await.unwrap();
    let replay = Box::pin(fixture.service.create(
        spec("phase-timeout"),
        input,
        "create-other",
        &caller(),
    ))
    .await
    .unwrap_err();
    assert_eq!(replay.message(), error.message());
    assert!(adx_protocol::deadline_failure(&replay).is_some());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn completed_cache_capacity_never_rejects_new_creates() {
    let fixture = Fixture::new().await;
    for index in 0..12 {
        let id = format!("environment-{index}");
        let result = fixture
            .create(&id, &format!("request-{index}"))
            .await
            .unwrap();
        assert_eq!(result["environmentId"], id);
    }
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
    // The first response was evicted: current ownership still converges to it.
    assert_eq!(
        fixture.create("environment-0", "request-0").await.unwrap()["environmentId"],
        "environment-0"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 12);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn e2b_create_through_http_listener_completes_on_default_worker_stack() {
    let fixture = Fixture::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api = fixture.api.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(socket),
                service_fn(move |request| api.clone().serve(request)),
            )
            .await
            .unwrap();
    });
    let request = br#"{"templateID":"base","envVars":{"REQUEST_VALUE":"ok"}}"#;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST /v2/sandboxes HTTP/1.1\r\nHost: localhost\r\nX-API-Key: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        "k".repeat(32),
        request.len(),
        std::str::from_utf8(request).unwrap(),
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    server.await.unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 201"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(
        *fixture.state.snapshot_requests.lock().unwrap(),
        vec![("tenant".into(), "base-snapshot".into())]
    );
    let owner = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let spec = owner.record.unwrap().spec.unwrap();
    assert_eq!(
        spec.image,
        format!("registry.example/base@sha256:{}", "a".repeat(64))
    );
    assert_eq!(spec.snapshot_id.as_deref(), Some("base-snapshot"));
    assert_eq!(spec.compatibility.as_ref().unwrap().template_id, FAMILY_ID);
    assert_eq!(spec.env["SNAPSHOT_SOURCE"], "true");
    assert_eq!(spec.env["REQUEST_VALUE"], "ok");
    assert_eq!(spec.resources.as_ref().unwrap().cpu_millis, 1250);
    let execution = spec.execution.as_ref().unwrap();
    assert!(execution.inherit_entrypoint);
    assert_eq!(execution.ports, vec![8080]);
    assert_eq!(execution.limits.as_ref().unwrap().cpu_millis, 1500);
    assert_eq!(
        execution.limits.as_ref().unwrap().memory_bytes,
        1024 * 1_048_576
    );
    assert_eq!(execution.init_timeout_seconds, 30);
    assert!(execution.readiness_probe.is_some());
    assert_eq!(spec.lifecycle.as_ref().unwrap().idle_timeout_seconds, 0);
    let policy = spec.run_policy.as_ref().unwrap();
    assert_eq!(policy.timeout_seconds, 300);
    assert_eq!(policy.action, pb::RunExpiryAction::Delete as i32);
    assert_eq!(
        spec.scheduling.unwrap().required_node[0].match_labels["adx.runtime.profile"],
        "e2b-v1"
    );
    assert!(spec.runtime_profile.is_some());
    assert!(spec.envd.as_ref().unwrap().gateway_auth);
    assert!(spec.envd.as_ref().unwrap().access_token.is_none());
    assert_eq!(execution.private_service_ports, vec![49983]);
    assert_eq!(execution.protected_ingress.len(), 2);
    assert_eq!(fixture.state.preparation_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_sandbox_create_route_reuses_snapshot_source_create() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/sandboxes",
        &headers,
        r#"{"templateID":"base","secure":true}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        *fixture.state.snapshot_requests.lock().unwrap(),
        vec![("tenant".into(), "base-snapshot".into())]
    );
}

#[tokio::test]
async fn e2b_secure_false_rejects_before_template_or_create() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/sandboxes",
        &headers,
        r#"{"templateID":"base","secure":false}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(
        serde_json::from_slice::<Value>(response.body()).unwrap()["error_code"],
        "capability_not_supported"
    );
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn template_detail_reuses_owned_preparations_and_exposes_real_resources() {
    let fixture = Fixture::new().await;
    let path = format!("/templates/{FAMILY_ID}");
    let response = http::request(
        fixture.api.clone(),
        "GET",
        &path,
        &[("X-API-Key", "k".repeat(32))],
        "",
    )
    .await;
    assert_eq!(
        response.status(),
        200,
        "{}",
        String::from_utf8_lossy(response.body())
    );
    let detail: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(detail["templateID"], FAMILY_ID);
    assert_eq!(detail["buildID"], BUILD_ID);
    assert_eq!(detail["createdAt"], "1970-01-01T00:00:00.001Z");
    assert_eq!(detail["updatedAt"], "1970-01-01T00:00:00.005Z");
    assert_eq!(detail["spawnCount"], 2);
    assert_eq!(detail["lastSpawnedAt"], "1970-01-01T00:00:00.005Z");
    assert_eq!(detail["builds"].as_array().unwrap().len(), 1);
    assert_eq!(detail["builds"][0]["status"], "ready");
    assert_eq!(detail["builds"][0]["memoryMB"], 768);
    assert_eq!(detail["builds"][0]["envdVersion"], "0.9.0");
    assert_eq!(detail["builds"][0]["createdAt"], "1970-01-01T00:00:00.002Z");
    assert_eq!(detail["builds"][0]["updatedAt"], "1970-01-01T00:00:00.003Z");
    assert!(detail.get("nextToken").is_none());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    for (headers, expected) in [
        (vec![], 401),
        (
            vec![("X-API-Key", format!("other-{}", "k".repeat(32)))],
            404,
        ),
    ] {
        let response = http::request(fixture.api.clone(), "GET", &path, &headers, "").await;
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
async fn template_detail_pages_builds_through_response_headers() {
    const CURRENT_ID: &str = "550e8400-e29b-41d4-a716-446655440012";
    let fixture = Fixture::new().await;
    *fixture.state.current_preparation.lock().unwrap() = Some(pb::SnapshotPreparationView {
        template_id: FAMILY_ID.into(),
        preparation_id: CURRENT_ID.into(),
        name: "base".into(),
        phase: pb::SnapshotPreparationPhase::Failed as i32,
        resources: Some(pb::Resources {
            cpu_millis: 4000,
            memory_bytes: 1024 * 1_048_576,
            ..Default::default()
        }),
        created_at_unix_ms: 1,
        template_updated_at_unix_ms: 8,
        preparation_created_at_unix_ms: 6,
        preparation_updated_at_unix_ms: 8,
        last_spawned_at_unix_ms: Some(5),
        spawn_count: Some(2),
        ..Default::default()
    });
    let headers = [("X-API-Key", "k".repeat(32))];
    let first = http::request(
        fixture.api.clone(),
        "GET",
        &format!("/templates/{FAMILY_ID}?limit=1"),
        &headers,
        "",
    )
    .await;
    assert_eq!(first.status(), 200);
    let cursor = first.headers()["x-next-token"].to_str().unwrap();
    assert_eq!(cursor, CURRENT_ID);
    let first_body: Value = serde_json::from_slice(first.body()).unwrap();
    assert!(first_body.get("nextToken").is_none());
    assert_eq!(first_body["buildID"], BUILD_ID);
    assert_eq!(first_body["buildStatus"], "ready");
    assert_eq!(first_body["createdAt"], "1970-01-01T00:00:00.001Z");
    assert_eq!(first_body["updatedAt"], "1970-01-01T00:00:00.008Z");
    assert_eq!(first_body["spawnCount"], 2);
    assert_eq!(first_body["lastSpawnedAt"], "1970-01-01T00:00:00.005Z");
    assert_eq!(first_body["builds"].as_array().unwrap().len(), 1);
    assert_eq!(first_body["builds"][0]["buildID"], CURRENT_ID);
    assert_eq!(first_body["builds"][0]["status"], "error");
    assert_eq!(
        first_body["builds"][0]["createdAt"],
        "1970-01-01T00:00:00.006Z"
    );
    assert_eq!(
        first_body["builds"][0]["updatedAt"],
        "1970-01-01T00:00:00.008Z"
    );

    let second = http::request(
        fixture.api.clone(),
        "GET",
        &format!("/templates/{FAMILY_ID}?limit=1&nextToken={cursor}"),
        &headers,
        "",
    )
    .await;
    assert_eq!(second.status(), 200);
    assert!(second.headers().get("x-next-token").is_none());
    let second_body: Value = serde_json::from_slice(second.body()).unwrap();
    assert!(second_body.get("nextToken").is_none());
    assert_eq!(second_body["buildStatus"], "ready");
    assert_eq!(second_body["updatedAt"], "1970-01-01T00:00:00.008Z");
    assert_eq!(second_body["spawnCount"], 2);
    assert_eq!(second_body["lastSpawnedAt"], "1970-01-01T00:00:00.005Z");
    assert_eq!(second_body["builds"].as_array().unwrap().len(), 1);
    assert_eq!(second_body["builds"][0]["buildID"], BUILD_ID);
    assert_eq!(second_body["builds"][0]["status"], "ready");
    assert_eq!(
        second_body["builds"][0]["createdAt"],
        "1970-01-01T00:00:00.002Z"
    );
    assert_eq!(
        second_body["builds"][0]["updatedAt"],
        "1970-01-01T00:00:00.003Z"
    );
}

#[tokio::test]
async fn template_detail_distinguishes_unused_templates_from_unknown_history() {
    let fixture = Fixture::new().await;
    let current = pb::SnapshotPreparationView {
        template_id: FAMILY_ID.into(),
        preparation_id: BUILD_ID.into(),
        name: "base".into(),
        phase: pb::SnapshotPreparationPhase::Registered as i32,
        resources: Some(pb::Resources {
            cpu_millis: 2000,
            memory_bytes: 768 * 1_048_576,
            ..Default::default()
        }),
        created_at_unix_ms: 1,
        template_updated_at_unix_ms: 1,
        preparation_created_at_unix_ms: 1,
        preparation_updated_at_unix_ms: 1,
        spawn_count: Some(0),
        ..Default::default()
    };
    *fixture.state.current_preparation.lock().unwrap() = Some(current.clone());
    *fixture.state.snapshot.lock().unwrap() = None;
    let path = format!("/templates/{FAMILY_ID}");
    let headers = [("X-API-Key", "k".repeat(32))];
    let response = http::request(fixture.api.clone(), "GET", &path, &headers, "").await;
    assert_eq!(response.status(), 200);
    let detail: Value = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(detail["buildStatus"], "waiting");
    assert_eq!(detail["createdAt"], "1970-01-01T00:00:00.001Z");
    assert_eq!(detail["updatedAt"], "1970-01-01T00:00:00.001Z");
    assert_eq!(detail["spawnCount"], 0);
    assert!(detail.get("lastSpawnedAt").unwrap().is_null());
    assert_eq!(detail["builds"][0]["createdAt"], "1970-01-01T00:00:00.001Z");
    assert_eq!(detail["builds"][0]["updatedAt"], "1970-01-01T00:00:00.001Z");

    for missing in 0..5 {
        let mut historical = current.clone();
        match missing {
            0 => historical.created_at_unix_ms = 0,
            1 => historical.template_updated_at_unix_ms = 0,
            2 => historical.preparation_created_at_unix_ms = 0,
            3 => historical.preparation_updated_at_unix_ms = 0,
            _ => historical.spawn_count = None,
        }
        *fixture.state.current_preparation.lock().unwrap() = Some(historical);
        let response = http::request(fixture.api.clone(), "GET", &path, &headers, "").await;
        assert_eq!(response.status(), 409, "missing metadata case {missing}");
        assert!(String::from_utf8_lossy(response.body())
            .contains("historical template metadata missing"));
    }
}

#[tokio::test]
async fn template_alias_lookup_is_tenant_scoped_and_read_only() {
    let fixture = Fixture::new().await;
    let tenant = [("X-API-Key", "k".repeat(32))];
    let other_tenant = [("X-API-Key", format!("other-{}", "k".repeat(32)))];
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &[],
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 401);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &tenant,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        serde_json::from_slice::<Value>(response.body()).unwrap(),
        json!({"public": false, "templateID": FAMILY_ID})
    );
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &other_tenant,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *fixture.state.preparation_requests.lock().unwrap(),
        vec![
            ("tenant".into(), "base".into(), true),
            ("other-tenant".into(), "base".into(), true),
        ]
    );
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/unregistered",
        &tenant,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn platform_alias_source_failure_is_not_reported_as_not_found() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .preparation_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &[("X-API-Key", "k".repeat(32))],
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 503);
}

#[tokio::test]
async fn template_registration_authenticates_and_rejects_missing_name() {
    let fixture = Fixture::new().await;
    let response = http::request(fixture.api.clone(), "POST", "/v3/templates", &[], "{}").await;
    assert_eq!(response.status().as_u16(), 401);
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v3/templates",
        &[("X-API-Key", "k".repeat(32))],
        "{}",
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn e2b_create_rejects_control_environment_overrides_before_template_lookup() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let response = crate::http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base","envVars":{"ADX_ENVD_PATH":"injected"}}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn known_family_with_missing_golden_does_not_fallback_to_same_named_raw_snapshot() {
    let fixture = Fixture::new().await;
    // The directory still binds base to base-snapshot; a raw snapshot named base is unrelated.
    fixture.state.snapshot.lock().unwrap().as_mut().unwrap().id = "base".into();
    let headers = [("X-API-Key", "k".repeat(32))];
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &headers,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        serde_json::from_slice::<Value>(response.body()).unwrap()["templateID"],
        FAMILY_ID
    );
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base:default",
        &headers,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 409);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.preparation_calls.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bare_registered_alias_exists_before_ready_while_explicit_tag_does_not() {
    let fixture = Fixture::new().await;
    fixture.state.resolved_snapshot_id.lock().unwrap().clear();
    let headers = [("X-API-Key", "k".repeat(32))];
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base",
        &headers,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        serde_json::from_slice::<Value>(response.body()).unwrap()["templateID"],
        FAMILY_ID
    );
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base:default",
        &headers,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn custom_template_selectors_are_explicitly_unsupported() {
    let fixture = Fixture::new().await;
    let headers = [("X-API-Key", "k".repeat(32))];
    for (reference, expected) in [("base:missing", 400), ("unknown:release", 400)] {
        let response = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &headers,
            &json!({"templateID":reference}).to_string(),
        )
        .await;
        assert_eq!(response.status().as_u16(), expected);
    }
    let response = http::request(
        fixture.api.clone(),
        "GET",
        "/templates/aliases/base:missing",
        &headers,
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn default_selectors_on_family_ids_and_aliases_select_the_registered_snapshot() {
    for reference in [format!("{FAMILY_ID}:default"), "base:default".into()] {
        let fixture = Fixture::new().await;
        let response = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &[("X-API-Key", "k".repeat(32))],
            &json!({"templateID":reference}).to_string(),
        )
        .await;
        assert_eq!(response.status().as_u16(), 201);
        assert_eq!(
            serde_json::from_slice::<Value>(response.body()).unwrap()["templateID"],
            FAMILY_ID
        );
        assert_eq!(
            *fixture.state.snapshot_requests.lock().unwrap(),
            vec![("tenant".into(), "base-snapshot".into())]
        );
    }
    let fixture = Fixture::new().await;
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        &json!({"templateID":format!("base:{BUILD_ID}")}).to_string(),
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.preparation_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn common_family_rebinding_controls_the_next_same_name_sandbox_source() {
    let fixture = Fixture::new().await;
    *fixture.state.resolved_snapshot_id.lock().unwrap() = "rebuilt-golden".into();
    fixture.state.snapshot.lock().unwrap().as_mut().unwrap().id = "rebuilt-golden".into();
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 201);
    let owners = fixture.state.owners.lock().unwrap();
    let created = owners
        .values()
        .next()
        .unwrap()
        .record
        .as_ref()
        .unwrap()
        .spec
        .as_ref()
        .unwrap();
    assert_eq!(created.snapshot_id.as_deref(), Some("rebuilt-golden"));
    assert_eq!(
        created.compatibility.as_ref().unwrap().template_id,
        FAMILY_ID
    );
    assert_eq!(
        *fixture.state.snapshot_requests.lock().unwrap(),
        vec![("tenant".into(), "rebuilt-golden".into())]
    );
}

#[tokio::test]
async fn tenant_own_base_family_resolves_without_a_global_alias() {
    let fixture = Fixture::new().await;
    *fixture.state.registered_tenant.lock().unwrap() = "other-tenant".into();
    fixture
        .state
        .snapshot
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .template
        .as_mut()
        .unwrap()
        .tenant_id = "other-tenant".into();
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", format!("other-{}", "k".repeat(32)))],
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 201);
    let owners = fixture.state.owners.lock().unwrap();
    let created = owners
        .values()
        .next()
        .unwrap()
        .record
        .as_ref()
        .unwrap()
        .spec
        .as_ref()
        .unwrap();
    assert_eq!(created.tenant_id, "other-tenant");
    assert_eq!(
        created.compatibility.as_ref().unwrap().template_id,
        FAMILY_ID
    );
}

#[tokio::test]
async fn unregistered_reference_falls_back_to_snapshot_only_after_preparation_not_found() {
    let fixture = Fixture::new().await;
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        r#"{"templateID":"base-snapshot"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(fixture.state.preparation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preparation_dependency_failure_does_not_fallback_to_an_existing_snapshot() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .preparation_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        r#"{"templateID":"base-snapshot"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 503);
    assert_eq!(fixture.state.preparation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn registered_family_cannot_create_for_a_different_tenant() {
    let fixture = Fixture::new().await;
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", format!("other-{}", "k".repeat(32)))],
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn create_replay_keeps_labels_and_native_name_reuse() {
    let fixture = Fixture::new().await;
    let mut original = spec("metadata-replay");
    original.scheduling = Some(pb::SchedulingPolicy {
        labels: [("purpose".into(), "original".into())].into(),
        ..Default::default()
    });
    fixture
        .service
        .create(
            original.clone(),
            json!({"name":"metadata-replay"}),
            "metadata-request",
            &caller(),
        )
        .await
        .unwrap();
    let mut changed = original;
    changed
        .scheduling
        .as_mut()
        .unwrap()
        .labels
        .insert("purpose".into(), "changed".into());
    fixture
        .service
        .create(
            changed,
            json!({"name":"metadata-replay"}),
            "metadata-request",
            &caller(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.state.owners.lock().unwrap()["metadata-replay"]
            .record
            .as_ref()
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .scheduling
            .as_ref()
            .unwrap()
            .labels["purpose"],
        "original"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);

    fixture
        .create("native-reuse", "native-first")
        .await
        .unwrap();
    let mut changed = spec("native-reuse");
    changed.image = "different-image".into();
    fixture
        .service
        .create(
            changed,
            json!({"name":"native-reuse"}),
            "native-second",
            &caller(),
        )
        .await
        .unwrap();
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn e2b_create_rejects_oversized_metadata_before_any_create_rpc() {
    let fixture = Fixture::new().await;
    let metadata = (0..17)
        .map(|index| (format!("key-{index}"), "x".repeat(4096)))
        .collect::<BTreeMap<_, _>>();
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        &json!({"templateID":"base", "metadata":metadata}).to_string(),
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn legacy_e2b_resume_restores_with_its_own_timeout_contract() {
    for (body, timeout) in [("", 15), (r#"{"timeout":73}"#, 73), (r#"{"timeout":0}"#, 0)] {
        let fixture = Fixture::new().await;
        let headers = [("X-API-Key", "k".repeat(32))];
        let response = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &headers,
            r#"{"templateID":"base"}"#,
        )
        .await;
        assert_eq!(response.status(), 201);
        let created: Value = serde_json::from_slice(response.body()).unwrap();
        let id = created["sandboxID"].as_str().unwrap();
        fixture
            .state
            .owners
            .lock()
            .unwrap()
            .get_mut(id)
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .state = pb::EnvironmentState::Paused as i32;
        fixture
            .state
            .control_owner_unavailable
            .store(true, Ordering::SeqCst);
        let path = format!("/sandboxes/{id}/resume");
        let response = http::request(fixture.api.clone(), "POST", &path, &headers, body).await;
        assert_eq!(
            response.status(),
            201,
            "{}",
            String::from_utf8_lossy(response.body())
        );
        let resumed: Value = serde_json::from_slice(response.body()).unwrap();
        for key in [
            "sandboxID",
            "envdAccessToken",
            "trafficAccessToken",
            "domain",
        ] {
            assert_eq!(resumed[key], created[key], "{key}");
        }
        assert_eq!(resumed["state"], "running");
        {
            let updates = fixture.state.control_updates.lock().unwrap();
            assert_eq!(updates.last().unwrap().timeout_seconds, timeout);
            assert!(!updates.last().unwrap().extend_only);
            let requests = fixture.state.resume_requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].run_timeout_seconds,
                Some(if timeout == 0 { 300 } else { timeout })
            );
        }
        let repeated = http::request(fixture.api.clone(), "POST", &path, &headers, "{}").await;
        assert_eq!(
            repeated.status(),
            409,
            "legacy resume is not running-state Connect"
        );
    }
}

async fn paused_e2b_fixture(auto_pause: bool) -> (Fixture, String) {
    let fixture = Fixture::new().await;
    let created = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &[("X-API-Key", "k".repeat(32))],
        &json!({"templateID":"base","autoPause":auto_pause}).to_string(),
    )
    .await;
    assert_eq!(created.status(), 201);
    let created: Value = serde_json::from_slice(created.body()).unwrap();
    let id = created["sandboxID"].as_str().unwrap().to_owned();
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .record
        .as_mut()
        .unwrap()
        .state = pb::EnvironmentState::Paused as i32;
    (fixture, id)
}

#[tokio::test]
async fn legacy_resume_preserves_expiry_when_response_preparation_fails() {
    for (timeout, missing_resources, expected_status) in
        [(0, false, 503), (0, true, 500), (73, false, 503)]
    {
        let (fixture, id) = paused_e2b_fixture(false).await;
        if missing_resources {
            fixture
                .state
                .missing_resume_resources
                .store(true, Ordering::SeqCst);
        } else {
            fixture
                .state
                .next_control_read_error
                .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
        }
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &format!("/sandboxes/{id}/resume"),
            &[("X-API-Key", "k".repeat(32))],
            &json!({"timeout":timeout}).to_string(),
        )
        .await;
        assert_eq!(response.status(), expected_status);
        let control = fixture_control(&fixture.state, &id, &caller()).unwrap();
        assert_eq!(control.expires_at_unix_ms, timeout * 1000 + 1);
        if timeout == 0 {
            let updates = fixture.state.control_updates.lock().unwrap();
            assert_eq!(updates.last().unwrap().timeout_seconds, 0);
            assert!(!updates.last().unwrap().extend_only);
        }
    }
}

#[tokio::test]
async fn legacy_resume_reports_unconfirmed_expiry_updates_without_repeating_them() {
    for (timeout, error, expected_status) in [
        (0, tonic::Code::Unavailable, 503),
        (0, tonic::Code::DeadlineExceeded, 504),
        (0, tonic::Code::PermissionDenied, 403),
        (73, tonic::Code::Unavailable, 503),
    ] {
        let (fixture, id) = paused_e2b_fixture(false).await;
        fixture
            .state
            .next_control_update_error
            .store(error as usize, Ordering::SeqCst);
        let headers = [("X-API-Key", "k".repeat(32))];
        let path = format!("/sandboxes/{id}/resume");
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &path,
            &headers,
            &json!({"timeout":timeout}).to_string(),
        )
        .await;
        assert_eq!(response.status(), expected_status);
        let control = fixture_control(&fixture.state, &id, &caller()).unwrap();
        let expected_timeout = if timeout == 0 { 300 } else { timeout };
        assert_eq!(control.expires_at_unix_ms, expected_timeout * 1000 + 1);
        let repeated = http::request(fixture.api.clone(), "POST", &path, &headers, "{}").await;
        assert_eq!(repeated.status(), 409);
    }
}

#[tokio::test]
async fn legacy_zero_resume_does_not_claim_success_when_expiry_reply_is_lost() {
    for auto_pause in [false, true] {
        let (fixture, id) = paused_e2b_fixture(auto_pause).await;
        fixture
            .state
            .lose_zero_update_reply
            .store(true, Ordering::SeqCst);
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &format!("/sandboxes/{id}/resume"),
            &[("X-API-Key", "k".repeat(32))],
            r#"{"timeout":0}"#,
        )
        .await;
        assert_eq!(response.status(), 504);
        let control = fixture_control(&fixture.state, &id, &caller()).unwrap();
        assert_eq!(control.expires_at_unix_ms, 1);
        assert_eq!(
            fixture.state.owners.lock().unwrap()[&id]
                .record
                .as_ref()
                .unwrap()
                .state,
            if auto_pause {
                pb::EnvironmentState::Paused
            } else {
                pb::EnvironmentState::Deleted
            } as i32
        );
        assert!(
            fixture.state.staged_run_timeouts.lock().unwrap().is_empty(),
            "lost replies must not expire a later wake"
        );
    }
}

#[tokio::test]
async fn legacy_e2b_resume_rejects_unsupported_options_before_recovery() {
    let fixture = Fixture::new().await;
    let headers = [("X-API-Key", "k".repeat(32))];
    let created = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base"}"#,
    )
    .await;
    let created: Value = serde_json::from_slice(created.body()).unwrap();
    let id = created["sandboxID"].as_str().unwrap();
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .get_mut(id)
        .unwrap()
        .record
        .as_mut()
        .unwrap()
        .state = pb::EnvironmentState::Paused as i32;
    for body in [
        r#"{"memory":false}"#,
        r#"{"autoPause":true}"#,
        r#"{"timeout":86401}"#,
        r#"{"unknown":true}"#,
    ] {
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &format!("/sandboxes/{id}/resume"),
            &headers,
            body,
        )
        .await;
        assert_eq!(
            response.status(),
            400,
            "{body}: {}",
            String::from_utf8_lossy(response.body())
        );
    }
    assert!(fixture.state.resume_requests.lock().unwrap().is_empty());
    assert!(fixture.state.control_updates.lock().unwrap().is_empty());
}

#[tokio::test]
async fn paused_e2b_connect_carries_duration_through_recovery_and_only_extends_afterwards() {
    for unavailable in [false, true] {
        let fixture = Fixture::new().await;
        let headers = [
            ("X-API-Key", "k".repeat(32)),
            ("Content-Type", "application/json".into()),
        ];
        let created = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &headers,
            r#"{"templateID":"base"}"#,
        )
        .await;
        assert_eq!(created.status().as_u16(), 201);
        let created: Value = serde_json::from_slice(created.body()).unwrap();
        let id = created["sandboxID"].as_str().unwrap();
        fixture
            .state
            .owners
            .lock()
            .unwrap()
            .get_mut(id)
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .state = pb::EnvironmentState::Paused as i32;
        fixture
            .state
            .control_owner_unavailable
            .store(unavailable, Ordering::SeqCst);
        let path = format!("/v2/sandboxes/{id}/connect");
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &path,
            &headers,
            r#"{"timeout":73}"#,
        )
        .await;
        assert_eq!(response.status().as_u16(), 201);
        let value: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(value["state"], "running");
        assert_eq!(value["endAt"], "1970-01-01T00:01:13.001Z");
        assert_eq!(
            *fixture.state.control_order.lock().unwrap(),
            ["ensure_running", "update"]
        );
        {
            let updates = fixture.state.control_updates.lock().unwrap();
            assert_eq!(updates.len(), 1);
            assert_eq!(updates[0].id, id);
            assert_eq!(updates[0].timeout_seconds, 73);
            assert!(updates[0].extend_only);
            let requests = fixture.state.resume_requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].run_timeout_seconds, Some(73));
            assert_eq!(
                requests[0].timeout_seconds, 300,
                "RPC wait budget stays independent"
            );
        }
        fixture.state.control_order.lock().unwrap().clear();
        let response = http::request(
            fixture.api.clone(),
            "POST",
            &path,
            &headers,
            r#"{"timeout":5}"#,
        )
        .await;
        assert_eq!(response.status().as_u16(), 200);
        let value: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(value["endAt"], "1970-01-01T00:01:13.001Z");
        assert_eq!(*fixture.state.control_order.lock().unwrap(), ["update"]);
        let updates = fixture.state.control_updates.lock().unwrap();
        assert_eq!(updates[1].timeout_seconds, 5);
        assert!(updates[1].extend_only);
    }
}

#[tokio::test]
async fn e2b_list_reuses_filtered_directory_pages_and_one_batch_without_credentials() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    for role in ["one", "two"] {
        let response = http::request(
            fixture.api.clone(),
            "POST",
            "/v2/sandboxes",
            &headers,
            &json!({"templateID":"base","metadata":{"team":"core","purpose":role}}).to_string(),
        )
        .await;
        assert_eq!(response.status().as_u16(), 201);
    }
    let example = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    for (id, tenant, e2b, state) in [
        ("00-native", "tenant", false, pb::EnvironmentState::Running),
        (
            "01-other-tenant",
            "other",
            true,
            pb::EnvironmentState::Running,
        ),
        ("02-paused", "tenant", true, pb::EnvironmentState::Paused),
    ] {
        let mut owner = example.clone();
        let record = owner.record.as_mut().unwrap();
        let spec = record.spec.as_mut().unwrap();
        spec.id = id.into();
        spec.tenant_id = tenant.into();
        if !e2b {
            spec.compatibility = None;
        }
        record.assignment.as_mut().unwrap().environment_id = id.into();
        record.state = state as i32;
        fixture.clients.put_owner(owner.clone()).await.unwrap();
        fixture
            .state
            .owners
            .lock()
            .unwrap()
            .insert(id.into(), owner);
    }
    fixture.state.control_read_calls.store(0, Ordering::SeqCst);
    fixture.state.batch_control_calls.store(0, Ordering::SeqCst);
    let first = http::request(
        fixture.api.clone(),
        "GET",
        "/v2/sandboxes?metadata=team%3Dcore&limit=1",
        &headers,
        "",
    )
    .await;
    assert_eq!(first.status().as_u16(), 200);
    let cursor = first
        .headers()
        .get("x-next-token")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!cursor.is_empty());
    let first: Value = serde_json::from_slice(first.body()).unwrap();
    assert_eq!(first.as_array().unwrap().len(), 1);
    assert_eq!(first[0]["metadata"]["team"], "core");
    assert_eq!(first[0]["metadata"]["native-source"], "inherited");
    assert!(first[0].get("envdAccessToken").is_none());
    assert!(first[0].get("trafficAccessToken").is_none());
    assert_eq!(fixture.state.control_read_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.batch_control_calls.load(Ordering::SeqCst), 1);
    let encoded: String = url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect();
    let next = http::request(
        fixture.api.clone(),
        "GET",
        &format!("/v2/sandboxes?metadata=team%3Dcore&limit=1&nextToken={encoded}"),
        &headers,
        "",
    )
    .await;
    assert_eq!(next.status().as_u16(), 200);
    let next: Value = serde_json::from_slice(next.body()).unwrap();
    assert_eq!(next.as_array().unwrap().len(), 1);
    assert_ne!(first[0]["sandboxID"], next[0]["sandboxID"]);
    let paused = http::request(
        fixture.api.clone(),
        "GET",
        "/v2/sandboxes?metadata=team%3Dcore&state=paused",
        &headers,
        "",
    )
    .await;
    assert_eq!(paused.status().as_u16(), 200);
    let paused: Value = serde_json::from_slice(paused.body()).unwrap();
    assert_eq!(paused[0]["sandboxID"], "02-paused");
    assert_eq!(paused[0]["state"], "paused");
    assert_eq!(paused[0]["endAt"], "9999-12-31T23:59:59Z");
    for (key, filter) in [
        ("k".repeat(32), "metadata=team%3Dother"),
        (format!("other-{}", "k".repeat(32)), "metadata=team%3Dcore"),
    ] {
        let response = http::request(
            fixture.api.clone(),
            "GET",
            &format!("/v2/sandboxes?{filter}&nextToken={encoded}"),
            &[("X-API-Key", key)],
            "",
        )
        .await;
        assert_eq!(response.status().as_u16(), 400);
    }
}

#[tokio::test]
async fn generic_run_policy_does_not_make_a_native_environment_an_e2b_sandbox() {
    let fixture = Fixture::new().await;
    let mut native = spec("native-with-runtime-policy");
    native.run_policy = Some(pb::RunPolicy {
        timeout_seconds: 90_000,
        action: pb::RunExpiryAction::Delete as i32,
    });
    fixture
        .service
        .create(
            native,
            json!({"name":"native-with-runtime-policy"}),
            "native-runtime-policy",
            &caller(),
        )
        .await
        .unwrap();
    // The generic control exists independently of E2B.
    assert!(fixture
        .clients
        .environment_control("native-with-runtime-policy", &caller())
        .await
        .is_ok());
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    for (method, suffix, body) in [
        ("GET", "", ""),
        ("DELETE", "", ""),
        ("POST", "/timeout", r#"{"timeout":60}"#),
        ("POST", "/pause", "{}"),
        ("POST", "/snapshots", "{}"),
        ("PUT", "/network", "{}"),
    ] {
        let response = http::request(
            fixture.api.clone(),
            method,
            &format!("/sandboxes/native-with-runtime-policy{suffix}"),
            &headers,
            body,
        )
        .await;
        assert_eq!(response.status().as_u16(), 404, "{method} {suffix}");
    }
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes/native-with-runtime-policy/connect",
        &headers,
        "{}",
    )
    .await;
    assert_eq!(response.status().as_u16(), 404);
    let list = http::request(fixture.api.clone(), "GET", "/v2/sandboxes", &headers, "").await;
    assert_eq!(list.status().as_u16(), 200);
    assert!(serde_json::from_slice::<Value>(list.body())
        .unwrap()
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert!(fixture
        .state
        .owners
        .lock()
        .unwrap()
        .contains_key("native-with-runtime-policy"));
}

#[tokio::test]
async fn e2b_snapshot_clone_uses_the_persisted_spec_after_alias_resolution() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let first = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base","metadata":{"source":"one"}}"#,
    )
    .await;
    assert_eq!(first.status().as_u16(), 201);
    let mut original = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .record
        .as_ref()
        .unwrap()
        .spec
        .clone()
        .unwrap();
    // The published alias may be removed or repointed after the snapshot. Restore must consume
    // the persisted specification and never attempt to resolve this historical identity.
    original.compatibility.as_mut().unwrap().template_id = "deleted-alias".into();
    original.envd.as_mut().unwrap().version = "0.9.1".into();
    *fixture.state.snapshot.lock().unwrap() = Some(pb::ReusableSnapshot {
        id: "snapshot-1".into(),
        template: Some(original.clone()),
        state: pb::SnapshotState::Ready as i32,
        ..Default::default()
    });

    let cloned = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"snapshot-1","timeout":60,"autoPause":true,"metadata":{"clone":"two","source":"overridden"}}"#,
    )
    .await;
    assert_eq!(cloned.status().as_u16(), 201);
    assert_eq!(
        serde_json::from_slice::<Value>(cloned.body()).unwrap()["envdVersion"],
        "0.9.1"
    );
    let cloned = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .map(|owner| {
            owner
                .record
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .clone()
        })
        .find(|spec| spec.id != original.id)
        .unwrap();
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *fixture.state.snapshot_requests.lock().unwrap(),
        vec![
            ("tenant".into(), "base-snapshot".into()),
            ("tenant".into(), "snapshot-1".into())
        ]
    );
    assert_eq!(cloned.image, original.image);
    assert_eq!(cloned.resources, original.resources);
    assert_eq!(cloned.env, original.env);
    assert_eq!(cloned.runtime_profile, original.runtime_profile);
    assert_eq!(cloned.envd, original.envd);
    assert_eq!(cloned.execution, original.execution);
    assert_eq!(cloned.snapshot_id.as_deref(), Some("snapshot-1"));
    assert_eq!(original.run_policy.as_ref().unwrap().timeout_seconds, 300);
    assert_eq!(cloned.run_policy.as_ref().unwrap().timeout_seconds, 60);
    assert_eq!(
        cloned.run_policy.as_ref().unwrap().action,
        pb::RunExpiryAction::Pause as i32
    );
    let original_compatibility = original.compatibility.unwrap();
    let cloned_compatibility = cloned.compatibility.unwrap();
    assert_ne!(
        cloned_compatibility.envd_access_token,
        original_compatibility.envd_access_token
    );
    assert_ne!(
        cloned_compatibility.traffic_access_token,
        original_compatibility.traffic_access_token
    );
    assert_eq!(
        original.scheduling.as_ref().unwrap().labels["native-source"],
        "inherited"
    );
    assert_eq!(
        cloned.scheduling.as_ref().unwrap().labels["native-source"],
        "inherited"
    );
    assert_eq!(
        original.scheduling.as_ref().unwrap().labels["source"],
        "one"
    );
    assert_eq!(cloned.scheduling.as_ref().unwrap().labels["clone"], "two");
    assert_eq!(
        cloned.scheduling.as_ref().unwrap().labels["source"],
        "overridden"
    );
}

#[tokio::test]
async fn legacy_e2b_snapshot_clone_is_rejected_before_create() {
    let fixture = Fixture::new().await;
    let headers = [
        ("X-API-Key", "k".repeat(32)),
        ("Content-Type", "application/json".into()),
    ];
    let first = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"base"}"#,
    )
    .await;
    assert_eq!(first.status().as_u16(), 201);
    let mut source = fixture
        .state
        .owners
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .record
        .as_ref()
        .unwrap()
        .spec
        .clone()
        .unwrap();
    let envd = source.envd.as_mut().unwrap();
    envd.gateway_auth = false;
    envd.access_token = Some("legacy-token-012345678901234567890123456789".into());
    envd.ingress_sources.clear();
    *fixture.state.snapshot.lock().unwrap() = Some(pb::ReusableSnapshot {
        id: "legacy-snapshot".into(),
        template: Some(source),
        state: pb::SnapshotState::Ready as i32,
        ..Default::default()
    });
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    let response = http::request(
        fixture.api.clone(),
        "POST",
        "/v2/sandboxes",
        &headers,
        r#"{"templateID":"legacy-snapshot"}"#,
    )
    .await;
    assert_eq!(response.status().as_u16(), 409);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
}
#[tokio::test]
async fn unknown_results_do_not_block_new_names_and_retry_original_identity() {
    let fixture = Fixture::new().await;
    let unknown_codes = [
        tonic::Code::Unavailable,
        tonic::Code::DeadlineExceeded,
        tonic::Code::Cancelled,
        tonic::Code::Unknown,
        tonic::Code::Internal,
    ];
    for (index, code) in unknown_codes.into_iter().enumerate() {
        fixture
            .state
            .response_error
            .store(code as usize, Ordering::SeqCst);
        let error = fixture
            .create(&format!("unknown-{index}"), &format!("request-{index}"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
    }
    fixture.state.response_error.store(0, Ordering::SeqCst);
    fixture.create("new", "new-request").await.unwrap();
    // A repeated request reuses its confirmed result even when arguments differ.
    for index in 0..unknown_codes.len() {
        assert_eq!(
            fixture
                .create("other-name", &format!("request-{index}"))
                .await
                .unwrap()["environmentId"],
            format!("unknown-{index}")
        );
    }
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    fixture.create("unknown-0", "request-0").await.unwrap();
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
}
#[tokio::test]
async fn active_creates_are_not_evicted_and_same_request_is_serialized() {
    let fixture = Fixture::with_retention(1).await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let spawn = |id: &'static str, request_id: &'static str| {
        let service = fixture.service.clone();
        tokio::spawn(async move {
            Box::pin(service.create(spec(id), json!({"name":id}), request_id, &caller())).await
        })
    };
    let first = spawn("one", "request-one");
    fixture.wait_starts(1).await;
    // GC runs during this blocked RPC, but cannot remove its active identity.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let duplicate = spawn("one", "request-one");
    let second = spawn("two", "request-two");
    fixture.wait_starts(2).await;
    fixture.state.block_response.store(false, Ordering::SeqCst);
    fixture.state.release.notify_waiters();
    assert_eq!(
        first.await.unwrap().unwrap(),
        duplicate.await.unwrap().unwrap()
    );
    assert_eq!(second.await.unwrap().unwrap()["environmentId"], "two");
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn expired_unknown_request_is_collected_without_querying_or_changing_environment() {
    let fixture = Fixture::with_retention(1).await;
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert_eq!(
        fixture.create("old", "request").await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        fixture.create("new", "request").await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    let lookups = fixture.state.lookups.load(Ordering::SeqCst);
    let calls = fixture.state.create_calls.load(Ordering::SeqCst);
    // Allow the first unknown TTL and the following periodic sweep to pass.
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), lookups);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        fixture.state.owners.lock().unwrap()["old"]
            .record
            .as_ref()
            .unwrap()
            .state,
        pb::EnvironmentState::Running as i32
    );
    fixture.state.response_error.store(0, Ordering::SeqCst);
    // Evicting the process cache does not discard the durable request receipt.
    assert_eq!(
        fixture.create("new", "request").await.unwrap()["environmentId"],
        "old"
    );
    let starts = fixture.state.starts.load(Ordering::SeqCst);
    assert_eq!(
        fixture.create("old", "retry-old").await.unwrap()["environmentId"],
        "old"
    );
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), starts);
}

#[tokio::test]
async fn pending_owner_retry_drives_the_original_assignment() {
    let fixture = Fixture::new().await;
    fixture.state.pending_response.store(true, Ordering::SeqCst);
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert_eq!(
        fixture
            .create("pending", "request")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let assignment = fixture.state.owners.lock().unwrap()["pending"]
        .record
        .as_ref()
        .unwrap()
        .assignment
        .clone();
    fixture
        .state
        .pending_response
        .store(false, Ordering::SeqCst);
    fixture.state.response_error.store(0, Ordering::SeqCst);
    assert_eq!(
        fixture.create("pending", "request").await.unwrap()["environmentId"],
        "pending"
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 2);
    let deadlines = fixture.state.deadlines.lock().unwrap();
    assert!(deadlines[0] > 0);
    assert_eq!(deadlines[0], deadlines[1]);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.state.owners.lock().unwrap()["pending"]
            .record
            .as_ref()
            .unwrap()
            .assignment,
        assignment
    );
}

#[tokio::test]
async fn failed_owner_retry_is_terminal_and_does_not_repeat_start() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert!(fixture.create("failed", "request").await.is_err());
    {
        let mut owners = fixture.state.owners.lock().unwrap();
        let record = owners.get_mut("failed").unwrap().record.as_mut().unwrap();
        record.state = pb::EnvironmentState::Failed as i32;
        record.resources_held = false;
    }
    fixture.state.response_error.store(0, Ordering::SeqCst);
    assert_eq!(
        fixture
            .create("failed", "request")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_retry_accepts_a_reassigned_unconfirmed_generation() {
    let fixture = Fixture::new().await;
    fixture.state.pending_response.store(true, Ordering::SeqCst);
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert!(fixture.create("pending-fenced", "request").await.is_err());
    // First retry observes and binds the Pending assignment, then loses its response.
    assert!(fixture.create("pending-fenced", "request").await.is_err());
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .get_mut("pending-fenced")
        .unwrap()
        .record
        .as_mut()
        .unwrap()
        .assignment
        .as_mut()
        .unwrap()
        .generation = 2;
    fixture
        .state
        .pending_response
        .store(false, Ordering::SeqCst);
    fixture.state.response_error.store(0, Ordering::SeqCst);
    assert_eq!(
        fixture.create("pending-fenced", "request").await.unwrap()["environmentId"],
        "pending-fenced"
    );
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn instance_details_preserve_visibility_configuration_and_read_only_lookup() {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::new().await;
    let id = "other-api+agent";
    let environment_spec = adx_apiserver::contract::create_spec(
        json!({
            "namespace":"other", "name":"api+agent", "cpu":1000, "memory":1024,
            "rootfs":{"type":"image","imageurl":"app:1","runtime":"runc"},
            "env":{"MODEL_KEY":"private-value"},
            "inheritEntrypoint":true,
            "mounts":[{"type":"bind","host_path":"/srv/alice","target":"/workspace","options":["rw"]}],
            "healthcheckProbe":{"tcpSocket":{"port":8080},"failureThreshold":3}
        }),
        &caller(),
    )
    .unwrap();
    assert_eq!(environment_spec.id, id);
    let owner = pb::GetEnvironmentResponse {
        record: Some(pb::EnvironmentRecord {
            spec: Some(environment_spec),
            assignment: Some(pb::Assignment {
                environment_id: id.into(),
                node_id: "worker".into(),
                generation: 7,
                ..Default::default()
            }),
            runtime: Some(pb::Runtime {
                id: "other-api+agent-7".into(),
                ip: "100.64.1.2".into(),
            }),
            state: pb::EnvironmentState::Running as i32,
            revision: 9,
            resources_held: true,
            ..Default::default()
        }),
        node_address: "127.0.0.1:9000".into(),
        relay_address: "127.0.0.1:9443".into(),
    };
    // The authoritative service knows this object; this API's synchronized
    // directory has deliberately received no delta for it.
    fixture
        .state
        .owners
        .lock()
        .unwrap()
        .insert(id.into(), owner);
    assert_eq!(
        fixture
            .clients
            .owner(id, &caller(), false)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = api.clone();
            tokio::spawn(async move {
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        hyper_util::rt::TokioIo::new(socket),
                        hyper::service::service_fn(move |request| api.clone().serve(request)),
                    )
                    .await
                    .unwrap();
            });
        }
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let uri = format!("http://{address}/api/instances?instance_id=other-api%2Bagent");
    let foreign_uncached = client
        .get(&uri)
        .bearer_auth("foreign-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(foreign_uncached.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), 1);
    let ordinary = client
        .get(&uri)
        .bearer_auth("ordinary-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(ordinary.status(), reqwest::StatusCode::OK);
    assert_eq!(ordinary.headers()["cache-control"], "no-store");
    let ordinary: Value = serde_json::from_slice(&ordinary.bytes().await.unwrap()).unwrap();
    assert_eq!(ordinary[0]["runtime_id"], "other-api+agent-7");
    assert!(ordinary[0].get("execution").is_none());
    assert!(ordinary[0].get("env_vars").is_none());
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), 2);
    let response = client
        .get(&uri)
        .bearer_auth("admin-detail-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value[0]["generation"], 7);
    assert_eq!(value[0]["runtime_class"], "runc");
    assert_eq!(value[0]["revision"], 9);
    assert_eq!(value[0]["env_vars"]["MODEL_KEY"], "private-value");
    assert_eq!(
        value[0]["execution"]["mounts"][0]["source"]["Local"],
        "/srv/alice"
    );
    assert_eq!(
        value[0]["execution"]["healthcheck_probe"]["failure_threshold"],
        3
    );
    assert!(value[0].get("record").is_none());
    let listing = client
        .get(format!("http://{address}/api/instances"))
        .bearer_auth("admin-detail-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    let listing: Value = serde_json::from_slice(&listing.bytes().await.unwrap()).unwrap();
    assert_eq!(listing["items"][0]["id"], id);
    assert!(listing["items"][0].get("env_vars").is_none());
    assert!(listing["items"][0].get("execution").is_none());
    let denied = client
        .get(&uri)
        .bearer_auth("foreign-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), 2);
    let missing = client
        .get(format!("http://{address}/api/instances?instance_id=absent"))
        .bearer_auth("ordinary-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(fixture.state.lookups.load(Ordering::SeqCst), 3);
    let removed = client
        .get(format!(
            "http://{address}/api/environment/v1/environments/other-api%2Bagent/inspect"
        ))
        .bearer_auth("admin-detail-key-00000000000000000000")
        .send()
        .await
        .unwrap();
    assert_eq!(removed.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(fixture.state.starts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.create_calls.load(Ordering::SeqCst), 0);
    http.abort();
}
static TRACE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn traced_create(
    exporter: &opentelemetry_sdk::trace::InMemorySpanExporter,
    mode: &str,
    failure: bool,
    local: bool,
) -> (Vec<opentelemetry_sdk::trace::SpanData>, String) {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::with_mode(600, local).await;
    if local {
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.clients.nodes().await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    fixture.state.block_response.store(true, Ordering::SeqCst);
    if failure {
        fixture
            .state
            .response_error
            .store(tonic::Code::InvalidArgument as usize, Ordering::SeqCst);
    }
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serving = api.clone();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = serving.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| api.clone().serve(request));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    let before = exporter.get_finished_spans().unwrap().len();
    let input = json!({"name":"trace-create", "image":"image", "runtimeClass":"runsc"});
    let embedded = mode == "embedded";
    let service = api.environment_service.clone();
    let mode = mode.to_owned();
    let request = tokio::spawn(async move {
        if embedded {
            let result = adx_observability::trace::Trace::child("test.embedded_create")
                .run(service.create_request(input, "trace-create", &caller()))
                .await;
            assert_eq!(result.is_err(), failure);
        } else {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let response = client
                .post(format!("http://{address}/api/environment/v1/environments"))
                .header(
                    "authorization",
                    format!("Bearer {}", "trace-test".repeat(4)),
                )
                .header(
                    "accept",
                    if mode == "sse" {
                        "text/event-stream"
                    } else {
                        "application/json"
                    },
                )
                .body(input.to_string())
                .send()
                .await
                .unwrap();
            let text = response.text().await.unwrap();
            if mode == "sse" {
                assert!(text.contains("event: final"), "{text}");
            }
            assert_eq!(text.contains("error"), failure, "{text}");
        }
    });
    fixture.wait_starts(1).await;
    let in_progress = exporter.get_finished_spans().unwrap();
    assert!(
        !in_progress[before..]
            .iter()
            .any(|span| span.name == "environment.create"
                && span
                    .attributes
                    .iter()
                    .any(|a| a.key.as_str() == "environment.id"
                        && a.value.to_string() == "default-trace-create")),
        "create must not finish before confirmation"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    fixture.state.release.notify_one();
    request.await.unwrap();
    tokio::task::yield_now().await;
    let completed = exporter.get_finished_spans().unwrap();
    let total = completed[before..]
        .iter()
        .find(|span| {
            span.name == "environment.create"
                && span.attributes.iter().any(|a| {
                    a.key.as_str() == "environment.id"
                        && a.value.to_string() == "default-trace-create"
                })
        })
        .expect("total create span missing");
    let result = completed[before..]
        .iter()
        .filter(|span| span.span_context.trace_id() == total.span_context.trace_id())
        .cloned()
        .collect();
    let metrics = api.environment_service.metrics();
    http.abort();
    (result, metrics)
}

#[tokio::test]
async fn create_total_span_matches_metric_boundary_for_json_sse_and_embedded() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for mode in ["json", "sse", "embedded"] {
        let (spans, metrics) = traced_create(&exporter, mode, false, false).await;
        let creates: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "environment.create")
            .collect();
        assert_eq!(
            creates.len(),
            1,
            "{mode}: exactly one total create span required"
        );
        let create = creates[0];
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "environment.id"
                && a.value.to_string() == "default-trace-create"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "result" && a.value.to_string() == "success"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "instance_id"
                && a.value.to_string() == "default-trace-create"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "user.id" && a.value.to_string() == "tenant"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "newly_created" && a.value.to_string() == "true"));
        let elapsed = create
            .end_time
            .duration_since(create.start_time)
            .unwrap()
            .as_secs_f64();
        assert!(elapsed >= 0.03, "{mode}: confirmation wait missing");
        let measured: f64 = metrics
            .lines()
            .find_map(|line| line.strip_prefix("adx_environment_create_duration_seconds_sum "))
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            (elapsed - measured).abs() < 0.02,
            "{mode}: span={elapsed}, metric={measured}"
        );
        let parent = spans
            .iter()
            .find(|s| s.span_context.span_id() == create.parent_span_id)
            .unwrap();
        assert_eq!(
            parent.span_context.trace_id(),
            create.span_context.trace_id()
        );
        assert_eq!(
            parent.name,
            if mode == "embedded" {
                "test.embedded_create"
            } else {
                "POST /api/environment/v1/environments"
            }
        );
        if mode == "sse" {
            assert!(parent.end_time < create.end_time);
        }
    }
}

#[tokio::test]
async fn create_rpc_server_spans_are_children_of_the_matching_client_span() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for local in [false, true] {
        let (spans, _) = traced_create(&exporter, "sse", false, local).await;
        let (client, server) = if local {
            (
                "apiserver.create_local",
                "test.node.create_local_environment",
            )
        } else {
            ("apiserver.create", "test.coordinator.create_environment")
        };
        let client = spans.iter().find(|s| s.name == client).unwrap();
        let server = spans.iter().find(|s| s.name == server).unwrap();
        assert_eq!(server.parent_span_id, client.span_context.span_id());
        assert_eq!(
            server.span_context.trace_id(),
            client.span_context.trace_id()
        );
        assert!(client.start_time <= server.start_time && client.end_time >= server.end_time);
    }
}

#[tokio::test]
async fn local_first_create_propagates_trace_into_node_rpc() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());

    let (spans, _) = traced_create(&exporter, "json", false, true).await;
    let create = spans
        .iter()
        .find(|span| span.name == "environment.create")
        .expect("total create span missing");
    assert!(create
        .attributes
        .iter()
        .any(|a| { a.key.as_str() == "source" && a.value.to_string() == "local-first" }));
    let select = spans
        .iter()
        .find(|span| span.name == "apiserver.select_local")
        .expect("local-first selection span missing");
    let client = spans
        .iter()
        .find(|span| span.name == "apiserver.create_local")
        .expect("local-first create client span missing");
    let server = spans
        .iter()
        .find(|span| span.name == "test.node.create_local_environment")
        .expect("local-first node server span missing");
    assert_eq!(select.parent_span_id, create.span_context.span_id());
    assert_eq!(client.parent_span_id, create.span_context.span_id());
    assert_eq!(server.parent_span_id, client.span_context.span_id());
    assert_eq!(
        server.span_context.trace_id(),
        create.span_context.trace_id()
    );
}

#[tokio::test]
async fn create_total_span_records_errors_for_json_sse_and_embedded() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    for mode in ["json", "sse", "embedded"] {
        let (spans, _) = traced_create(&exporter, mode, true, false).await;
        let create = spans
            .iter()
            .find(|span| span.name == "environment.create")
            .expect("total create span missing");
        assert!(
            matches!(create.status, opentelemetry::trace::Status::Error { .. }),
            "{mode}: {:?}",
            create.status
        );
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "result" && a.value.to_string() == "fail"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "error.code"));
        assert!(create
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "error.message"));
        assert!(
            create.events.iter().any(|event| event.name == "exception"),
            "{mode}: create total span is missing the exception event"
        );
    }
}

#[tokio::test]
async fn cancelled_create_ends_total_span_without_losing_its_parent() {
    let _serial = TRACE_TEST_LOCK.lock().await;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let fixture = Fixture::new().await;
    fixture.state.block_response.store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let request = tokio::spawn(
        adx_observability::trace::Trace::child("test.cancelled_create").run(async move {
            service
                .create_request(
                    json!({"name":"cancelled-create","image":"image"}),
                    "cancelled-create",
                    &caller(),
                )
                .await
        }),
    );
    fixture.wait_starts(1).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let spans = exporter.get_finished_spans().unwrap();
    let parent = spans
        .iter()
        .find(|s| s.name == "test.cancelled_create")
        .unwrap();
    let create = spans
        .iter()
        .find(|s| {
            s.name == "environment.create"
                && s.parent_span_id == parent.span_context.span_id()
                && s.span_context.trace_id() == parent.span_context.trace_id()
        })
        .unwrap();
    assert_eq!(create.parent_span_id, parent.span_context.span_id());
    assert!(create
        .attributes
        .iter()
        .any(|a| a.key.as_str() == "environment.create.outcome"
            && a.value.to_string() == "cancelled"));
    let metrics = fixture.service.metrics();
    assert!(metrics.contains("adx_environment_create_in_flight 0\n"));
    assert!(metrics.contains("adx_environment_create_responses_total{outcome=\"cancelled\"} 1\n"));
    fixture.state.release.notify_one();
}

#[tokio::test]
async fn local_first_claim_persistence_is_an_independent_opt_in() {
    for persist in [None, Some(true)] {
        let fixture = Fixture::with_claim_mode(600, true, persist).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.clients.nodes().await.is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        fixture
            .create("local-persistence-option", "local-request")
            .await
            .unwrap();
        assert_eq!(
            *fixture.state.local_persistence.lock().unwrap(),
            vec![persist.unwrap_or(false)]
        );
    }
}

#[path = "../../../build/observability/tests/prometheus.rs"]
mod prometheus;

#[tokio::test]
#[ignore = "requires native Prometheus and evidence directory"]
async fn prometheus_collects_new_creation_latency_without_replay_samples() {
    let fixture = Fixture::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let metrics = tokio::spawn(adx_apiserver::metrics::serve(
        listener,
        fixture.service.clone(),
    ));
    let mut collector = prometheus::Collector::start("apiserver", address).await;
    collector
        .wait_value(
            "adx_environment_create_success_call_duration_seconds_count",
            0.0,
        )
        .await;
    fixture.create("collected", "first").await.unwrap();
    fixture.create("collected", "first").await.unwrap();
    fixture.create("collected", "reuse").await.unwrap();
    fixture
        .state
        .response_error
        .store(tonic::Code::Unavailable as usize, Ordering::SeqCst);
    assert!(fixture
        .create("response-lost", "response-lost")
        .await
        .is_err());
    fixture.state.response_error.store(0, Ordering::SeqCst);
    fixture
        .create("response-lost", "response-lost")
        .await
        .unwrap();
    fixture
        .state
        .scheduling_expired
        .store(true, Ordering::SeqCst);
    assert!(fixture
        .create("schedule-failed", "schedule-failed")
        .await
        .is_err());
    collector
        .wait_value(
            "adx_environment_create_responses_total{outcome=\"unknown\"}",
            1.0,
        )
        .await;
    collector.wait_value("adx_environment_create_errors_total{outcome=\"failure\",stage=\"scheduling\",reason=\"timeout\"}",1.0).await;
    collector
        .wait_value(
            "adx_environment_create_success_call_duration_seconds_count",
            1.0,
        )
        .await;
    collector
        .wait_value(
            "adx_environment_create_responses_total{outcome=\"success\"}",
            4.0,
        )
        .await;
    let p95 = "histogram_quantile(0.95,sum by(le)(rate(adx_environment_create_success_call_duration_seconds_bucket[1m])))";
    let value = collector.query(p95).await.unwrap();
    assert!(value.is_finite() && value > 0.0, "{value}");
    collector.capture().await;
    metrics.abort();
}

#[tokio::test]
async fn unnamed_requests_keep_identity_across_api_replicas_and_tenants() {
    let fixture = Fixture::new().await;
    let replica = EnvironmentService::new(fixture.clients.clone());
    let input = json!({"image":"image"});
    let a = fixture
        .service
        .prepare_create_with_request(input.clone(), &caller(), "same")
        .unwrap();
    let b = replica
        .prepare_create_with_request(input.clone(), &caller(), "same")
        .unwrap();
    assert_eq!(a.id, b.id);
    let other_tenant = pb::CallerContext {
        tenant_id: "other".into(),
        administrator: false,
    };
    let c = replica
        .prepare_create_with_request(input.clone(), &other_tenant, "same")
        .unwrap();
    assert_ne!(a.id, c.id);
    assert!(replica
        .prepare_create_with_request(input, &caller(), "")
        .is_err());
}

#[tokio::test]
async fn invalid_name_types_are_rejected_before_default_identity() {
    let fixture = Fixture::new().await;
    for name in [json!(42), json!(true), json!(null), json!([]), json!({})] {
        let input = json!({"image":"image", "name":name});
        let result = fixture
            .service
            .prepare_create_with_request(input, &caller(), "request");
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
    let absent = fixture
        .service
        .prepare_create_with_request(json!({"image":"image"}), &caller(), "request")
        .unwrap();
    let empty = fixture
        .service
        .prepare_create_with_request(json!({"image":"image", "name":""}), &caller(), "request")
        .unwrap();
    assert_eq!(absent.id, empty.id);
    let input = json!({"image":"image", "name":"explicit"});
    assert_eq!(
        fixture
            .service
            .prepare_create_with_request(input.clone(), &caller(), "request")
            .unwrap(),
        fixture.service.prepare_create(input, &caller()).unwrap()
    );
}

#[tokio::test]
async fn busy_template_delete_uses_e2b_bad_request_without_snapshot_fallback() {
    let fixture = Fixture::new().await;
    fixture
        .state
        .preparation_error
        .store(tonic::Code::FailedPrecondition as usize, Ordering::SeqCst);
    let response = http::request(
        fixture.api.clone(),
        "DELETE",
        "/templates/family",
        &[("X-API-Key", "k".repeat(32))],
        "",
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert_eq!(fixture.state.snapshot_calls.load(Ordering::SeqCst), 0);
}
