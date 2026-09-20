use adx_agent_api::{
    dispatcher::{DispatcherClient, Membership},
    managed::ManagedService,
    Error,
};
use adx_agent_core::{
    routing::{DispatcherMember, HashRing},
    sandbox::*,
    *,
};
use adx_agent_store::{AgentState, MemoryRepository, RedisRepository};
use adx_dispatcher::{server, Config, Dispatcher};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;
const TOKEN: &str = "dispatcher-service-test-token-at-least-32";
#[derive(Default)]
struct Backend {
    values: Mutex<BTreeMap<String, SandboxObservation>>,
    creates: AtomicUsize,
}
#[async_trait]
impl Sandbox for Backend {
    async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        let value = SandboxObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: SandboxPhase::Running,
            ready: true,
            runtime_id: Some(format!("{}-1", r.id)),
            message: None,
        };
        Ok(self
            .values
            .lock()
            .await
            .entry(encode_key(&[&r.tenant, &r.id]))
            .or_insert(value)
            .clone())
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        Ok(self
            .values
            .lock()
            .await
            .get(&encode_key(&[tenant, id]))
            .cloned())
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        let value = SandboxObservation {
            id: id.into(),
            tenant: tenant.into(),
            phase: SandboxPhase::Deleted,
            ready: false,
            runtime_id: None,
            message: None,
        };
        self.values
            .lock()
            .await
            .insert(encode_key(&[tenant, id]), value.clone());
        Ok(value)
    }
}
struct Members {
    values: Vec<DispatcherMember>,
    reads: AtomicUsize,
}
#[async_trait]
impl Membership for Members {
    async fn members(&self) -> adx_agent_api::Result<Vec<DispatcherMember>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.values.clone())
    }
}
async fn node(
    state: AgentState,
    backend: Arc<Backend>,
    name: &str,
) -> (DispatcherMember, tokio::task::JoinHandle<()>) {
    let boot = uuid::Uuid::new_v4().to_string();
    let dispatcher =
        Arc::new(Dispatcher::new(state, backend, boot.clone(), Config::default()).unwrap());
    let router = server::router(dispatcher, TOKEN, Arc::new(AtomicBool::new(true))).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (
        DispatcherMember {
            node_id: name.into(),
            boot_id: boot,
            address,
        },
        task,
    )
}
fn template() -> TemplateVersion {
    serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/app/start"],"resources":{"cpu_millis":1000,"memory_mib":1024},"service":[{"protocol":"http","port":8080},{"protocol":"ws","port":8080},{"protocol":"ssh","port":22}]})).unwrap()
}
fn scope() -> Scope {
    Scope {
        tenant: "tenant".into(),
        template: "demo".into(),
        version: "1".into(),
        session_id: "ctx".into(),
    }
}
fn client(membership: Arc<dyn Membership>) -> Arc<DispatcherClient> {
    Arc::new(
        DispatcherClient::new(membership, TOKEN.into(), Duration::from_secs(3), None, true)
            .unwrap(),
    )
}
#[tokio::test]
async fn managed_http_client_cold_start_stickiness_failover_and_release() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let backend = Arc::new(Backend::default());
    let (a, ta) = node(state.clone(), backend.clone(), "a").await;
    let (b, tb) = node(state.clone(), backend.clone(), "b").await;
    let members = Arc::new(Members {
        values: vec![a.clone(), b.clone()],
        reads: AtomicUsize::new(0),
    });
    let managed = ManagedService::new(state.clone(), client(members.clone()));
    let scope = scope();
    managed.publish("tenant", &template()).await.unwrap();
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    managed.create_session(scope.clone()).await.unwrap();
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    let (first, port) = managed
        .resolve(&scope, Some("affinity".into()), Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(port, 8080);
    let (ws, port) = managed
        .resolve(&scope, Some("affinity".into()), Protocol::Ws, None)
        .await
        .unwrap();
    assert_eq!(port, 8080);
    assert_eq!(first, ws);
    assert_eq!(members.reads.load(Ordering::SeqCst), 1); // cached discovery, no per-request SCAN
    let preferred = HashRing::new(members.values.clone())
        .preferred(&scope)
        .unwrap()
        .node_id
        .clone();
    if preferred == "a" {
        ta.abort();
    } else {
        tb.abort();
    }
    let (after, port) = managed
        .resolve(&scope, Some("affinity".into()), Protocol::Ssh, Some(22))
        .await
        .unwrap();
    assert_eq!(port, 22);
    assert_eq!(first, after);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    assert!(matches!(
        managed
            .resolve(&scope, None, Protocol::Http, Some(9999))
            .await,
        Err(Error::Invalid(_))
    ));
    let other = Scope {
        tenant: "other".into(),
        ..scope.clone()
    };
    assert!(matches!(
        managed.resolve(&other, None, Protocol::Http, None).await,
        Err(Error::NotFound)
    ));
    // A new Gateway can select the surviving Dispatcher and recover the same sticky state.
    let alive = if preferred == "a" { b } else { a };
    let fresh = ManagedService::new(
        state.clone(),
        client(Arc::new(Members {
            values: vec![alive],
            reads: AtomicUsize::new(0),
        })),
    );
    fresh
        .release_instance(&scope, &first.instance_id)
        .await
        .unwrap();
    assert_eq!(
        state
            .instance("tenant", &first.instance_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        InstancePhase::Deleted
    );
    let (replacement, _) = fresh
        .resolve_with_cache(&scope, Some("affinity".into()), Protocol::Http, None, true)
        .await
        .unwrap();
    assert_ne!(first.instance_id, replacement.instance_id);
    fresh.release(&scope).await.unwrap();
    assert!(matches!(fresh.session(&scope).await, Err(Error::NotFound)));
    assert!(matches!(
        fresh
            .resolve_with_cache(&scope, Some("affinity".into()), Protocol::Http, None, true)
            .await,
        Err(Error::NotFound)
    ));
    ta.abort();
    tb.abort();
}
#[tokio::test]
async fn session_creation_is_empty_and_requests_reuse_cold_start() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let backend = Arc::new(Backend::default());
    let (member, task) = node(state.clone(), backend.clone(), "a").await;
    let managed = ManagedService::new(
        state,
        client(Arc::new(Members {
            values: vec![member],
            reads: AtomicUsize::new(0),
        })),
    );
    managed.publish("tenant", &template()).await.unwrap();
    managed.create_session(scope()).await.unwrap();
    assert!(managed.instances(&scope()).await.unwrap().is_empty());
    assert_eq!(backend.creates.load(Ordering::SeqCst), 0);
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..4 {
        seen.insert(
            managed
                .resolve(&scope(), None, Protocol::Http, None)
                .await
                .unwrap()
                .0
                .instance_id,
        );
    }
    assert_eq!(seen.len(), 1);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    task.abort();
}
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn redis_discovery_managed_gateway_replacement_preserves_binding() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let ns = format!("managed-{}", uuid::Uuid::new_v4());
    let repository = Arc::new(
        RedisRepository::connect(&url, &ns, Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let state = AgentState::new(repository.clone());
    let backend = Arc::new(Backend::default());
    let (member, task) = node(state.clone(), backend.clone(), "a").await;
    repository
        .register_dispatcher(&member, Duration::from_secs(30))
        .await
        .unwrap();
    let first = ManagedService::new(state.clone(), client(repository.clone()));
    first.publish("tenant", &template()).await.unwrap();
    first.create_session(scope()).await.unwrap();
    let (before, _) = first
        .resolve(&scope(), Some("sticky".into()), Protocol::Http, None)
        .await
        .unwrap();
    drop(first);
    task.abort();
    repository.unregister_dispatcher(&member).await.unwrap();
    let (replacement, task) = node(state.clone(), backend.clone(), "a").await;
    repository
        .register_dispatcher(&replacement, Duration::from_secs(30))
        .await
        .unwrap();
    let second = ManagedService::new(state, client(repository.clone()));
    let (after, _) = second
        .resolve(&scope(), Some("sticky".into()), Protocol::Http, None)
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(backend.creates.load(Ordering::SeqCst), 1);
    second.release(&scope()).await.unwrap();
    repository
        .unregister_dispatcher(&replacement)
        .await
        .unwrap();
    task.abort();
}
#[tokio::test]
async fn unknown_lifecycle_reply_is_not_automatically_replayed() {
    use adx_agent_api::dispatcher::Dispatch;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let router = axum::Router::new().route(
        "/internal/adx/v1/release",
        axum::routing::post(move || {
            let calls = observed.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(Error::OutcomeUnknown("write response lost".into())),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let members = Arc::new(Members {
        values: ["a", "b"]
            .into_iter()
            .map(|name| DispatcherMember {
                node_id: name.into(),
                boot_id: uuid::Uuid::new_v4().to_string(),
                address: address.clone(),
            })
            .collect(),
        reads: AtomicUsize::new(0),
    });
    assert!(matches!(
        client(members).release(&scope()).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    task.abort();
}
#[tokio::test]
async fn discovery_rejects_credential_urls_and_unapproved_plaintext() {
    for (address, allow_http) in [
        ("http://127.0.0.1:9", false),
        ("https://user:secret@127.0.0.1:9", false),
        ("https://127.0.0.1:9/path", false),
    ] {
        let members = Arc::new(Members {
            values: vec![DispatcherMember {
                node_id: "a".into(),
                boot_id: uuid::Uuid::new_v4().to_string(),
                address: address.into(),
            }],
            reads: AtomicUsize::new(0),
        });
        let client = DispatcherClient::new(
            members,
            TOKEN.into(),
            Duration::from_secs(1),
            None,
            allow_http,
        )
        .unwrap();
        assert!(client.refresh(false).await.is_err());
    }
}

#[tokio::test]
async fn dispatcher_failover_shares_one_deadline_and_uses_only_remaining_time() {
    use adx_agent_api::dispatcher::Dispatch;
    use axum::response::IntoResponse;
    let calls = Arc::new(AtomicUsize::new(0));
    let deadlines = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = calls.clone();
    let captured = deadlines.clone();
    let router = axum::Router::new().route(
        "/internal/adx/v1/resolve",
        axum::routing::post(move |headers: axum::http::HeaderMap| {
            let calls = observed.clone();
            let deadlines = captured.clone();
            async move {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                deadlines.lock().await.push(
                    headers[adx_agent_core::dispatcher::DEADLINE_HEADER]
                        .to_str()
                        .unwrap()
                        .to_owned(),
                );
                tokio::time::sleep(Duration::from_millis(300)).await;
                if attempt == 0 {
                    (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        axum::Json(Error::Unavailable("first node unavailable".into())),
                    )
                        .into_response()
                } else {
                    axum::Json(adx_agent_core::dispatcher::Target {
                        instance_id: "instance".into(),
                        sandbox_id: "sandbox".into(),
                        tenant: "tenant".into(),
                        scope: scope(),
                        session_generation: "generation".into(),
                    })
                    .into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let members = Arc::new(Members {
        values: ["a", "b"]
            .into_iter()
            .map(|id| DispatcherMember {
                node_id: id.into(),
                boot_id: uuid::Uuid::new_v4().to_string(),
                address: address.clone(),
            })
            .collect(),
        reads: AtomicUsize::new(0),
    });
    let client = DispatcherClient::new(
        members,
        TOKEN.into(),
        Duration::from_millis(500),
        None,
        true,
    )
    .unwrap();
    let result = client
        .resolve(&adx_agent_core::dispatcher::ResolveRequest {
            scope: scope(),
            affinity_key: None,
            bypass_cache: false,
        })
        .await;
    assert!(matches!(result, Err(Error::OutcomeUnknown(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let observed = deadlines.lock().await;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0], observed[1]);
    task.abort();
}

#[tokio::test]
async fn discovery_consumes_the_same_request_budget_before_dispatch() {
    use adx_agent_api::dispatcher::Dispatch;
    struct SlowMembers;
    #[async_trait]
    impl Membership for SlowMembers {
        async fn members(&self) -> adx_agent_api::Result<Vec<DispatcherMember>> {
            std::future::pending().await
        }
    }
    let client = DispatcherClient::new(
        Arc::new(SlowMembers),
        TOKEN.into(),
        Duration::from_millis(20),
        None,
        true,
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.resolve(&adx_agent_core::dispatcher::ResolveRequest {
            scope: scope(),
            affinity_key: None,
            bypass_cache: false,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::Unavailable(_))));
}
