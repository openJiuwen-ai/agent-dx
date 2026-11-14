//! Real PostgreSQL -> public Agent owner -> shared Ingress/Relay -> independent
//! standard-library HTTP/SQLite substitute. Platform execution, purpose identity
//! and receiver are controlled external ports, not actual DigitalMate acceptance.
use super::super::composio::WebhookVerifier;
use super::tests::{binding, body, headers, SECRET};
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::{
        auth::{AuthError, AuthenticatedIdentity, CredentialVerifier},
        *,
    },
    node::Relay,
};
use adx_agent_api::{local::LocalControl, managed::ManagedService, request::RequestContext};
use adx_agent_core::{environment::*, *};
use adx_agent_store::{AgentState, MemoryRepository};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
};

#[derive(Default)]
struct Platform(Mutex<BTreeMap<String, EnvironmentObservation>>);
#[async_trait::async_trait]
impl Environment for Platform {
    async fn get(
        &self,
        _: &str,
        id: &str,
    ) -> Result<Option<EnvironmentObservation>, EnvironmentError> {
        Ok(self.0.lock().unwrap().get(id).cloned())
    }
    async fn create(
        &self,
        r: &CreateEnvironment,
    ) -> Result<EnvironmentObservation, EnvironmentError> {
        let value = EnvironmentObservation {
            id: r.id.clone(),
            tenant: r.tenant.clone(),
            phase: EnvironmentPhase::Running,
            ready: true,
            runtime_id: None,
            message: None,
        };
        self.0.lock().unwrap().insert(r.id.clone(), value.clone());
        Ok(value)
    }
    async fn delete(&self, _: &str, id: &str) -> Result<EnvironmentObservation, EnvironmentError> {
        let mut value = self
            .0
            .lock()
            .unwrap()
            .remove(id)
            .ok_or(EnvironmentError::NotFound)?;
        value.phase = EnvironmentPhase::Deleted;
        Ok(value)
    }
}
#[derive(Debug)]
struct Identity;
#[async_trait::async_trait]
impl CredentialVerifier for Identity {
    async fn verify(&self, _: &str) -> Result<AuthenticatedIdentity, AuthError> {
        Err(AuthError::Invalid("not a management credential".into()))
    }
}
#[async_trait::async_trait]
impl crate::ingress::accounts::BusinessAuth for Identity {
    async fn login(
        &self,
        _: crate::ingress::accounts::LoginRequest,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::LoginResponse> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn authenticate(
        &self,
        _: &str,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::Principal> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn check(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<()> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn logout(&self, _: &str) -> crate::ingress::accounts::Result<()> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn quota(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<crate::ingress::accounts::QuotaResponse> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
    async fn model_parameters(
        &self,
        _: &crate::ingress::accounts::Principal,
    ) -> crate::ingress::accounts::Result<adx_agent_core::mounts::Parameters> {
        Err(crate::ingress::accounts::Error::Unauthorized)
    }
}
struct Purpose(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl DeliveryAuthorization for Purpose {
    async fn scope(&self, batch: &DeliveryBatch) -> Result<Scope, DeliveryFailure> {
        Ok(Scope {
            tenant: batch.binding.tenant.clone(),
            template: batch.binding.template.clone(),
            template_ref: batch
                .binding
                .template_ref
                .parse()
                .map_err(|_| DeliveryFailure::Rejected)?,
            binding_id: batch.binding.binding_id.clone(),
        })
    }
    async fn headers(&self, batch: &DeliveryBatch) -> Result<http::HeaderMap, DeliveryFailure> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut result = http::HeaderMap::new();
        result.insert(
            "authorization",
            "Bearer controlled-integration-purpose".parse().unwrap(),
        );
        result.insert("x-test-batch-id", batch.batch_id.parse().unwrap());
        result.insert("x-test-batch-digest", batch.digest.parse().unwrap());
        result.insert(
            "x-test-wire-bytes",
            serde_json::to_vec(batch)
                .unwrap()
                .len()
                .to_string()
                .parse()
                .unwrap(),
        );
        Ok(result)
    }
}

async fn receiver(
    path: &std::path::Path,
    binding_file: &std::path::Path,
    port: u16,
) -> (Child, serde_json::Value) {
    let python = std::env::var("ADX_INGESTION_TEST_PYTHON").unwrap_or_else(|_| "python3".into());
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ingestion_receiver.py");
    let mut child = Command::new(python)
        .arg("-B")
        .arg(script)
        .arg(path)
        .arg(binding_file)
        .arg(port.to_string())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(20),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    (
        child,
        serde_json::from_str(&line).expect("receiver startup JSON"),
    )
}

#[tokio::test]
async fn published_owner_refuses_missing_or_replaced_binding_without_creating_one() {
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Arc::new(adx_activator::Activator::new(
        state.clone(),
        Arc::new(Platform::default()),
    ));
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(
        activator.clone(),
    ))));
    let draft=serde_json::from_value(json!({"name":"digitalmate","image":"test:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":1234}]})).unwrap();
    activator.publish("tenant_one", &draft).await.unwrap();
    let scope = Scope {
        tenant: "tenant_one".into(),
        template: "digitalmate".into(),
        template_ref: "1".parse().unwrap(),
        binding_id: "binding_one".into(),
    };
    use crate::ingress::agent_service::{AgentV2Access, ServiceSelector};
    let access = AgentV2Access::new(managed.clone());
    assert!(access
        .select_with_initializer(
            Arc::new(RequestContext::new(Duration::from_secs(3))),
            scope.clone(),
            ServiceSelector {
                protocol: Protocol::Http,
                port: Some(1234)
            },
            async { Err::<adx_agent_core::mounts::Parameters, _>(adx_agent_api::Error::NotFound) },
        )
        .await
        .is_err());
    assert!(state.binding(&scope).await.unwrap().is_none());
    let context = RequestContext::new(Duration::from_secs(3));
    let (_target, _) = managed
        .resolve(&context, &scope, Protocol::Http, Some(1234))
        .await
        .unwrap();
    let mut selected = access
        .select_with_initializer(
            Arc::new(RequestContext::new(Duration::from_secs(3))),
            scope.clone(),
            ServiceSelector {
                protocol: Protocol::Http,
                port: Some(1234),
            },
            async { Err::<adx_agent_core::mounts::Parameters, _>(adx_agent_api::Error::NotFound) },
        )
        .await
        .unwrap();
    activator.delete_binding(&scope).await.unwrap();
    assert!(access.retry(&mut selected).await.is_err());
    assert!(state.binding(&scope).await.unwrap().is_none());
    assert!(access
        .select_with_initializer(
            Arc::new(RequestContext::new(Duration::from_secs(3))),
            scope.clone(),
            ServiceSelector {
                protocol: Protocol::Http,
                port: Some(1234)
            },
            async { Err::<adx_agent_core::mounts::Parameters, _>(adx_agent_api::Error::NotFound) },
        )
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL and Python standard library"]
async fn postgres_worker_delivers_over_real_relay_and_recovers_python_restart() {
    let inbox = super::persistence_tests::store().await;
    let temporary = tempfile::tempdir().unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let current = binding();
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let activator = Arc::new(adx_activator::Activator::new(
        state.clone(),
        Arc::new(Platform::default()),
    ));
    let managed = Arc::new(ManagedService::new(Arc::new(LocalControl::new(
        activator.clone(),
    ))));
    let draft=serde_json::from_value(json!({"name":"digitalmate","image":"test:1","isolation_runtime":"runc","resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"http","port":port}]})).unwrap();
    activator.publish(&current.tenant, &draft).await.unwrap();
    let scope = Scope {
        tenant: current.tenant.clone(),
        template: current.template.clone(),
        template_ref: current.template_ref.parse().unwrap(),
        binding_id: current.binding_id.clone(),
    };
    let (target, _) = managed
        .resolve(
            &RequestContext::new(Duration::from_secs(3)),
            &scope,
            Protocol::Http,
            Some(port),
        )
        .await
        .unwrap();
    let binding_file = temporary.path().join("binding.json");
    std::fs::write(&binding_file, serde_json::to_vec(&current).unwrap()).unwrap();
    let database = temporary.path().join("receiver.sqlite");
    let (mut process, start) = receiver(&database, &binding_file, port).await;
    assert_eq!(start["counts"], json!([0, 0]));
    let relay = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let serving = relay.clone();
    let relay_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let serving = serving.clone();
            tokio::spawn(async move {
                let _ = serving.serve_h2(stream).await;
            });
        }
    });
    let route:crate::common::route::RouteInfo=serde_json::from_value(json!({"instanceID":target.binding.current.environment_id,"instanceStatus":{"code":3},"tenantID":current.tenant,"environmentID":"runtime-local","environmentIP":"127.0.0.1","nodeProxyAddress":address.to_string()})).unwrap();
    relay
        .activate_route(
            route.instance_id.clone(),
            route.environment_id.clone(),
            route.environment_ip.parse().unwrap(),
        )
        .await;
    let routes = Arc::new(RouteStore::new());
    routes.set_ready(true);
    routes.put(route);
    let config = serde_json::from_value(json!({"tenant":"tenant_one", "jiuwen":{"agent_type":"jiuwen","template":"jiuwen","version":"1"}})).unwrap();
    let api = super::super::entrypoint::DigitalMateGatewayApi::new(
        config,
        managed.clone(),
        Arc::new(Identity),
    )
    .unwrap()
    .with_ingestion_webhook(Arc::new(WebhookHttp::new(
        Arc::new(WebhookVerifier::new(&[SECRET]).unwrap()),
        inbox.clone(),
    )));
    let ingress = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(routes)),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::with_verifier(Arc::new(Identity)),
            port,
            8765,
            "127.0.0.1:1",
            vec![],
        )
        .with_client_acl(vec!["127.0.0.0/8".parse().unwrap()], false)
        .with_digitalmategateway_api(Arc::new(api)),
    );
    let purpose = Arc::new(Purpose(std::sync::atomic::AtomicUsize::new(0)));
    let delivery = Arc::new(
        RelayDelivery::new(
            managed.clone(),
            ingress.clone(),
            purpose.clone(),
            Duration::from_secs(8),
            port,
        )
        .unwrap_or_else(|_| panic!("delivery configuration")),
    );
    inbox.register(&current).await.unwrap();
    let raw = body("msg_cross_language", "private mail");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}{}", listener.local_addr().unwrap(), WEBHOOK_PATH);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let http_task = tokio::spawn(ingress.serve_http(listener, shutdown));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(&url)
            .body(raw.clone())
            .send()
            .await
            .unwrap()
            .status(),
        http::StatusCode::UNAUTHORIZED
    );
    let database_client = inbox.pool().get().await.unwrap();
    database_client
        .batch_execute("ALTER TABLE digitalmate_ingestion.events RENAME TO events_unavailable")
        .await
        .unwrap();
    assert_eq!(
        client
            .post(&url)
            .headers(headers(&raw, now, SECRET))
            .body(raw.clone())
            .send()
            .await
            .unwrap()
            .status(),
        http::StatusCode::SERVICE_UNAVAILABLE
    );
    database_client
        .batch_execute("ALTER TABLE digitalmate_ingestion.events_unavailable RENAME TO events")
        .await
        .unwrap();
    drop(database_client);
    assert_eq!(
        client
            .post(&url)
            .headers(headers(&raw, now, SECRET))
            .body(raw.clone())
            .send()
            .await
            .unwrap()
            .status(),
        http::StatusCode::OK
    );
    assert_eq!(
        client
            .post(&url)
            .headers(headers(&raw, now, SECRET))
            .body(raw)
            .send()
            .await
            .unwrap()
            .status(),
        http::StatusCode::OK
    );
    let lease = inbox.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    // Commit at Python, intentionally discard the reply before PostgreSQL ACK.
    let ack = delivery
        .deliver(&lease.batch)
        .await
        .unwrap_or_else(|_| panic!("real relay first delivery"));
    assert_eq!(ack.state, "accepted");
    assert!(
        purpose.0.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "each physical receipt query and batch POST must request fresh current-purpose proof"
    );
    process.kill().await.unwrap();
    process.wait().await.unwrap();
    let (mut restarted, start) = receiver(&database, &binding_file, port).await;
    assert_eq!(start["counts"], json!([1, 1]));
    inbox
        .retry(&lease, Duration::ZERO, "delivery_unknown")
        .await
        .unwrap();
    let runtime =
        IngestionRuntime::start(inbox.clone(), delivery, 2, Duration::from_secs(30)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let client = inbox.pool().get().await.unwrap();
            let state: String = client
                .query_one(
                    "SELECT state FROM digitalmate_ingestion.batches WHERE batch_id=$1",
                    &[&lease.batch.batch_id],
                )
                .await
                .unwrap()
                .get(0);
            if state == "accepted" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    runtime.stop().await.unwrap();
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
    restarted.kill().await.unwrap();
    restarted.wait().await.unwrap();
    let (mut final_process, start) = receiver(&database, &binding_file, port).await;
    assert_eq!(
        start["counts"],
        json!([1, 1]),
        "GET reconciliation must not create another effect"
    );
    final_process.kill().await.unwrap();
    final_process.wait().await.unwrap();
    stop.send(true).unwrap();
    http_task.await.unwrap().unwrap();
    relay_task.abort();
}
