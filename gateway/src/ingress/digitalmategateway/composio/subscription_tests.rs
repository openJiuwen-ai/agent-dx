//! Actual project HTTP client; all account/trigger responses are controlled fixtures.
use super::super::ingestion::{SubscriptionBinding, SubscriptionSnapshot, TriggerPort};
use super::client::Client;
use super::subscriptions::ProjectTriggers;
use super::*;
use ::http::{Request, Response};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    sync::{Arc, Mutex},
};

struct ControlledBudget;
#[async_trait]
impl Budget for ControlledBudget {
    async fn acquire(&self, _: &Scope) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
struct WireRequest {
    method: String,
    path: String,
    body: Value,
    has_business_bearer: bool,
}
struct Fixture {
    client: ProjectTriggers,
    scope: Scope,
    requests: Arc<Mutex<Vec<WireRequest>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    fn trace(&self) -> String {
        format!(
            "server_finished={} requests={:?}",
            self.server.is_finished(),
            self.requests.lock().unwrap()
        )
    }
    async fn ensured(&self) -> super::super::ingestion::ProviderTrigger {
        self.client
            .ensure(&self.scope, &snapshot())
            .await
            .unwrap_or_else(|error| panic!("{error:?}: {}", self.trace()))
    }
}

async fn fixture(replies: Vec<(u16, Value)>) -> Fixture {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let recorded = recorded.clone();
            let replies = replies.clone();
            http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request: Request<Incoming>| {
                        let recorded = recorded.clone();
                        let replies = replies.clone();
                        async move {
                            let method = request.method().to_string();
                            let path = request.uri().to_string();
                            let has_business_bearer =
                                request.headers().contains_key("authorization");
                            assert_eq!(
                                request.headers().get("x-api-key").unwrap(),
                                "controlled-project-key"
                            );
                            let bytes = request.into_body().collect().await.unwrap().to_bytes();
                            let body = if bytes.is_empty() {
                                Value::Null
                            } else {
                                serde_json::from_slice(&bytes).unwrap()
                            };
                            recorded.lock().unwrap().push(WireRequest {
                                method,
                                path,
                                body,
                                has_business_bearer,
                            });
                            let (status, body) = replies
                                .lock()
                                .unwrap()
                                .pop_front()
                                .expect("unexpected project request");
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("connection", "close")
                                    .body(Full::new(Bytes::from(
                                        serde_json::to_vec(&body).unwrap(),
                                    )))
                                    .unwrap(),
                            )
                        }
                    }),
                )
                .await
                .unwrap();
        }
    });
    let configs = MailAuthConfigs {
        gmail: None,
        outlook: Some(MailAuthConfig::custom("ac_fixture".into())),
    };
    let mut client = Client::new(
        "controlled-project-key".into(),
        configs,
        Arc::new(ControlledBudget),
    )
    .unwrap();
    client.origin = format!("http://{address}"); // Controlled loopback only; production origin is unchanged.
    Fixture {
        client: ProjectTriggers::new(client),
        scope: scope(),
        requests,
        server,
    }
}

fn scope() -> Scope {
    Scope::from_authenticated_foreground("tenant".into(), "user".into(), "workspace".into())
        .unwrap()
}
fn account() -> Value {
    json!({"id":"ca_fixture", "user_id":scope().composio_user_id(), "status":"ACTIVE", "experimental":{"account_type":"PRIVATE"}, "toolkit":{"slug":"outlook"},
        "auth_config":{"id":"ac_fixture", "auth_scheme":"OAUTH2", "is_composio_managed":false}})
}
fn trigger(id: &str, disabled: bool) -> Value {
    json!({"id":id, "connected_account_id":"ca_fixture", "trigger_name":"OUTLOOK_MESSAGE_TRIGGER",
        "user_id":scope().composio_user_id(), "trigger_config":{"folder_id":"inbox", "from_email":null}, "disabled_at":if disabled {json!("2026-11-13T00:00:00Z")} else {Value::Null}})
}
fn page(items: Vec<Value>) -> Value {
    json!({"items":items, "next_cursor":null})
}
fn snapshot() -> SubscriptionSnapshot {
    SubscriptionSnapshot {
        binding_scope: adx_agent_core::Scope {
            tenant: "tenant".into(),
            template: "digitalmate".into(),
            template_ref: "1".parse().unwrap(),
            binding_id: "binding".into(),
        },
        user_id: "user".into(),
        workspace: "workspace".into(),
        binding_generation: "epoch".into(),
        service_port: 18092,
        connection_id: "connection".into(),
        provider: "microsoft".into(),
        trigger_slug: "OUTLOOK_MESSAGE_TRIGGER".into(),
        account_id: "ca_fixture".into(),
        auth_config_id: "ac_fixture".into(),
        composio_user_id: scope().composio_user_id(),
        connection_generation: 1,
        record_revision: 1,
        intent_revision: 1,
        enabled: true,
        usable: true,
    }
}
fn binding() -> SubscriptionBinding {
    SubscriptionBinding {
        tenant: "tenant".into(),
        user_id: "user".into(),
        connection_id: "connection".into(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        subscription_id: "sub_fixture".into(),
        subscription_generation: 1,
        connection_generation: 1,
        workspace: "workspace".into(),
        binding_id: "binding".into(),
        binding_generation: "epoch".into(),
        service_port: 18092,
        trigger_id: "ti_fixture".into(),
        trigger_slug: "OUTLOOK_MESSAGE_TRIGGER".into(),
        account_id: "ca_fixture".into(),
        auth_config_id: "ac_fixture".into(),
        composio_user_id: scope().composio_user_id(),
        enabled: true,
    }
}

#[tokio::test]
async fn project_subscription_reuses_verified_instance_instead_of_creating_duplicates() {
    let f = fixture(vec![
        (200, account()),
        (200, page(vec![trigger("ti_fixture", false)])),
        (200, account()),
        (200, page(vec![trigger("ti_fixture", false)])),
    ])
    .await;
    for _ in 0..2 {
        let actual = f.ensured().await;
        assert_eq!(actual.trigger_id, "ti_fixture");
        assert!(actual.enabled);
    }
    assert!(f
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.method == "GET" && !r.has_business_bearer));
}

#[tokio::test]
async fn project_subscription_creation_pins_account_and_rechecks_upsert_result() {
    let f = fixture(vec![
        (200, account()),
        (200, page(vec![])),
        (201, json!({"trigger_id":"ti_fixture"})),
        (200, page(vec![trigger("ti_fixture", false)])),
    ])
    .await;
    let actual = f.ensured().await;
    assert_eq!(actual.trigger_id, "ti_fixture");
    let requests = f.requests.lock().unwrap();
    let post = requests.iter().find(|r| r.method == "POST").unwrap();
    assert_eq!(
        post.path,
        "/api/v3.1/trigger_instances/OUTLOOK_MESSAGE_TRIGGER/upsert"
    );
    assert_eq!(
        post.body,
        json!({"connected_account_id":"ca_fixture", "user_id":f.scope.composio_user_id(), "trigger_config":{}})
    );
    let listing = reqwest::Url::parse(&format!("http://localhost{}", requests[1].path)).unwrap();
    let query: BTreeMap<_, _> = listing.query_pairs().into_owned().collect();
    assert_eq!(query.get("connected_account_ids").unwrap(), "ca_fixture");
    assert_eq!(query.get("auth_config_ids").unwrap(), "ac_fixture");
    assert_eq!(query.get("user_ids").unwrap(), &f.scope.composio_user_id());
    assert!(requests.iter().all(|r| !r.has_business_bearer));
}

#[tokio::test]
async fn project_subscription_foreign_trigger_is_refused_before_an_effect() {
    let mut wrong = trigger("ti_foreign", false);
    wrong["user_id"] = json!("foreign_user");
    let f = fixture(vec![(200, account()), (200, page(vec![wrong]))]).await;
    let result = f.client.ensure(&f.scope, &snapshot()).await;
    assert!(
        matches!(&result, Err(Error::AccountMismatch)),
        "error={:?}: {}",
        result.err(),
        f.trace()
    );
    assert!(f.requests.lock().unwrap().iter().all(|r| r.method == "GET"));
}

#[tokio::test]
async fn project_subscription_unknown_upsert_is_reconciled_without_a_second_creation() {
    let f = fixture(vec![
        (200, account()),
        (200, page(vec![])),
        (500, json!({})),
        (200, account()),
        (200, page(vec![trigger("ti_fixture", false)])),
    ])
    .await;
    assert!(matches!(
        f.client.ensure(&f.scope, &snapshot()).await,
        Err(Error::OutcomeUnknown)
    ));
    assert_eq!(f.ensured().await.trigger_id, "ti_fixture");
    assert_eq!(
        f.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn project_subscription_disable_unknown_is_resolved_from_provider_state() {
    let f = fixture(vec![
        (200, page(vec![trigger("ti_fixture", false)])),
        (500, json!({})),
        (200, page(vec![trigger("ti_fixture", true)])),
    ])
    .await;
    assert!(matches!(
        f.client.disable(&f.scope, &binding()).await,
        Err(Error::OutcomeUnknown)
    ));
    f.client
        .disable(&f.scope, &binding())
        .await
        .unwrap_or_else(|error| panic!("{error:?}: {}", f.trace()));
    let requests = f.requests.lock().unwrap();
    let effects: Vec<_> = requests.iter().filter(|r| r.method == "PATCH").collect();
    assert_eq!(effects.len(), 1);
    assert_eq!(
        effects[0].path,
        "/api/v3.1/trigger_instances/manage/ti_fixture"
    );
    assert_eq!(
        effects[0].body,
        json!({"status":"disable", "user_id":f.scope.composio_user_id()})
    );
}

#[tokio::test]
async fn project_subscription_paginated_ambiguity_cannot_choose_a_first_instance() {
    let first = json!({"items":[trigger("ti_one",false)],"next_cursor":"page-two"});
    let f = fixture(vec![
        (200, account()),
        (200, first),
        (200, page(vec![trigger("ti_two", false)])),
    ])
    .await;
    assert!(matches!(
        f.client.ensure(&f.scope, &snapshot()).await,
        Err(Error::InvalidResponse)
    ));
    assert!(f.requests.lock().unwrap().iter().all(|r| r.method == "GET"));
}

#[tokio::test]
async fn project_subscription_does_not_reuse_a_filter_that_would_drop_ordinary_mail() {
    let mut filtered = trigger("ti_filtered", false);
    filtered["trigger_config"] = json!({"importance":"high"});
    let f = fixture(vec![
        (200, account()),
        (200, page(vec![filtered])),
        (201, json!({"trigger_id":"ti_fixture"})),
        (200, page(vec![trigger("ti_fixture", false)])),
    ])
    .await;
    assert_eq!(
        f.client
            .ensure(&f.scope, &snapshot())
            .await
            .unwrap()
            .trigger_id,
        "ti_fixture"
    );
    assert_eq!(
        f.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
}
