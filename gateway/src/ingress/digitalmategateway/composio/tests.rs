use super::*;
use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::any,
    Json, Router,
};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};

struct ControlledAuthority {
    scope: Scope,
    attempt: Mutex<Attempt>,
    grant: Mutex<ConnectionGrant>,
    revoked: AtomicBool,
    writes: AtomicUsize,
    replace_on_accept: AtomicBool,
    receipt: Mutex<Option<(u64, VerifiedAccount)>>,
}

impl ControlledAuthority {
    fn check_scope(&self, scope: &Scope) -> Result<(), Error> {
        if self.scope == *scope {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
}

#[async_trait]
impl Authority for ControlledAuthority {
    async fn attempt(&self, scope: &Scope, registration: &str) -> Result<Attempt, Error> {
        self.check_scope(scope)?;
        let attempt = self.attempt.lock().unwrap().clone();
        if attempt.registration != registration {
            return Err(Error::Forbidden);
        }
        Ok(attempt)
    }
    async fn cancel_attempt(&self, scope: &Scope, registration: &str) -> Result<(), Error> {
        self.attempt(scope, registration).await?;
        self.attempt.lock().unwrap().phase = AttemptPhase::Cancelled;
        Ok(())
    }
    async fn verified_receipt(
        &self,
        scope: &Scope,
        expected: &Attempt,
    ) -> Result<Option<VerifiedAccount>, Error> {
        self.check_scope(scope)?;
        let current = self.attempt.lock().unwrap();
        if current.revision != expected.revision
            || current.phase != AttemptPhase::Complete
            || self.revoked.load(Ordering::SeqCst)
        {
            return Err(Error::AuthorizationInactive);
        }
        Ok(self
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(revision, _)| *revision == current.revision)
            .map(|(_, proof)| proof.clone()))
    }
    async fn claim_start(&self, scope: &Scope, expected: &Attempt) -> Result<(), Error> {
        self.check_scope(scope)?;
        let mut current = self.attempt.lock().unwrap();
        current.check()?;
        if current.revision != expected.revision || current.phase != AttemptPhase::Ready {
            return Err(Error::AuthorizationInactive);
        }
        current.phase = AttemptPhase::Starting;
        Ok(())
    }
    async fn bind_started(
        &self,
        scope: &Scope,
        expected: &Attempt,
        started: &Started,
    ) -> Result<(), Error> {
        self.check_scope(scope)?;
        let mut current = self.attempt.lock().unwrap();
        current.check()?;
        if current.revision != expected.revision || current.phase != AttemptPhase::Starting {
            return Err(Error::AuthorizationInactive);
        }
        current.account_id.clone_from(&started.account_id);
        current.phase = AttemptPhase::Waiting;
        Ok(())
    }
    async fn accept_verified(
        &self,
        scope: &Scope,
        expected: &Attempt,
        account: &VerifiedAccount,
    ) -> Result<(), Error> {
        self.check_scope(scope)?;
        let mut current = self.attempt.lock().unwrap();
        if self.replace_on_accept.load(Ordering::SeqCst) {
            current.revision += 1;
        }
        current.check()?;
        if current.revision != expected.revision || current.phase != AttemptPhase::Waiting {
            return Err(Error::AuthorizationInactive);
        }
        current.phase = AttemptPhase::Complete;
        *self.receipt.lock().unwrap() = Some((current.revision, account.clone()));
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn connection(&self, scope: &Scope, connection: &str) -> Result<ConnectionGrant, Error> {
        self.check_scope(scope)?;
        if self.revoked.load(Ordering::SeqCst) || connection != "connection-one" {
            return Err(Error::Forbidden);
        }
        Ok(self.grant.lock().unwrap().clone())
    }
    async fn assert_connection(
        &self,
        scope: &Scope,
        connection: &str,
        grant: &ConnectionGrant,
    ) -> Result<(), Error> {
        let current = self.connection(scope, connection).await?;
        if current.generation == grant.generation && current.account_id == grant.account_id {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
    async fn fence_connection(
        &self,
        scope: &Scope,
        connection: &str,
    ) -> Result<ConnectionGrant, Error> {
        self.check_scope(scope)?;
        if connection != "connection-one" {
            return Err(Error::Forbidden);
        }
        self.revoked.store(true, Ordering::SeqCst);
        Ok(self.grant.lock().unwrap().clone())
    }
}

struct ControlledBudget {
    calls: AtomicUsize,
    refuse: AtomicBool,
}
#[async_trait]
impl Budget for ControlledBudget {
    async fn acquire(&self, _: &Scope) -> Result<(), Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.refuse.load(Ordering::SeqCst) {
            Err(Error::RateLimited {
                retry_after: Some(3),
            })
        } else {
            Ok(())
        }
    }
}

type Calls = Arc<Mutex<Vec<(String, Value)>>>;
struct HttpState {
    replies: Mutex<VecDeque<(StatusCode, Value)>>,
    calls: Calls,
    authority: Arc<ControlledAuthority>,
    revoke_during_mail: AtomicBool,
    rebind_during_mail: AtomicBool,
}

async fn upstream(
    State(state): State<Arc<HttpState>>,
    request: axum::http::Request<axum::body::Body>,
) -> (StatusCode, HeaderMap, Json<Value>) {
    assert_eq!(
        request.headers().get("x-api-key").unwrap(),
        "controlled-project-key"
    );
    let path = request.uri().path().to_string();
    let bytes = axum::body::to_bytes(request.into_body(), 100_000)
        .await
        .unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    if body
        .get("endpoint")
        .and_then(Value::as_str)
        .is_some_and(|url| url.contains("/messages"))
        && state.revoke_during_mail.load(Ordering::SeqCst)
    {
        state.authority.revoked.store(true, Ordering::SeqCst);
    }
    state.calls.lock().unwrap().push((path, body.clone()));
    let mut response = state
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected provider request");
    if state.rebind_during_mail.load(Ordering::SeqCst)
        && body
            .get("endpoint")
            .and_then(Value::as_str)
            .is_some_and(|url| url.contains("/messages"))
    {
        // Controlled Google semantics: a token rebound to another mailbox
        // selects that mailbox with `me`, but cannot read the pinned old userId.
        response = if body["endpoint"].as_str().unwrap().contains("/users/me/") {
            (
                StatusCode::OK,
                json!({"status":200,"data":{"messages":[{"id":"other-mailbox-secret"}]}}),
            )
        } else {
            (
                StatusCode::OK,
                json!({"status":403,"data":{"error":{"message":"wrong mailbox token"}}}),
            )
        };
    }
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", "17".parse().unwrap());
    (response.0, headers, Json(response.1))
}

struct Fixture {
    gateway: Composio,
    scope: Scope,
    authority: Arc<ControlledAuthority>,
    budget: Arc<ControlledBudget>,
    state: Arc<HttpState>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope::from_authenticated_foreground(
            "tenant".into(),
            "user".into(),
            "workspace".into(),
        )
        .unwrap();
        let authority = Arc::new(ControlledAuthority {
            scope: scope.clone(),
            attempt: Mutex::new(Attempt {
                registration: "registration-one".into(),
                revision: 1,
                deadline_epoch_seconds: u64::MAX,
                phase: AttemptPhase::Waiting,
                account_id: "ca_one".into(),
                expected_email: None,
            }),
            grant: Mutex::new(ConnectionGrant {
                account_id: "ca_one".into(),
                email: "person@example.com".into(),
                generation: 1,
                permissions: Permissions {
                    search: true,
                    read_headers: true,
                    read_body: false,
                },
            }),
            revoked: AtomicBool::new(false),
            writes: AtomicUsize::new(0),
            replace_on_accept: AtomicBool::new(false),
            receipt: Mutex::new(None),
        });
        let budget = Arc::new(ControlledBudget {
            calls: AtomicUsize::new(0),
            refuse: AtomicBool::new(false),
        });
        let state = Arc::new(HttpState {
            replies: Mutex::new(VecDeque::new()),
            calls: Arc::new(Mutex::new(vec![])),
            authority: authority.clone(),
            revoke_during_mail: AtomicBool::new(false),
            rebind_during_mail: AtomicBool::new(false),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(upstream))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut gateway = Composio::new(
            "controlled-project-key".into(),
            "ac_custom_gmail".into(),
            authority.clone(),
            budget.clone(),
        )
        .unwrap();
        gateway.client.origin = format!("http://{address}");
        Self {
            gateway,
            scope,
            authority,
            budget,
            state,
            server,
        }
    }
    fn reply(&self, status: u16, body: Value) {
        self.state
            .replies
            .lock()
            .unwrap()
            .push_back((StatusCode::from_u16(status).unwrap(), body));
    }
    fn account(&self) -> Value {
        json!({"id":"ca_one", "user_id":self.scope.composio_user_id(), "status":"ACTIVE", "authScheme":"OAUTH2",
            "alias":self.authority.attempt.lock().unwrap().alias(), "toolkit":{"slug":"gmail"},
            "auth_config":{"id":"ac_custom_gmail","auth_scheme":"OAUTH2","is_composio_managed":false,"is_disabled":false},
            "is_disabled":false, "state":{"val":{"access_token":"never-return-this", "refresh_token":"never-return-this"}}})
    }
    fn reads(&self, data: Value) {
        self.reply(200, self.account());
        self.reply(
            200,
            json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
        );
        self.reply(200, json!({"status":200,"data":data}));
    }
    fn search(&self) -> GmailRequest {
        GmailRequest::Search {
            query: "from:person@example.com".into(),
            limit: 5,
            page: "page-one".into(),
        }
    }
}

#[test]
fn scope_mapping_is_stable_and_unambiguous() {
    let a = Scope::from_authenticated_foreground("ab".into(), "c".into(), "w".into()).unwrap();
    let b = Scope::from_authenticated_foreground("a".into(), "bc".into(), "w".into()).unwrap();
    assert_ne!(a.composio_user_id(), b.composio_user_id());
    assert_eq!(a.composio_user_id(), a.clone().composio_user_id());
    assert!(Scope::from_authenticated_foreground("".into(), "u".into(), "w".into()).is_err());
}

#[tokio::test]
async fn restricted_search_pins_user_account_method_and_endpoint() {
    let f = Fixture::new().await;
    f.reads(json!({"messages":[{"id":"18abc","threadId":"18def"}],"nextPageToken":"page-two"}));
    let result = f
        .gateway
        .gmail(&f.scope, "connection-one", f.search())
        .await
        .unwrap();
    assert_eq!(result["nextPageToken"], "page-two");
    let calls = f.state.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[2].0, "/api/v3.1/tools/execute/proxy");
    assert_eq!(calls[2].1["method"], "GET");
    assert_eq!(calls[2].1["connected_account_id"], "ca_one");
    assert_eq!(calls[2].1["user_id"], f.scope.composio_user_id());
    assert!(calls[2].1["endpoint"]
        .as_str()
        .unwrap()
        .starts_with("https://gmail.googleapis.com/gmail/v1/users/person@example.com/messages?"));
    assert!(!result.to_string().contains("never-return-this"));
    assert_eq!(f.budget.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn other_scope_and_other_connection_rejected_without_http() {
    let f = Fixture::new().await;
    let other =
        Scope::from_authenticated_foreground("tenant".into(), "other".into(), "workspace".into())
            .unwrap();
    assert_eq!(
        f.gateway
            .gmail(&other, "connection-one", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-other", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn active_does_not_grant_body_permission() {
    let f = Fixture::new().await;
    assert_eq!(
        f.gateway
            .gmail(
                &f.scope,
                "connection-one",
                GmailRequest::Read {
                    message_id: "18abc".into(),
                    body: true
                }
            )
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn account_ownership_custom_oauth_and_status_checked_online() {
    for variant in [
        "user", "toolkit", "config", "managed", "disabled", "expired", "shared",
    ] {
        let f = Fixture::new().await;
        let mut account = f.account();
        match variant {
            "user" => account["user_id"] = json!("other"),
            "toolkit" => account["toolkit"]["slug"] = json!("slack"),
            "config" => account["auth_config"]["id"] = json!("ac_other"),
            "managed" => account["auth_config"]["is_composio_managed"] = json!(true),
            "disabled" => account["is_disabled"] = json!(true),
            "expired" => account["status"] = json!("EXPIRED"),
            "shared" => account["experimental"] = json!({"account_type":"SHARED"}),
            _ => unreachable!(),
        }
        f.reply(200, account);
        assert!(
            f.gateway
                .gmail(&f.scope, "connection-one", f.search())
                .await
                .is_err(),
            "{variant}"
        );
        assert_eq!(f.state.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn mailbox_switch_cannot_silently_change_account() {
    let f = Fixture::new().await;
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"other@example.com"}}),
    );
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::AccountMismatch)
    );
}

#[tokio::test]
async fn forged_callback_account_cannot_be_saved() {
    let f = Fixture::new().await;
    f.reply(
        200,
        json!({"connected_account_id":"ca_other", "toolkit_slug":"gmail"}),
    );
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "opaque-session")
            .await
            .err(),
        Some(Error::AccountMismatch)
    );
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
    let calls = f.state.calls.lock().unwrap();
    assert_eq!(calls[0].1["user_id"], f.scope.composio_user_id());
}

#[tokio::test]
async fn cancelled_expired_attempts_rejected_before_http() {
    for phase in [AttemptPhase::Cancelled, AttemptPhase::Expired] {
        let f = Fixture::new().await;
        f.authority.attempt.lock().unwrap().phase = phase;
        assert_eq!(
            f.gateway
                .confirm(&f.scope, "registration-one", "opaque")
                .await
                .err(),
            Some(Error::AuthorizationInactive)
        );
        assert!(f.state.calls.lock().unwrap().is_empty());
    }
    let f = Fixture::new().await;
    f.authority.attempt.lock().unwrap().deadline_epoch_seconds = 0;
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "opaque")
            .await
            .err(),
        Some(Error::AuthorizationInactive)
    );
}

#[tokio::test]
async fn late_completion_rechecks_owner_revision_at_write() {
    let f = Fixture::new().await;
    f.authority.replace_on_accept.store(true, Ordering::SeqCst);
    f.reply(
        200,
        json!({"connected_account_id":"ca_one", "toolkit_slug":"gmail"}),
    );
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
    );
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "opaque")
            .await
            .err(),
        Some(Error::AuthorizationInactive)
    );
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn disconnect_fences_access_even_when_remote_revoke_fails() {
    let f = Fixture::new().await;
    f.reply(200, f.account());
    f.reply(500, json!({"error":"secret body must never escape"}));
    assert!(f
        .gateway
        .disconnect(&f.scope, "connection-one")
        .await
        .is_err());
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn midflight_disconnect_suppresses_late_content() {
    let f = Fixture::new().await;
    f.state.revoke_during_mail.store(true, Ordering::SeqCst);
    f.reads(json!({"messages":[{"id":"18abc","threadId":"18def"}]}));
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
}

#[tokio::test]
async fn budget_refusal_never_dispatches_http() {
    let f = Fixture::new().await;
    f.budget.refuse.store(true, Ordering::SeqCst);
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::RateLimited {
            retry_after: Some(3)
        })
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rate_limit_and_upstream_scope_failures_remain_distinct() {
    let f = Fixture::new().await;
    f.reply(429, json!({"error":"sensitive"}));
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::RateLimited {
            retry_after: Some(17)
        })
    );
    let f = Fixture::new().await;
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":403,"data":{"error":{"message":"private mailbox content"}}}),
    );
    let error = f
        .gateway
        .gmail(&f.scope, "connection-one", f.search())
        .await
        .err()
        .unwrap();
    assert_eq!(error, Error::ConsentRequired);
    assert_eq!(error.to_string(), "consent_required");
}

#[tokio::test]
async fn invalid_message_id_and_page_size_do_not_dispatch() {
    let f = Fixture::new().await;
    for request in [
        GmailRequest::Read {
            message_id: "../profile".into(),
            body: false,
        },
        GmailRequest::Search {
            query: "".into(),
            limit: 0,
            page: "".into(),
        },
    ] {
        assert_eq!(
            f.gateway
                .gmail(&f.scope, "connection-one", request)
                .await
                .err(),
            Some(Error::InvalidRequest)
        );
    }
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn start_claims_once_and_binds_exact_provider_response() {
    let f = Fixture::new().await;
    f.authority.attempt.lock().unwrap().phase = AttemptPhase::Ready;
    f.reply(201, json!({"connected_account_id":"ca_new", "redirect_url":"https://connect.composio.dev/link/opaque",
        "expires_at":"2026-11-12T12:00:00Z", "link_token":"not-exposed"}));
    let started = f.gateway.start(&f.scope, "registration-one").await.unwrap();
    assert_eq!(started.account_id, "ca_new");
    assert_eq!(f.authority.attempt.lock().unwrap().account_id, "ca_new");
    assert_eq!(
        f.gateway.start(&f.scope, "registration-one").await.err(),
        Some(Error::OutcomeUnknown)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn creation_unknown_never_blindly_posts_again() {
    let f = Fixture::new().await;
    f.authority.attempt.lock().unwrap().phase = AttemptPhase::Ready;
    f.reply(500, json!({"error":"may have created account"}));
    assert_eq!(
        f.gateway.start(&f.scope, "registration-one").await.err(),
        Some(Error::OutcomeUnknown)
    );
    assert_eq!(
        f.gateway.start(&f.scope, "registration-one").await.err(),
        Some(Error::OutcomeUnknown)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn success_and_lost_complete_reply_reconcile_only_bound_account() {
    let f = Fixture::new().await;
    f.reply(
        200,
        json!({"connected_account_id":"ca_one", "toolkit_slug":"gmail"}),
    );
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
    );
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "opaque")
            .await
            .unwrap()
            .email,
        "person@example.com"
    );
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 1);
    let f = Fixture::new().await;
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
    );
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .unwrap()
            .account_id,
        "ca_one"
    );
    assert!(f
        .state
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(path, _)| !path.contains("complete_auth")));
}

#[tokio::test]
async fn mismatched_attempt_alias_is_not_authorization_proof() {
    let f = Fixture::new().await;
    let mut account = f.account();
    account["alias"] = json!("another-attempt");
    f.reply(200, account);
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .err(),
        Some(Error::AccountMismatch)
    );
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pending_authorization_is_not_a_read_grant_or_reauth_error() {
    let f = Fixture::new().await;
    let mut account = f.account();
    account["status"] = json!("INITIALIZING");
    f.reply(200, account);
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .err(),
        Some(Error::AuthorizationPending)
    );
    f.gateway
        .cancel(&f.scope, "registration-one")
        .await
        .unwrap();
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .err(),
        Some(Error::AuthorizationInactive)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn already_revoked_reconciles_without_repeating_effect() {
    let f = Fixture::new().await;
    let mut account = f.account();
    account["status"] = json!("REVOKED");
    f.reply(200, account);
    f.gateway
        .disconnect(&f.scope, "connection-one")
        .await
        .unwrap();
    assert!(f.authority.revoked.load(Ordering::SeqCst));
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn read_metadata_and_body_formats_are_explicit() {
    for body in [false, true] {
        let f = Fixture::new().await;
        f.authority.grant.lock().unwrap().permissions.read_body = true;
        f.reads(json!({"id":"18abc"}));
        f.gateway
            .gmail(
                &f.scope,
                "connection-one",
                GmailRequest::Read {
                    message_id: "18abc".into(),
                    body,
                },
            )
            .await
            .unwrap();
        let calls = f.state.calls.lock().unwrap();
        let endpoint = calls[2].1["endpoint"].as_str().unwrap();
        assert!(endpoint.contains(if body {
            "format=full"
        } else {
            "format=metadata"
        }));
        assert!(!endpoint.contains("attachments"));
    }
}

#[tokio::test]
async fn response_size_cap_and_project_key_failure_are_safe() {
    let f = Fixture::new().await;
    f.reply(200, json!({"untrusted": "x".repeat(1_048_577)}));
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::ResponseTooLarge)
    );
    let f = Fixture::new().await;
    f.reply(401, json!({"error":"project-key-secret"}));
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::NotConfigured)
    );
}

#[tokio::test]
async fn committed_receipt_survives_lost_reply_without_replaying_effects() {
    let f = Fixture::new().await;
    f.reply(
        200,
        json!({"connected_account_id":"ca_one", "toolkit_slug":"gmail"}),
    );
    f.reply(200, f.account());
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
    );
    // Pretend the successful response never reached the sandbox consumer.
    let _ = f
        .gateway
        .confirm(&f.scope, "registration-one", "single-use")
        .await
        .unwrap();
    let recovered = f
        .gateway
        .reconcile_authorization(&f.scope, "registration-one")
        .await
        .unwrap();
    assert_eq!(recovered.account_id, "ca_one");
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "single-use")
            .await
            .unwrap()
            .email,
        recovered.email
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 1);
    f.gateway
        .cancel(&f.scope, "registration-one")
        .await
        .unwrap();
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .err(),
        Some(Error::AuthorizationInactive)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn completed_without_current_receipt_never_fabricates_success() {
    let f = Fixture::new().await;
    f.authority.attempt.lock().unwrap().phase = AttemptPhase::Complete;
    *f.authority.receipt.lock().unwrap() = Some((
        0,
        VerifiedAccount {
            account_id: "ca_one".into(),
            email: "person@example.com".into(),
        },
    ));
    assert_eq!(
        f.gateway
            .reconcile_authorization(&f.scope, "registration-one")
            .await
            .err(),
        Some(Error::OutcomeUnknown)
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_success_for_effect_is_unknown_not_a_clean_failure() {
    for action in ["link", "complete", "revoke"] {
        let f = Fixture::new().await;
        if action == "revoke" {
            f.reply(200, f.account());
        }
        f.reply(200, json!([]));
        let error = match action {
            "link" => {
                f.authority.attempt.lock().unwrap().phase = AttemptPhase::Ready;
                f.gateway.start(&f.scope, "registration-one").await.err()
            }
            "complete" => f
                .gateway
                .confirm(&f.scope, "registration-one", "opaque")
                .await
                .err(),
            "revoke" => f.gateway.disconnect(&f.scope, "connection-one").await.err(),
            _ => unreachable!(),
        };
        assert_eq!(error, Some(Error::OutcomeUnknown), "{action}");
    }
}

#[tokio::test]
async fn mailbox_rebind_between_profile_and_read_cannot_return_other_mailbox() {
    let f = Fixture::new().await;
    f.state.rebind_during_mail.store(true, Ordering::SeqCst);
    f.reads(json!({"messages":[]}));
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::ConsentRequired)
    );
    let calls = f.state.calls.lock().unwrap();
    assert!(calls[2].1["endpoint"]
        .as_str()
        .unwrap()
        .contains("/users/person@example.com/messages"));
}
