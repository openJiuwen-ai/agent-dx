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

async fn http_candidate(f: &Fixture, path: &str, raw: &str) -> (u16, Value) {
    use http_body_util::BodyExt;
    let request = ::http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(http_body_util::Full::new(bytes::Bytes::copy_from_slice(
            raw.as_bytes(),
        )))
        .unwrap();
    let response = super::http::handle(&f.gateway, &f.scope, request).await;
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "no-referrer");
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn http_managed_start_uses_only_owner_selected_config() {
    let f = Fixture::outlook(true).await;
    f.authority.attempt.lock().unwrap().phase = AttemptPhase::Ready;
    f.reply(201, json!({"connected_account_id":"ca_one", "redirect_url":"https://connect.composio.dev/link", "expires_at":"2026-11-13T12:00:00Z", "link_token":"never-return"}));
    let (status, value) = http_candidate(
        &f,
        "/api/digitalmate/connectors/v1/composio/start",
        r#"{"registration":"registration-one"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(value["phase"], "waiting");
    assert_eq!(value["redirect_url"], "https://connect.composio.dev/link");
    assert!(!value.to_string().contains("never-return"));
    assert_eq!(
        f.state.calls.lock().unwrap()[0].1["auth_config_id"],
        "ac_mail_outlook"
    );
}

#[tokio::test]
async fn http_rejects_identity_fields_duplicate_fields_and_arbitrary_transport() {
    let f = Fixture::new().await;
    for raw in [
        r#"{"registration":"registration-one","user_id":"other"}"#,
        r#"{"registration":"registration-one","registration":"other"}"#,
        r#"{"registration":"registration-one","auth_config_id":"other"}"#,
    ] {
        assert_eq!(
            http_candidate(&f, "/api/digitalmate/connectors/v1/composio/start", raw)
                .await
                .0,
            400
        );
    }
    assert_eq!(http_candidate(&f, "/api/digitalmate/connectors/v1/composio/mail", r#"{"provider":"gmail","connection_id":"connection-one","request":{"operation":"read","message_id":"msg_A","body":false,"endpoint":"https://evil.test"}}"#).await.0, 400);
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_cancel_is_owned_and_stale_confirmation_never_dispatches() {
    let f = Fixture::new().await;
    assert_eq!(
        http_candidate(
            &f,
            "/api/digitalmate/connectors/v1/composio/cancel",
            r#"{"registration":"registration-one"}"#
        )
        .await,
        (200, json!({"phase":"cancelled"}))
    );
    let (status, value) = http_candidate(
        &f,
        "/api/digitalmate/connectors/v1/composio/confirm",
        r#"{"registration":"registration-one","session_uri":"opaque-secret"}"#,
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(value["error"], "authorization_inactive");
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_body_read_still_requires_actual_operation_permission() {
    let f = Fixture::new().await;
    let (status, value) = http_candidate(&f, "/api/digitalmate/connectors/v1/composio/mail", r#"{"provider":"gmail","connection_id":"connection-one","request":{"operation":"read","message_id":"msg_A","body":true}}"#).await;
    assert_eq!(status, 403);
    assert_eq!(value["error"], "forbidden");
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn http_account_validation_probes_mail_without_granting_or_reading_content() {
    let f = Fixture::outlook(true).await;
    f.authority.grant.lock().unwrap().permissions = Permissions::default();
    f.outlook_reads(true, json!({"value":[]}));
    let (status, proof) = http_candidate(
        &f,
        "/api/digitalmate/connectors/v1/composio/validate",
        r#"{"connection_id":"connection-one"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(proof["connection_id"], "connection-one");
    assert_eq!(proof["provider"], "microsoft");
    assert_eq!(proof["subject_id"], OUTLOOK_SUBJECT);
    assert_eq!(proof["granted_capabilities"], json!(["read_history"]));
    let calls = f.state.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let endpoint = url::Url::parse(calls[2].1["endpoint"].as_str().unwrap()).unwrap();
    assert_eq!(
        endpoint.path(),
        format!("/v1.0/users/{OUTLOOK_SUBJECT}/messages")
    );
    assert!(endpoint
        .query_pairs()
        .any(|(name, value)| name == "$top" && value == "1"));
    assert!(endpoint
        .query_pairs()
        .any(|(name, value)| name == "$select" && value == "id,conversationId,webLink"));
    assert!(f.authority.grant.lock().unwrap().permissions == Permissions::default());
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn gmail_first_validation_uses_online_profile_without_preexisting_read_permission() {
    let f = Fixture::new().await;
    {
        let mut grant = f.authority.grant.lock().unwrap();
        grant.email.clear();
        grant.permissions = Permissions::default();
    }
    f.reads(json!({"messages":[{"id":"probe_private_id", "threadId":"private_thread"}]}));
    let (status, proof) = http_candidate(
        &f,
        http::VALIDATE_PATH,
        r#"{"connection_id":"connection-one"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(proof["email"], "person@example.com");
    assert!(!proof.to_string().contains("private"));
    let calls = f.state.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let endpoint = url::Url::parse(calls[2].1["endpoint"].as_str().unwrap()).unwrap();
    assert!(endpoint.path().contains("person@example.com"));
    assert!(endpoint
        .query_pairs()
        .any(|(name, value)| name == "maxResults" && value == "1"));
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn validation_discards_late_probe_after_revocation_and_denies_body_or_account_json() {
    let f = Fixture::outlook(true).await;
    for raw in [
        r#"{"connection_id":"connection-one","account_id":"ca_other"}"#,
        r#"{"connection_id":"connection-one","request":{"body":true}}"#,
        r#"{"connection_id":"connection-one","connection_id":"other"}"#,
    ] {
        assert_eq!(http_candidate(&f, http::VALIDATE_PATH, raw).await.0, 400);
    }
    assert!(f.state.calls.lock().unwrap().is_empty());
    f.state.revoke_during_mail.store(true, Ordering::SeqCst);
    f.outlook_reads(true, json!({"value":[]}));
    let (status, value) = http_candidate(
        &f,
        http::VALIDATE_PATH,
        r#"{"connection_id":"connection-one"}"#,
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(value, json!({"error":"forbidden"}));
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn confirmation_unknown_outcome_never_redeems_a_single_use_session_twice() {
    let f = Fixture::new().await;
    f.reply(503, json!({"error":"upstream failure"}));
    f.reply(
        503,
        json!({"error":"must not dispatch a second confirmation"}),
    );
    f.reply(
        503,
        json!({"error":"must not dispatch a replacement session"}),
    );
    for session in ["opaque-first", "opaque-first", "opaque-replacement"] {
        assert_eq!(
            f.gateway
                .confirm(&f.scope, "registration-one", session)
                .await
                .err(),
            Some(Error::OutcomeUnknown),
        );
    }
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_outlook_read_keeps_subject_pin_and_header_only_projection() {
    let f = Fixture::outlook(true).await;
    f.outlook_reads(true, json!({"id":"message_A", "subject":"Header", "bodyPreview":"private-preview", "body":{"content":"private-body"}}));
    let (status, value) = http_candidate(&f, "/api/digitalmate/connectors/v1/composio/mail", r#"{"provider":"outlook","connection_id":"connection-one","request":{"operation":"read","message_id":"message_A","body":false}}"#).await;
    assert_eq!(status, 200);
    assert!(!value.to_string().contains("private-"));
    assert!(value.to_string().contains("Header"));
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
    assert!(f.state.calls.lock().unwrap()[2].1["endpoint"]
        .as_str()
        .unwrap()
        .contains(OUTLOOK_SUBJECT));
}

#[tokio::test]
async fn confirmation_invalid_input_does_not_consume_the_owner_claim() {
    let f = Fixture::new().await;
    for session in ["", "opaque\nsecret"] {
        assert_eq!(
            f.gateway
                .confirm(&f.scope, "registration-one", session)
                .await
                .err(),
            Some(Error::InvalidRequest)
        );
    }
    assert!(f.authority.confirmation.lock().unwrap().is_none());
    f.reply(503, json!({"error":"unknown upstream outcome"}));
    assert_eq!(
        f.gateway
            .confirm(&f.scope, "registration-one", "opaque-valid")
            .await
            .err(),
        Some(Error::OutcomeUnknown)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 1);
    assert!(!f
        .authority
        .confirmation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .1
        .contains("opaque"));
}

#[tokio::test]
async fn http_query_origin_methods_and_body_size_fail_before_effects() {
    let f = Fixture::new().await;
    for (method, path, origin, raw) in [
        (
            "GET",
            "/api/digitalmate/connectors/v1/composio/start",
            false,
            "{}".to_string(),
        ),
        (
            "POST",
            "/api/digitalmate/connectors/v1/composio/start?user_id=other",
            false,
            "{}".to_string(),
        ),
        (
            "POST",
            "/api/digitalmate/connectors/v1/composio/start",
            true,
            "{}".to_string(),
        ),
        (
            "POST",
            "/api/digitalmate/connectors/v1/composio/start",
            false,
            "x".repeat(16385),
        ),
    ] {
        let mut builder = ::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if origin {
            builder = builder.header("origin", "https://evil.test");
        }
        let response = super::http::handle(
            &f.gateway,
            &f.scope,
            builder
                .body(http_body_util::Full::new(bytes::Bytes::from(raw)))
                .unwrap(),
        )
        .await;
        assert!(response.status().is_client_error());
    }
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[test]
fn http_browser_landing_never_echoes_or_redeems_callback_material() {
    use http_body_util::BodyExt;
    let response = super::http::callback_landing();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["referrer-policy"], "no-referrer");
    assert!(response.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .contains("default-src 'none'"));
    let body = response.into_body().collect();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bytes = runtime.block_on(body).unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&bytes).contains("session_uri"));
}

struct ControlledAuthority {
    scope: Scope,
    attempt: Mutex<Attempt>,
    grant: Mutex<ConnectionGrant>,
    revoked: AtomicBool,
    writes: AtomicUsize,
    replace_on_accept: AtomicBool,
    receipt: Mutex<Option<(u64, VerifiedAccount)>>,
    confirmation: Mutex<Option<(u64, String)>>,
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
    async fn metadata(&self, scope: &Scope, connection: &str) -> Result<ConnectionMetadata, Error> {
        self.check_scope(scope)?;
        if self.revoked.load(Ordering::SeqCst) || connection != "connection-one" {
            return Err(Error::Forbidden);
        }
        let grant = self.grant.lock().unwrap();
        Ok(ConnectionMetadata {
            connection_id: connection.into(),
            provider: match grant.provider {
                MailProvider::Google => "google",
                MailProvider::Microsoft => "microsoft",
            }
            .into(),
            backend: "composio".into(),
            external_account_id: grant.account_id.clone(),
            external_subject_id: grant.subject_id.clone(),
            authorization_generation: grant.generation,
            record_revision: 1,
            created_at: 1.0,
            identity_user_id: if grant.provider == MailProvider::Google {
                grant.email.clone()
            } else {
                String::new()
            },
        })
    }
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
        if current.revision != expected.revision
            || current.provider != expected.provider
            || current.phase != AttemptPhase::Ready
        {
            return Err(Error::AuthorizationInactive);
        }
        current.phase = AttemptPhase::Starting;
        Ok(())
    }
    async fn claim_confirmation(
        &self,
        scope: &Scope,
        expected: &Attempt,
        digest: &str,
    ) -> Result<(), Error> {
        self.check_scope(scope)?;
        let current = self.attempt.lock().unwrap();
        current.check()?;
        if current.registration != expected.registration
            || current.revision != expected.revision
            || current.provider != expected.provider
            || current.account_id != expected.account_id
            || current.phase != AttemptPhase::Waiting
        {
            return Err(Error::AuthorizationInactive);
        }
        let mut claim = self.confirmation.lock().unwrap();
        if claim
            .as_ref()
            .is_some_and(|(revision, _)| *revision == current.revision)
        {
            return Err(Error::OutcomeUnknown);
        }
        assert_eq!(digest.len(), 64);
        assert!(digest.bytes().all(|v| v.is_ascii_hexdigit()));
        *claim = Some((current.revision, digest.into()));
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
        if current.revision != expected.revision
            || current.provider != expected.provider
            || current.phase != AttemptPhase::Starting
        {
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
        if current.revision != expected.revision
            || current.provider != expected.provider
            || current.phase != AttemptPhase::Waiting
        {
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
        if current.generation == grant.generation
            && current.account_id == grant.account_id
            && current.provider == grant.provider
            && current.subject_id == grant.subject_id
            && current.email == grant.email
            && current.permissions == grant.permissions
        {
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

pub(crate) struct Fixture {
    pub(crate) gateway: Composio,
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
        Self::with_scope(scope).await
    }
    pub(crate) async fn with_scope(scope: Scope) -> Self {
        let authority = Arc::new(ControlledAuthority {
            scope: scope.clone(),
            attempt: Mutex::new(Attempt {
                provider: MailProvider::Google,
                registration: "registration-one".into(),
                revision: 1,
                deadline_epoch_seconds: u64::MAX,
                phase: AttemptPhase::Waiting,
                account_id: "ca_one".into(),
                expected_email: None,
            }),
            grant: Mutex::new(ConnectionGrant {
                provider: MailProvider::Google,
                subject_id: String::new(),
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
            confirmation: Mutex::new(None),
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
    pub(crate) fn reply(&self, status: u16, body: Value) {
        self.state
            .replies
            .lock()
            .unwrap()
            .push_back((StatusCode::from_u16(status).unwrap(), body));
    }
    #[cfg(feature = "mock-e2e")]
    pub(crate) fn use_budget(&mut self, budget: Arc<dyn Budget>) {
        self.gateway.client.budget = budget;
    }
    #[cfg(feature = "mock-e2e")]
    pub(crate) fn call_count(&self) -> usize {
        self.state.calls.lock().unwrap().len()
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
async fn outlook_shared_mail_path_pins_provider_subject_and_metadata() {
    let mut f = Fixture::new().await;
    let origin = f.gateway.client.origin.clone();
    f.gateway = Composio::with_configs(
        "controlled-project-key".into(),
        MailAuthConfigs {
            gmail: Some(MailAuthConfig::custom("ac_custom_gmail".into())),
            outlook: Some(MailAuthConfig::custom("ac_custom_outlook".into())),
        },
        f.authority.clone(),
        f.budget.clone(),
    )
    .unwrap();
    f.gateway.client.origin = origin;
    {
        let mut grant = f.authority.grant.lock().unwrap();
        grant.provider = MailProvider::Microsoft;
        grant.subject_id = "01234567-89ab-cdef-0123-456789abcdef".into();
    }
    let mut account = f.account();
    account["toolkit"]["slug"] = json!("outlook");
    account["auth_config"]["id"] = json!("ac_custom_outlook");
    f.reply(200, account);
    f.reply(
        200,
        json!({"status":200,"data":{
            "id":"01234567-89ab-cdef-0123-456789abcdef","mail":"person@example.com"
        }}),
    );
    f.reply(
        200,
        json!({"status":200,"data":{
            "id":"AAMk_message+=","conversationId":"AAQk_thread=",
            "subject":"Subject","body":{"content":"must not cross metadata boundary"},
            "bodyPreview":"must not cross metadata boundary"
        }}),
    );
    let result = f
        .gateway
        .mail(
            &f.scope,
            "connection-one",
            MailRequest::Outlook(OutlookRequest::Read {
                message_id: "AAMk_message+=".into(),
                body: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["id"], "AAMk_message+=");
    assert!(result.get("body").is_none());
    assert!(result.get("bodyPreview").is_none());
    let calls = f.state.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let endpoint = reqwest::Url::parse(calls[2].1["endpoint"].as_str().unwrap()).unwrap();
    assert_eq!(endpoint.host_str(), Some("graph.microsoft.com"));
    assert_eq!(
        endpoint.path(),
        "/v1.0/users/01234567-89ab-cdef-0123-456789abcdef/messages/AAMk_message+="
    );
    assert!(!endpoint.query_pairs().any(|(key, value)| {
        key == "$select"
            && value
                .split(',')
                .any(|field| field == "body" || field == "bodyPreview")
    }));
    assert_eq!(f.budget.calls.load(Ordering::SeqCst), 3);
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
async fn gmail_metadata_excludes_preview_body_and_unrequested_headers() {
    let f = Fixture::new().await;
    f.reads(json!({"id":"18abc","threadId":"18def","internalDate":"1793491200000",
        "snippet":"private body preview", "payload":{"body":{"data":"c2VjcmV0"},
        "parts":[{"body":{"data":"c2VjcmV0"}}], "headers":[
            {"name":"Subject","value":"Allowed subject"}, {"name":"X-Private","value":"not requested"}]}}));
    let result = f
        .gateway
        .gmail(
            &f.scope,
            "connection-one",
            GmailRequest::Read {
                message_id: "18abc".into(),
                body: false,
            },
        )
        .await
        .unwrap();
    assert!(result.get("snippet").is_none());
    assert_eq!(
        result["payload"],
        json!({"headers":[{"name":"Subject","value":"Allowed subject"}]})
    );
    let calls = f.state.calls.lock().unwrap();
    let endpoint = reqwest::Url::parse(calls[2].1["endpoint"].as_str().unwrap()).unwrap();
    assert!(endpoint.query_pairs().any(
        |(key, value)| key == "fields" && value == "id,threadId,internalDate,payload(headers)"
    ));
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
            provider: MailProvider::Google,
            subject_id: String::new(),
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

const OUTLOOK_SUBJECT: &str = "01234567-89ab-cdef-0123-456789abcdef";

#[tokio::test]
async fn missing_provider_config_does_not_claim_authorization_or_send_http() {
    let f = Fixture::new().await;
    {
        let mut attempt = f.authority.attempt.lock().unwrap();
        attempt.provider = MailProvider::Microsoft;
        attempt.phase = AttemptPhase::Ready;
    }
    assert_eq!(
        f.gateway.start(&f.scope, "registration-one").await.err(),
        Some(Error::NotConfigured)
    );
    assert!(f.authority.attempt.lock().unwrap().phase == AttemptPhase::Ready);
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_provider_auth_config_is_rejected_at_construction() {
    let f = Fixture::new().await;
    assert!(matches!(
        Composio::with_configs(
            "controlled-project-key".into(),
            MailAuthConfigs {
                gmail: Some(MailAuthConfig::managed("ac_same".into())),
                outlook: Some(MailAuthConfig::managed("ac_same".into())),
            },
            f.authority.clone(),
            f.budget.clone()
        ),
        Err(Error::NotConfigured)
    ));
}

impl Fixture {
    async fn outlook(managed: bool) -> Self {
        let mut f = Self::new().await;
        let origin = f.gateway.client.origin.clone();
        f.gateway = Composio::with_configs(
            "controlled-project-key".into(),
            MailAuthConfigs {
                gmail: Some(MailAuthConfig::custom("ac_custom_gmail".into())),
                outlook: Some(if managed {
                    MailAuthConfig::managed("ac_mail_outlook".into())
                } else {
                    MailAuthConfig::custom("ac_mail_outlook".into())
                }),
            },
            f.authority.clone(),
            f.budget.clone(),
        )
        .unwrap();
        f.gateway.client.origin = origin;
        {
            let mut grant = f.authority.grant.lock().unwrap();
            grant.provider = MailProvider::Microsoft;
            grant.subject_id = OUTLOOK_SUBJECT.into();
        }
        f.authority.attempt.lock().unwrap().provider = MailProvider::Microsoft;
        f
    }
    fn outlook_account(&self, managed: bool) -> Value {
        let mut value = self.account();
        value["toolkit"]["slug"] = json!("outlook");
        value["auth_config"]["id"] = json!("ac_mail_outlook");
        value["auth_config"]["is_composio_managed"] = json!(managed);
        value
    }
    fn outlook_reads(&self, managed: bool, data: Value) {
        self.reply(200, self.outlook_account(managed));
        self.reply(
            200,
            json!({"status":200,"data":{"id":OUTLOOK_SUBJECT,"mail":"person@example.com"}}),
        );
        self.reply(200, json!({"status":200,"data":data}));
    }
    fn outlook_search(&self, page: &str) -> MailRequest {
        MailRequest::Outlook(OutlookRequest::Search {
            sender: String::new(),
            since: String::new(),
            until: String::new(),
            limit: 5,
            page: page.into(),
        })
    }
}

#[tokio::test]
async fn managed_outlook_authorization_proves_subject_without_exposing_tokens() {
    let f = Fixture::outlook(true).await;
    f.reply(
        200,
        json!({"connected_account_id":"ca_one","toolkit_slug":"outlook"}),
    );
    f.reply(200, f.outlook_account(true));
    f.reply(
        200,
        json!({"status":200,"data":{"id":OUTLOOK_SUBJECT,"mail":"person@example.com",
        "access_token":"not-a-profile-field"}}),
    );
    let proof = f
        .gateway
        .confirm(&f.scope, "registration-one", "opaque-session")
        .await
        .unwrap();
    assert_eq!(proof.provider, MailProvider::Microsoft);
    assert_eq!(proof.subject_id, OUTLOOK_SUBJECT);
    assert_eq!(proof.account_id, "ca_one");
    assert_eq!(f.authority.writes.load(Ordering::SeqCst), 1);
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn managed_link_selects_only_the_owner_provider_config() {
    let f = Fixture::outlook(true).await;
    f.authority.attempt.lock().unwrap().phase = AttemptPhase::Ready;
    f.reply(
        201,
        json!({"connected_account_id":"ca_one","redirect_url":"https://connect.composio.dev/link",
        "expires_at":"2026-11-13T12:00:00Z"}),
    );
    f.gateway.start(&f.scope, "registration-one").await.unwrap();
    assert_eq!(
        f.state.calls.lock().unwrap()[0].1["auth_config_id"],
        "ac_mail_outlook"
    );
    assert_eq!(f.authority.attempt.lock().unwrap().account_id, "ca_one");
}

#[tokio::test]
async fn outlook_upn_fallback_still_requires_the_exact_verified_subject() {
    let f = Fixture::outlook(true).await;
    f.reply(200, f.outlook_account(true));
    f.reply(200, json!({"status":200,"data":{"id":OUTLOOK_SUBJECT,"mail":null,"userPrincipalName":"person@example.com"}}));
    f.reply(200, json!({"status":200,"data":{"value":[]}}));
    f.gateway
        .mail(&f.scope, "connection-one", f.outlook_search(""))
        .await
        .unwrap();
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn managed_gmail_is_an_explicit_config_choice_not_a_global_bypass() {
    let mut f = Fixture::new().await;
    let origin = f.gateway.client.origin.clone();
    f.gateway = Composio::with_configs(
        "controlled-project-key".into(),
        MailAuthConfigs {
            gmail: Some(MailAuthConfig::managed("ac_custom_gmail".into())),
            outlook: None,
        },
        f.authority.clone(),
        f.budget.clone(),
    )
    .unwrap();
    f.gateway.client.origin = origin;
    let mut account = f.account();
    account["auth_config"]["is_composio_managed"] = json!(true);
    f.reply(200, account);
    f.reply(
        200,
        json!({"status":200,"data":{"emailAddress":"person@example.com"}}),
    );
    f.reply(200, json!({"status":200,"data":{"messages":[]}}));
    f.gateway
        .mail(&f.scope, "connection-one", MailRequest::Gmail(f.search()))
        .await
        .unwrap();
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn provider_mismatch_and_body_permission_are_refused_before_http() {
    let f = Fixture::outlook(true).await;
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert_eq!(
        f.gateway
            .mail(
                &f.scope,
                "connection-one",
                MailRequest::Outlook(OutlookRequest::Read {
                    message_id: "AAMk_message+=".into(),
                    body: true
                })
            )
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn outlook_refuses_wrong_config_toolkit_and_oauth_ownership() {
    for variant in ["config", "toolkit", "ownership", "user", "disabled"] {
        let f = Fixture::outlook(true).await;
        let mut account = f.outlook_account(true);
        match variant {
            "config" => account["auth_config"]["id"] = json!("ac_other"),
            "toolkit" => account["toolkit"]["slug"] = json!("gmail"),
            "ownership" => account["auth_config"]["is_composio_managed"] = json!(false),
            "user" => account["user_id"] = json!("other"),
            "disabled" => account["is_disabled"] = json!(true),
            _ => unreachable!(),
        }
        f.reply(200, account);
        assert_eq!(
            f.gateway
                .mail(&f.scope, "connection-one", f.outlook_search(""))
                .await
                .err(),
            Some(if variant == "disabled" {
                Error::ReauthRequired
            } else {
                Error::AccountMismatch
            }),
            "{variant}"
        );
        assert_eq!(f.state.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn outlook_mailbox_subject_is_checked_before_reading_mail() {
    let f = Fixture::outlook(true).await;
    f.reply(200, f.outlook_account(true));
    f.reply(
        200,
        json!({"status":200,"data":{"id":"another-subject","mail":"person@example.com"}}),
    );
    assert_eq!(
        f.gateway
            .mail(&f.scope, "connection-one", f.outlook_search(""))
            .await
            .err(),
        Some(Error::AccountMismatch)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn outlook_late_read_after_revocation_is_not_returned() {
    let f = Fixture::outlook(true).await;
    f.state.revoke_during_mail.store(true, Ordering::SeqCst);
    f.outlook_reads(true, json!({"value":[]}));
    assert_eq!(
        f.gateway
            .mail(&f.scope, "connection-one", f.outlook_search(""))
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn outlook_rebind_during_read_is_pinned_to_original_subject() {
    let f = Fixture::outlook(true).await;
    f.state.rebind_during_mail.store(true, Ordering::SeqCst);
    f.outlook_reads(true, json!({"value":[]}));
    assert_eq!(
        f.gateway
            .mail(&f.scope, "connection-one", f.outlook_search(""))
            .await
            .err(),
        Some(Error::ConsentRequired)
    );
    assert!(f.state.calls.lock().unwrap()[2].1["endpoint"]
        .as_str()
        .unwrap()
        .contains(OUTLOOK_SUBJECT));
}

#[tokio::test]
async fn outlook_pagination_preserves_exact_provider_next_link_and_query() {
    let f = Fixture::outlook(true).await;
    let request = OutlookRequest::Search {
        sender: "sender@example.com".into(),
        since: "2026-11-12T00:00:00Z".into(),
        until: "2026-11-13T00:00:00Z".into(),
        limit: 5,
        page: String::new(),
    };
    let mut next = request.endpoint(OUTLOOK_SUBJECT).unwrap();
    next.query_pairs_mut()
        .append_pair("$skiptoken", "opaque+token=/");
    f.outlook_reads(
        true,
        json!({"value":[{"id":"AAMk_message+=","conversationId":"thread=",
        "webLink":"https://outlook.live.com/owa/?id=1","body":"must not cross list boundary"}],
        "@odata.nextLink":next.as_str()}),
    );
    let result = f
        .gateway
        .mail(&f.scope, "connection-one", MailRequest::Outlook(request))
        .await
        .unwrap();
    assert!(result["value"][0].get("body").is_none());
    assert_eq!(result["@odata.nextLink"], next.as_str());
    f.outlook_reads(true, json!({"value":[]}));
    f.gateway
        .mail(
            &f.scope,
            "connection-one",
            MailRequest::Outlook(OutlookRequest::Search {
                sender: "sender@example.com".into(),
                since: "2026-11-12T00:00:00Z".into(),
                until: "2026-11-13T00:00:00Z".into(),
                limit: 5,
                page: next.to_string(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        f.state.calls.lock().unwrap()[5].1["endpoint"],
        next.as_str()
    );
}

#[tokio::test]
async fn outlook_pages_cannot_change_mailbox_select_filter_or_origin() {
    let f = Fixture::outlook(true).await;
    let initial = OutlookRequest::Search {
        sender: String::new(),
        since: String::new(),
        until: String::new(),
        limit: 5,
        page: String::new(),
    }
    .endpoint(OUTLOOK_SUBJECT)
    .unwrap();
    let mut valid = initial.clone();
    valid.query_pairs_mut().append_pair("$skip", "5");
    let cases = [
        valid.as_str().replace("graph.microsoft.com", "evil.test"),
        valid.as_str().replace(OUTLOOK_SUBJECT, "another-subject"),
        valid
            .as_str()
            .replace("users/01234567-89ab-cdef-0123-456789abcdef", "me"),
        format!("{valid}&$select=body"),
        format!("{valid}&$top=100"),
        format!("{valid}&$filter=subject%20eq%20'secret'"),
        format!("{valid}&$expand=attachments"),
        format!("{valid}&$skiptoken=another"),
        format!("{valid}#fragment"),
        valid.as_str().replace("https://", "http://"),
        valid.as_str().replace("https://", "https://user@"),
        valid
            .as_str()
            .replace("graph.microsoft.com/", "graph.microsoft.com:444/"),
    ];
    for page in cases {
        assert_eq!(
            f.gateway
                .mail(&f.scope, "connection-one", f.outlook_search(&page))
                .await
                .err(),
            Some(Error::InvalidRequest),
            "{page}"
        );
    }
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn outlook_invalid_provider_next_link_is_not_returned() {
    let f = Fixture::outlook(true).await;
    f.outlook_reads(
        true,
        json!({"value":[],"@odata.nextLink":"https://evil.test/steal"}),
    );
    assert_eq!(
        f.gateway
            .mail(&f.scope, "connection-one", f.outlook_search(""))
            .await
            .err(),
        Some(Error::InvalidResponse)
    );
}

#[tokio::test]
async fn outlook_body_read_is_explicit_and_uses_same_budget() {
    let f = Fixture::outlook(true).await;
    f.authority.grant.lock().unwrap().permissions.read_body = true;
    f.outlook_reads(
        true,
        json!({"id":"AAMk_message+=","body":{"contentType":"text","content":"requested"},
        "bodyPreview":"requested preview","attachments":[{"content":"not allowed"}]}),
    );
    let result = f
        .gateway
        .mail(
            &f.scope,
            "connection-one",
            MailRequest::Outlook(OutlookRequest::Read {
                message_id: "AAMk_message+=".into(),
                body: true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["body"]["content"], "requested");
    assert!(result.get("attachments").is_none());
    assert_eq!(f.budget.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn outlook_uses_immutable_subject_when_owner_has_no_display_email() {
    let f = Fixture::outlook(true).await;
    f.authority.grant.lock().unwrap().email.clear();
    f.outlook_reads(true, json!({"id":"message_A", "subject":"Headers"}));
    let result = f
        .gateway
        .mail(
            &f.scope,
            "connection-one",
            MailRequest::Outlook(OutlookRequest::Read {
                message_id: "message_A".into(),
                body: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["id"], "message_A");
    assert_eq!(f.state.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn gmail_still_requires_an_owner_verified_profile_address() {
    let f = Fixture::new().await;
    f.authority.grant.lock().unwrap().email.clear();
    assert_eq!(
        f.gateway
            .gmail(&f.scope, "connection-one", f.search())
            .await
            .err(),
        Some(Error::Forbidden)
    );
    assert!(f.state.calls.lock().unwrap().is_empty());
}

#[test]
fn outlook_search_checks_calendar_dates_ids_and_increasing_bounds() {
    for invalid in [
        "2026-02-29T00:00:00Z",
        "2100-02-29T00:00:00Z",
        "2026-11-31T00:00:00Z",
        "2026-11-13T24:00:00Z",
        "2026-11-13T00:60:00Z",
        "2026-11-13T00:00:60Z",
        "2026-11-13T00:00:00+00:00",
        "0000-01-01T00:00:00Z",
    ] {
        let request = OutlookRequest::Search {
            sender: String::new(),
            since: invalid.into(),
            until: String::new(),
            limit: 5,
            page: String::new(),
        };
        assert_eq!(
            request.validate().err(),
            Some(Error::InvalidRequest),
            "{invalid}"
        );
    }
    for invalid in ["../message", "message/id", "id?body=true", "id\n"] {
        assert_eq!(
            OutlookRequest::Read {
                message_id: invalid.into(),
                body: false
            }
            .validate()
            .err(),
            Some(Error::InvalidRequest)
        );
    }
    assert!(OutlookRequest::Search {
        sender: String::new(),
        since: "2000-02-29T00:00:00Z".into(),
        until: "2000-03-01T00:00:00Z".into(),
        limit: 5,
        page: String::new()
    }
    .validate()
    .is_ok());
}
