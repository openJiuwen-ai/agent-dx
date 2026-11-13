//! Existing signer and typed owner protocol; session and Binding I/O are controlled.
use super::super::connector_owner::{
    Actor, OwnerAuthority, OwnerClaims, OwnerOperation, OWNER_PATH,
};
use super::super::privacy_center::BindingReader;
use super::super::privacy_grants::{now, TokenSigner};
use super::*;
use adx_agent_core::{AgentBinding, EnvironmentRef};
use async_trait::async_trait;
use sha2::Digest;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

#[path = "connector_owner_persistence_tests.rs"]
mod persistence_tests;

struct Auth(AtomicBool);
#[async_trait]
impl BusinessAuth for Auth {
    async fn login(&self, _: accounts::LoginRequest) -> accounts::Result<accounts::LoginResponse> {
        Err(accounts::Error::Unavailable)
    }
    async fn authenticate(&self, _: &str) -> accounts::Result<accounts::Principal> {
        Err(accounts::Error::Unauthorized)
    }
    async fn check(&self, principal: &accounts::Principal) -> accounts::Result<()> {
        if self.0.load(Ordering::SeqCst)
            || principal.user_id != "alice"
            || principal.token_digest != "session-digest"
        {
            Err(accounts::Error::Unauthorized)
        } else {
            Ok(())
        }
    }
    async fn logout(&self, _: &str) -> accounts::Result<()> {
        Ok(())
    }
    async fn quota(&self, _: &accounts::Principal) -> accounts::Result<accounts::QuotaResponse> {
        Err(accounts::Error::Unavailable)
    }
    async fn model_parameters(
        &self,
        _: &accounts::Principal,
    ) -> accounts::Result<adx_agent_core::mounts::Parameters> {
        panic!("owner identity must not fetch model credentials")
    }
}

struct Bindings(Mutex<AgentBinding>);
#[async_trait]
impl BindingReader for Bindings {
    async fn binding(&self, _: &Scope) -> accounts::Result<AgentBinding> {
        Ok(self.0.lock().unwrap().clone())
    }
}

fn setup() -> (
    OwnerAuthority,
    ForegroundIdentity,
    Arc<Auth>,
    Arc<Bindings>,
    Arc<TokenSigner>,
) {
    let auth = Arc::new(Auth(AtomicBool::new(false)));
    let application = application();
    let scope = application
        .scope("tenant", "alice", Application::DigitalMate)
        .unwrap();
    let workspace = workspace_parameters("tenant", "digitalmate", "alice")
        .unwrap()
        .remove("workspace")
        .unwrap();
    let binding = AgentBinding {
        scope: scope.clone(),
        generation: "generation-one".into(),
        current: EnvironmentRef::new("1".into(), "environment-one".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: [("workspace".into(), workspace.clone())].into(),
        volumes: Default::default(),
        creation_attempts: Default::default(),
    };
    let bindings = Arc::new(Bindings(Mutex::new(binding)));
    let signer = Arc::new(TokenSigner::new(&[7; 32], "tenant"));
    let identity = ForegroundIdentity {
        guard: Arc::new(SessionGuard {
            auth: auth.clone(),
            principal: accounts::Principal {
                user_id: "alice".into(),
                token_digest: "session-digest".into(),
                expires_at: now().unwrap() + 3600,
            },
        }),
        scope,
        workspace,
    };
    (
        OwnerAuthority::new(
            "tenant".into(),
            application,
            signer.clone(),
            bindings.clone(),
            auth.clone(),
        )
        .unwrap(),
        identity,
        auth,
        bindings,
        signer,
    )
}

fn application() -> ApplicationWsConfig {
    ApplicationWsConfig {
        request_timeout_seconds: 5,
        template: "digitalmate".into(),
        template_ref: "1".into(),
        backend_path: "/ws".into(),
        service_port: 18092,
    }
}

fn token(value: &super::super::connector_owner::OwnerAuthorization) -> &str {
    value.headers()[http::header::AUTHORIZATION]
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap()
}
fn digest(body: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(body))
}

#[tokio::test]
async fn delivery_bootstrap_target_requires_exact_actual_binding_and_port() {
    use super::super::ingestion::SubscriptionBinding;
    let (owner, identity, _, bindings, _) = setup();
    let record = SubscriptionBinding {
        tenant: "tenant".into(),
        user_id: "alice".into(),
        connection_id: "connection".into(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        subscription_id: "subscription".into(),
        subscription_generation: 1,
        connection_generation: 1,
        workspace: identity.workspace().into(),
        binding_id: identity.binding_scope().binding_id.clone(),
        binding_generation: "generation-one".into(),
        service_port: 18092,
        trigger_id: "ti_test".into(),
        trigger_slug: "GMAIL_NEW_GMAIL_MESSAGE".into(),
        account_id: "ca_test".into(),
        auth_config_id: "ac_test".into(),
        composio_user_id: "dm_test".into(),
        enabled: true,
    };
    assert_eq!(
        owner.delivery_target(&record).await.unwrap()["environment_id"],
        "environment-one"
    );
    for changed in [
        "tenant",
        "user",
        "workspace",
        "template",
        "ref",
        "id",
        "generation",
        "port",
        "disabled",
    ] {
        let mut other = record.clone();
        match changed {
            "tenant" => other.tenant = "other".into(),
            "user" => other.user_id = "other".into(),
            "workspace" => other.workspace = "other".into(),
            "template" => other.template = "other".into(),
            "ref" => other.template_ref = "2".into(),
            "id" => other.binding_id = "other".into(),
            "generation" => other.binding_generation = "other".into(),
            "port" => other.service_port = 8081,
            _ => other.enabled = false,
        }
        assert!(owner.delivery_target(&other).await.is_err(), "{changed}");
    }
    bindings.0.lock().unwrap().phase = BindingPhase::Deleting;
    assert!(owner.delivery_target(&record).await.is_err());
}

#[tokio::test]
async fn owner_purpose_fixes_raw_body_operation_and_actual_binding() {
    let (owner, identity, _, _, _) = setup();
    let body = br#"{"operation":"current","registration":"auth-one"}"#;
    let issued = owner
        .foreground_headers(&identity, OwnerOperation::Current, body)
        .await
        .unwrap();
    assert!(issued.headers()[http::header::AUTHORIZATION].is_sensitive());
    assert_eq!(issued.generation(), "generation-one");
    assert_eq!(issued.environment_id(), "environment-one");
    assert_eq!(issued.service_port(), 18092);
    assert_eq!(issued.binding_scope(), identity.binding_scope());
    let verified = owner
        .verify(token(&issued), OwnerOperation::Current, &digest(body), None)
        .await
        .unwrap();
    assert_eq!(verified["purpose"], "connector.authorization-owner");
    assert_eq!(verified["actor"], "foreground");
    assert_eq!(verified["path"], OWNER_PATH);
    assert_eq!(verified["method"], "POST");
    assert_eq!(verified["user_id"], "alice");
    assert_eq!(verified["binding_generation"], "generation-one");
    assert!(!verified.as_object().unwrap().contains_key("verified"));
    assert!(!verified.as_object().unwrap().contains_key("token_digest"));
    let reformatted = br#"{ "operation":"current", "registration":"auth-one" }"#;
    assert!(owner
        .verify(
            token(&issued),
            OwnerOperation::Current,
            &digest(reformatted),
            None
        )
        .await
        .is_err());
    assert!(owner
        .verify(token(&issued), OwnerOperation::Record, &digest(body), None)
        .await
        .is_err());
}

#[tokio::test]
async fn owner_purpose_rechecks_session_and_generation_before_ack() {
    let (owner, identity, auth, bindings, _) = setup();
    let body = b"{}";
    let issued = owner
        .foreground_headers(&identity, OwnerOperation::Claim, body)
        .await
        .unwrap();
    auth.0.store(true, Ordering::SeqCst);
    assert!(owner
        .verify(token(&issued), OwnerOperation::Claim, &digest(body), None)
        .await
        .is_err());
    assert!(owner
        .foreground_headers(&identity, OwnerOperation::Claim, body)
        .await
        .is_err());
    auth.0.store(false, Ordering::SeqCst);
    let original = bindings.0.lock().unwrap().clone();
    for changed in ["generation", "environment", "workspace", "deleting"] {
        let mut binding = original.clone();
        match changed {
            "generation" => binding.generation = "generation-two".into(),
            "environment" => binding.current.environment_id = "environment-two".into(),
            "workspace" => {
                binding
                    .parameters
                    .insert("workspace".into(), "other".into());
            }
            _ => binding.phase = BindingPhase::Deleting,
        }
        *bindings.0.lock().unwrap() = binding;
        assert!(
            owner
                .verify(token(&issued), OwnerOperation::Claim, &digest(body), None)
                .await
                .is_err(),
            "{changed}"
        );
    }
}

#[tokio::test]
async fn owner_purpose_rechecks_session_after_binding_io() {
    struct RevokingBinding(Arc<Bindings>, Arc<Auth>);
    #[async_trait]
    impl BindingReader for RevokingBinding {
        async fn binding(&self, scope: &Scope) -> accounts::Result<AgentBinding> {
            self.1 .0.store(true, Ordering::SeqCst);
            self.0.binding(scope).await
        }
    }
    let (owner, identity, auth, bindings, signer) = setup();
    let packet = owner
        .foreground_headers(&identity, OwnerOperation::Claim, b"{}")
        .await
        .unwrap();
    let owner = OwnerAuthority::new(
        "tenant".into(),
        application(),
        signer,
        Arc::new(RevokingBinding(bindings, auth.clone())),
        auth,
    )
    .unwrap();
    assert!(owner
        .verify(token(&packet), OwnerOperation::Claim, &digest(b"{}"), None)
        .await
        .is_err());
}

#[tokio::test]
async fn owner_purpose_rejects_expired_wrong_audience_and_actor_escalation() {
    let (owner, identity, _, bindings, signer) = setup();
    let binding = bindings.0.lock().unwrap().clone();
    let base = OwnerClaims {
        scope: binding.scope,
        user_id: identity.user_id().into(),
        workspace: identity.workspace().into(),
        binding_generation: binding.generation,
        environment_id: binding.current.environment_id,
        service_port: 18092,
        operation: OwnerOperation::Metadata,
        body_sha256: digest(b"{}"),
        audience: OWNER_PATH.into(),
        method: "POST".into(),
        expires_at: now().unwrap() + 30,
        actor: Actor::Foreground {
            token_digest: "session-digest".into(),
            expires_at: now().unwrap() + 3600,
        },
    };
    for change in ["expired", "future", "audience", "method", "actor"] {
        let mut claims = base.clone();
        match change {
            "expired" => claims.expires_at = now().unwrap() - 1,
            "future" => claims.expires_at = now().unwrap() + 300,
            "audience" => claims.audience = "/v1/secret/get".into(),
            "method" => claims.method = "GET".into(),
            _ => {
                claims.actor = Actor::ConnectorWorkload {
                    grant_id: "a".repeat(64),
                };
                claims.operation = OwnerOperation::Record;
            }
        }
        let signed = signer.sign_owner(&claims).unwrap();
        assert!(
            owner
                .verify(&signed, claims.operation, &claims.body_sha256, None)
                .await
                .is_err(),
            "{change}"
        );
    }
    let issued = owner
        .foreground_headers(&identity, OwnerOperation::Metadata, b"{}")
        .await
        .unwrap();
    let wrong = token(&issued).replace("dm-authorization-owner-v1", "dm-privacy-v1");
    assert!(owner
        .verify(&wrong, OwnerOperation::Metadata, &digest(b"{}"), None)
        .await
        .is_err());
}

#[test]
fn owner_purpose_header_parser_never_reuses_the_business_bearer_limit() {
    use super::super::connector_owner::bearer;
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {}", "a".repeat(1000)).parse().unwrap(),
    );
    assert_eq!(bearer(&headers).unwrap().len(), 1000);
    headers.append(
        http::header::AUTHORIZATION,
        "Bearer second".parse().unwrap(),
    );
    assert!(bearer(&headers).is_err());
    headers.clear();
    headers.insert(
        http::header::AUTHORIZATION,
        "Bearer single".parse().unwrap(),
    );
    headers.insert("x-auth", "ambiguous".parse().unwrap());
    assert!(bearer(&headers).is_err());
}
