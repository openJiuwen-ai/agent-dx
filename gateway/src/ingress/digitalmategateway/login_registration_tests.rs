//! HTTP login responses must not expose a session until model configuration is registered.
use super::*;
use crate::ingress::digitalmategateway::privacy_center::{
    ConfigurationRegistry, ConfigurationScope, RegisteredConfiguration,
};
use adx_agent_core::mounts::Parameters;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
use tokio::net::TcpListener;

struct Auth {
    events: Arc<Mutex<Vec<&'static str>>>,
    parameters: Mutex<Parameters>,
    logins: AtomicUsize,
    fail_login: AtomicBool,
    revoked: AtomicBool,
}
impl Auth {
    fn login_result(&self, provider: &'static str) -> accounts::Result<accounts::LoginResponse> {
        self.events.lock().unwrap().push(provider);
        if self.fail_login.load(Ordering::SeqCst) {
            return Err(accounts::Error::Unauthorized);
        }
        let sequence = self.logins.fetch_add(1, Ordering::SeqCst);
        Ok(accounts::LoginResponse {
            user_id: "alice".into(),
            token: format!("synthetic-login-{sequence}"),
            token_type: "Bearer",
            expires_in: 3600,
        })
    }
}
#[async_trait::async_trait]
impl BusinessAuth for Auth {
    async fn login(&self, _: accounts::LoginRequest) -> accounts::Result<accounts::LoginResponse> {
        self.login_result("huawei")
    }
    async fn agc_login(
        &self,
        _: accounts::AgcLoginRequest,
    ) -> accounts::Result<accounts::LoginResponse> {
        self.login_result("agc")
    }
    async fn authenticate(&self, token: &str) -> accounts::Result<Principal> {
        self.events.lock().unwrap().push("authenticate");
        assert!(token.starts_with("synthetic-login-"));
        Ok(Principal {
            user_id: "alice".into(),
            expires_at: i64::MAX,
            registered_at_ms: None,
            token_digest: "synthetic".into(),
        })
    }
    async fn check(&self, _: &Principal) -> accounts::Result<()> {
        self.events.lock().unwrap().push("check");
        if self.revoked.load(Ordering::SeqCst) {
            Err(accounts::Error::Unauthorized)
        } else {
            Ok(())
        }
    }
    async fn logout(&self, token: &str) -> accounts::Result<()> {
        assert!(token.starts_with("synthetic-login-"));
        self.events.lock().unwrap().push("logout");
        Ok(())
    }
    async fn quota(&self, _: &Principal) -> accounts::Result<accounts::QuotaResponse> {
        Err(accounts::Error::Unavailable)
    }
    async fn model_parameters(&self, _: &Principal) -> accounts::Result<Parameters> {
        self.events.lock().unwrap().push("model");
        Ok(self.parameters.lock().unwrap().clone())
    }
}
struct Registry {
    auth: Arc<Auth>,
    values: Mutex<Vec<Parameters>>,
    failure: AtomicUsize,
    pause: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ConfigurationRegistry for Registry {
    async fn get(
        &self,
        _: ConfigurationScope<'_>,
        _: &str,
    ) -> accounts::Result<Option<RegisteredConfiguration>> {
        panic!("login must register current configuration directly")
    }
    async fn register(
        &self,
        scope: ConfigurationScope<'_>,
        name: &str,
        value: &Parameters,
    ) -> accounts::Result<String> {
        assert_eq!(scope.tenant, "tenant");
        assert_eq!(scope.user, "alice");
        assert_eq!(
            scope.workspace,
            workspace_parameters("tenant", "digitalmate", "alice").unwrap()["workspace"]
        );
        assert_eq!(name, "MODEL_CONFIG");
        self.auth.events.lock().unwrap().push("register");
        self.entered.notify_one();
        if self.pause.load(Ordering::SeqCst) {
            self.resume.notified().await;
        }
        match self.failure.load(Ordering::SeqCst) {
            1 => return Err(accounts::Error::Unavailable),
            2 => return Err(accounts::Error::Conflict),
            3 => self.auth.revoked.store(true, Ordering::SeqCst),
            _ => (),
        }
        self.values.lock().unwrap().push(value.clone());
        Ok("revision".into())
    }
}
fn fixture() -> (Arc<Auth>, Arc<Registry>) {
    let auth = Arc::new(Auth {
        events: Arc::new(Mutex::new(vec![])),
        parameters: Mutex::new(Parameters::from([
            ("model_api_key".into(), "synthetic-first".into()),
            ("model_api_base".into(), "https://first.test/v1".into()),
            ("unrelated".into(), "not-registered".into()),
        ])),
        logins: AtomicUsize::new(0),
        fail_login: AtomicBool::new(false),
        revoked: AtomicBool::new(false),
    });
    let registry = Arc::new(Registry {
        auth: auth.clone(),
        values: Mutex::new(vec![]),
        failure: AtomicUsize::new(0),
        pause: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
    });
    (auth, registry)
}
async fn login_request(
    auth: Arc<Auth>,
    registry: Arc<Registry>,
    provider: &'static str,
    registration_configured: bool,
) -> (StatusCode, serde_json::Value) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(stream),
                hyper::service::service_fn(move |request| {
                    let auth = auth.clone();
                    let registry = registry.clone();
                    async move {
                        let registration = registration_configured.then_some(ModelRegistration {
                            tenant: "tenant",
                            template: "digitalmate",
                            name: "MODEL_CONFIG",
                            registry: registry.as_ref(),
                        });
                        Ok::<_, std::convert::Infallible>(
                            auth_request(auth.as_ref(), request, registration).await,
                        )
                    }
                }),
            )
            .await
            .unwrap();
    });
    let payload = if provider == "agc" {
        serde_json::json!({"agcAccessToken":"synthetic"})
    } else {
        serde_json::json!({"authorizationCode":"synthetic"})
    };
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("http://{address}/api/v1/auth/{provider}/login"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let value = response.json().await.unwrap();
    server.abort();
    (status, value)
}

#[tokio::test]
async fn both_login_providers_register_before_returning_session() {
    for provider in ["huawei", "agc"] {
        let (auth, registry) = fixture();
        let (status, value) = login_request(auth.clone(), registry.clone(), provider, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["token"], "synthetic-login-0");
        assert_eq!(
            *auth.events.lock().unwrap(),
            vec![provider, "authenticate", "model", "register", "check"]
        );
        assert_eq!(
            *registry.values.lock().unwrap(),
            vec![Parameters::from([
                ("API_KEY".into(), "synthetic-first".into()),
                ("API_BASE".into(), "https://first.test/v1".into()),
            ])]
        );
    }
}

#[tokio::test]
async fn next_login_registers_current_key_and_base() {
    let (auth, registry) = fixture();
    assert_eq!(
        login_request(auth.clone(), registry.clone(), "huawei", true)
            .await
            .0,
        StatusCode::OK
    );
    auth.parameters.lock().unwrap().extend([
        ("model_api_key".into(), "synthetic-updated".into()),
        ("model_api_base".into(), "https://updated.test/v1".into()),
    ]);
    assert_eq!(
        login_request(auth, registry.clone(), "agc", true).await.0,
        StatusCode::OK
    );
    let values = registry.values.lock().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[1]["API_KEY"], "synthetic-updated");
    assert_eq!(values[1]["API_BASE"], "https://updated.test/v1");
}

#[tokio::test]
async fn failed_registration_withholds_session_and_logs_out() {
    for (failure, status, code) in [
        (1, StatusCode::SERVICE_UNAVAILABLE, "AUTH_UNAVAILABLE"),
        (2, StatusCode::CONFLICT, "REVISION_CONFLICT"),
        (3, StatusCode::UNAUTHORIZED, "UNAUTHORIZED"),
    ] {
        let (auth, registry) = fixture();
        registry.failure.store(failure, Ordering::SeqCst);
        let (actual, value) = login_request(auth.clone(), registry, "huawei", true).await;
        assert_eq!(actual, status);
        assert_eq!(value["code"], code);
        assert!(value.get("token").is_none());
        assert_eq!(auth.events.lock().unwrap().last(), Some(&"logout"));
    }
}

#[tokio::test]
async fn failed_login_and_unconfigured_registration_do_not_register() {
    for registration_configured in [false, true] {
        let (auth, registry) = fixture();
        auth.fail_login
            .store(registration_configured, Ordering::SeqCst);
        let (status, _) = login_request(
            auth.clone(),
            registry.clone(),
            "agc",
            registration_configured,
        )
        .await;
        assert_eq!(
            status,
            if registration_configured {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::OK
            }
        );
        assert_eq!(*auth.events.lock().unwrap(), vec!["agc"]);
        assert!(registry.values.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn login_response_waits_for_registration_completion() {
    let (auth, registry) = fixture();
    registry.pause.store(true, Ordering::SeqCst);
    let login = tokio::spawn(login_request(auth, registry.clone(), "huawei", true));
    tokio::time::timeout(Duration::from_secs(2), registry.entered.notified())
        .await
        .unwrap();
    assert!(!login.is_finished());
    registry.resume.notify_one();
    assert_eq!(login.await.unwrap().0, StatusCode::OK);
}

#[tokio::test]
async fn registration_timeout_withholds_session_and_drops_waiting_work() {
    let (auth, registry) = fixture();
    registry.pause.store(true, Ordering::SeqCst);
    let login = tokio::spawn(login_request(auth.clone(), registry.clone(), "agc", true));
    tokio::time::timeout(Duration::from_secs(2), registry.entered.notified())
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(46)).await;
    let (status, value) = login.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(value["code"], "AUTH_UNAVAILABLE");
    assert!(value.get("token").is_none());
    assert!(registry.values.lock().unwrap().is_empty());
    assert_eq!(auth.events.lock().unwrap().last(), Some(&"logout"));
}

#[tokio::test]
async fn missing_model_parameters_withholds_session_without_registering() {
    for missing in ["model_api_key", "model_api_base"] {
        let (auth, registry) = fixture();
        auth.parameters.lock().unwrap().remove(missing);
        let (status, value) = login_request(auth.clone(), registry.clone(), "huawei", true).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(value.get("token").is_none());
        assert!(registry.values.lock().unwrap().is_empty());
        assert_eq!(auth.events.lock().unwrap().last(), Some(&"logout"));
    }
}
