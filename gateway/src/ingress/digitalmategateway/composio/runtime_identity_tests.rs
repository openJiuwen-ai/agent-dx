//! Actual PG grants and existing TLS service -> independent Python runtime carrier.
//! Binding observations are controlled; no phone, provider, mailbox or report participates.
use super::super::{
    privacy_center::{BindingReader, PrivacyCenter},
    privacy_cipher::PrivacyCipher,
    privacy_grants::{now, Claims, PrivacyIdentity, TokenSigner},
    privacy_http::PrivacyHttp,
};
use crate::ingress::accounts::{AccountStore, Error};
use adx_agent_core::{AgentBinding, BindingPhase, EnvironmentRef, Scope};
use async_trait::async_trait;
use deadpool_postgres::{Manager, Pool};
use serde_json::json;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Bindings(Mutex<AgentBinding>);
#[async_trait]
impl BindingReader for Bindings {
    async fn binding(&self, _: &Scope) -> Result<AgentBinding, Error> {
        Ok(self.0.lock().unwrap().clone())
    }
}

async fn python(script: &Path, command: &str, config: &Path) {
    let interpreter = std::env::var("ADX_CONNECTOR_TEST_PYTHON").expect("explicit Python runtime");
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(interpreter)
            .kill_on_drop(true)
            .arg("-B")
            .arg(script)
            .arg(command)
            .arg(config)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "Python runtime failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), command);
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual DigitalMate Python consumer"]
async fn normal_connector_recovers_over_real_https_after_access_expiry_without_mobile() {
    let url = std::env::var("ADX_INGESTION_TEST_DATABASE_URL").unwrap();
    let pg: tokio_postgres::Config = url.parse().unwrap();
    assert!(pg
        .get_dbname()
        .unwrap_or("")
        .starts_with("dm_ingestion_test_"));
    let pool = Pool::builder(Manager::new(pg, tokio_postgres::NoTls))
        .max_size(4)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .batch_execute(include_str!("../../accounts/schema.sql"))
        .await
        .unwrap();
    let user = format!("consumer-{}", uuid::Uuid::new_v4());
    client.execute("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,'test','consumer-test','test',$1,'1')", &[&user]).await.unwrap();
    let scope = Scope {
        tenant: "tenant".into(),
        template: "digitalmate".into(),
        template_ref: "1".parse().unwrap(),
        binding_id: format!("binding-{user}"),
    };
    let workspace = format!("workspace-{user}");
    let bindings = Arc::new(Bindings(Mutex::new(AgentBinding {
        scope: scope.clone(),
        generation: "consumer-generation".into(),
        current: EnvironmentRef::new("1".into(), "consumer-environment".into()),
        secondary: None,
        phase: BindingPhase::Active,
        parameters: [("workspace".into(), workspace.clone())].into(),
        volumes: Default::default(),
        creation_attempts: Default::default(),
    })));
    let store = AccountStore::from_url(&url, true, None).await.unwrap();
    let signer = Arc::new(TokenSigner::new(&[9; 32], "tenant"));
    let identity = Arc::new(PrivacyIdentity::new(
        "digitalmate".into(),
        store.clone(),
        bindings.clone(),
        signer.clone(),
    ));
    let credentials = identity
        .admit_workload(&scope, &workspace, &user)
        .await
        .unwrap();
    // Issuer-side construction of an actually expired access token for the same
    // persisted normal grant. The consumer never decodes or signs this payload.
    let claims = signer
        .verify_connector(&credentials.connector, false)
        .unwrap();
    let mut expired = serde_json::to_value(claims).unwrap();
    expired["exp"] = json!(now().unwrap() - 1);
    let expired: Claims = serde_json::from_value(expired).unwrap();
    let token = signer.sign_connector(&expired).unwrap();
    assert!(identity.connector_identity(&token).await.is_err());
    let center = Arc::new(PrivacyCenter::new(
        store,
        PrivacyCipher::new("test".into(), [("test".into(), [9; 32])].into()).unwrap(),
        signer,
    ));
    let http = Arc::new(PrivacyHttp::new(center, identity));
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let tls = adx_transport::tls::http_server_acceptor(
        &fixture.join("ingress-cert.pem"),
        &fixture.join("ingress-key.pem"),
        None,
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stop) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(http.serve(listener, tls, stop));
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("consumer.json");
    std::fs::create_dir(directory.path().join("workspace")).unwrap();
    std::fs::write(&config, serde_json::to_vec(&json!({
        "origin":format!("https://{address}"), "ca":fixture.join("ingress-cert.pem"),
        "workspace_path":directory.path().join("workspace"), "identity_path":directory.path().join("connector.token"),
        "token":token.as_str(), "target":{"tenant":"tenant","user_id":user,"workspace":workspace,
        "template":"digitalmate","template_ref":"1","binding_id":scope.binding_id,
        "binding_generation":"consumer-generation","environment_id":"consumer-environment","service_port":18092}
    })).unwrap()).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
    let script = std::env::var("ADX_CONNECTOR_TEST_SCRIPT").unwrap();
    python(Path::new(&script), "restore-expired", &config).await;
    python(Path::new(&script), "restart-existing", &config).await;
    // No phone session or second grant was created during the two processes.
    let counts = client.query_one("SELECT (SELECT count(*) FROM adx_accounts.sessions WHERE user_id=$1), (SELECT count(*) FROM adx_accounts.privacy_grants WHERE user_id=$1)", &[&user]).await.unwrap();
    assert_eq!(counts.get::<_, i64>(0), 0);
    assert_eq!(counts.get::<_, i64>(1), 1);
    bindings.0.lock().unwrap().generation = "replaced-generation".into();
    python(Path::new(&script), "refuse-replaced", &config).await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
