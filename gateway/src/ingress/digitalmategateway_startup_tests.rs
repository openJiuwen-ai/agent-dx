//! Actual IngressService bind/serve, account configuration and PG. Provider,
//! subscription intent and upstream service endpoints are controlled fixtures.
use super::*;
use crate::ingress::digitalmategateway::{
    composio::WebhookVerifier,
    entrypoint::DigitalMateGatewayConfig,
    ingestion::SubscriptionBinding,
    server_config::{import_composio_key, import_composio_webhook_secret},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use deadpool_postgres::{Manager, Pool};
use serde_json::json;
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};

static STARTUP_ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct ScopedEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);
impl ScopedEnvironment {
    fn set<const N: usize>(values: [(&'static str, std::ffi::OsString); N]) -> Self {
        Self(
            values
                .into_iter()
                .map(|(name, value)| {
                    let previous = std::env::var_os(name);
                    std::env::set_var(name, value);
                    (name, previous)
                })
                .collect(),
        )
    }
}
impl Drop for ScopedEnvironment {
    fn drop(&mut self) {
        for (name, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

fn write(path: &Path, value: serde_json::Value) {
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn formal_ingestion_startup_requires_secret_mounts_webhook_and_stops() {
    run_startup_case(false, false).await;
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn formal_composio_startup_requires_project_key_and_shared_budget_without_ingestion() {
    run_startup_case(true, false).await;
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn formal_composio_and_ingestion_startup_share_subscription_entry_and_stop() {
    run_startup_case(false, true).await;
}

async fn run_startup_case(composio_only: bool, combined: bool) {
    // These external-database tests run serially; restore every changed process
    // setting, including after panic. Ordinary parallel tests do not mutate them.
    let _serial = STARTUP_ENVIRONMENT.lock().await;
    let url = std::env::var("ADX_INGESTION_TEST_DATABASE_URL").unwrap();
    let pg: tokio_postgres::Config = url.parse().unwrap();
    assert!(pg
        .get_dbname()
        .unwrap_or("")
        .starts_with("dm_ingestion_test_"));
    let pool = Pool::builder(Manager::new(pg, tokio_postgres::NoTls))
        .max_size(2)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    // Published operator schemas, applied with the task-owned schema role.
    client
        .batch_execute(include_str!("accounts/schema.sql"))
        .await
        .unwrap();
    client
        .batch_execute(include_str!("digitalmategateway/ingestion/schema.sql"))
        .await
        .unwrap();
    client
        .execute(
            "DELETE FROM adx_accounts.service_secrets WHERE tenant_id='startup-test'",
            &[],
        )
        .await
        .unwrap();
    drop(client);
    pool.close();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root.hex");
    std::fs::write(&root, hex::encode([7; 32])).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o600)).unwrap();
    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let gateway = directory.path().join("gateway.json");
    let mut settings = json!({
        "tenant":"startup-test", "jiuwen":{"agent_type":"jiuwen","template":"jiuwen","version":"1"},
        "digitalmate":{"template":"digitalmate","template_ref":"1","backend_path":"/digitalmate/ws","service_port":8080},
        "privacy":{"listen":"127.0.0.1:0","template":"digitalmate","tls_certificate":fixture.join("ingress-cert.pem"),"tls_private_key":fixture.join("ingress-key.pem")},
        "ingestion":{"database_url_env":"ADX_STARTUP_DB_URL","database_allow_plaintext":true,"database_max_connections":8,
            "workers":1,"lease_seconds":30,"delivery_timeout_seconds":5,"max_pending_events":100,"max_pending_bytes":8000000,"per_subscription_events":50,"debounce_milliseconds":0,"dedupe_retention_seconds":604800},
    });
    if composio_only || combined {
        if composio_only {
            settings.as_object_mut().unwrap().remove("ingestion");
        }
        settings["composio"] = json!({
            "auth_configs":{"outlook":{"id":"ac_startup","ownership":"composio_managed"}},
            "organization":"startup-test",
            "database":{"database_url_env":"ADX_STARTUP_DB_URL","database_allow_plaintext":true,"database_max_connections":2},
            "limits":{"organization_per_minute":60,"user_per_minute":10}
        });
    }
    write(&gateway, settings);
    let account = directory.path().join("account.json");
    write(
        &account,
        json!({
            "developer_scope":"startup-test", "agreement_version":"1", "database_url_env":"ADX_STARTUP_DB_URL", "database_allow_plaintext":true,
            "litellm":{"management_url":"http://127.0.0.1:1","api_base":"http://127.0.0.1:1","admin_key_env":"STARTUP_MODEL_KEY","max_budget":1.0,"rpm_limit":1,"tpm_limit":128,"allow_plaintext":true},
        }),
    );
    let agent = directory.path().join("agent.json");
    write(
        &agent,
        json!({"activator":{"urls":["http://127.0.0.1:1"],"token_env":"STARTUP_ACTIVATOR_TOKEN","allow_plaintext":true}}),
    );
    let files = directory.path().join("files.json");
    write(
        &files,
        json!({"port":50090,"token_env":"STARTUP_EXECD_TOKEN","directory":{"url":"http://127.0.0.1:1","allow_http":true},"api_key_envs":{"startup-test":"STARTUP_FILE_KEY"}}),
    );
    let secret = directory.path().join("webhook.json");
    write(
        &secret,
        if composio_only || combined {
            json!({"project_api_key":"startup-project-key"})
        } else {
            json!({"webhook_secrets":["startup-signing-secret"]})
        },
    );
    let _environment = ScopedEnvironment::set([
        ("ADX_STARTUP_DB_URL", std::ffi::OsString::from(url)),
        ("ADX_JIUWEN_CONFIG", gateway.into_os_string()),
        ("ADX_ACCOUNT_CONFIG", account.into_os_string()),
        ("ADX_AGENT_CONFIG", agent.into_os_string()),
        ("ADX_ENVIRONMENT_FILES_CONFIG", files.into_os_string()),
        ("ADX_CREDENTIAL_KEY_FILE", root.into_os_string()),
        ("STARTUP_WEBHOOK_INPUT", secret.into_os_string()),
        ("HUAWEI_OAUTH_CLIENT_ID", "synthetic-client".into()),
        ("HUAWEI_OAUTH_CLIENT_SECRET", "synthetic-secret".into()),
        ("STARTUP_MODEL_KEY", "synthetic-model-key".into()),
        (
            "STARTUP_ACTIVATOR_TOKEN",
            "synthetic-activator-token-fixture-at-least-32-bytes".into(),
        ),
        (
            "STARTUP_EXECD_TOKEN",
            "synthetic-execd-token-fixture-at-least-32-bytes".into(),
        ),
        (
            "STARTUP_FILE_KEY",
            "synthetic-file-key-fixture-at-least-32-bytes".into(),
        ),
    ]);
    if composio_only || combined {
        composio_child(combined).await;
    } else {
        child().await;
    }
}

async fn composio_child(combined: bool) {
    let (mut config, control) = super::tests::settings();
    config.listeners = Some(vec![super::super::ListenerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: false,
        allow_http_credentials: true,
        allowed_routes: None,
    }]);
    assert!(
        IngressService::bind(config.clone(), control.clone())
            .await
            .is_err(),
        "project key must come from the configuration owner"
    );
    import_composio_key(
        Path::new(&std::env::var("STARTUP_WEBHOOK_INPUT").unwrap()),
        "new",
    )
    .await
    .unwrap();
    assert!(
        IngressService::bind(config.clone(), control.clone())
            .await
            .is_err(),
        "missing shared accounting must not become an unlimited local budget"
    );
    let pg: tokio_postgres::Config = std::env::var("ADX_STARTUP_DB_URL")
        .unwrap()
        .parse()
        .unwrap();
    let pool = Pool::builder(Manager::new(pg, tokio_postgres::NoTls))
        .max_size(2)
        .build()
        .unwrap();
    let database = pool.get().await.unwrap();
    assert!(
        database
            .query_one(
                "SELECT to_regclass('digitalmate_composio_budget.version')::text",
                &[]
            )
            .await
            .unwrap()
            .get::<_, Option<String>>(0)
            .is_none(),
        "formal startup must not create the budget schema"
    );
    // The separately applied operator migration, never application startup DDL.
    database
        .batch_execute(include_str!("digitalmategateway/composio/budget.sql"))
        .await
        .unwrap();
    drop(database);
    if combined {
        assert!(
            IngressService::bind(config.clone(), control.clone())
                .await
                .is_err(),
            "ingestion additionally requires its independently stored webhook secret"
        );
        let input = std::path::PathBuf::from(std::env::var("STARTUP_WEBHOOK_INPUT").unwrap())
            .with_file_name("combined-webhook.json");
        write(
            &input,
            json!({"webhook_secrets":["startup-signing-secret"]}),
        );
        import_composio_webhook_secret(&input, "new").await.unwrap();
    }
    let service = IngressService::bind(config, control).await.unwrap();
    let address = service.public_listeners[0].0.local_addr().unwrap();
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(service.serve(async {
        let _ = stop.await;
    }));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let callback = client.get(format!("http://{address}/api/digitalmate/connectors/v1/composio/callback?session_uri=must-not-echo")).send().await.unwrap();
    assert_eq!(callback.status(), http::StatusCode::OK);
    assert!(!callback.text().await.unwrap().contains("must-not-echo"));
    let denied = client
        .post(format!(
            "http://{address}/api/digitalmate/connectors/v1/composio/cancel"
        ))
        .json(&json!({"registration":"startup"}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), http::StatusCode::UNAUTHORIZED);
    for bearer in [
        None,
        Some("alice-business-session"),
        Some("dm-privacy-v1.controlled"),
        Some("dm-ingestion-v1.controlled"),
    ] {
        let mut request = client
            .post(format!(
                "http://{address}/api/digitalmate/connectors/v1/composio/subscription"
            ))
            .json(&json!({"connection_id":"connection-one"}));
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            http::StatusCode::UNAUTHORIZED
        );
    }
    let webhook = client
        .post(format!(
            "http://{address}/api/digitalmate/ingestion/v1/composio/webhook"
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(
        webhook.status(),
        if combined {
            http::StatusCode::UNAUTHORIZED
        } else {
            http::StatusCode::NOT_FOUND
        }
    );
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    pool.close();
}

async fn child() {
    let (mut config, control) = super::tests::settings();
    config.listeners = Some(vec![super::super::ListenerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: false,
        allow_http_credentials: true,
        allowed_routes: None,
    }]);
    let missing = IngressService::bind(config.clone(), control.clone()).await;
    assert!(
        missing.is_err(),
        "enabled ingestion must fail startup without its server-managed signing secret"
    );
    let secret = std::env::var("STARTUP_WEBHOOK_INPUT").unwrap();
    import_composio_webhook_secret(Path::new(&secret), "new")
        .await
        .unwrap();
    let settings: DigitalMateGatewayConfig = serde_json::from_slice(
        &std::fs::read(std::env::var("ADX_JIUWEN_CONFIG").unwrap()).unwrap(),
    )
    .unwrap();
    let inbox = settings.ingestion.as_ref().unwrap().open().await.unwrap();
    let binding = SubscriptionBinding {
        tenant: "startup-test".into(),
        user_id: "startup-user".into(),
        workspace: "startup-workspace".into(),
        template: "digitalmate".into(),
        template_ref: "1".into(),
        binding_id: "startup-binding".into(),
        binding_generation: "startup-generation".into(),
        service_port: 8080,
        connection_id: "startup-connection".into(),
        connection_generation: 1,
        subscription_id: "startup-subscription".into(),
        subscription_generation: 1,
        trigger_id: "ti_startup".into(),
        trigger_slug: "GMAIL_NEW_GMAIL_MESSAGE".into(),
        account_id: "ca_startup".into(),
        auth_config_id: "ac_startup".into(),
        composio_user_id: "dm_startup".into(),
        enabled: true,
    };
    // Controlled subscription intent; this test proves process assembly, not consent.
    inbox.register(&binding).await.unwrap();
    let service = IngressService::bind(config, control).await.unwrap();
    let address = service.public_listeners[0].0.local_addr().unwrap();
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let running = tokio::spawn(service.serve(async {
        let _ = stop.await;
    }));
    let raw = serde_json::to_vec(&json!({"id":"startup-event","type":"composio.trigger.message","metadata":{"trigger_id":"ti_startup","trigger_slug":"GMAIL_NEW_GMAIL_MESSAGE","connected_account_id":"ca_startup","auth_config_id":"ac_startup","user_id":"dm_startup"},"data":{"text":"synthetic"}})).unwrap();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut signed = format!("wh_startup.{timestamp}.").into_bytes();
    signed.extend_from_slice(&raw);
    let signature = STANDARD.encode(
        ring::hmac::sign(
            &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"startup-signing-secret"),
            &signed,
        )
        .as_ref(),
    );
    let mut headers = http::HeaderMap::new();
    headers.insert("webhook-id", "wh_startup".parse().unwrap());
    headers.insert("webhook-timestamp", timestamp.to_string().parse().unwrap());
    headers.insert(
        "webhook-signature",
        format!("v1,{signature}").parse().unwrap(),
    );
    WebhookVerifier::new(&[b"startup-signing-secret"])
        .unwrap()
        .verify(&headers, &raw, timestamp)
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!(
        "http://{address}{}",
        crate::ingress::digitalmategateway::ingestion::WEBHOOK_PATH
    );
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
    let response = client
        .post(&url)
        .headers(headers)
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "durable");
    shutdown.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
