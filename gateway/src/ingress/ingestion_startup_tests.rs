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

fn write(path: &Path, value: serde_json::Value) {
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn formal_ingestion_startup_requires_secret_mounts_webhook_and_stops() {
    run_startup_case(false).await;
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn formal_composio_startup_requires_project_key_and_shared_budget_without_ingestion() {
    run_startup_case(true).await;
}

async fn run_startup_case(composio_only: bool) {
    if std::env::var_os("ADX_INGESTION_STARTUP_CHILD").is_some() {
        if composio_only {
            composio_child().await;
        } else {
            child().await;
        }
        return;
    }
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
    if composio_only {
        settings.as_object_mut().unwrap().remove("ingestion");
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
        if composio_only {
            json!({"project_api_key":"startup-project-key"})
        } else {
            json!({"webhook_secrets":["startup-signing-secret"]})
        },
    );
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command.env_clear().kill_on_drop(true)
        .arg("--exact").arg(if composio_only {"ingress::service::ingestion_startup_tests::formal_composio_startup_requires_project_key_and_shared_budget_without_ingestion"} else {"ingress::service::ingestion_startup_tests::formal_ingestion_startup_requires_secret_mounts_webhook_and_stops"})
        .arg("--ignored").arg("--nocapture").arg("--test-threads=1")
        .env("ADX_INGESTION_STARTUP_CHILD", "1").env("ADX_STARTUP_DB_URL", url)
        .env("ADX_JIUWEN_CONFIG", &gateway).env("ADX_ACCOUNT_CONFIG", account)
        .env("ADX_AGENT_CONFIG", agent).env("ADX_ENVIRONMENT_FILES_CONFIG", files)
        .env("ADX_CREDENTIAL_KEY_FILE", root).env("STARTUP_WEBHOOK_INPUT", secret)
        .env("HUAWEI_OAUTH_CLIENT_ID", "synthetic-client").env("HUAWEI_OAUTH_CLIENT_SECRET", "synthetic-secret")
        .env("STARTUP_MODEL_KEY", "synthetic-model-key").env("STARTUP_ACTIVATOR_TOKEN", "synthetic-activator-token-fixture-at-least-32-bytes")
        .env("STARTUP_EXECD_TOKEN", "synthetic-execd-token-fixture-at-least-32-bytes").env("STARTUP_FILE_KEY", "synthetic-file-key-fixture-at-least-32-bytes")
        .env("NO_PROXY", "127.0.0.1,localhost").env("no_proxy", "127.0.0.1,localhost");
    let output = tokio::time::timeout(Duration::from_secs(30), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "child startup verification failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn composio_child() {
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
    let webhook = client
        .post(format!(
            "http://{address}/api/digitalmate/ingestion/v1/composio/webhook"
        ))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(webhook.status(), http::StatusCode::NOT_FOUND);
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
