//! Model configuration names are validated before acquiring runtime resources.
use super::DigitalMateGatewayConfig;
use serde_json::json;

fn model_name_config(name: Option<&str>) -> DigitalMateGatewayConfig {
    serde_json::from_value(json!({
        "tenant": "tenant",
        "jiuwen": {"agent_type": "assistant", "template": "demo", "version": "1"},
        "digitalmate": {"template": "digitalmate", "template_ref": "1", "backend_path": "/ws", "service_port": 18092},
        "privacy": {
            "listen": "127.0.0.1:18444",
            "tls_certificate": "/nonexistent/test-certificate.pem",
            "tls_private_key": "/nonexistent/test-key.pem",
            "template": "digitalmate",
            "model_config_name": name
        }
    })).unwrap()
}

#[test]
fn invalid_model_configuration_names_fail_during_config_validation() {
    for name in [
        "",
        "with space",
        "with/slash",
        "with:colon",
        "with\nnewline",
        "with\ttab",
        "模型配置",
        &"x".repeat(129),
    ] {
        assert!(
            matches!(
                model_name_config(Some(name)).validate(),
                Err(adx_agent_api::Error::Invalid(_))
            ),
            "invalid model configuration name passed validation: {name:?}"
        );
    }
}

#[test]
fn valid_model_configuration_names_need_no_runtime_resources() {
    // Validation must work without reading TLS files, opening a listener or connecting to accounts.
    for name in [
        None,
        Some("DIGITALMATE_MODEL_CONFIG"),
        Some("a"),
        Some("model.v1_key-2"),
        Some(&"x".repeat(128)),
    ] {
        model_name_config(name).validate().unwrap();
    }
}

#[tokio::test]
async fn push_without_composio_validates_its_own_startup_dependencies() {
    let mut config: DigitalMateGatewayConfig =
        serde_json::from_str(include_str!("../../../examples/digitalmategateway.json")).unwrap();
    assert!(config.composio.is_none());
    config.validate().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let settings = config.push.as_mut().unwrap();
    settings.service_account_file = dir.path().join("missing.json");
    assert!(matches!(
        settings.prepare().await,
        Err(super::super::composio::Error::NotConfigured)
    ));
    write_push_account(&settings.service_account_file);
    settings.database.database_url_env = "ADX_PUSH_TEST_MISSING_DATABASE".into();
    assert!(matches!(
        settings.prepare().await,
        Err(super::super::composio::Error::NotConfigured)
    ));
    config.privacy = None;
    assert!(config.validate().is_err());
}

fn write_push_account(path: &std::path::Path) {
    let account = json!({
        "project_id":"123", "key_id":"fixture", "sub_account":"fixture",
        "private_key": include_str!("../../../tests/fixtures/huawei-test-key.pem")
    });
    std::fs::write(path, account.to_string()).unwrap();
}

#[tokio::test]
#[ignore = "requires an empty, task-owned PostgreSQL database"]
async fn push_only_startup_requires_the_shared_schema() {
    const ENV: &str = "ADX_PUSH_TEST_DATABASE_URL";
    let url = std::env::var(ENV).expect("explicit test database URL required");
    let pg: tokio_postgres::Config = url.parse().unwrap();
    assert!(pg.get_dbname().unwrap_or("").starts_with("dm_push_test_"));
    let config: DigitalMateGatewayConfig =
        serde_json::from_str(include_str!("../../../examples/digitalmategateway.json")).unwrap();
    assert!(config.composio.is_none());
    let mut settings = config.push.unwrap();
    let dir = tempfile::tempdir().unwrap();
    settings.service_account_file = dir.path().join("account.json");
    write_push_account(&settings.service_account_file);
    settings.database.database_url_env = ENV.into();
    settings.database.database_allow_plaintext = true;
    let pool = settings.database.open().await.unwrap();
    let db = pool.get().await.unwrap();
    let existing: bool = db
        .query_one(
            "SELECT to_regnamespace('digitalmate_composio_budget') IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!existing, "use a fresh task-owned database");
    assert!(settings.prepare().await.is_err());
    db.batch_execute(include_str!("composio/budget.sql"))
        .await
        .unwrap();
    assert!(settings.prepare().await.is_ok());
}
