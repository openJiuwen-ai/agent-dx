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
