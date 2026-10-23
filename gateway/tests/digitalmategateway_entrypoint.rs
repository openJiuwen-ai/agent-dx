#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::digitalmategateway::entrypoint::DigitalMateGatewayConfig;
use serde_json::{json, Value};
fn settings() -> Value {
    json!({"tenant":"tenant","jiuwen":{"agent_type":"assistant","template":"jiuwen","version":"1"}})
}
#[test]
fn authenticated_user_selects_a_stable_and_unambiguous_binding() {
    let config: DigitalMateGatewayConfig = serde_json::from_value(settings()).unwrap();
    config.validate().unwrap();
    let a = config.scope("alice").unwrap();
    assert_eq!(a, config.scope("alice").unwrap());
    assert_ne!(a.binding_id, config.scope("bob").unwrap().binding_id);
    let mut next = settings();
    next["jiuwen"]["version"] = json!("2");
    let next: DigitalMateGatewayConfig = serde_json::from_value(next).unwrap();
    assert_eq!(a.binding_id, next.scope("alice").unwrap().binding_id);
    assert_ne!(a, next.scope("alice").unwrap());
    for field in ["tenant", "agent_type"] {
        let mut other = settings();
        if field == "tenant" {
            other[field] = json!("other");
        } else {
            other["jiuwen"][field] = json!("other");
        }
        let other: DigitalMateGatewayConfig = serde_json::from_value(other).unwrap();
        assert_ne!(a.binding_id, other.scope("alice").unwrap().binding_id);
    }
    assert!(config.scope("").is_err());
}
#[test]
fn configuration_rejects_unbounded_limits() {
    for (field, value) in [
        ("queue_capacity", json!(0)),
        ("write_timeout_seconds", json!(0)),
        ("output_queue_timeout_seconds", json!(0)),
        ("idle_timeout_seconds", json!(1)),
    ] {
        let mut value_config = settings();
        value_config["jiuwen"][field] = value;
        let config: DigitalMateGatewayConfig = serde_json::from_value(value_config).unwrap();
        assert!(config.validate().is_err(), "{field}");
    }
}

#[test]
fn workspace_is_stable_across_versions_but_isolates_users_tenants_and_templates() {
    let config: DigitalMateGatewayConfig = serde_json::from_value(settings()).unwrap();
    let parameters = config.parameters("alice").unwrap();
    assert_eq!(parameters["workspace"].len(), 66);
    assert!(parameters["workspace"].starts_with("w-"));
    assert_ne!(parameters, config.parameters("bob").unwrap());
    for field in ["version", "agent_type"] {
        let mut value = settings();
        if field == "tenant" {
            value[field] = json!("other");
        } else {
            value["jiuwen"][field] = json!("other");
        }
        let other: DigitalMateGatewayConfig = serde_json::from_value(value).unwrap();
        assert_eq!(parameters, other.parameters("alice").unwrap());
    }
    for field in ["tenant", "template"] {
        let mut value = settings();
        if field == "tenant" {
            value[field] = json!("other");
        } else {
            value["jiuwen"][field] = json!("other");
        }
        let other: DigitalMateGatewayConfig = serde_json::from_value(value).unwrap();
        assert_ne!(parameters, other.parameters("alice").unwrap());
    }
}

#[test]
fn checked_in_application_examples_load_and_validate() {
    for source in [
        include_str!("../examples/jiuwen.json"),
        include_str!("../examples/digitalmategateway.json"),
    ] {
        let config: DigitalMateGatewayConfig = serde_json::from_str(source).unwrap();
        config.validate().unwrap();
    }
}

#[test]
fn each_application_validates_its_own_timeout() {
    let source = include_str!("../examples/digitalmategateway.json");
    for app in ["jiuwen", "orbit"] {
        for (field, value) in [
            ("request_timeout_seconds", json!(0)),
            ("request_timeout_seconds", json!(86401)),
        ] {
            let mut config: Value = serde_json::from_str(source).unwrap();
            config[app][field] = value;
            let config: DigitalMateGatewayConfig = serde_json::from_value(config).unwrap();
            assert!(config.validate().is_err(), "{app}.{field}");
        }
    }
    let mut config: Value = serde_json::from_str(source).unwrap();
    config["orbit"]["tenant"] = json!("other");
    assert!(serde_json::from_value::<DigitalMateGatewayConfig>(config).is_err());
}

#[test]
fn agentos_workspace_uses_existing_user_directory_across_templates() {
    let mut value = settings();
    value["jiuwen"]["workspace_identity"] = json!("user");
    let config: DigitalMateGatewayConfig = serde_json::from_value(value.clone()).unwrap();
    let expected = config.parameters("alice").unwrap();
    assert_eq!(expected["workspace"], "alice");
    for field in ["template", "version", "agent_type"] {
        let mut other = value.clone();
        other["jiuwen"][field] = json!("other");
        let other: DigitalMateGatewayConfig = serde_json::from_value(other).unwrap();
        assert_eq!(expected, other.parameters("alice").unwrap());
        assert_ne!(
            config.scope("alice").unwrap(),
            other.scope("alice").unwrap()
        );
    }
    for unsafe_user in ["..", "/root", "a/b", "a\\b", "a${b}"] {
        assert!(config.parameters(unsafe_user).is_err());
    }
}
