#![cfg(feature = "agent-api")]
use data_plane_gateway::ingress::jiuwen::entrypoint::JiuwenConfig;
use serde_json::{json, Value};
fn settings() -> Value {
    json!({"tenant":"tenant","agent_type":"assistant","template":"jiuwen","version":"1","allowed_hosts":["localhost"],"allowed_origins":["https://localhost"]})
}
#[test]
fn authenticated_user_selects_a_stable_and_unambiguous_binding() {
    let config: JiuwenConfig = serde_json::from_value(settings()).unwrap();
    config.validate().unwrap();
    let a = config.scope("alice").unwrap();
    assert_eq!(a, config.scope("alice").unwrap());
    assert_ne!(a.binding_id, config.scope("bob").unwrap().binding_id);
    let mut next = settings();
    next["version"] = json!("2");
    let next: JiuwenConfig = serde_json::from_value(next).unwrap();
    assert_eq!(a.binding_id, next.scope("alice").unwrap().binding_id);
    assert_ne!(a, next.scope("alice").unwrap());
    for field in ["tenant", "agent_type"] {
        let mut other = settings();
        other[field] = json!("other");
        let other: JiuwenConfig = serde_json::from_value(other).unwrap();
        assert_ne!(a.binding_id, other.scope("alice").unwrap().binding_id);
    }
    assert!(config.scope("").is_err());
}
#[test]
fn configuration_rejects_unsafe_origins_and_unbounded_limits() {
    for (field, value) in [
        ("allowed_origins", json!(["https://localhost/path"])),
        ("allowed_hosts", json!([])),
        ("queue_capacity", json!(0)),
        ("write_timeout_seconds", json!(0)),
        ("idle_timeout_seconds", json!(1)),
    ] {
        let mut value_config = settings();
        value_config[field] = value;
        let config: JiuwenConfig = serde_json::from_value(value_config).unwrap();
        assert!(config.validate().is_err(), "{field}");
    }
}

#[test]
fn origin_allowlist_accepts_http_and_https_without_choosing_listener_security() {
    for origin in [
        "http://localhost",
        "http://localhost:8080",
        "https://example.com:20004",
    ] {
        let mut value = settings();
        value["allowed_origins"] = json!([origin]);
        let config: JiuwenConfig = serde_json::from_value(value).unwrap();
        config.validate().unwrap();
    }
    for origin in [
        "null",
        "file:///tmp",
        "ftp://example.com",
        "https://user@example.com",
        "https://example.com/",
        "https://example.com?x=1",
    ] {
        let mut value = settings();
        value["allowed_origins"] = json!([origin]);
        let config: JiuwenConfig = serde_json::from_value(value).unwrap();
        assert!(config.validate().is_err(), "{origin}");
    }
}
