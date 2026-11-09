use adx_apiserver::config::{Config, IngressMode, WorkspaceAffinityErrorPolicy};
use serde_json::{json, Value};

fn base() -> Value {
    json!({
        "listen":"127.0.0.1:8888",
        "loopback_http":true,
        "coordinator_address":"https://127.0.0.1:19000",
        "discovery":null,
        "ca":"/etc/adx/tls/ca.pem",
        "certificate":"/etc/adx/tls/apiserver.pem",
        "private_key":"/etc/adx/tls/apiserver.key",
        "server_name":"adx.internal",
        "rpc_timeout_seconds":30,
        "cache_entries":1024,
        "auth_cache_ttl_seconds":10,
        "agent_address":""
    })
}

fn ingress_control() -> Value {
    json!({
        "redis_url":"redis://127.0.0.1:6379/",
        "namespace":"adx",
        "tls":{
            "ca":"/etc/adx/tls/ca.pem",
            "certificate":"/etc/adx/tls/ingress.pem",
            "private_key":"/etc/adx/tls/ingress.key",
            "server_name":"adx.internal",
            "peers":{"coordinator":"/etc/adx/tls/coordinator.der"}
        },
        "rpc_timeout_seconds":5,
        "refresh_seconds":2,
        "auth_cache_seconds":10,
        "auth_cache_entries":4096
    })
}

#[test]
fn rendered_embedded_ingress_contract_is_accepted() {
    let mut value = base();
    value["ingress_mode"] = json!("embedded");
    value["ingress_control"] = ingress_control();
    let config: Config = serde_json::from_value(value).unwrap();
    assert!(config.ingress_mode == IngressMode::Embedded);
    config.validate().unwrap();
}

#[test]
fn ingress_modes_reject_ambiguous_control_configuration() {
    let mut missing = base();
    missing["ingress_mode"] = json!("embedded");
    let missing: Config = serde_json::from_value(missing).unwrap();
    assert!(missing.validate().is_err());

    let mut extra = base();
    extra["ingress_mode"] = json!("standalone");
    extra["ingress_control"] = ingress_control();
    let extra: Config = serde_json::from_value(extra).unwrap();
    assert!(extra.validate().is_err());
}

#[test]
fn apiserver_defaults_to_embedded_ingress() {
    let config: Config = serde_json::from_value(base()).unwrap();
    assert!(config.ingress_mode == IngressMode::Embedded);
    assert!(config.validate().is_err());

    let mut standalone = base();
    standalone["ingress_mode"] = json!("standalone");
    let standalone: Config = serde_json::from_value(standalone).unwrap();
    standalone.validate().unwrap();
}

#[test]
fn unknown_create_retention_is_positive_and_configurable() {
    let mut value = base();
    value["ingress_mode"] = json!("standalone");
    let defaults: Config = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(defaults.create_unknown_retention_seconds, 600);
    value["create_unknown_retention_seconds"] = json!(1);
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(config.create_unknown_retention_seconds, 1);
    config.validate().unwrap();
    value["create_unknown_retention_seconds"] = json!(0);
    let config: Config = serde_json::from_value(value).unwrap();
    assert!(config.validate().is_err());
}

fn affinity() -> Value {
    json!({
        "root":"/home/agentos/users",
        "endpoints":["http://127.0.0.1:18889","http://10.0.0.2:18889"],
        "timeout_seconds":3
    })
}

#[test]
fn workspace_affinity_is_optional_and_validated() {
    let mut value = base();
    value["ingress_mode"] = json!("standalone");
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert!(config.workspace_affinity.is_none());
    config.validate().unwrap();

    value["workspace_affinity"] = affinity();
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    config.validate().unwrap();

    for (field, invalid) in [
        ("root", json!("relative/users")),
        ("root", json!("/")),
        ("timeout_seconds", json!(0)),
        ("endpoints", json!(["ftp://127.0.0.1:18889"])),
        ("endpoints", json!(["http://user:pass@127.0.0.1:18889"])),
    ] {
        let mut invalid_value = value.clone();
        invalid_value["workspace_affinity"][field] = invalid;
        let config: Config = serde_json::from_value(invalid_value).unwrap();
        assert!(
            config.validate().is_err(),
            "{field} should reject its invalid value"
        );
    }
}

#[test]
fn workspace_affinity_error_policy_defaults_and_parses() {
    let mut value = base();
    value["ingress_mode"] = json!("standalone");
    value["workspace_affinity"] = affinity();

    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        config.workspace_affinity.unwrap().on_error,
        WorkspaceAffinityErrorPolicy::FailOpen
    );

    value["workspace_affinity"]["on_error"] = json!("fail_closed");
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        config.workspace_affinity.unwrap().on_error,
        WorkspaceAffinityErrorPolicy::FailClosed
    );

    value["workspace_affinity"]["on_error"] = json!("fail_open");
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        config.workspace_affinity.unwrap().on_error,
        WorkspaceAffinityErrorPolicy::FailOpen
    );

    let mut invalid = value;
    invalid["workspace_affinity"]["on_error"] = json!("fallback");
    assert!(
        serde_json::from_value::<Config>(invalid).is_err(),
        "unknown on_error values must be rejected"
    );
}
