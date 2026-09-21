use adx_api_server::config::{Config, EdgeMode};
use serde_json::{json, Value};

fn base() -> Value {
    json!({
        "listen":"127.0.0.1:8888",
        "loopback_http":true,
        "master_address":"https://127.0.0.1:19000",
        "discovery":null,
        "ca":"/etc/adx/tls/ca.pem",
        "certificate":"/etc/adx/tls/api-server.pem",
        "private_key":"/etc/adx/tls/api-server.key",
        "server_name":"adx.internal",
        "rpc_timeout_seconds":30,
        "cache_entries":1024,
        "auth_cache_ttl_seconds":10,
        "agent_address":""
    })
}

fn edge_control() -> Value {
    json!({
        "redis_url":"redis://127.0.0.1:6379/",
        "namespace":"adx",
        "tls":{
            "ca":"/etc/adx/tls/ca.pem",
            "certificate":"/etc/adx/tls/edge.pem",
            "private_key":"/etc/adx/tls/edge.key",
            "server_name":"adx.internal",
            "peers":{"master":"/etc/adx/tls/master.der"}
        },
        "rpc_timeout_seconds":5,
        "refresh_seconds":2,
        "auth_cache_seconds":10,
        "auth_cache_entries":4096
    })
}

#[test]
fn rendered_embedded_edge_contract_is_accepted() {
    let mut value = base();
    value["edge_mode"] = json!("embedded");
    value["edge_control"] = edge_control();
    let config: Config = serde_json::from_value(value).unwrap();
    assert!(config.edge_mode == EdgeMode::Embedded);
    config.validate().unwrap();
}

#[test]
fn edge_modes_reject_ambiguous_control_configuration() {
    let mut missing = base();
    missing["edge_mode"] = json!("embedded");
    let missing: Config = serde_json::from_value(missing).unwrap();
    assert!(missing.validate().is_err());

    let mut extra = base();
    extra["edge_mode"] = json!("standalone");
    extra["edge_control"] = edge_control();
    let extra: Config = serde_json::from_value(extra).unwrap();
    assert!(extra.validate().is_err());
}

#[test]
fn api_server_defaults_to_embedded_edge() {
    let config: Config = serde_json::from_value(base()).unwrap();
    assert!(config.edge_mode == EdgeMode::Embedded);
    assert!(config.validate().is_err());

    let mut standalone = base();
    standalone["edge_mode"] = json!("standalone");
    let standalone: Config = serde_json::from_value(standalone).unwrap();
    standalone.validate().unwrap();
}
