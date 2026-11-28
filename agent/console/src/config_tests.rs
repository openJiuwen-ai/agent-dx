use crate::config::ConsoleConfig;

fn config() -> serde_json::Value {
    serde_json::json!({"listen":"127.0.0.1:8088","public_origin":"https://console.example.com",
        "agent_api":{"base_url":"https://gateway/api/agent/v2","timeout_ms":1000},
        "apiserver":{"base_url":"https://platform","timeout_ms":1000},
        "ingress":{"base_url":"https://gateway","execd_port":49983,"connect_timeout_ms":1000},
        "cookie_secret_file":"/secret","platform_admins":[],
        "tenants":[{"tenant_id":"std","display_name":"Standard","type":"standard",
            "agent":{"enabled":true},"upstream_token_file":"/token","admin_token_sha256":"ab".repeat(32)}]})
}

#[test]
fn upstream_ca_is_optional_and_accepts_deployment_file_reference() {
    let absent: ConsoleConfig = serde_json::from_value(config()).unwrap();
    assert!(absent.upstream_ca_file.is_none());
    let mut value = config();
    value["upstream_ca_file"] = serde_json::json!("/etc/console/ca.pem");
    let configured: ConsoleConfig = serde_json::from_value(value).unwrap();
    assert_eq!(configured.upstream_ca_file.as_deref(),Some("/etc/console/ca.pem"));
}

#[test]
fn digitalmate_uses_accounts_endpoint_without_database_credentials() {
    let mut raw = config();
    raw["tenants"][0]["type"] = serde_json::json!("digitalmate");
    raw["tenants"][0]["business"] = serde_json::json!({"accounts_api":{"base_url":"https://gateway/api/accounts/v1/admin","timeout_ms":1000,"token_file":"/tenant-token"},"bindings":[{"kind":"digitalmate","template":"app","ref":{"kind":"version","name":"1"}}]});
    let parsed: ConsoleConfig = serde_json::from_value(raw.clone()).unwrap();
    parsed.validate().unwrap();
    raw["tenants"][0]["business"]["bindings"] = serde_json::json!([]);
    serde_json::from_value::<ConsoleConfig>(raw.clone()).unwrap().validate().unwrap();
    raw["tenants"][0]["business"]["database"] = serde_json::json!({"dsn_file":"/old","role":"readonly"});
    assert!(serde_json::from_value::<ConsoleConfig>(raw).is_err());
}
#[test]
fn digitalmate_requires_business_and_standard_has_agent_only() {
    let mut value = config();
    let parsed: ConsoleConfig = serde_json::from_value(value.clone()).unwrap();
    parsed.validate().unwrap();
    value["tenants"][0]["type"] = "digitalmate".into();
    assert!(serde_json::from_value::<ConsoleConfig>(value.clone()).unwrap().validate().is_err());
    value["tenants"][0]["type"] = "standard".into();
    value["tenants"][0]["business"] = serde_json::json!({});
    assert!(serde_json::from_value::<ConsoleConfig>(value).is_err());
}
#[test]
fn config_rejects_duplicate_tenants_bad_digest_and_insecure_origin() {
    let original = config();
    let mut value = original.clone();
    value["tenants"].as_array_mut().unwrap().push(original["tenants"][0].clone());
    assert!(serde_json::from_value::<ConsoleConfig>(value).unwrap().validate().is_err());
    let mut value = original.clone();
    value["tenants"][0]["admin_token_sha256"] = "x".repeat(64).into();
    assert!(serde_json::from_value::<ConsoleConfig>(value).unwrap().validate().is_err());
    let mut value = original;
    value["public_origin"] = "http://console.example.com".into();
    assert!(serde_json::from_value::<ConsoleConfig>(value).unwrap().validate().is_err());
}

#[test]
fn config_rejects_public_origin_path_and_unbounded_resource_limits() {
    for (key,value) in [("public_origin",serde_json::json!("https://console.example.com/unexpected")),
        ("terminal_idle_timeout_seconds",serde_json::json!(0)),("terminal_lost_timeout_seconds",serde_json::json!(0)),
        ("file_preview_bytes_limit",serde_json::json!(0))] {
        let mut raw = config();
        raw[key] = value;
        assert!(serde_json::from_value::<ConsoleConfig>(raw).unwrap().validate().is_err(),"{key}");
    }
}

#[test]
fn accounts_endpoint_rejects_insecure_url_or_relative_credential_file() {
    let mut raw = config();
    raw["tenants"][0]["type"] = "digitalmate".into();
    raw["tenants"][0]["business"] = serde_json::json!({"accounts_api":{"base_url":"http://accounts","timeout_ms":1000,"token_file":"/key"},
        "bindings":[{"kind":"jiuwen","agent_type":"assistant","template":"app","ref":{"kind":"version","name":"1"}}]});
    assert!(serde_json::from_value::<ConsoleConfig>(raw.clone()).unwrap().validate().is_err());
    raw["tenants"][0]["business"]["accounts_api"]["base_url"] = "https://accounts".into();
    raw["tenants"][0]["business"]["accounts_api"]["token_file"] = "relative".into();
    assert!(serde_json::from_value::<ConsoleConfig>(raw).unwrap().validate().is_err());
}
