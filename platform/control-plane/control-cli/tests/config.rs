use adx_control_cli::config::{Deployment, Role};
use serde_json::json;
fn config(root: &std::path::Path) -> Deployment {
    serde_json::from_value(json!({"schema_version":1,"package_dir":root,"state_dir":root,"redis_url":"redis://localhost:6379/","namespace":"test","restart_limit":2,"restart_delay_ms":20,"stop_timeout_seconds":3,"services":[{"id":"node","role":"node-manager","config":{"discovery":{"namespace":"wrong"}}},{"id":"master","role":"master","config":{}}]})).unwrap()
}
#[test]
fn roles_order_common_discovery_and_private_configuration() {
    let root = tempfile::tempdir().unwrap();
    let d = config(root.path());
    let dir = root.path().join("generated");
    let ps = d.render(&dir).unwrap();
    assert_eq!(ps[0].role, Role::Master);
    let node: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("node.json")).unwrap()).unwrap();
    assert_eq!(node["discovery"]["namespace"], "test");
    assert_eq!(
        node["admin_socket"],
        root.path().join("node-admin.sock").to_str().unwrap()
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(dir.join("node.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(d.render(&dir).is_err());
}
#[test]
fn invalid_or_duplicate_ids_and_unknown_roles_fail() {
    let root = tempfile::tempdir().unwrap();
    let mut d = config(root.path());
    d.services[1].id = d.services[0].id.clone();
    assert!(d.validate().is_err());
    d.services[1].id = "../escape".into();
    assert!(d.validate().is_err());
    assert!(serde_json::from_value::<Role>(json!("sandboxd")).is_err());
}

#[test]
fn malformed_discovery_and_unbounded_timeouts_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let mut d = config(root.path());
    d.services[0].config["discovery"] = json!("invalid");
    assert!(d.validate().is_err());
    d.services[0].config["discovery"] = json!({});
    d.stop_timeout_seconds = u64::MAX;
    assert!(d.validate().is_err());
}

#[test]
fn redis_aof_is_generated_from_the_shared_deployment() {
    let root = tempfile::tempdir().unwrap();
    let mut d = config(root.path());
    d.services=vec![serde_json::from_value(json!({"id":"redis","role":"redis","config":{"bind":"127.0.0.1","port":6379,"data_dir":root.path().join("data"),"appendfsync":"everysec"}})).unwrap()];
    let dir = root.path().join("generated");
    let ps = d.render(&dir).unwrap();
    let text = std::fs::read_to_string(&ps[0].args[0]).unwrap();
    assert!(text.contains("appendonly yes\nappendfsync everysec"));
    d.services[0].config["appendfsync"] = json!("invalid");
    assert!(d.validate().is_err());
}

#[test]
fn remote_redis_requires_private_password_configuration() {
    let root = tempfile::tempdir().unwrap();
    let mut d = config(root.path());
    d.services = vec![serde_json::from_value(json!({"id":"redis","role":"redis","config":{"bind":"0.0.0.0","port":6379,"data_dir":root.path().join("data"),"appendfsync":"always"}})).unwrap()];
    assert!(d.validate().is_err());
    let password = root.path().join("redis-password");
    std::fs::write(&password, "01234567890123456789012345678901\"\\\n").unwrap();
    d.services[0].config["password_file"] = json!(password);
    let processes = d.render(&root.path().join("generated")).unwrap();
    let text = std::fs::read_to_string(&processes[0].args[0]).unwrap();
    assert!(text.contains("protected-mode yes"));
    assert!(text.starts_with("requirepass \""));
    assert_eq!(text.lines().count(), 8);
    std::fs::write(password, "invalid\nrequirepass injected").unwrap();
    assert!(d.render(&root.path().join("bad")).is_err());
}

#[test]
fn embedded_proxy_has_one_socket_owner_and_matches_control_path() {
    let root = tempfile::tempdir().unwrap();
    let mut d = config(root.path());
    let dir = root.path().join("proxy");
    d.services[0].config["proxy_mode"] = json!("embedded");
    d.services[0].config["proxy_socket"] = json!(dir.join("route.sock"));
    d.services[0].env.insert(
        "ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR".into(),
        dir.to_string_lossy().into_owned(),
    );
    d.validate().unwrap();
    d.services.push(serde_json::from_value(json!({"id":"proxy","role":"node-proxy","config":{},"env":{"ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR":dir}})).unwrap());
    assert!(
        d.validate().is_err(),
        "two services cannot own the same binding socket"
    );
    d.services.pop();
    d.services[0].config["proxy_socket"] = json!(root.path().join("different.sock"));
    assert!(
        d.validate().is_err(),
        "embedded proxy must serve the configured binding path"
    );
    d.services[0].config["proxy_mode"] = json!("invalid");
    assert!(d.validate().is_err());
}

#[test]
fn sandbox_api_discovery_defaults_and_overrides_are_usable() {
    let root = tempfile::tempdir().unwrap();
    let mut d: Deployment = serde_json::from_str(include_str!(
        "../../../../build/config/examples/deployment.json"
    ))
    .unwrap();
    d.state_dir = root.path().to_owned();
    d.render(&root.path().join("defaults")).unwrap();
    let generated: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("defaults/sandbox-api.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(generated["discovery"]["poll_seconds"], 5);
    let api = d
        .services
        .iter()
        .position(|s| s.role == Role::SandboxApi)
        .unwrap();
    d.services[api].config["discovery"] = json!({"poll_seconds": 2});
    d.render(&root.path().join("override")).unwrap();
    let generated: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("override/sandbox-api.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(generated["discovery"]["poll_seconds"], 2);
    for invalid in [json!(0), json!(-1), json!("5"), json!(null), json!(86401)] {
        d.services[api].config["discovery"]["poll_seconds"] = invalid;
        assert!(d.validate().is_err());
    }
}
