use adx_deployment::config::{Deployment, Role};
use serde_json::json;

const MINIMAL_YAML: &str = r#"
schema_version: 1
package_dir: /opt/adx
state_dir: /run/adx/test
redis_url: redis://127.0.0.1:6379/
namespace: test
restart_limit: 2
restart_delay_ms: 1000
stop_timeout_seconds: 30
services:
  - id: master
    role: master
    config: {}
"#;

fn test_deployment(root: &std::path::Path) -> Deployment {
    serde_json::from_value(json!({"schema_version":1,"package_dir":root,"state_dir":root,"redis_url":"redis://localhost:6379/","namespace":"test","restart_limit":2,"restart_delay_ms":20,"stop_timeout_seconds":3,"services":[{"id":"node","role":"node-manager","config":{"discovery":{"namespace":"wrong"},"proxy_socket":root.join("route.sock")},"env":{"ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR":root}},{"id":"master","role":"master","config":{}}]})).unwrap()
}

#[test]
fn load_accepts_yaml_and_rejects_json_deployment_files() {
    let root = tempfile::tempdir().unwrap();
    let yaml_path = root.path().join("deployment.yaml");
    std::fs::write(&yaml_path, MINIMAL_YAML).unwrap();
    let deployment = Deployment::load(&yaml_path).unwrap();
    assert_eq!(deployment.namespace, "test");
    assert_eq!(deployment.services.len(), 1);

    let json_path = root.path().join("deployment.json");
    std::fs::write(&json_path, r#"{"schema_version":1}"#).unwrap();
    let error = match Deployment::load(&json_path) {
        Ok(_) => panic!("JSON deployment input must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains(".yaml or .yml"));
}

#[test]
fn load_expands_environment_in_string_values_after_yaml_parsing() {
    let root = tempfile::tempdir().unwrap();
    let yaml_path = root.path().join("deployment.yaml");
    let yaml = MINIMAL_YAML
        .replace(
            "namespace: test",
            "namespace: ${ADX_TEST_DEPLOYMENT_NAMESPACE}",
        )
        .replace(
            "redis_url: redis://127.0.0.1:6379/",
            "redis_url: ${ADX_TEST_REDIS_URL:-redis://127.0.0.1:6380/}",
        );
    std::fs::write(&yaml_path, yaml).unwrap();
    // SAFETY: this test owns a uniquely named variable and no other test mutates it.
    unsafe { std::env::set_var("ADX_TEST_DEPLOYMENT_NAMESPACE", "environment-test") };

    let deployment = Deployment::load(&yaml_path).unwrap();

    assert_eq!(deployment.namespace, "environment-test");
    assert_eq!(deployment.redis_url, "redis://127.0.0.1:6380/");
    // SAFETY: this test removes the same uniquely named variable before returning.
    unsafe { std::env::remove_var("ADX_TEST_DEPLOYMENT_NAMESPACE") };
}

#[test]
fn load_rejects_missing_or_malformed_environment_references() {
    let root = tempfile::tempdir().unwrap();
    for (name, namespace, expected) in [
        (
            "missing.yaml",
            "${ADX_TEST_DEPLOYMENT_UNSET}",
            "ADX_TEST_DEPLOYMENT_UNSET",
        ),
        (
            "malformed.yaml",
            "${INVALID-NAME}",
            "invalid environment variable",
        ),
    ] {
        let path = root.path().join(name);
        std::fs::write(
            &path,
            MINIMAL_YAML.replace("namespace: test", &format!("namespace: {namespace}")),
        )
        .unwrap();
        let error = match Deployment::load(&path) {
            Ok(_) => panic!("invalid environment reference must fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

#[test]
fn profile_configuration_recursively_overrides_a_typed_default() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("deployment.yaml");
    std::fs::write(
        &path,
        r#"
schema_version: 1
profile: node
state_dir: /run/adx/custom-node
redis_url: redis://redis.example:6379/
namespace: profile-test
logging:
  max_files: 9
service_overrides:
  node-manager:
    config:
      node_id: custom-node
      tls:
        certificate: /etc/adx/tls/custom-node.pem
    env:
      ADX_DATA_PLANE_NODE_PROXY_BIND: 192.0.2.10:19002
"#,
    )
    .unwrap();

    let deployment = Deployment::load(&path).unwrap();

    assert_eq!(
        deployment.state_dir,
        std::path::Path::new("/run/adx/custom-node")
    );
    assert_eq!(deployment.namespace, "profile-test");
    assert_eq!(deployment.logging.max_files, 9);
    assert_eq!(deployment.logging.max_file_bytes, 104_857_600);
    assert_eq!(deployment.services.len(), 1);
    let node = deployment
        .services
        .iter()
        .find(|service| service.role == Role::NodeManager)
        .unwrap();
    assert_eq!(node.role, Role::NodeManager);
    assert_eq!(node.config["node_id"], "custom-node");
    assert_eq!(node.config["listen"], "0.0.0.0:19001");
    assert_eq!(node.config["tls"]["ca"], "/opt/adx/config/tls/ca.pem");
    assert_eq!(
        node.config["tls"]["certificate"],
        "/etc/adx/tls/custom-node.pem"
    );
    assert_eq!(
        node.env["ADX_DATA_PLANE_NODE_PROXY_BIND"],
        "192.0.2.10:19002"
    );
    assert!(node
        .env
        .contains_key("ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR"));

    let effective_path = root.path().join("effective.yaml");
    std::fs::write(&effective_path, deployment.effective_yaml().unwrap()).unwrap();
    let reloaded = Deployment::load(&effective_path).unwrap();
    assert_eq!(reloaded.services.len(), deployment.services.len());
    assert_eq!(reloaded.namespace, deployment.namespace);
}

#[test]
fn profile_configuration_rejects_unknown_roles_and_full_service_lists() {
    let root = tempfile::tempdir().unwrap();
    for (name, body, expected) in [
        (
            "unknown-role.yaml",
            r#"
schema_version: 1
profile: master
service_overrides:
  redis:
    config: {}
"#,
            "profile does not contain role: redis",
        ),
        (
            "mixed-mode.yaml",
            r#"
schema_version: 1
profile: master
services: []
"#,
            "unknown field",
        ),
    ] {
        let path = root.path().join(name);
        std::fs::write(&path, body).unwrap();
        let error = match Deployment::load(&path) {
            Ok(_) => panic!("invalid profile configuration must fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

#[test]
fn worker_profile_requires_an_explicit_cluster_node_id() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("node.yaml");
    std::fs::write(&path, "schema_version: 1\nprofile: node\n").unwrap();

    let error = match Deployment::load(&path) {
        Ok(_) => panic!("worker profile without node identity must fail"),
        Err(error) => error.to_string(),
    };

    assert!(error.contains("node profile requires config.node_id"));
}

#[test]
fn every_minimal_profile_resolves_to_its_expected_roles() {
    let root = tempfile::tempdir().unwrap();
    for (profile, extra, roles) in [
        (
            "standalone",
            "",
            vec![
                Role::Redis,
                Role::Master,
                Role::NodeManager,
                Role::ApiServer,
                Role::Edge,
            ],
        ),
        (
            "standalone-external-redis",
            "",
            vec![Role::Master, Role::NodeManager, Role::ApiServer, Role::Edge],
        ),
        ("master", "", vec![Role::Master]),
        (
            "node",
            "service_overrides:\n  node-manager:\n    config:\n      node_id: worker-a\n",
            vec![Role::NodeManager],
        ),
        ("edge-api", "", vec![Role::ApiServer, Role::Edge]),
    ] {
        let path = root.path().join(format!("{profile}.yaml"));
        std::fs::write(
            &path,
            format!("schema_version: 1\nprofile: {profile}\n{extra}"),
        )
        .unwrap();
        let deployment = Deployment::load(&path).unwrap();
        assert_eq!(
            deployment
                .services
                .iter()
                .map(|service| service.role)
                .collect::<Vec<_>>(),
            roles
        );
    }
}

#[test]
fn edge_api_profile_embeds_edge_in_api_server_by_default() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment: Deployment = serde_saphyr::from_str(include_str!(
        "../../../build/config/examples/deployment-edge-api.yaml"
    ))
    .unwrap();
    deployment.package_dir = root.path().join("package");
    deployment.state_dir = root.path().join("state");

    let output = root.path().join("embedded-edge");
    let processes = deployment.render(&output).unwrap();
    assert_eq!(processes.len(), 1);
    let api = &processes[0];
    assert_eq!(api.role, Role::ApiServer);
    assert_eq!(api.binary.file_name().unwrap(), "adx-api-server");
    assert_eq!(
        api.env
            .get("ADX_DATA_PLANE_EDGE_FRONTEND_TLS_BIND")
            .map(String::as_str),
        Some("0.0.0.0:8443")
    );
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("api-server.json")).unwrap()).unwrap();
    assert_eq!(config["edge_mode"], "embedded");
    assert_eq!(config["edge_control"]["namespace"], "adx");
    assert_eq!(
        config["edge_control"]["tls"]["certificate"],
        "/opt/adx/config/tls/edge.pem"
    );
    assert!(!output.join("edge.json").exists());
}

#[test]
fn embedded_edge_accepts_identical_shared_environment_and_rejects_conflicts() {
    let mut deployment: Deployment = serde_saphyr::from_str(include_str!(
        "../../../build/config/examples/deployment-edge-api.yaml"
    ))
    .unwrap();
    let api = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::ApiServer)
        .unwrap();
    api.env.insert("RUST_LOG".into(), "info".into());
    let edge = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::Edge)
        .unwrap();
    edge.env.insert("RUST_LOG".into(), "info".into());

    deployment.validate().unwrap();

    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::Edge)
        .unwrap()
        .env
        .insert("RUST_LOG".into(), "debug".into());
    let error = deployment.validate().unwrap_err().to_string();
    assert!(error.contains("conflict"), "unexpected error: {error}");
}

#[test]
fn edge_api_profile_can_render_explicit_standalone_edge() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment: Deployment = serde_saphyr::from_str(include_str!(
        "../../../build/config/examples/deployment-edge-api.yaml"
    ))
    .unwrap();
    deployment.package_dir = root.path().join("package");
    deployment.state_dir = root.path().join("state");
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::ApiServer)
        .unwrap()
        .config["edge_mode"] = json!("standalone");

    let processes = deployment
        .render(&root.path().join("standalone-edge"))
        .unwrap();
    assert_eq!(processes.len(), 2);
    assert!(processes
        .iter()
        .any(|process| process.binary.file_name().unwrap() == "adx-api-server"));
    assert!(processes
        .iter()
        .any(|process| process.binary.file_name().unwrap() == "adx-edge-frontend"));
}
#[test]
fn roles_order_common_discovery_and_private_configuration() {
    let root = tempfile::tempdir().unwrap();
    let deployment = test_deployment(root.path());
    let output_directory = root.path().join("generated");
    let processes = deployment.render(&output_directory).unwrap();
    assert_eq!(
        processes.first().map(|process| process.role),
        Some(Role::Master)
    );
    let node: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_directory.join("node.json")).unwrap())
            .unwrap();
    assert_eq!(node["discovery"]["namespace"], "test");
    assert_eq!(
        node["admin_socket"],
        root.path().join("node-admin.sock").to_str().unwrap()
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(output_directory.join("node.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(deployment.render(&output_directory).is_err());
}
#[test]
fn invalid_or_duplicate_ids_and_unknown_roles_fail() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    let node_id = deployment
        .services
        .iter()
        .find(|service| service.role == Role::NodeManager)
        .unwrap()
        .id
        .clone();
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::Master)
        .unwrap()
        .id
        .clone_from(&node_id);
    assert!(deployment.validate().is_err());
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::Master)
        .unwrap()
        .id = "../escape".into();
    assert!(deployment.validate().is_err());
    assert!(serde_json::from_value::<Role>(json!("sandboxd")).is_err());
}

#[test]
fn malformed_discovery_and_unbounded_timeouts_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap()
        .config["discovery"] = json!("invalid");
    assert!(deployment.validate().is_err());
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap()
        .config["discovery"] = json!({});
    deployment.stop_timeout_seconds = u64::MAX;
    assert!(deployment.validate().is_err());
}

#[test]
fn redis_aof_is_generated_from_the_shared_deployment() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    deployment.services=vec![serde_json::from_value(json!({"id":"redis","role":"redis","config":{"bind":"127.0.0.1","port":6379,"data_dir":root.path().join("data"),"appendfsync":"everysec"}})).unwrap()];
    let output_directory = root.path().join("generated");
    let processes = deployment.render(&output_directory).unwrap();
    let redis_config_path = processes
        .first()
        .and_then(|process| process.args.first())
        .unwrap();
    let text = std::fs::read_to_string(redis_config_path).unwrap();
    assert!(text.contains("appendonly yes\nappendfsync everysec"));
    deployment.services.first_mut().unwrap().config["appendfsync"] = json!("invalid");
    assert!(deployment.validate().is_err());
}

#[test]
fn remote_redis_requires_private_password_configuration() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    deployment.services = vec![serde_json::from_value(json!({"id":"redis","role":"redis","config":{"bind":"0.0.0.0","port":6379,"data_dir":root.path().join("data"),"appendfsync":"always"}})).unwrap()];
    assert!(deployment.validate().is_err());
    let password = root.path().join("redis-password");
    std::fs::write(&password, "01234567890123456789012345678901\"\\\n").unwrap();
    deployment.services.first_mut().unwrap().config["password_file"] = json!(password);
    let processes = deployment.render(&root.path().join("generated")).unwrap();
    let redis_config_path = processes
        .first()
        .and_then(|process| process.args.first())
        .unwrap();
    let text = std::fs::read_to_string(redis_config_path).unwrap();
    assert!(text.contains("protected-mode yes"));
    assert!(text.starts_with("requirepass \""));
    assert_eq!(text.lines().count(), 8);
    std::fs::write(password, "invalid\nrequirepass injected").unwrap();
    assert!(deployment.render(&root.path().join("bad")).is_err());
}

#[test]
fn embedded_proxy_has_one_socket_owner_and_matches_control_path() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    let proxy_directory = root.path().join("proxy");
    let node = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap();
    node.config["proxy_socket"] = json!(proxy_directory.join("route.sock"));
    node.env.insert(
        "ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR".into(),
        proxy_directory.to_string_lossy().into_owned(),
    );
    deployment.validate().unwrap();
    deployment.services.push(serde_json::from_value(json!({"id":"proxy","role":"node-proxy","config":{},"env":{"ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR":proxy_directory}})).unwrap());
    assert!(
        deployment.validate().is_err(),
        "two services cannot own the same binding socket"
    );
    deployment.services.pop();
    let node = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap();
    node.config["proxy_socket"] = json!(root.path().join("different.sock"));
    assert!(
        deployment.validate().is_err(),
        "embedded proxy must serve the configured binding path"
    );
    deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap()
        .config["proxy_mode"] = json!("invalid");
    assert!(deployment.validate().is_err());
}

#[test]
fn standalone_proxy_is_an_explicit_two_process_deployment() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    let node = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::NodeManager)
        .unwrap();
    node.config["proxy_mode"] = json!("standalone");
    node.env.clear();
    deployment.services.push(
        serde_json::from_value(json!({
            "id": "node-proxy",
            "role": "node-proxy",
            "env": {"ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR": root.path()}
        }))
        .unwrap(),
    );

    deployment.validate().unwrap();
    let processes = deployment.render(&root.path().join("standalone")).unwrap();
    assert!(processes
        .iter()
        .any(|process| process.role == Role::NodeProxy));
    assert!(processes
        .iter()
        .any(|process| process.role == Role::NodeManager));
}

#[test]
fn default_embedded_mode_rejects_a_second_proxy_process() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    let other = root.path().join("other-proxy");
    deployment.services.push(
        serde_json::from_value(json!({
            "id": "node-proxy",
            "role": "node-proxy",
            "env": {"ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR": other}
        }))
        .unwrap(),
    );

    let error = deployment.validate().unwrap_err().to_string();
    assert!(error.contains("embedded Node Proxy cannot be combined"));
}

#[test]
fn sandbox_api_discovery_defaults_and_overrides_are_usable() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment: Deployment = serde_saphyr::from_str(include_str!(
        "../../../build/config/examples/deployment.yaml"
    ))
    .unwrap();
    deployment.state_dir = root.path().to_owned();
    deployment.render(&root.path().join("defaults")).unwrap();
    let generated: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("defaults/api-server.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(generated["discovery"]["poll_seconds"], 5);
    let api_server = deployment
        .services
        .iter_mut()
        .find(|service| service.role == Role::ApiServer)
        .unwrap();
    api_server.config["discovery"] = json!({"poll_seconds": 2});
    deployment.render(&root.path().join("override")).unwrap();
    let generated: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.path().join("override/api-server.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(generated["discovery"]["poll_seconds"], 2);
    for invalid in [json!(0), json!(-1), json!("5"), json!(null), json!(86401)] {
        deployment
            .services
            .iter_mut()
            .find(|service| service.role == Role::ApiServer)
            .unwrap()
            .config["discovery"]["poll_seconds"] = invalid;
        assert!(deployment.validate().is_err());
    }
}

#[test]
fn metrics_endpoints_are_preserved_by_deployment_rendering() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    for (role, address) in [
        (Role::NodeManager, "127.0.0.1:19091"),
        (Role::Master, "127.0.0.1:19090"),
    ] {
        deployment
            .services
            .iter_mut()
            .find(|service| service.role == role)
            .unwrap()
            .config["metrics_listen"] = json!(address);
    }
    let output_directory = root.path().join("metrics-config");
    deployment.render(&output_directory).unwrap();
    for (id, port) in [("node", 19091), ("master", 19090)] {
        let config: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output_directory.join(format!("{id}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(config["metrics_listen"], format!("127.0.0.1:{port}"));
    }
}

#[test]
fn unified_deployment_renders_control_and_data_plane_services() {
    let root = tempfile::tempdir().unwrap();
    let mut deployment = test_deployment(root.path());
    let proxy_directory = root.path().join("node-proxy");
    deployment.services = serde_json::from_value(json!([
        {"id":"master","role":"master"},
        {"id":"api","role":"api-server"},
        {"id":"edge","role":"edge"},
        {
            "id":"proxy",
            "role":"node-proxy",
            "env": {
                "ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR": proxy_directory
            }
        },
        {
            "id":"node",
            "role":"node-manager",
            "config":{"proxy_mode":"standalone"}
        }
    ]))
    .unwrap();

    let rendered = deployment
        .render(&root.path().join("generated-unified-deployment"))
        .unwrap();
    let binaries = rendered
        .iter()
        .map(|process| {
            (
                process.id.as_str(),
                process
                    .binary
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let expected = std::collections::BTreeMap::from([
        ("api", "adx-api-server".to_owned()),
        ("master", "adx-master".to_owned()),
        ("node", "adx-node-manager".to_owned()),
        ("proxy", "adx-node-proxy".to_owned()),
    ]);
    assert_eq!(binaries, expected);
}

#[test]
fn deployment_accepts_bounded_log_rotation_policy() {
    let deployment: Deployment = serde_json::from_value(json!({"schema_version":1,"package_dir":"/tmp/package","state_dir":"/tmp/state","redis_url":"redis://localhost:6379/","namespace":"test","restart_limit":2,"restart_delay_ms":20,"stop_timeout_seconds":3,"services":[{"id":"master","role":"master","config":{}}],"logging":{"enabled":true,"max_file_bytes":1024,"rotate_seconds":60,"compress":true,"max_files":4,"max_age_seconds":3600,"max_total_bytes":8192}})).unwrap();
    deployment.validate().unwrap();
}

#[test]
fn shipped_role_deployment_examples_are_valid() {
    let examples = [
        include_str!("../../../build/config/examples/deployment.yaml"),
        include_str!("../../../build/config/examples/deployment-standalone-managed-redis.yaml"),
        include_str!("../../../build/config/examples/deployment-master.yaml"),
        include_str!("../../../build/config/examples/deployment-node.yaml"),
        include_str!("../../../build/config/examples/deployment-edge-api.yaml"),
    ];

    for example in examples {
        let deployment: Deployment = serde_saphyr::from_str(example).unwrap();
        deployment.validate().unwrap();
    }
}

#[test]
fn common_environment_is_rendered_to_node_and_api() {
    let root = tempfile::tempdir().unwrap();
    let environment = json!({"rootfs":{"runtime_class":"runsc","type":"local","path":"/opt/adx/root.img","readonly":false},
       "bootstrap":{"type":"erofs","root":"/opt/adx/root.img","target":"/__adx","entrypoint":["/__adx/usr/local/bin/rrt-runtime"]}});
    let mut deployment = test_deployment(root.path());
    deployment.environment = Some(serde_json::from_value(environment).unwrap());
    deployment
        .services
        .push(serde_json::from_value(json!({"id":"api","role":"api-server","config":{}})).unwrap());
    let output_directory = root.path().join("env-config");
    deployment.render(&output_directory).unwrap();
    for role in ["node", "api"] {
        let rendered_config: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output_directory.join(format!("{role}.json"))).unwrap(),
        )
        .unwrap();
        let actual: adx_core::environment::EnvironmentSpec =
            serde_json::from_value(rendered_config["environment"].clone()).unwrap();
        assert_eq!(Some(actual), deployment.environment);
    }
}

#[test]
fn common_oci_environment_is_rendered_to_node_and_api() {
    let root = tempfile::tempdir().unwrap();
    let image = "registry.local/adx-runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let environment = json!({"rootfs":{"runtime_class":"runc","type":"image","image":image,"readonly":false},
       "bootstrap":{"type":"image","image":image,"target":"/__adx","entrypoint":["/__adx/usr/local/bin/rrt-runtime"]}});
    let mut deployment = test_deployment(root.path());
    deployment.environment = Some(serde_json::from_value(environment).unwrap());
    deployment
        .services
        .push(serde_json::from_value(json!({"id":"api","role":"api-server","config":{}})).unwrap());
    let output_directory = root.path().join("oci-env-config");
    deployment.render(&output_directory).unwrap();
    for role in ["node", "api"] {
        let rendered_config: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output_directory.join(format!("{role}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(rendered_config["environment"]["rootfs"]["image"], image);
        assert_eq!(rendered_config["environment"]["bootstrap"]["image"], image);
    }
}

#[test]
fn network_profile_removes_internal_certificates_but_keeps_public_https() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("deployment.yaml");
    std::fs::write(
        &path,
        "schema_version: 1\nprofile: standalone\ninternal_security: network\n",
    )
    .unwrap();
    let deployment = Deployment::load(&path).unwrap();
    let master = deployment
        .services
        .iter()
        .find(|s| s.role == Role::Master)
        .unwrap();
    assert_eq!(master.config["tls"], json!({"mode": "network"}));
    assert!(master.config["advertised_address"]
        .as_str()
        .unwrap()
        .starts_with("http://"));
    let node = deployment
        .services
        .iter()
        .find(|s| s.role == Role::NodeManager)
        .unwrap();
    assert_eq!(node.config["tls"], json!({"mode": "network"}));
    assert!(!node.env.contains_key("ADX_DATA_PLANE_NODE_PROXY_TLS_KEY"));
    let api = deployment
        .services
        .iter()
        .find(|s| s.role == Role::ApiServer)
        .unwrap();
    assert_eq!(api.config["internal_security"], "network");
    assert!(api.config.get("certificate").is_none());
    let edge = deployment
        .services
        .iter()
        .find(|s| s.role == Role::Edge)
        .unwrap();
    assert_eq!(edge.config["tls"], json!({"mode": "network"}));
    assert_eq!(
        edge.env["ADX_DATA_PLANE_EDGE_FRONTEND_NODE_SECURITY_MODE"],
        "network"
    );
    assert!(edge
        .env
        .contains_key("ADX_DATA_PLANE_EDGE_FRONTEND_TLS_CERT"));
    assert!(edge
        .env
        .contains_key("ADX_DATA_PLANE_EDGE_FRONTEND_TLS_KEY"));
}
