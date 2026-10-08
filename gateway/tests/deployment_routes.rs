use data_plane_gateway::{config::DEFAULT_CONTROL_PLANE_ROUTES, ingress::parse_static_routes};
use serde_json::Value;

fn example() -> Value {
    serde_saphyr::from_str(include_str!("../../build/config/examples/deployment.yaml")).unwrap()
}

#[test]
fn default_routes_forward_key_management_to_authenticated_frontend() {
    let routes = parse_static_routes(DEFAULT_CONTROL_PLANE_ROUTES).unwrap();
    for path in ["/api/admin/v1/keys", "/api/admin/v1/keys/key-1"] {
        assert!(
            routes.iter().any(|r| r.matches(path)),
            "missing route {path}"
        );
    }
    assert!(!routes.iter().any(|r| r.matches("/api/admin/v1/keys-other")));
}

#[test]
fn shipped_example_routes_reach_its_frontend_listener() {
    let config = example();
    let services = config["services"].as_array().unwrap();
    let ingress = services.iter().find(|s| s["role"] == "ingress").unwrap();
    let api = services.iter().find(|s| s["role"] == "apiserver").unwrap();
    assert_eq!(api["config"]["loopback_http"], true);
    assert_eq!(
        ingress["env"]["ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS"],
        api["config"]["listen"]
    );
    let routes = parse_static_routes(
        ingress["env"]["ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    for path in [
        "/api/instances",
        "/api/sandbox/v1/snapshots",
        "/api/admin/v1/keys",
    ] {
        assert!(
            routes.iter().any(|r| r.matches(path)),
            "missing example route {path}"
        );
    }
}

#[test]
fn shipped_example_configures_both_sides_of_proxy_mtls() {
    let config = example();
    let services = config["services"].as_array().unwrap();
    let ingress = &services.iter().find(|s| s["role"] == "ingress").unwrap()["env"];
    let proxy = &services.iter().find(|s| s["role"] == "adxlet").unwrap()["env"];
    for env in [ingress, proxy] {
        assert_eq!(env["ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE"], "mtls");
    }
    for key in [
        "ADX_DATA_PLANE_RELAY_TLS_CERT",
        "ADX_DATA_PLANE_RELAY_TLS_KEY",
        "ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA",
    ] {
        assert!(
            !proxy[key].as_str().unwrap_or_default().is_empty(),
            "missing {key}"
        );
    }
    for key in [
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CA",
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_SERVER_NAME",
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_CERT",
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_KEY",
    ] {
        assert!(
            !ingress[key].as_str().unwrap_or_default().is_empty(),
            "missing {key}"
        );
    }
    assert_eq!(
        ingress["ADX_DATA_PLANE_INGRESS_NODE_TLS_CA"],
        proxy["ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA"]
    );
}
