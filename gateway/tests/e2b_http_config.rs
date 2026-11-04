use data_plane_gateway::config::IngressConfig;

#[test]
fn e2b_http_flag_validates_feature_and_defaults() {
    const CHILD: &str = "ADX_TEST_E2B_HTTP_CONFIG_CASE";
    if let Ok(case) = std::env::var(CHILD) {
        let config = IngressConfig::from_env();
        match case.as_str() {
            "default" => assert!(!config.unwrap().e2b_allow_http),
            "enabled" => assert!(config.unwrap().e2b_allow_http),
            "requires-e2b" => assert!(config
                .unwrap_err()
                .to_string()
                .contains("requires E2B_ENABLED")),
            "invalid" => assert!(config.is_err()),
            _ => panic!("unknown test case"),
        }
        return;
    }
    for (case, enabled, allow) in [
        ("default", "true", None),
        ("enabled", "true", Some("true")),
        ("requires-e2b", "false", Some("true")),
        ("invalid", "true", Some("invalid")),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("ADX_DATA_PLANE_") {
                command.env_remove(key);
            }
        }
        command
            .args(["--exact", "e2b_http_flag_validates_feature_and_defaults"])
            .env(CHILD, case)
            .env("ADX_DATA_PLANE_INGRESS_TLS_CERT", "test-cert")
            .env("ADX_DATA_PLANE_INGRESS_TLS_KEY", "test-key")
            .env("ADX_DATA_PLANE_INGRESS_ALLOW_ANY_CLIENT", "true")
            .env("ADX_DATA_PLANE_INGRESS_E2B_ENABLED", enabled)
            .env("ADX_DATA_PLANE_INGRESS_PORT_HOST_DOMAIN", "sandbox.test");
        if let Some(allow) = allow {
            command.env("ADX_DATA_PLANE_INGRESS_E2B_ALLOW_HTTP", allow);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "case {case}: {output:?}");
    }
}
