#![cfg(feature = "agent-api")]
use adx_agent_core::{sandbox::ExecutionSpec, TemplateVersion};
use data_plane_gateway::ingress::jiuwen::download_config::DownloadConfig;
use serde_json::json;

fn template() -> TemplateVersion {
    serde_json::from_value(json!({
        "name":"jiuwen","version":"1","image":"jiuwen:1","isolation_runtime":"runc",
        "resources":{"cpu_millis":1000,"memory_mib":512},
        "env":{"JIUWENSWARM_WORKSPACE":"/data/jiuwen", "JIUWENSWARM_DOWNLOAD_ASSET_ROOT":"/data/assets"},
        "service":[{"protocol":"ws","port":18091}]
    })).unwrap()
}

#[test]
fn download_settings_match_the_environment_passed_to_agentserver() {
    let template = template();
    let config = DownloadConfig::from_template(&template).unwrap();
    let execution = ExecutionSpec::from(&template);
    assert_eq!(config.workspace(), execution.env["JIUWENSWARM_WORKSPACE"]);
    assert_eq!(
        config.asset_root(),
        execution.env["JIUWENSWARM_DOWNLOAD_ASSET_ROOT"]
    );
    assert_eq!(
        config.secret_file(),
        Some("/data/jiuwen/config/.file_download_secret")
    );
    assert_eq!(config.configured_secret(), None);
}

#[test]
fn configured_secret_has_precedence_and_is_never_trimmed_or_debugged() {
    let mut template = template();
    let secret = format!(" {} ", "s".repeat(32));
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), secret.clone());
    let config = DownloadConfig::from_template(&template).unwrap();
    assert_eq!(config.configured_secret(), Some(secret.as_str()));
    assert_eq!(config.secret_file(), None);
    assert!(!format!("{config:?}").contains(&secret));
}

#[test]
fn missing_or_empty_secret_uses_the_current_sandbox_workspace_file() {
    let mut template = template();
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), String::new());
    template
        .env
        .insert("JIUWENSWARM_WORKSPACE".into(), "/data/other/".into());
    let config = DownloadConfig::from_template(&template).unwrap();
    assert_eq!(
        config.secret_file(),
        Some("/data/other/config/.file_download_secret")
    );
    assert_eq!(config.configured_secret(), None);
}

#[test]
fn explicit_paths_are_required_instead_of_ingress_home_or_uid_defaults() {
    for name in ["JIUWENSWARM_WORKSPACE", "JIUWENSWARM_DOWNLOAD_ASSET_ROOT"] {
        let mut template = template();
        template.env.remove(name);
        assert!(DownloadConfig::from_template(&template).is_err());
        for path in [
            "",
            "relative",
            "~/assets",
            "/data/../elsewhere",
            "/data/\0assets",
        ] {
            template.env.insert(name.into(), path.into());
            assert!(
                DownloadConfig::from_template(&template).is_err(),
                "{name}: {path:?}"
            );
        }
    }
}

#[test]
fn invalid_explicit_secret_fails_without_echoing_its_value() {
    let mut template = template();
    let secret = "short-sensitive-value";
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), secret.into());
    let error = DownloadConfig::from_template(&template).unwrap_err();
    assert!(!error.to_string().contains(secret));
    // Python's len(secret) counts Unicode characters rather than UTF-8 bytes.
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), "密".repeat(16));
    assert!(DownloadConfig::from_template(&template).is_err());
    template
        .env
        .insert("JIUWENSWARM_FILE_DOWNLOAD_SECRET".into(), "密".repeat(32));
    assert!(DownloadConfig::from_template(&template).is_ok());
}

#[test]
fn asset_root_whitespace_matches_agentserver_and_paths_are_normalized_lexically() {
    let mut template = template();
    template.env.insert(
        "JIUWENSWARM_DOWNLOAD_ASSET_ROOT".into(),
        "  /data//assets/./  ".into(),
    );
    let config = DownloadConfig::from_template(&template).unwrap();
    assert_eq!(config.asset_root(), "/data/assets");
}
