use adx_deployment::cli::{Cli, Command, ConfigCommand, ConfigProfile, DEFAULT_CONFIG_PATH};
use adx_deployment::config::Role;
use clap::Parser;
use std::path::PathBuf;

#[test]
fn parses_typed_subcommands_without_positional_argument_assumptions() {
    let cli = Cli::try_parse_from([
        "adxctl",
        "render",
        "--output",
        "/tmp/generated",
        "--config",
        "/opt/adx/config/deployment.yaml",
    ])
    .expect("render command should accept named options in either order");

    assert_eq!(cli.config, PathBuf::from("/opt/adx/config/deployment.yaml"));
    assert_eq!(
        cli.command,
        Command::Render {
            output: PathBuf::from("/tmp/generated"),
        }
    );
}

#[test]
fn start_is_an_explicit_alias_for_run() {
    let cli = Cli::try_parse_from([
        "adxctl",
        "start",
        "--config",
        "/opt/adx/config/deployment.yaml",
    ])
    .expect("start alias should remain compatible");

    assert_eq!(cli.config, PathBuf::from("/opt/adx/config/deployment.yaml"));
    assert_eq!(cli.command, Command::Run);
}

#[test]
fn uses_the_system_deployment_path_without_repeating_config() {
    let cli = Cli::try_parse_from(["adxctl", "validate"]).unwrap();

    assert_eq!(cli.config, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert_eq!(cli.command, Command::Validate);
}

#[test]
fn accepts_config_before_or_after_the_subcommand() {
    for arguments in [
        ["adxctl", "--config", "/tmp/adx.yaml", "status"],
        ["adxctl", "status", "--config", "/tmp/adx.yaml"],
    ] {
        let cli = Cli::try_parse_from(arguments).unwrap();
        assert_eq!(cli.config, PathBuf::from("/tmp/adx.yaml"));
        assert_eq!(cli.command, Command::Status);
    }
}

#[test]
fn parses_default_and_role_specific_config_templates() {
    let default = Cli::try_parse_from(["adxctl", "config", "init"]).unwrap();
    assert_eq!(default.config, PathBuf::from(DEFAULT_CONFIG_PATH));
    assert_eq!(
        default.command,
        Command::Config {
            command: ConfigCommand::Init {
                profile: ConfigProfile::Standalone,
                force: false,
                compact: false,
            },
        }
    );

    let node = Cli::try_parse_from([
        "adxctl",
        "-c",
        "/tmp/node.yaml",
        "config",
        "init",
        "--profile",
        "node",
        "--force",
    ])
    .unwrap();
    assert_eq!(node.config, PathBuf::from("/tmp/node.yaml"));
    assert_eq!(
        node.command,
        Command::Config {
            command: ConfigCommand::Init {
                profile: ConfigProfile::Node,
                force: true,
                compact: false,
            },
        }
    );
}

#[test]
fn parses_effective_configuration_dump() {
    let cli = Cli::try_parse_from(["adxctl", "config", "dump"]).unwrap();
    assert_eq!(
        cli.command,
        Command::Config {
            command: ConfigCommand::Dump,
        }
    );
}

#[tokio::test]
async fn initializes_a_private_default_without_overwriting_it() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("etc/adx/deployment.yaml");
    let arguments = [
        "adxctl",
        "--config",
        path.to_str().unwrap(),
        "config",
        "init",
    ];
    Cli::try_parse_from(arguments)
        .unwrap()
        .execute()
        .await
        .unwrap();
    let deployment = std::fs::read_to_string(&path).unwrap();
    assert!(deployment.contains("role: redis"));
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let error = Cli::try_parse_from(arguments)
        .unwrap()
        .execute()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("already exists"));
}

#[tokio::test]
async fn initializes_a_compact_profile_that_resolves_to_the_full_deployment() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("deployment.yaml");
    Cli::try_parse_from([
        "adxctl",
        "--config",
        path.to_str().unwrap(),
        "config",
        "init",
        "--profile",
        "node",
        "--compact",
    ])
    .unwrap()
    .execute()
    .await
    .unwrap();

    let compact = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        compact,
        concat!(
            "schema_version: 1\n",
            "profile: node\n",
            "service_overrides:\n",
            "  node-manager:\n",
            "    config:\n",
            "      node_id: \"${ADX_NODE_ID}\"\n",
        )
    );
    std::fs::write(&path, compact.replace("${ADX_NODE_ID}", "worker-test")).unwrap();
    let deployment = adx_deployment::config::Deployment::load(&path).unwrap();
    assert_eq!(deployment.services.len(), 1);
    let node = deployment.services.first().unwrap();
    assert_eq!(node.role, Role::NodeManager);
    assert_eq!(node.config["node_id"], "worker-test");
    assert!(node
        .env
        .contains_key("ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR"));
}

#[tokio::test]
async fn missing_configuration_points_to_the_initializer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing.yaml");
    let error = Cli::try_parse_from(["adxctl", "--config", path.to_str().unwrap(), "validate"])
        .unwrap()
        .execute()
        .await
        .unwrap_err();

    assert!(error.to_string().contains("config init"));
    assert!(error.to_string().contains(path.to_str().unwrap()));
}
