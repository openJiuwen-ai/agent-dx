use crate::{
    config::{Deployment, ShutdownMode},
    supervisor::{self, Request},
    Result,
};
use clap::{Parser, Subcommand};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub const DEFAULT_CONFIG_PATH: &str = "/opt/adx/config/deployment.yaml";
pub use crate::config::Profile as ConfigProfile;

/// Deploy and supervise ADX control-plane and data-plane services.
#[derive(Debug, Parser, PartialEq, Eq)]
#[command(name = "adxctl", version, about)]
pub struct Cli {
    /// Deployment YAML. ADX_DEPLOYMENT_CONFIG provides the same override.
    #[arg(
        short = 'c',
        long,
        global = true,
        env = "ADX_DEPLOYMENT_CONFIG",
        default_value = DEFAULT_CONFIG_PATH,
        value_name = "FILE"
    )]
    pub config: PathBuf,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Validate the deployment YAML without changing the host.
    Validate,
    /// Render per-service configuration into a new directory.
    Render {
        #[arg(long, value_name = "DIRECTORY")]
        output: PathBuf,
    },
    /// Run and supervise the configured services in the foreground.
    #[command(visible_alias = "start")]
    Run,
    /// Query the running deployment supervisor.
    Status,
    /// Observe runtimes and manage standalone component upgrades and local caches.
    Rollout {
        #[command(subcommand)]
        command: RolloutCommand,
    },
    /// Stop supervised services using the running supervisor's shutdown mode.
    Stop {
        /// Override the shutdown mode for this stop request only.
        #[arg(long, value_enum)]
        mode: Option<ShutdownMode>,
    },
    /// Inspect or initialize deployment configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ConfigCommand {
    /// Create the deployment YAML selected by --config.
    Init {
        /// Topology template to create.
        #[arg(long, value_enum, default_value_t = ConfigProfile::Standalone)]
        profile: ConfigProfile,
        /// Replace an existing file.
        #[arg(long)]
        force: bool,
        /// Write a profile reference instead of the expanded template.
        #[arg(long)]
        compact: bool,
    },
    /// Print a topology template to standard output.
    Template {
        #[arg(long, value_enum, default_value_t = ConfigProfile::Standalone)]
        profile: ConfigProfile,
    },
    /// Print the fully resolved deployment configuration.
    Dump,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum RolloutCommand {
    /// Observe owned runtime identities; unavailable evidence remains unknown.
    NodeInfo,
    /// Submit a task against an explicitly selected running incarnation.
    Submit(Box<RolloutSubmitArgs>),
    /// Query a durable rollout task; never restarts it or extends its deadline.
    Query {
        #[arg(long)]
        task_id: String,
    },
    /// Request one local adxlet step of an existing prepared task.
    Step {
        #[arg(long)]
        task_id: String,
        #[arg(long, value_enum)]
        step: RolloutStep,
        /// Exact attempt recorded in the task; empty only for the first exit.
        #[arg(long, default_value = "")]
        attempt: String,
        /// Exact node session recorded in the task; empty only for the first exit.
        #[arg(long, default_value = "")]
        node_session: String,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum RolloutStep {
    Exit,
    Start,
    #[value(name = "rollback")]
    Rollback,
}

#[derive(Debug, clap::Args, PartialEq, Eq)]
pub struct RolloutSubmitArgs {
    #[arg(long)]
    task_id: String,
    #[arg(long, value_enum)]
    role: RolloutRole,
    #[arg(long)]
    service_id: String,
    #[arg(long)]
    commit: String,
    #[arg(long, required_unless_present = "archive_url")]
    sha256: Option<String>,
    #[arg(long, requires = "archive_sha256")]
    archive_url: Option<String>,
    #[arg(long, requires = "archive_url")]
    archive_sha256: Option<String>,
    #[arg(long)]
    component_config_path: Option<std::path::PathBuf>,
    /// Frozen process-maintenance identity JSON (adxlet only).
    #[arg(long, value_parser = parse_process_identity)]
    process_identity: Option<crate::rollout::ProcessIdentity>,
    /// Frozen NodeInfo runtime JSON. Required for publisher submissions.
    #[arg(long, value_parser = parse_expected_runtime, required_unless_present = "local_manual", conflicts_with = "local_manual")]
    expected_runtime: Option<crate::rollout::ExpectedRuntime>,
    /// Explicit local manual mode, without publisher incarnation guarantees.
    #[arg(long)]
    local_manual: bool,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum RolloutRole {
    Adxlet,
    Relay,
}

impl RolloutCommand {
    fn request(self) -> crate::rollout::Request {
        match self {
            Self::NodeInfo => crate::rollout::Request::NodeInfo {},
            Self::Query { task_id } => crate::rollout::Request::Query { task_id },
            Self::Step {
                task_id,
                step,
                attempt,
                node_session,
            } => crate::rollout::Request::Step {
                task_id,
                step: match step {
                    RolloutStep::Exit => crate::rollout::Step::Exit,
                    RolloutStep::Start => crate::rollout::Step::Start,
                    RolloutStep::Rollback => crate::rollout::Step::Rollback,
                },
                attempt,
                node_session,
            },
            Self::Submit(args) => {
                let RolloutSubmitArgs {
                    task_id,
                    role,
                    service_id,
                    commit,
                    sha256,
                    archive_url,
                    archive_sha256,
                    component_config_path,
                    process_identity,
                    expected_runtime,
                    local_manual: _,
                } = *args;
                crate::rollout::Request::Submit {
                    submission: Box::new(crate::rollout::Submission {
                        component_config: component_config_path
                            .map(|mount_path| crate::config::ConfigInput { mount_path }),
                        task_id,
                        service_id,
                        commit,
                        sha256,
                        remote_archive: archive_url.zip(archive_sha256).map(
                            |(url, archive_sha256)| crate::rollout::RemoteArchive {
                                url,
                                archive_sha256,
                            },
                        ),
                        process: process_identity,
                        selection: match expected_runtime {
                            Some(expected) => crate::rollout::Selection::Publisher { expected },
                            None => crate::rollout::Selection::LocalManual,
                        },
                        role: match role {
                            RolloutRole::Adxlet => crate::config::Role::Adxlet,
                            RolloutRole::Relay => crate::config::Role::Relay,
                        },
                    }),
                }
            }
        }
    }
}

fn parse_process_identity(
    value: &str,
) -> std::result::Result<crate::rollout::ProcessIdentity, String> {
    serde_json::from_str(value).map_err(|_| "invalid frozen process identity".into())
}

fn parse_expected_runtime(
    value: &str,
) -> std::result::Result<crate::rollout::ExpectedRuntime, String> {
    serde_json::from_str(value).map_err(|_| "invalid observed runtime identity".into())
}

impl Cli {
    pub async fn execute(self) -> Result<()> {
        let config_path = self.config;
        match self.command {
            Command::Validate => {
                load_deployment(&config_path)?;
                println!("configuration valid: {}", config_path.display());
            }
            Command::Render { output } => {
                let deployment = load_deployment(&config_path)?;
                let processes = deployment.render(&output)?;
                println!("rendered {} services", processes.len());
            }
            Command::Run => {
                supervisor::run(load_deployment(&config_path)?).await?;
            }
            Command::Status => {
                print_supervisor_response(&config_path, Request::Status).await?;
            }
            Command::Rollout { command } => {
                let deployment = load_deployment(&config_path)?;
                let response =
                    supervisor::rollout_request(&deployment.state_dir, command.request()).await?;
                println!("{response}");
            }
            Command::Stop { mode } => {
                print_supervisor_response(&config_path, Request::Stop { mode }).await?;
            }
            Command::Config {
                command:
                    ConfigCommand::Init {
                        profile,
                        force,
                        compact,
                    },
            } => {
                write_template(&config_path, profile, force, compact)?;
                println!(
                    "created deployment configuration: {}",
                    config_path.display()
                );
            }
            Command::Config {
                command: ConfigCommand::Template { profile },
            } => print!("{}", profile.template()),
            Command::Config {
                command: ConfigCommand::Dump,
            } => print!("{}", load_deployment(&config_path)?.effective_yaml()?),
        }
        Ok(())
    }
}

pub fn write_template(
    path: &Path,
    profile: ConfigProfile,
    force: bool,
    compact: bool,
) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let mut options = OpenOptions::new();
    options.write(true).mode(0o600);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let mut file = options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            format!(
                "deployment configuration already exists: {}; use --force to replace it",
                path.display()
            )
            .into()
        } else {
            Box::new(error) as Box<dyn std::error::Error + Send + Sync>
        }
    })?;
    if compact {
        file.write_all(profile.compact_template().as_bytes())?;
    } else {
        file.write_all(profile.template().as_bytes())?;
    }
    file.sync_all()?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

async fn print_supervisor_response(config_path: &Path, request: Request) -> Result<()> {
    let deployment = load_deployment(config_path)?;
    let response = supervisor::request(
        &deployment.state_dir,
        request,
        deployment.supervisor_request_timeout(),
    )
    .await?;
    println!("{response}");
    Ok(())
}

fn load_deployment(path: &Path) -> Result<Deployment> {
    if !path.try_exists()? {
        return Err(format!(
            "deployment configuration not found: {}; create it with `adxctl --config {} config init`",
            path.display(),
            path.display()
        )
        .into());
    }
    Deployment::load(path)
}

#[cfg(test)]
mod remote_archive_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn rollback_v2_cli_uses_single_rollback_step_without_alias() {
        let args = [
            "adxctl",
            "rollout",
            "step",
            "--task-id",
            "task-1",
            "--step",
            "rollback",
            "--attempt",
            "A",
            "--node-session",
            "session-A",
        ];
        let cli = Cli::try_parse_from(args).expect("protocol 2 rollback CLI");
        let Command::Rollout { command } = cli.command else {
            panic!("rollout expected")
        };
        assert_eq!(
            serde_json::to_value(command.request()).unwrap(),
            serde_json::json!({"action":"rollout_step", "task_id":"task-1",
                "step":"rollback", "attempt":"A", "node_session":"session-A"})
        );
        let mut legacy = args;
        legacy[6] = "rollback_stop";
        assert!(Cli::try_parse_from(legacy).is_err());
    }

    #[test]
    fn rollout_step_cli_builds_exact_wire_request() {
        let cli = Cli::try_parse_from([
            "adxctl",
            "rollout",
            "step",
            "--task-id",
            "task-1",
            "--step",
            "rollback",
            "--attempt",
            "a1",
            "--node-session",
            "s1",
        ])
        .unwrap();
        let Command::Rollout { command } = cli.command else {
            panic!("rollout expected")
        };
        let request = command.request();
        assert_eq!(
            request,
            crate::rollout::Request::Step {
                task_id: "task-1".into(),
                step: crate::rollout::Step::Rollback,
                attempt: "a1".into(),
                node_session: "s1".into(),
            }
        );
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"action":"rollout_step","task_id":"task-1","step":"rollback","attempt":"a1","node_session":"s1"}"#
        );
        let cli = Cli::try_parse_from([
            "adxctl",
            "rollout",
            "step",
            "--task-id",
            "t",
            "--step",
            "exit",
        ])
        .unwrap();
        let Command::Rollout { command } = cli.command else {
            panic!("rollout expected")
        };
        assert_eq!(
            command.request(),
            crate::rollout::Request::Step {
                task_id: "t".into(),
                step: crate::rollout::Step::Exit,
                attempt: String::new(),
                node_session: String::new(),
            }
        );
        for argv in [
            vec!["adxctl", "rollout", "step", "--step", "exit"],
            vec!["adxctl", "rollout", "step", "--task-id", "t"],
            vec![
                "adxctl",
                "rollout",
                "step",
                "--task-id",
                "t",
                "--step",
                "replace-session",
            ],
            vec![
                "adxctl",
                "rollout",
                "step",
                "--task-id",
                "t",
                "--step",
                "verify",
            ],
            vec![
                "adxctl",
                "rollout",
                "step",
                "--task-id",
                "t",
                "--step",
                "exit",
                "--expected-runtime",
                "{}",
            ],
        ] {
            assert!(Cli::try_parse_from(argv).is_err());
        }
    }

    #[test]
    fn archive_flags_are_paired_and_config_accepts_path_only() {
        let base = [
            "adxctl",
            "rollout",
            "submit",
            "--task-id",
            "task",
            "--role",
            "relay",
            "--service-id",
            "relay",
            "--commit",
            &"a".repeat(40),
            "--sha256",
            &"b".repeat(64),
            "--local-manual",
        ];
        for flag in ["--archive-url", "--archive-sha256"] {
            let mut argv = base.to_vec();
            argv.extend([flag, "value"]);
            assert!(Cli::try_parse_from(argv).is_err());
        }
        let digest = "c".repeat(64);
        let mut argv = base.to_vec();
        argv.extend([
            "--archive-url",
            "https://host/archive.tar.gz",
            "--archive-sha256",
            &digest,
        ]);
        let cli = Cli::try_parse_from(argv).unwrap();
        let Command::Rollout { command } = cli.command else {
            panic!("rollout expected")
        };
        let crate::rollout::Request::Submit { submission } = command.request() else {
            panic!("submit expected")
        };
        assert_eq!(
            submission.remote_archive.unwrap(),
            crate::rollout::RemoteArchive {
                url: "https://host/archive.tar.gz".into(),
                archive_sha256: digest,
            }
        );
        let mut argv = base.to_vec();
        argv.extend(["--component-config-path", "/mounted/relay.json"]);
        let cli = Cli::try_parse_from(argv.clone()).unwrap();
        let Command::Rollout { command } = cli.command else {
            panic!("rollout expected")
        };
        let crate::rollout::Request::Submit { submission } = command.request() else {
            panic!("submit expected")
        };
        assert_eq!(
            serde_json::to_value(submission.component_config.unwrap()).unwrap(),
            serde_json::json!({"mount_path":"/mounted/relay.json"})
        );
        let removed_digest = "c".repeat(64);
        argv.extend(["--component-config-sha256", &removed_digest]);
        assert!(Cli::try_parse_from(argv).is_err());
    }
}
