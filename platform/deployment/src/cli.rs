use crate::{
    config::Deployment,
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
    /// Drain local environments and stop the supervised services.
    Stop,
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
            Command::Stop => {
                print_supervisor_response(&config_path, Request::Stop).await?;
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
