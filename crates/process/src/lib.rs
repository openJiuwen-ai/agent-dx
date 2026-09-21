use clap::Parser;
use serde::de::DeserializeOwned;
use std::{error::Error, path::PathBuf};

pub mod resource;

/// Common command-line arguments accepted by ADX service processes.
#[derive(Debug, Parser, PartialEq, Eq)]
#[command(disable_version_flag = true)]
pub struct ServiceArgs {
    /// Path to the service-specific JSON configuration.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,
}

/// Parse the process arguments and deserialize the selected service configuration.
pub fn read_config<T: DeserializeOwned>() -> Result<T, Box<dyn Error>> {
    let args = ServiceArgs::try_parse()?;
    read_config_file(&args.config)
}

/// Deserialize one service configuration without exposing its contents in errors.
pub fn read_config_file<T: DeserializeOwned>(path: &std::path::Path) -> Result<T, Box<dyn Error>> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|_| "invalid service configuration".into())
}

/// Wait until the process receives its supported shutdown signal.
pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Convenience wrapper for service loops that do not report signal setup errors.
pub async fn shutdown() {
    let _ = shutdown_signal().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct Config {
        name: String,
    }

    #[test]
    fn service_args_require_named_config_path() {
        assert_eq!(
            ServiceArgs::try_parse_from(["service", "--config", "service.json"]).unwrap(),
            ServiceArgs {
                config: PathBuf::from("service.json")
            }
        );
        assert!(ServiceArgs::try_parse_from(["service", "service.json"]).is_err());
        assert!(ServiceArgs::try_parse_from(["service", "--config"]).is_err());
    }

    #[test]
    fn configuration_parse_errors_do_not_echo_file_contents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        std::fs::write(&path, br#"{"secret":"do-not-print"}"#).unwrap();

        let error = read_config_file::<Config>(&path).unwrap_err().to_string();
        assert_eq!(error, "invalid service configuration");
        assert!(!error.contains("do-not-print"));
    }

    #[test]
    fn reads_typed_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("service.json");
        std::fs::write(&path, br#"{"name":"master"}"#).unwrap();

        assert_eq!(
            read_config_file::<Config>(&path).unwrap(),
            Config {
                name: "master".into()
            }
        );
    }
}
