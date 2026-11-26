use clap::Parser;
use serde::de::DeserializeOwned;
use std::{error::Error, path::PathBuf};

pub mod resource;

/// Match launch-time parent identity against caller-supplied kernel observations.
/// PID 1 is a valid parent; zero, negative and mismatched identities are rejected.
/// Callers must obtain the current parent and peer PID from kernel credentials.
pub fn launch_parent_matches(
    captured_parent: i32,
    current_parent: i32,
    peer_pid: Option<i32>,
) -> bool {
    captured_parent > 0 && current_parent == captured_parent && peer_pid == Some(captured_parent)
}

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

    #[test]
    fn launch_parent_requires_exact_positive_kernel_identities_including_pid_one() {
        assert!(launch_parent_matches(1, 1, Some(1)));
        assert!(launch_parent_matches(42, 42, Some(42)));
        for (captured, current, peer) in [
            (0, 0, Some(0)),
            (-1, -1, Some(-1)),
            (42, 43, Some(42)),
            (42, 42, Some(43)),
            (42, 42, None),
            (42, 1, Some(1)),
            (42, 1, Some(42)),
        ] {
            assert!(!launch_parent_matches(captured, current, peer));
        }
    }

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
        std::fs::write(&path, br#"{"name":"coordinator"}"#).unwrap();

        assert_eq!(
            read_config_file::<Config>(&path).unwrap(),
            Config {
                name: "coordinator".into()
            }
        );
    }

    #[test]
    fn missing_configuration_file_preserves_io_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        let missing = read_config_file::<Config>(&path).unwrap_err();
        assert!(missing.is::<std::io::Error>());
        assert_eq!(
            missing.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::NotFound
        );
    }
}
