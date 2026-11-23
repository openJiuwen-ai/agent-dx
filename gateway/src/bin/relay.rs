use adx_process::{resource::raise_nofile_soft_limit_from_env, shutdown_signal};
use data_plane_gateway::{config::RelayConfig, node::RelayService};
#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let configuration = error.is::<adx_process::ConfigurationError>();
            eprintln!("Relay initialization failed: {error}");
            std::process::ExitCode::from(if configuration {
                adx_process::CONFIGURATION_EXIT_CODE
            } else {
                1
            })
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    adx_transport::install_crypto_provider();
    let _logging_guard =
        adx_observability::logging::init("relay", false).map_err(|e| e.to_string())?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env().map_err(
        |error| -> Box<dyn std::error::Error + Send + Sync> {
            if adx_process::resource::is_configuration_error(&error) {
                adx_process::ConfigurationError("invalid Relay FD limit configuration").into()
            } else {
                error.into()
            }
        },
    )?;
    tracing::info!(nofile_soft_limit, "Relay FD limit configured");
    let route_socket =
        std::env::var_os("ADX_DATA_PLANE_RELAY_ROUTE_SOCKET").map(std::path::PathBuf::from);
    let config =
        RelayConfig::from_env().map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
            if error.is_configuration_error() {
                adx_process::ConfigurationError("invalid Relay environment configuration").into()
            } else {
                error.into()
            }
        })?;
    let service = RelayService::bind_standalone(config, route_socket.as_deref()).await?;
    service
        .serve_standalone(async {
            let _ = shutdown_signal().await;
        })
        .await
}
