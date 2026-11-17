#![cfg(feature = "activity-client")]

use adx_process::{resource::raise_nofile_soft_limit_from_env, shutdown_signal};
use data_plane_gateway::{
    config::IngressConfig,
    ingress::{coordinator_routes::ControlConfig, IngressService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    adx_transport::install_crypto_provider();
    #[cfg(feature = "agent-api")]
    if let Some(result) = data_plane_gateway::ingress::digitalmategateway::server_config::run_cli(
        std::env::args_os().skip(1).collect(),
    )
    .await
    {
        return result;
    }
    #[cfg(not(feature = "agent-api"))]
    if std::env::args_os().nth(1).is_some() {
        return Err("operator configuration requires --features agent-api".into());
    }
    let _logging_guard = adx_observability::logging::init("ingress-frontend", true)?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Ingress FD limit configured");
    let path = std::env::var("ADX_INGRESS_CONTROL_CONFIG")
        .map_err(|_| "ADX_INGRESS_CONTROL_CONFIG is required")?;
    let control: ControlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    IngressService::bind(IngressConfig::from_env()?, control)
        .await
        .map_err(local_error)?
        .serve(async {
            if let Err(error) = shutdown_signal().await {
                tracing::warn!(%error, "Ingress shutdown signal failed");
            }
        })
        .await
        .map_err(local_error)
}

fn local_error(error: Box<dyn std::error::Error + Send + Sync>) -> Box<dyn std::error::Error> {
    std::io::Error::other(error.to_string()).into()
}
