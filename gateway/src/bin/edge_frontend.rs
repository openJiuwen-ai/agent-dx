#![cfg(feature = "activity-client")]

use data_plane_gateway::{
    common::{resource::raise_nofile_soft_limit_from_env, shutdown::shutdown_signal},
    config::EdgeFrontendConfig,
    edge::{master_routes::ControlConfig, EdgeFrontendService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    data_plane_gateway::common::install_crypto_provider();
    let _logging_guard = data_plane_gateway::common::logging::init("edge-frontend", true)?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Edge Frontend FD limit configured");
    let path = std::env::var("ADX_EDGE_CONTROL_CONFIG")
        .map_err(|_| "ADX_EDGE_CONTROL_CONFIG is required")?;
    let control: ControlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    EdgeFrontendService::bind(EdgeFrontendConfig::from_env()?, control)
        .await
        .map_err(local_error)?
        .serve(async {
            if let Err(error) = shutdown_signal().await {
                tracing::warn!(%error, "Edge shutdown signal failed");
            }
        })
        .await
        .map_err(local_error)
}

fn local_error(error: Box<dyn std::error::Error + Send + Sync>) -> Box<dyn std::error::Error> {
    std::io::Error::other(error.to_string()).into()
}
