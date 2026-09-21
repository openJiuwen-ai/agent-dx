use adx_process::{resource::raise_nofile_soft_limit_from_env, shutdown_signal};
use data_plane_gateway::{config::NodeProxyConfig, node::NodeProxyService};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    adx_transport::install_crypto_provider();
    let _logging_guard =
        adx_observability::logging::init("node-proxy", false).map_err(|e| e.to_string())?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Node Proxy FD limit configured");
    let service = NodeProxyService::bind(NodeProxyConfig::from_env()?).await?;
    service
        .serve(async {
            let _ = shutdown_signal().await;
        })
        .await
}
