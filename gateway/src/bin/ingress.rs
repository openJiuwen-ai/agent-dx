#![cfg(feature = "activity-client")]

use adx_process::{resource::raise_nofile_soft_limit_from_env, shutdown_signal};
use data_plane_gateway::{
    config::IngressConfig,
    ingress::{coordinator_routes::ControlConfig, IngressService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    adx_transport::install_crypto_provider();
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if !arguments.is_empty() {
        if arguments.len() != 3
            || !matches!(
                arguments[0].to_str(),
                Some("import-composio-key" | "import-composio-webhook-secret")
            )
        {
            return Err(
                "usage: adx-ingress [import-composio-key|import-composio-webhook-secret <private-input.json> <new|revision>]"
                    .into(),
            );
        }
        #[cfg(feature = "agent-api")]
        {
            let expected = arguments[2].to_str().ok_or("invalid secret revision")?;
            use data_plane_gateway::ingress::digitalmategateway::server_config;
            let path = std::path::Path::new(&arguments[1]);
            let revision = if arguments[0] == "import-composio-key" {
                server_config::import_composio_key(path, expected).await?
            } else {
                server_config::import_composio_webhook_secret(path, expected).await?
            };
            println!("Composio service secret imported; revision={revision}");
            return Ok(());
        }
        #[cfg(not(feature = "agent-api"))]
        return Err("Composio configuration requires --features agent-api".into());
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
