use adx_activator::{config::ActivatorConfig, server, transport::HttpEnvironment};
use serde::Deserialize;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    #[serde(flatten)]
    activator: ActivatorConfig,
    listen: SocketAddr,
    environment_url: String,
    environment_api_key_envs: BTreeMap<String, String>,
    allow_plaintext_transport: bool,
    environment_ca_file: Option<String>,
    /// Optional private metrics listener; omitted means no monitoring port.
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings: Settings =
        serde_json::from_slice(&std::fs::read(std::env::var("ADX_ACTIVATOR_CONFIG")?)?)?;
    if !settings.allow_plaintext_transport {
        return Err(
            "private listener requires explicit plaintext authorization behind a TLS service proxy"
                .into(),
        );
    }
    let timeout = settings.activator.request_timeout();
    let ca = settings
        .environment_ca_file
        .map(std::fs::read)
        .transpose()?;
    let environment = Arc::new(HttpEnvironment::new(
        &settings.environment_url,
        settings
            .environment_api_key_envs
            .into_iter()
            .map(|(tenant, variable)| std::env::var(variable).map(|key| (tenant, key)))
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        timeout,
        ca.as_deref(),
        settings.allow_plaintext_transport,
        settings.activator.egress_isolation.clone(),
    )?);
    let (activator, store) = settings.activator.connect(environment).await?;
    let app = server::router(
        activator.clone(),
        &std::env::var("ADX_ACTIVATOR_SERVICE_TOKEN")?,
        timeout,
    )?;
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    // Bind before accepting traffic so a bad monitoring address fails startup.
    let metrics_listener = match settings.metrics_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    if let Some(listener) = metrics_listener {
        let metrics = activator.cache_metrics();
        tokio::spawn(async move {
            if let Err(error) = adx_activator::metrics::serve(listener, metrics).await {
                tracing::warn!(%error, "Activator metrics listener stopped");
            }
        });
    }
    let reconciliation = settings
        .activator
        .start_reconciliation(activator, store)
        .await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut signal =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! { _=signal.recv()=>(), _=tokio::signal::ctrl_c()=>() }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            if let Some(reconciliation) = reconciliation {
                if let Err(error) = reconciliation.shutdown().await { tracing::warn!(%error, "Activator reconciliation shutdown failed; lease will expire"); }
            }
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Settings;
    use serde_json::{json, Value};

    #[test]
    fn standalone_settings_read_the_shared_config_at_the_root() {
        let mut config: Value =
            serde_json::from_str(include_str!("../examples/discovery.json")).unwrap();
        let settings: Settings = serde_json::from_value(config.clone()).unwrap();
        assert!(settings.activator.egress_isolation.is_some());
        assert!(settings.activator.registration.is_some());

        config["request_timeout_second"] = json!(10);
        assert!(serde_json::from_value::<Settings>(config).is_err());
    }
}
