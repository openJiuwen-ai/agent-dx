use adx_activator::{server, transport::HttpSandbox, Activator};
use adx_agent_store::{AgentState, RedisRepository};
use serde::Deserialize;
use std::{net::SocketAddr, sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    listen: SocketAddr,
    redis_url: String,
    namespace: String,
    sandbox_url: String,
    allow_plaintext_transport: bool,
    sandbox_ca_file: Option<String>,
    request_timeout_seconds: u64,
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
    let timeout = Duration::from_secs(settings.request_timeout_seconds);
    let ca = settings.sandbox_ca_file.map(std::fs::read).transpose()?;
    let sandbox = Arc::new(HttpSandbox::new(
        &settings.sandbox_url,
        std::env::var("ADX_SANDBOX_SERVICE_TOKEN")?,
        timeout,
        ca.as_deref(),
        settings.allow_plaintext_transport,
    )?);
    let store = Arc::new(
        RedisRepository::connect(
            &settings.redis_url,
            &settings.namespace,
            Duration::from_secs(3),
        )
        .await?,
    );
    let activator = Arc::new(Activator::new(AgentState::new(store), sandbox));
    let app = server::router(
        activator,
        &std::env::var("ADX_ACTIVATOR_SERVICE_TOKEN")?,
        timeout,
    )?;
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
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
        })
        .await?;
    Ok(())
}
