use adx_activator::{
    reconciliation::{ReconcileSettings, Reconciliation},
    registration::RegistrationConfig,
    server,
    transport::HttpEnvironment,
    Activator, CacheSettings,
};
use adx_agent_store::{AgentState, RedisRepository};
use serde::Deserialize;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    listen: SocketAddr,
    redis_url: String,
    namespace: String,
    environment_url: String,
    environment_api_key_envs: BTreeMap<String, String>,
    sandbox_network: adx_agent_core::network::SandboxNetworkPolicy,
    allow_plaintext_transport: bool,
    environment_ca_file: Option<String>,
    request_timeout_seconds: u64,
    #[serde(default)]
    binding_cache: CacheSettings,
    registration: Option<RegistrationConfig>,
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
        settings.sandbox_network,
    )?);
    let store = Arc::new(
        RedisRepository::connect(
            &settings.redis_url,
            &settings.namespace,
            Duration::from_secs(3),
        )
        .await?,
    );
    let activator = Arc::new(Activator::with_cache(
        AgentState::new(store.clone()),
        environment,
        settings.binding_cache,
    )?);
    let app = server::router(
        activator.clone(),
        &std::env::var("ADX_ACTIVATOR_SERVICE_TOKEN")?,
        timeout,
    )?;
    let listener = tokio::net::TcpListener::bind(settings.listen).await?;
    let reconciliation = match settings.registration {
        Some(config) => Some(
            Reconciliation::start(
                activator,
                store.as_ref().clone(),
                adx_agent_store::discovery::RedisRegistry::new(
                    &settings.redis_url,
                    &settings.namespace,
                    Duration::from_secs(3),
                )?,
                config,
                ReconcileSettings::default(),
            )
            .await?,
        ),
        None => None,
    };
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
