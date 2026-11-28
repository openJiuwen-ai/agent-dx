use adx_agent_console::{config::ConsoleConfig, routes::router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().json().with_max_level(tracing::Level::INFO).try_init()
        .map_err(|_| "console audit logging initialization failed")?;
    tracing::info!(target: "adx_console_audit", action = "console.starting");
    let path = std::env::var("ADX_CONSOLE_CONFIG")?;
    let config = ConsoleConfig::from_path(&path)?;
    let secret = std::fs::read(&config.cookie_secret_file).map_err(|_| "console session secret unreadable")?;
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    axum::serve(listener, router(config, secret)?).await?;
    Ok(())
}
