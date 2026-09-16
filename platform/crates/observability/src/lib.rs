//! Process logging setup; collection and storage remain deployment services.
pub mod trace;
pub use tracing::{debug, error, info, warn};
pub fn json_enabled() -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match std::env::var("ADX_LOG_FORMAT").as_deref().unwrap_or("text") {
        "text" => Ok(false),
        "json" => Ok(true),
        _ => Err("ADX_LOG_FORMAT must be text or json".into()),
    }
}
pub fn init() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if json_enabled()? {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .with_ansi(false)
            .try_init()?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .try_init()?;
    }
    Ok(())
}
