// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! adx-execd binary: environment runtime-mode entrypoint.
//! Start with `adx-execd`; Adxlet supplies the explicit Environment identity.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(code) = adx_execd::init::enter()? {
        std::process::exit(code);
    }
    // Fork-based warm starts hold here until the child is ready. Refresh the
    // restored environment before constructing Tokio or reading runtime args.
    adx_execd::startup::prepare_environment()?;
    let _logging_guard = adx_observability::logging::init("adx-execd", false)?;
    build_runtime()?.block_on(run())
}

fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    // Keep checkpoint-restored control, transport and listener futures on one
    // scheduler. File and command work already uses Tokio's blocking pool.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// Resolve a configured listener port. Only an absent (or blank) variable is
/// allowed to fall back to the default; a value that cannot be parsed must not
/// silently move the listener to another port than the one the caller expects.
fn configured_port(
    name: &str,
    value: Option<&str>,
    default: u16,
) -> Result<u16, Box<dyn std::error::Error>> {
    match value {
        None => Ok(default),
        Some(value) if value.trim().is_empty() => Ok(default),
        Some(value) => value
            .trim()
            .parse::<u16>()
            .map_err(|error| format!("{name} must be a port in 0..=65535: {error}").into()),
    }
}

fn env_port(name: &str, default: u16) -> Result<u16, Box<dyn std::error::Error>> {
    let value = std::env::var(name).ok();
    configured_port(name, value.as_deref(), default)
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Isolated verification mode: start only the EXECD atomic-operation HTTP server without the optional tunnel listeners.
    // EXECD_HTTP_ONLY=1 EXECD_HTTP_PORT=<port> [EXECD_HTTP_TOKEN=<tok>] adx-execd
    if std::env::var("EXECD_HTTP_ONLY").is_ok() {
        let port = env_port("EXECD_HTTP_PORT", 50090)?;
        let token = std::env::var("EXECD_HTTP_TOKEN").ok();
        return adx_execd::runtime::serve_http_only(port, token).await;
    }
    // Isolated verification mode: start only the native Rust reverse-tunnel server for interop with the real Python TunnelClient.
    // EXECD_TUNNEL_ONLY=1 [EXECD_TUNNEL_WS_PORT=8765] [EXECD_TUNNEL_HTTP_PORT=8766] adx-execd
    if std::env::var("EXECD_TUNNEL_ONLY").is_ok() {
        let ws_port = env_port("EXECD_TUNNEL_WS_PORT", 8765)?;
        let http_port = env_port("EXECD_TUNNEL_HTTP_PORT", 8766)?;
        adx_execd::runtime::serve_tunnel_only(ws_port, http_port).await;
        return Ok(());
    }
    adx_execd::runtime::run().await
}

#[cfg(test)]
mod tests {
    use super::configured_port;

    #[test]
    fn defaults_apply_only_when_a_port_is_not_configured() {
        assert_eq!(
            configured_port("EXECD_HTTP_PORT", None, 50090).unwrap(),
            50090
        );
        assert_eq!(
            configured_port("EXECD_HTTP_PORT", Some("  "), 50090).unwrap(),
            50090
        );
        assert_eq!(
            configured_port("EXECD_HTTP_PORT", Some(" 51234 "), 50090).unwrap(),
            51234
        );
    }

    #[test]
    fn invalid_configured_ports_are_rejected_instead_of_defaulting() {
        for value in ["notaport", "5009O", "-1", "65536"] {
            let error = configured_port("EXECD_HTTP_PORT", Some(value), 50090)
                .unwrap_err()
                .to_string();
            assert!(error.contains("EXECD_HTTP_PORT"), "{value}: {error}");
        }
    }
}
