// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! adx-execd binary: sandbox runtime-mode entrypoint.
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Isolated verification mode: start only the EXECD atomic-operation HTTP server without the optional tunnel listeners.
    // EXECD_HTTP_ONLY=1 EXECD_HTTP_PORT=<port> [EXECD_HTTP_TOKEN=<tok>] adx-execd
    if std::env::var("EXECD_HTTP_ONLY").is_ok() {
        let port = std::env::var("EXECD_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(50090);
        let token = std::env::var("EXECD_HTTP_TOKEN").ok();
        return adx_execd::runtime::serve_http_only(port, token).await;
    }
    // Isolated verification mode: start only the native Rust reverse-tunnel server for interop with the real Python TunnelClient.
    // EXECD_TUNNEL_ONLY=1 [EXECD_TUNNEL_WS_PORT=8765] [EXECD_TUNNEL_HTTP_PORT=8766] adx-execd
    if std::env::var("EXECD_TUNNEL_ONLY").is_ok() {
        let ws_port = std::env::var("EXECD_TUNNEL_WS_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8765);
        let http_port = std::env::var("EXECD_TUNNEL_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8766);
        adx_execd::runtime::serve_tunnel_only(ws_port, http_port).await;
        return Ok(());
    }
    adx_execd::runtime::run().await
}
