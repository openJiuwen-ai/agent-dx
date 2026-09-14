// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! rrt-runtime binary: sandbox runtime-mode entrypoint.
//! Start with `rrt-runtime`; runtime context comes from environment variables injected by functionsystem.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Fork-based warm starts hold here until the child is ready. Refresh the
    // restored environment before constructing Tokio or reading runtime args.
    rrt_daemon::startup::prepare_runtime_environment()?;
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
    // Isolated verification mode: start only the RRT atomic-operation HTTP server without the function-proxy worker.
    // RRT_HTTP_ONLY=1 RRT_HTTP_PORT=<port> [RRT_HTTP_TOKEN=<tok>] rrt-runtime
    if std::env::var("RRT_HTTP_ONLY").is_ok() {
        let port = std::env::var("RRT_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(50090);
        let token = std::env::var("RRT_HTTP_TOKEN").ok();
        return rrt_daemon::runtime::serve_http_only(port, token).await;
    }
    // Isolated verification mode: start only the native Rust reverse-tunnel server for interop with the real Python TunnelClient.
    // RRT_TUNNEL_ONLY=1 [RRT_TUNNEL_WS_PORT=8765] [RRT_TUNNEL_HTTP_PORT=8766] rrt-runtime
    if std::env::var("RRT_TUNNEL_ONLY").is_ok() {
        let ws_port = std::env::var("RRT_TUNNEL_WS_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8765);
        let http_port = std::env::var("RRT_TUNNEL_HTTP_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8766);
        rrt_daemon::runtime::serve_tunnel_only(ws_port, http_port).await;
        return Ok(());
    }
    rrt_daemon::runtime::run().await
}
