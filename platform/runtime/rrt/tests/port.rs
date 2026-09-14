// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

#![cfg(target_os = "linux")]

use rrt_daemon::pb::port_client::PortClient;
use rrt_daemon::pb::ListPortsRequest;
use std::time::Duration;

#[tokio::test]
async fn list_ports_finds_a_real_listener() {
    // Start an ephemeral rrt server.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        rrt_daemon::serve_with_listener(listener).await.unwrap();
    });
    // Start another listener to be discovered.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let probe_port = probe.local_addr().unwrap().port() as u32;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut c = PortClient::connect(format!("http://{addr}")).await.unwrap();
    let resp = c
        .list_ports(ListPortsRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.ports.iter().any(|p| p.port == probe_port),
        "probe port {probe_port} should be discovered, got: {:?}",
        resp.ports.iter().map(|p| p.port).collect::<Vec<_>>()
    );
}
