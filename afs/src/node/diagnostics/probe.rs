//! `/v1/diagnostics` 的探测流程：Node B→Meta Ping→A 控制 Ping→A 数据写/读。
//!
//! 调用正式 caller/adapter（`rpc::meta::ping`、`NodeControl.Ping`、`rpc::peer` 的
//! `DataPeerClient`），写读的是对端的诊断对象存储，不是 OwnerFs/DFS 文件或发布。

use std::{sync::Arc, time::Duration};

use afs_error::{
    CLIENT_ARGUMENT_INVALID, CLIENT_CONNECTION_UNAVAILABLE, DIAGNOSTICS_NOT_CONFIGURED, Error,
    NODE_TRANSFER_CORRUPT_DATA,
};
use afs_protocol::node_control::{PingRequest, node_control_client::NodeControlClient};
use serde_json::{Value, json};

use crate::node::{
    Node,
    rpc::{
        meta,
        peer::{DataClientOptions, DataMode, connect_data_client},
    },
};

pub(in crate::node) async fn diagnose(node: Arc<Node>) -> afs_error::Result<Value> {
    let config = &node.config;
    let meta_endpoint = config.meta_endpoint.as_ref().ok_or_else(|| {
        Error::coded(
            DIAGNOSTICS_NOT_CONFIGURED,
            "diagnostics requires meta_endpoint",
        )
    })?;
    let peer_endpoint = config.peer_endpoint.as_ref().ok_or_else(|| {
        Error::coded(
            DIAGNOSTICS_NOT_CONFIGURED,
            "diagnostics requires peer_endpoint",
        )
    })?;
    let timeout = Duration::from_millis(config.timeout_ms);
    let tls = config.tls_config();
    let meta_pong = meta::ping(meta_endpoint, &config.id, timeout, tls.clone()).await?;
    let grpc_config = afs_transport::grpc::GrpcConfig {
        connect_timeout: timeout,
        request_timeout: timeout,
        ..Default::default()
    };
    let security = afs_transport::grpc::SecurityManager::new(tls.clone())
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
    let endpoint = grpc_config.configure_client(
        tonic::transport::Endpoint::from_shared(security.endpoint_uri(peer_endpoint))
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = security
        .connect(endpoint)
        .await
        .map_err(|e| Error::coded(CLIENT_CONNECTION_UNAVAILABLE, e.to_string()))?;
    let mut control = NodeControlClient::new(afs_tracing::traced_channel(channel));
    let pong = control
        .ping(afs_tracing::request_with_current_context(PingRequest {
            payload: "ping".into(),
        }))
        .await
        .map_err(afs_transport::grpc::error_status::status_to_error)?
        .into_inner();
    let mode = match config.data_mode.as_str() {
        "grpc" => DataMode::Grpc,
        "rdma" => DataMode::Rdma,
        _ => DataMode::Auto,
    };
    let mut data = connect_data_client(DataClientOptions {
        endpoint: peer_endpoint.clone(),
        mode,
        rdma_device: config.rdma_device.clone(),
        timeout,
        tls,
    })
    .await?;
    let chosen = data.mode().to_owned();
    // Dedicated foundation diagnostic file, not an OwnerFs/DFS file or publication.
    let name = format!("probe-{}", config.id);
    let transfer = async {
        let written = data.write(&name, 0, b"AFShello".to_vec()).await?;
        let read = data.read(&name, 0, 8).await?;
        if written != 8 || read != b"AFShello" {
            return Err::<(), Error>(Error::coded(
                NODE_TRANSFER_CORRUPT_DATA,
                "data roundtrip mismatch",
            ));
        }
        Ok(())
    }
    .await;
    let closed = data.close().await;
    transfer?;
    closed?;
    afs_logging::info!("node.diagnostics completed";"mode"=>&chosen,"bytes"=>8);
    Ok(json!({"ok":true,"mode":chosen,"bytes":8,"meta":meta_pong,"peer_control":pong.payload}))
}
