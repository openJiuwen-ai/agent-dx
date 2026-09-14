use super::NodeProxy;
use prost::Message;
use std::net::IpAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

pub mod proto {
    tonic::include_proto!("data_plane_gateway_activity");
}

use proto::{
    DataPlaneGatewayRouteState, DataPlaneGatewaySetRouteRequest, DataPlaneGatewaySetRouteResponse,
};

const MAX_ROUTE_FRAME_SIZE: usize = 64 * 1024;

pub async fn bind_route_control(uds_path: &str) -> std::io::Result<UnixListener> {
    let path = Path::new(uds_path);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660)).await?;
    Ok(listener)
}

pub async fn serve_route_control(
    gateway: Arc<NodeProxy>,
    listener: UnixListener,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let gateway = gateway.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_route_connection(gateway, stream).await {
                tracing::warn!(%error, "Node Proxy route control request failed");
            }
        });
    }
}

async fn handle_route_connection(
    gateway: Arc<NodeProxy>,
    mut stream: UnixStream,
) -> std::io::Result<()> {
    let request = read_frame::<DataPlaneGatewaySetRouteRequest>(&mut stream).await?;
    let response = apply_route_request(gateway, request).await;
    write_frame(&mut stream, &response).await?;
    stream.shutdown().await
}

async fn apply_route_request(
    gateway: Arc<NodeProxy>,
    request: DataPlaneGatewaySetRouteRequest,
) -> DataPlaneGatewaySetRouteResponse {
    let error = if request.instance_id.trim().is_empty() || request.workload_id.trim().is_empty() {
        Some("instance_id and workload_id are required".to_string())
    } else {
        match DataPlaneGatewayRouteState::try_from(request.state) {
            Ok(DataPlaneGatewayRouteState::Active) => match request.sandbox_ip.parse::<IpAddr>() {
                Ok(sandbox_ip) => {
                    gateway
                        .activate_route(request.instance_id, request.workload_id, sandbox_ip)
                        .await;
                    None
                }
                Err(_) => Some("sandbox_ip is invalid".to_string()),
            },
            Ok(DataPlaneGatewayRouteState::Retired) => {
                gateway
                    .retire_route(request.instance_id, request.workload_id)
                    .await;
                None
            }
            _ => Some("route state is invalid".to_string()),
        }
    };
    match error {
        Some(message) => DataPlaneGatewaySetRouteResponse { code: 1, message },
        None => DataPlaneGatewaySetRouteResponse {
            code: 0,
            message: String::new(),
        },
    }
}

async fn read_frame<M: Message + Default>(stream: &mut UnixStream) -> std::io::Result<M> {
    let size = stream.read_u32().await? as usize;
    if size == 0 || size > MAX_ROUTE_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "route control frame size is invalid",
        ));
    }
    let mut payload = vec![0u8; size];
    stream.read_exact(&mut payload).await?;
    M::decode(payload.as_slice()).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("route control protobuf decode failed: {error}"),
        )
    })
}

async fn write_frame<M: Message>(stream: &mut UnixStream, message: &M) -> std::io::Result<()> {
    let payload = message.encode_to_vec();
    if payload.len() > MAX_ROUTE_FRAME_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "route control response is too large",
        ));
    }
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(&payload).await
}
