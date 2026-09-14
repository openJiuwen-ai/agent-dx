use super::server::NodeProxy;
use crate::common::listener::accept_with_backoff;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;

pub async fn serve_health(
    gateway: Arc<NodeProxy>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            (stream, peer) = accept_with_backoff(&listener, "node-health") => {
                let gateway = gateway.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request| health_response(gateway.clone(), request));
                    if let Err(error) = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        tracing::debug!(%peer, %error, "Node health connection closed");
                    }
                });
            }
        }
    }
}

async fn health_response(
    gateway: Arc<NodeProxy>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body) = match request.uri().path() {
        "/healthz" => (StatusCode::OK, "ok".to_owned()),
        "/readyz" if gateway.ready() => (StatusCode::OK, "ready".to_owned()),
        "/readyz" => (StatusCode::SERVICE_UNAVAILABLE, "draining".to_owned()),
        "/metrics" => {
            let metrics = gateway.metrics();
            (
                StatusCode::OK,
                format!(
                    "data_plane_node_proxy_ready {}\ndata_plane_node_proxy_active_streams {}\ndata_plane_node_proxy_max_streams {}\ndata_plane_node_proxy_connect_total {}\ndata_plane_node_proxy_connect_errors {}\ndata_plane_node_proxy_completed_streams {}\ndata_plane_node_proxy_bytes_up {}\ndata_plane_node_proxy_bytes_down {}\ndata_plane_node_proxy_route_mismatch_total {}\ndata_plane_node_proxy_forbidden_target_total {}\ndata_plane_node_proxy_overload_rejections_total {}\n",
                    usize::from(gateway.ready()),
                    gateway.active_streams(),
                    gateway.max_active_streams(),
                    metrics.connect_total,
                    metrics.connect_errors,
                    metrics.completed_streams,
                    metrics.bytes_up,
                    metrics.bytes_down,
                    metrics.route_mismatch,
                    metrics.forbidden_target,
                    metrics.overload_rejections,
                ),
            )
        }
        _ => (StatusCode::NOT_FOUND, "not found".to_owned()),
    };
    Ok(Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body)))
        .unwrap())
}
