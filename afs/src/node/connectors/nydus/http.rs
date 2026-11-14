//! Nydus HEAD/GET transport. There is no mandatory management prepare endpoint.
use super::service::{BlobStat, NydusService};
use afs_error::{Error, ErrorKind, Result};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use sha2::{Digest, Sha256};
use std::{io::Read, net::SocketAddr, path::Path as FilePath, sync::Arc, time::Instant};
use tokio::sync::OwnedSemaphorePermit;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

pub(super) fn load_token_hash(path: &FilePath) -> Result<[u8; 32]> {
    let mut secret = String::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_string(&mut secret)?;
    let token = secret.trim();
    if secret.len() > 4096 || token.len() < 32 || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(Error::coded(
            afs_error::CONFIG_INVALID,
            "Nydus HTTP token must contain 32–4096 visible ASCII bytes",
        ));
    }
    Ok(Sha256::digest(token).into())
}

pub fn routes(service: Arc<NydusService>, token_hash: Option<[u8; 32]>) -> Router {
    Router::new()
        .route("/v2/", get(registry_ping))
        .route("/v2/{*reference}", get(registry_read).head(registry_read))
        .route(
            "/nydus/{source}/{manifest}/blobs/{blob}",
            get(get_blob).head(head_blob),
        )
        .route_layer(middleware::from_fn_with_state(token_hash, require_access))
        .with_state(service)
}

const DISTRIBUTION_VERSION: &str = "docker-distribution-api-version";
const CONTENT_DIGEST: &str = "docker-content-digest";

async fn registry_ping() -> impl IntoResponse {
    ([(DISTRIBUTION_VERSION, "registry/2.0")], StatusCode::OK)
}

async fn registry_read(
    State(service): State<Arc<NydusService>>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let Some((repository, kind, reference)) = registry_reference(&path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response =
        match serve_registry(service, repository, kind, reference, method, headers).await {
            Ok(response) => response,
            Err(error) => failure(error),
        };
    response.headers_mut().insert(
        DISTRIBUTION_VERSION,
        HeaderValue::from_static("registry/2.0"),
    );
    response
}

fn registry_reference(path: &str) -> Option<(&str, &str, &str)> {
    let (prefix, reference) = path.rsplit_once('/')?;
    let (repository, kind) = prefix.rsplit_once('/')?;
    if repository.is_empty() || reference.is_empty() || !matches!(kind, "blobs" | "manifests") {
        return None;
    }
    Some((repository, kind, reference))
}

async fn serve_registry(
    service: Arc<NydusService>,
    repository: &str,
    kind: &str,
    reference: &str,
    method: Method,
    headers: HeaderMap,
) -> Result<Response> {
    let deadline = service.deadline();
    let permit = service.admit(deadline).await?;
    if kind == "manifests" {
        let manifest = service
            .registry_manifest(repository, reference, deadline)
            .await?;
        let length = manifest.body.len();
        let body = if method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(manifest.body)
        };
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, length)
            .header(header::CONTENT_TYPE, manifest.content_type)
            .header(CONTENT_DIGEST, manifest.digest)
            .body(body)
            .map_err(|_| invalid("cannot construct Registry manifest response"));
    }
    let stat = service
        .stat_registry_blob(repository, reference, deadline, permit.clone())
        .await?;
    let mut reply = if method == Method::HEAD {
        response(StatusCode::OK, Body::empty(), stat.size, None)?
    } else {
        read_stat(service, stat, &headers, deadline, permit).await?
    };
    if reply.status().is_success() {
        reply.headers_mut().insert(
            CONTENT_DIGEST,
            HeaderValue::from_str(reference)
                .map_err(|_| invalid("invalid Registry response digest"))?,
        );
    }
    Ok(reply)
}

async fn require_access(
    State(token_hash): State<Option<[u8; 32]>>,
    request: Request,
    next: Next,
) -> Response {
    let local = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .is_some_and(|ConnectInfo(peer)| peer.ip().to_canonical().is_loopback());
    let authorized = token_hash.is_some_and(|expected| {
        let Some(token) = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
        else {
            return false;
        };
        Sha256::digest(token)
            .iter()
            .zip(expected)
            .fold(0, |different, (actual, expected)| {
                different | (actual ^ expected)
            })
            == 0
    });
    if local || authorized {
        next.run(request).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

async fn head_blob(
    State(service): State<Arc<NydusService>>,
    Path((source, manifest, blob)): Path<(String, String, String)>,
) -> Response {
    match head(service, source, manifest, blob).await {
        Ok(response) => response,
        Err(error) => failure(error),
    }
}
async fn head(
    service: Arc<NydusService>,
    source: String,
    manifest: String,
    blob: String,
) -> Result<Response> {
    let deadline = service.deadline();
    let permit = service.admit(deadline).await?;
    let stat = service
        .stat_blob(source, manifest, blob, deadline, permit.clone())
        .await?;
    response(StatusCode::OK, Body::empty(), stat.size, None)
}
async fn get_blob(
    State(service): State<Arc<NydusService>>,
    Path((source, manifest, blob)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    match read(service, source, manifest, blob, headers).await {
        Ok(response) => response,
        Err(error) => failure(error),
    }
}
async fn read(
    service: Arc<NydusService>,
    source: String,
    manifest: String,
    blob: String,
    headers: HeaderMap,
) -> Result<Response> {
    let deadline = service.deadline();
    let permit = service.admit(deadline).await?;
    let stat = service
        .stat_blob(source, manifest, blob, deadline, permit.clone())
        .await?;
    read_stat(service, stat, &headers, deadline, permit).await
}

async fn read_stat(
    service: Arc<NydusService>,
    stat: BlobStat,
    headers: &HeaderMap,
    deadline: Instant,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<Response> {
    let range = headers
        .get(header::RANGE)
        .map(|v| v.to_str().map_err(|_| invalid("invalid Range header")))
        .transpose()?;
    let (offset, length) = match parse_range(range, stat.size) {
        Err(error) if error.kind() == ErrorKind::OutOfRange => {
            return response(
                StatusCode::RANGE_NOT_SATISFIABLE,
                Body::empty(),
                0,
                Some(format!("bytes */{}", stat.size)),
            );
        }
        result => result?,
    };
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let content_range =
        range.map(|_| format!("bytes {offset}-{}/{}", offset + length - 1, stat.size));
    if length == 0 {
        return response(status, Body::empty(), 0, content_range);
    }
    let mut reader = service
        .read_blob(stat.entry, offset, length, deadline, permit)
        .await?;
    // Resolve first-read failures before sending successful HTTP headers.
    let first = reader
        .recv()
        .await
        .ok_or_else(|| invalid("empty AFS body stream"))??;
    let body = Body::from_stream(
        tokio_stream::once(Ok::<_, Error>(first)).chain(ReceiverStream::new(reader)),
    );
    response(status, body, length, content_range)
}
fn response(
    status: StatusCode,
    body: Body,
    length: u64,
    range: Option<String>,
) -> Result<Response> {
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if let Some(range) = range {
        builder = builder.header(header::CONTENT_RANGE, range);
    }
    builder
        .body(body)
        .map_err(|_| invalid("cannot construct Nydus response"))
}
fn failure(error: Error) -> Response {
    let status = match error.kind() {
        ErrorKind::InvalidArgument => StatusCode::BAD_REQUEST,
        ErrorKind::OutOfRange => StatusCode::RANGE_NOT_SATISFIABLE,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorKind::AlreadyExists => StatusCode::CONFLICT,
        ErrorKind::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::BAD_GATEWAY,
    };
    (status, error.to_string()).into_response()
}
fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}
fn unsatisfiable() -> Error {
    Error::coded(afs_error::NODE_VFS_RANGE, "unsatisfiable byte range")
}

fn parse_range(range: Option<&str>, size: u64) -> Result<(u64, u64)> {
    let Some(range) = range else {
        return Ok((0, size));
    };
    let value = range
        .strip_prefix("bytes=")
        .ok_or_else(|| invalid("only byte ranges are supported"))?;
    if value.contains(',') {
        return Err(invalid("multipart ranges are not supported"));
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| invalid("invalid byte range"))?;
    if size == 0 {
        return Err(unsatisfiable());
    }
    if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .map_err(|_| invalid("invalid suffix range"))?;
        if suffix == 0 {
            return Err(unsatisfiable());
        }
        let length = suffix.min(size);
        return Ok((size - length, length));
    }
    let offset = start
        .parse::<u64>()
        .map_err(|_| invalid("invalid range offset"))?;
    if offset >= size {
        return Err(unsatisfiable());
    }
    let last = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>()
            .map_err(|_| invalid("invalid range end"))?
            .min(size - 1)
    };
    if last < offset {
        return Err(unsatisfiable());
    }
    Ok((offset, last - offset + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;
    use tower::ServiceExt;

    #[tokio::test]
    async fn nydus_access_uses_socket_peer_and_checks_remote_token() {
        const TOKEN: &str = "test-nydus-token-0000000000000000";
        let routes = Router::new()
            .route("/blob", get(|| async { StatusCode::OK }))
            .route_layer(middleware::from_fn_with_state(
                Some(<[u8; 32]>::from(Sha256::digest(TOKEN))),
                require_access,
            ));
        for (method, peer, token, expected) in [
            (Method::HEAD, None, None, StatusCode::FORBIDDEN),
            (Method::GET, Some("127.0.0.1:5400"), None, StatusCode::OK),
            (
                Method::HEAD,
                Some("192.0.2.1:5400"),
                Some("invalid"),
                StatusCode::FORBIDDEN,
            ),
            (
                Method::GET,
                Some("192.0.2.1:5400"),
                Some(TOKEN),
                StatusCode::OK,
            ),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri("/blob")
                .header("X-Forwarded-For", "127.0.0.1");
            if let Some(peer) = peer {
                request = request.extension(ConnectInfo(peer.parse::<SocketAddr>().unwrap()));
            }
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            let response = routes
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
    }

    #[test]
    fn nydus_single_range_contract() {
        assert_eq!(parse_range(None, 10).unwrap(), (0, 10));
        assert_eq!(parse_range(Some("bytes=3-5"), 10).unwrap(), (3, 3));
        assert_eq!(parse_range(Some("bytes=8-99"), 10).unwrap(), (8, 2));
        assert_eq!(parse_range(Some("bytes=3-"), 10).unwrap(), (3, 7));
        assert_eq!(parse_range(Some("bytes=-3"), 10).unwrap(), (7, 3));
        for bad in [
            "bytes=10-",
            "bytes=4-3",
            "bytes=-0",
            "bytes=0-1,3-4",
            "bytes=18446744073709551616-",
        ] {
            assert!(parse_range(Some(bad), 10).is_err());
        }
    }
}
