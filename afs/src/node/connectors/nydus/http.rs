//! Nydus HEAD/GET transport. There is no mandatory management prepare endpoint.
use super::service::NydusService;
use afs_error::{Error, ErrorKind, Result};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use std::sync::Arc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

pub fn routes(service: Arc<NydusService>) -> Router {
    Router::new()
        .route(
            "/nydus/{source}/{manifest}/blobs/{blob}",
            get(get_blob).head(head_blob),
        )
        .with_state(service)
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
    let range = headers
        .get(header::RANGE)
        .map(|v| v.to_str().map_err(|_| invalid("invalid Range header")))
        .transpose()?;
    let (offset, length) = parse_range(range, stat.size)?;
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
