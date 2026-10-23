//! One request per UDS connection. A detached create retains its response future.
use adx_core::{BackendFailureCode, Error, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::{path::PathBuf, time::Duration};
use tokio::net::{TcpStream, UnixStream};

pub(super) async fn execd_status(
    address: std::net::SocketAddr,
    token: Option<&str>,
    timeout: Duration,
) -> Result<adx_core::runtime::RuntimeStatus> {
    tokio::time::timeout(timeout, async {
        let stream = TcpStream::connect(address).await.map_err(unavailable)?;
        stream.set_nodelay(true).map_err(unavailable)?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(unavailable)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let mut request = Request::builder()
            .method(Method::GET)
            .uri("/control/v1/status")
            .header("host", address.to_string());
        if let Some(token) = token {
            request = request.header("X-Auth", token);
        }
        let response = sender
            .send_request(request.body(Full::new(Bytes::new())).map_err(unavailable)?)
            .await
            .map_err(unavailable)?;
        if !response.status().is_success() {
            return Err(unavailable("Execd status unavailable"));
        }
        let bytes = Limited::new(response.into_body(), 65536)
            .collect()
            .await
            .map_err(unavailable)?
            .to_bytes();
        serde_json::from_slice(&bytes).map_err(unavailable)
    })
    .await
    .map_err(|_| unavailable("Execd status timed out"))?
}

#[derive(Clone)]
pub(super) struct Client {
    pub socket: PathBuf,
    pub token: Option<String>,
    pub timeout: Duration,
}

pub(super) fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("Jiuwenbox: {error}"))
}

impl Client {
    pub async fn epoch(&self) -> Result<String> {
        use std::os::unix::fs::MetadataExt;
        let stream = tokio::time::timeout(self.timeout, UnixStream::connect(&self.socket))
            .await
            .map_err(|_| unavailable("peer identity connect timed out"))?
            .map_err(unavailable)?;
        let metadata = std::fs::metadata(&self.socket).map_err(unavailable)?;
        let identity = format!("{}:{}", metadata.dev(), metadata.ino());
        #[cfg(target_os = "linux")]
        {
            let pid = stream
                .peer_cred()
                .map_err(unavailable)?
                .pid()
                .ok_or_else(|| unavailable("backend peer PID unavailable"))?;
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(unavailable)?;
            let start = stat
                .rsplit_once(')')
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .ok_or_else(|| unavailable("invalid backend process identity"))?;
            let boot =
                std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(unavailable)?;
            Ok(format!("{identity}:{pid}:{start}:{}", boot.trim()))
        }
        #[cfg(not(target_os = "linux"))]
        let _ = stream;
        #[cfg(not(target_os = "linux"))]
        {
            Ok(identity)
        }
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        detached: bool,
    ) -> Result<Value> {
        let request = async {
            let stream = tokio::time::timeout(self.timeout, UnixStream::connect(&self.socket))
                .await
                .map_err(|_| unavailable("UDS connect timed out"))?
                .map_err(unavailable)?;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream))
                    .await
                    .map_err(unavailable)?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "localhost")
                .header("content-type", "application/json");
            if let Some(token) = &self.token {
                request = request.header("authorization", format!("Bearer {token}"));
            }
            let body = body
                .map(|body| serde_json::to_vec(&body))
                .transpose()
                .map_err(unavailable)?
                .unwrap_or_default();
            let response = sender
                .send_request(
                    request
                        .body(Full::new(Bytes::from(body)))
                        .map_err(unavailable)?,
                )
                .await
                .map_err(unavailable)?;
            let status = response.status();
            let bytes = Limited::new(response.into_body(), 8 * 1024 * 1024)
                .collect()
                .await
                .map_err(unavailable)?
                .to_bytes();
            if !status.is_success() {
                let code = match status.as_u16() {
                    400 | 422 => BackendFailureCode::InvalidArgument,
                    401 => BackendFailureCode::Unauthenticated,
                    403 => BackendFailureCode::PermissionDenied,
                    404 => return Err(Error::NotFound),
                    409 => return Err(Error::Conflict),
                    408 | 504 => BackendFailureCode::DeadlineExceeded,
                    429 => BackendFailureCode::ResourceExhausted,
                    _ => BackendFailureCode::Unavailable,
                };
                // Do not include request bodies, environment values or credentials.
                return Err(Error::BackendFailure {
                    code,
                    message: format!("Jiuwenbox HTTP {status}"),
                });
            }
            if path == "/health" {
                return Ok(Value::Null);
            }
            if bytes.is_empty() {
                Ok(Value::Null)
            } else {
                serde_json::from_slice(&bytes).map_err(unavailable)
            }
        };
        // Create is owned by a spawned driver task. A caller deadline must not
        // cancel the HTTP response and erase the only evidence of completion.
        if detached {
            request.await
        } else {
            tokio::time::timeout(self.timeout, request)
                .await
                .map_err(|_| Error::DeadlineExceeded(format!("Jiuwenbox {path}")))?
        }
    }
}
