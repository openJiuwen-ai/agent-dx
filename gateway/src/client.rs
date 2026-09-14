use crate::common::protocol::{H_ACCESS_KIND, H_REQUEST_ID};
use crate::edge::AccessKind;
use bytes::Bytes;
use http::{header, Method, Request, Uri};
use http_body_util::{BodyExt, Empty};
use hyper::client::conn::http1;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const MAX_ERROR_BODY_BYTES: usize = 16 * 1024;

trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedIo = Box<dyn AsyncIo>;

#[derive(Clone)]
pub struct EdgeTlsConfig {
    pub client: Arc<rustls::ClientConfig>,
    pub server_name: String,
}

#[derive(Clone)]
pub struct ConnectClientConfig {
    pub edge_address: String,
    pub instance_id: String,
    pub target_port: u16,
    pub access_kind: AccessKind,
    pub bearer_token: String,
    pub request_id: String,
    pub tls: Option<EdgeTlsConfig>,
}

impl ConnectClientConfig {
    pub fn authority(&self) -> io::Result<String> {
        if self.instance_id.trim().is_empty() || self.target_port == 0 {
            return Err(invalid("instance ID and target port are required"));
        }
        let authority = format!("{}:{}", self.instance_id, self.target_port);
        authority
            .parse::<http::uri::Authority>()
            .map_err(|error| invalid(&format!("invalid CONNECT authority: {error}")))?;
        Ok(authority)
    }
}

/// Open a standards-based HTTP/1.1 CONNECT tunnel to Edge.
///
/// The returned stream carries only the target TCP bytes. Edge is responsible
/// for resolving the logical instance authority to the owning Node Proxy.
pub async fn connect_edge(config: ConnectClientConfig) -> io::Result<TokioIo<Upgraded>> {
    if config.tls.is_none() && !config.bearer_token.trim().is_empty() {
        return Err(invalid(
            "bearer tokens are not allowed on the plaintext Edge listener",
        ));
    }
    let tcp = TcpStream::connect(&config.edge_address).await?;
    let io: BoxedIo = if let Some(tls) = config.tls.clone() {
        let server_name = ServerName::try_from(tls.server_name)
            .map_err(|error| invalid(&format!("invalid Edge TLS server name: {error}")))?;
        Box::new(
            TlsConnector::from(tls.client)
                .connect(server_name, tcp)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::ConnectionRefused, error))?,
        )
    } else {
        Box::new(tcp)
    };

    let (mut sender, connection) = http1::handshake(TokioIo::new(io)).await.map_err(other)?;
    tokio::spawn(async move {
        if let Err(error) = connection.with_upgrades().await {
            tracing::debug!(%error, "Edge CONNECT client connection closed");
        }
    });

    let authority = config.authority()?;
    let uri = authority
        .parse::<Uri>()
        .map_err(|error| invalid(&format!("invalid CONNECT URI: {error}")))?;
    let mut builder = Request::builder()
        .method(Method::CONNECT)
        .uri(uri)
        .header(header::HOST, &authority)
        .header(H_ACCESS_KIND, config.access_kind.as_str())
        .header(H_REQUEST_ID, &config.request_id);
    if !config.bearer_token.trim().is_empty() {
        builder = builder.header(
            header::AUTHORIZATION,
            format!("Bearer {}", config.bearer_token.trim()),
        );
    }
    let request = builder
        .body(Empty::<Bytes>::new())
        .map_err(|error| invalid(&format!("build CONNECT request: {error}")))?;
    let mut response = sender.send_request(request).await.map_err(other)?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(other)?
            .to_bytes();
        let body = &body[..body.len().min(MAX_ERROR_BODY_BYTES)];
        let message = String::from_utf8_lossy(body);
        return Err(io::Error::new(
            status_error_kind(status),
            format!("Edge CONNECT failed with {status}: {message}"),
        ));
    }
    hyper::upgrade::on(&mut response)
        .await
        .map(TokioIo::new)
        .map_err(other)
}

fn status_error_kind(status: http::StatusCode) -> io::ErrorKind {
    match status.as_u16() {
        400 => io::ErrorKind::InvalidInput,
        401 | 403 => io::ErrorKind::PermissionDenied,
        404 => io::ErrorKind::NotFound,
        408 | 504 => io::ErrorKind::TimedOut,
        409 => io::ErrorKind::InvalidData,
        429 => io::ErrorKind::WouldBlock,
        502 => io::ErrorKind::ConnectionRefused,
        503 => io::ErrorKind::NotConnected,
        _ => io::ErrorKind::Other,
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_instance_and_port_form_connect_authority() {
        let config = ConnectClientConfig {
            edge_address: "127.0.0.1:8080".into(),
            instance_id: "instance-a".into(),
            target_port: 22,
            access_kind: AccessKind::Ssh,
            bearer_token: String::new(),
            request_id: "request-a".into(),
            tls: None,
        };
        assert_eq!(config.authority().unwrap(), "instance-a:22");
    }
}
