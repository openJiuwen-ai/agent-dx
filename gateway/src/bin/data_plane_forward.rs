use data_plane_gateway::client::{connect_edge, ConnectClientConfig, EdgeTlsConfig};
use data_plane_gateway::common::listener::accept_with_backoff;
use data_plane_gateway::common::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::edge::AccessKind;
use rustls::pki_types::ServerName;
use std::fs::File;
use std::io::{self, BufReader};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

const L4_COPY_BUFFER_SIZE: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    data_plane_gateway::common::install_crypto_provider();
    adx_observability::init().map_err(|e| -> Box<dyn std::error::Error> { e })?;
    let nofile_soft_limit = raise_nofile_soft_limit_from_env()?;
    tracing::info!(nofile_soft_limit, "Data Plane Forward FD limit configured");
    let config = ForwardConfig::from_args(std::env::args().skip(1))?;
    match config.mode.clone() {
        ForwardMode::Stdio => forward_stdio(config).await?,
        ForwardMode::Listen(address) => serve_local(config, &address).await?,
    }
    Ok(())
}

#[derive(Clone)]
struct ForwardConfig {
    connect: ConnectClientConfig,
    mode: ForwardMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ForwardMode {
    Stdio,
    Listen(String),
}

impl ForwardConfig {
    fn from_args(args: impl Iterator<Item = String>) -> Result<Self, io::Error> {
        let mut config =
            Self::from_args_with_token(args, std::env::var("ADX_TOKEN").unwrap_or_default())?;
        config.connect.tls = tls_from_env(&config.connect.edge_address)?;
        Ok(config)
    }

    fn from_args_with_token(
        mut args: impl Iterator<Item = String>,
        bearer_token: String,
    ) -> Result<Self, io::Error> {
        let usage = "usage:\n  adx-data-plane-forward connect <edge-host:port> <instance-id> <target-port> [tunnel|port-forwarding|ssh]\n  adx-data-plane-forward port-forward <edge-host:port> <instance-id> <target-port> [listen-host:port]\noptional env: ADX_TOKEN, ADX_DATA_PLANE_FORWARD_TLS_CA, ADX_DATA_PLANE_FORWARD_TLS_SERVER_NAME";
        let command = args.next().ok_or_else(|| invalid(usage))?;
        let edge_address = args.next().ok_or_else(|| invalid(usage))?;
        let instance_id = args.next().ok_or_else(|| invalid(usage))?;
        let target_port = args
            .next()
            .ok_or_else(|| invalid(usage))?
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| invalid("target port must be in 1..65535"))?;

        let (mode, access_kind) = match command.as_str() {
            "connect" => {
                let kind = args
                    .next()
                    .map(|value| {
                        AccessKind::from_str(&value).map_err(|_| invalid("invalid access kind"))
                    })
                    .transpose()?
                    .unwrap_or(AccessKind::Ssh);
                (ForwardMode::Stdio, kind)
            }
            "port-forward" => (
                ForwardMode::Listen(args.next().unwrap_or_else(|| "127.0.0.1:0".into())),
                AccessKind::PortForwarding,
            ),
            _ => return Err(invalid(usage)),
        };
        if args.next().is_some()
            || instance_id.trim().is_empty()
            || edge_address.trim().is_empty()
            || access_kind == AccessKind::Direct
        {
            return Err(invalid(usage));
        }
        Ok(Self {
            connect: ConnectClientConfig {
                edge_address,
                instance_id,
                target_port,
                access_kind,
                bearer_token,
                request_id: uuid::Uuid::new_v4().to_string(),
                tls: None,
            },
            mode,
        })
    }
}

async fn serve_local(config: ForwardConfig, address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    tracing::info!(
        listen = %listener.local_addr()?,
        edge = %config.connect.edge_address,
        instance = %config.connect.instance_id,
        port = config.connect.target_port,
        "local Data Plane port-forward ready"
    );
    loop {
        let (local, peer) = accept_with_backoff(&listener, "data-plane-forward").await;
        let mut connect = config.connect.clone();
        connect.request_id = uuid::Uuid::new_v4().to_string();
        tokio::spawn(async move {
            if let Err(error) = forward_local(local, connect).await {
                tracing::warn!(%peer, %error, "local forwarding connection failed");
            }
        });
    }
}

async fn forward_local(mut local: TcpStream, config: ConnectClientConfig) -> io::Result<()> {
    let mut edge = connect_edge(config).await?;
    tokio::io::copy_bidirectional_with_sizes(
        &mut local,
        &mut edge,
        L4_COPY_BUFFER_SIZE,
        L4_COPY_BUFFER_SIZE,
    )
    .await?;
    Ok(())
}

async fn forward_stdio(config: ForwardConfig) -> io::Result<()> {
    let mut stdio = StdioStream {
        input: tokio::io::stdin(),
        output: tokio::io::stdout(),
    };
    let mut edge = connect_edge(config.connect).await?;
    tokio::io::copy_bidirectional_with_sizes(
        &mut stdio,
        &mut edge,
        L4_COPY_BUFFER_SIZE,
        L4_COPY_BUFFER_SIZE,
    )
    .await?;
    Ok(())
}

struct StdioStream {
    input: tokio::io::Stdin,
    output: tokio::io::Stdout,
}

impl AsyncRead for StdioStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.input).poll_read(cx, buf)
    }
}

impl AsyncWrite for StdioStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        std::pin::Pin::new(&mut self.output).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::pin::Pin::new(&mut self.output).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::pin::Pin::new(&mut self.output).poll_shutdown(cx)
    }
}

fn tls_from_env(edge_address: &str) -> io::Result<Option<EdgeTlsConfig>> {
    let ca_path = std::env::var("ADX_DATA_PLANE_FORWARD_TLS_CA").unwrap_or_default();
    if ca_path.trim().is_empty() {
        return Ok(None);
    }
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca_path.trim())?);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(&format!("read Edge TLS CA: {error}")))?;
    if certificates.is_empty() {
        return Err(invalid("Edge TLS CA contains no certificates"));
    }
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| invalid(&format!("add Edge TLS CA: {error}")))?;
    }
    let server_name = std::env::var("ADX_DATA_PLANE_FORWARD_TLS_SERVER_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| edge_host(edge_address));
    ServerName::try_from(server_name.clone())
        .map_err(|error| invalid(&format!("invalid Edge TLS server name: {error}")))?;
    Ok(Some(EdgeTlsConfig {
        client: Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ),
        server_name,
    }))
}

fn edge_host(address: &str) -> String {
    address
        .parse::<http::uri::Authority>()
        .map(|authority| authority.host().to_owned())
        .unwrap_or_else(|_| address.to_owned())
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ssh_stdio_connect() {
        let config = ForwardConfig::from_args_with_token(
            ["connect", "127.0.0.1:8080", "instance-a", "22", "ssh"]
                .into_iter()
                .map(str::to_owned),
            "test-token".into(),
        )
        .unwrap();
        assert_eq!(config.connect.target_port, 22);
        assert_eq!(config.connect.access_kind, AccessKind::Ssh);
        assert_eq!(config.mode, ForwardMode::Stdio);
    }

    #[test]
    fn parses_local_port_forward() {
        let config = ForwardConfig::from_args_with_token(
            [
                "port-forward",
                "127.0.0.1:8080",
                "instance-a",
                "5432",
                "127.0.0.1:15432",
            ]
            .into_iter()
            .map(str::to_owned),
            String::new(),
        )
        .unwrap();
        assert_eq!(config.mode, ForwardMode::Listen("127.0.0.1:15432".into()));
        assert_eq!(config.connect.access_kind, AccessKind::PortForwarding);
    }
}
