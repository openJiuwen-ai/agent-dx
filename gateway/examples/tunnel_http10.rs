//! Loopback TLS Ingress fixture for the real Execd / Python SDK interop test.
use data_plane_gateway::ingress::{
    DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator, IngressRouteResolver,
    ProxyRoute, RouteStore,
};
use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

#[derive(Deserialize)]
struct Config {
    listen: SocketAddr,
    tunnel_ws_port: u16,
    tunnel_http_port: u16,
    tls: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    adx_transport::install_crypto_provider();
    let path = std::env::args()
        .nth(1)
        .ok_or("fixture config path required")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(File::open(
        config.tls.join("ingress.pem"),
    )?))
    .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(
        config.tls.join("ingress.key"),
    )?))?
    .ok_or("fixture key required")?;
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    let routes = [
        ("control", "/tunnel", config.tunnel_ws_port),
        ("business", "/app", config.tunnel_http_port),
    ]
    .into_iter()
    .map(|(name, prefix, port)| ProxyRoute {
        name: name.into(),
        path_prefix: prefix.into(),
        upstream: format!("http://127.0.0.1:{port}"),
        strip_prefix: true,
        host: None,
    })
    .collect();
    let ingress = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(Arc::new(RouteStore::new()))),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::new(true, false, "", Duration::from_secs(30))?,
            8888,
            config.tunnel_ws_port,
            "127.0.0.1:9",
            Vec::new(),
        )
        .with_proxy_routes(routes)
        .with_client_acl(vec!["127.0.0.1/32".parse()?], false),
    );
    let listener = TcpListener::bind(config.listen).await?;
    let (_shutdown, receiver) = watch::channel(false);
    ingress
        .serve_http_tls(listener, TlsAcceptor::from(Arc::new(tls)), receiver)
        .await?;
    Ok(())
}
