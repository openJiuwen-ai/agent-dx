//! Connection authentication with optional sustained TLS encryption.
use super::TlsConfig;
use rustls::pki_types::{CertificateDer, ServerName};
use std::{
    io,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tonic::transport::{Channel, Endpoint, server::Connected};

const MAGIC: [u8; 4] = *b"AFS1";
#[derive(Clone)]
pub struct AuthenticatedConnection {
    certificates: Arc<Vec<CertificateDer<'static>>>,
    remote: SocketAddr,
}
#[derive(Clone)]
pub(super) struct AuthenticatedTransport {
    encrypted: bool,
    client: Arc<rustls::ClientConfig>,
    server: Arc<rustls::ServerConfig>,
    name: ServerName<'static>,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(path)?)).collect()
}
impl AuthenticatedTransport {
    pub(super) fn new(tls: &TlsConfig) -> io::Result<Self> {
        let TlsConfig::MutualTls {
            ca_certificate,
            identity_certificate,
            identity_private_key,
            server_name,
            encryption_enabled,
        } = tls
        else {
            return Err(invalid("authenticated transport requires mTLS credentials"));
        };
        let mut roots = rustls::RootCertStore::empty();
        for c in certificates(ca_certificate)? {
            roots.add(c).map_err(io::Error::other)?;
        }
        let certs = certificates(identity_certificate)?;
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(
            identity_private_key,
        )?))?
        .ok_or_else(|| invalid("peer identity key missing"))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(certs.clone(), key.clone_key())
            .map_err(io::Error::other)?;
        client.alpn_protocols = vec![b"h2".to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .map_err(io::Error::other)?;
        let mut server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(io::Error::other)?;
        server.alpn_protocols = vec![b"h2".to_vec()];
        // No encrypted post-handshake tickets may remain after the cutover.
        server.send_tls13_tickets = 0;
        Ok(Self {
            encrypted: *encryption_enabled,
            client: Arc::new(client),
            server: Arc::new(server),
            name: ServerName::try_from(server_name.clone()).map_err(io::Error::other)?,
        })
    }
    pub(super) async fn accept(&self, socket: TcpStream) -> io::Result<AuthenticatedIo> {
        tokio::time::timeout(Duration::from_secs(5), self.accept_inner(socket))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer handshake deadline"))?
    }
    async fn accept_inner(&self, socket: TcpStream) -> io::Result<AuthenticatedIo> {
        let remote = socket.peer_addr()?;
        let mut tls = TlsAcceptor::from(self.server.clone())
            .accept(socket)
            .await?;
        let certificates = Arc::new(
            tls.get_ref()
                .1
                .peer_certificates()
                .ok_or_else(|| invalid("verified peer certificate missing"))?
                .to_vec(),
        );
        let identity = AuthenticatedConnection {
            certificates,
            remote,
        };
        let mut first = [0];
        tls.read_exact(&mut first).await?;
        if first[0] == b'P' && self.encrypted {
            // Ordinary TLS control clients send the HTTP/2 preface. Keep TLS,
            // replay the one consumed byte, and retain the same peer identity.
            log_identity(&identity, true);
            return Ok(AuthenticatedIo {
                inner: Wire::Tls(Box::new(tls)),
                identity,
                prefix: Some(b'P'),
            });
        }
        let mut hello = [0; 5];
        hello[0] = first[0];
        tls.read_exact(&mut hello[1..]).await?;
        if hello[..4] != MAGIC || hello[4] != u8::from(self.encrypted) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer bulk mode mismatch",
            ));
        }
        tls.write_all(&hello).await?;
        tls.flush().await?;
        // The client waits for this authenticated ACK before sending any raw
        // bytes; neither side can pre-read clear HTTP/2 into a TLS record buffer.
        let inner = if self.encrypted {
            Wire::Tls(Box::new(tls))
        } else {
            Wire::Plain(tls.into_inner().0)
        };
        let mut peer = AuthenticatedIo {
            inner,
            identity,
            prefix: None,
        };
        // Wait until the client has consumed the encrypted ACK and detached its
        // TLS parser. Only then may hyper send clear server SETTINGS frames.
        let mut ready = [0; 2];
        peer.read_exact(&mut ready).await?;
        if &ready != b"GO" {
            return Err(invalid("peer cutover barrier differs"));
        }
        log_identity(&peer.identity, self.encrypted);
        Ok(peer)
    }
    async fn connect_socket(
        &self,
        uri: tonic::codegen::http::Uri,
    ) -> io::Result<hyper_util::rt::TokioIo<WireClient>> {
        let host = uri
            .host()
            .ok_or_else(|| invalid("peer endpoint host absent"))?
            .to_string();
        let port = uri
            .port_u16()
            .ok_or_else(|| invalid("peer endpoint port absent"))?;
        let socket = TcpStream::connect((host.as_str(), port)).await?;
        socket.set_nodelay(true)?;
        let mut tls = TlsConnector::from(self.client.clone())
            .connect(self.name.clone(), socket)
            .await?;
        let mut hello = [0; 5];
        hello[..4].copy_from_slice(&MAGIC);
        hello[4] = u8::from(self.encrypted);
        tls.write_all(&hello).await?;
        tls.flush().await?;
        let mut reply = [0; 5];
        tls.read_exact(&mut reply).await?;
        if reply != hello {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer bulk ACK differs",
            ));
        }
        let mut io = if self.encrypted {
            WireClient::Tls(Box::new(tls))
        } else {
            WireClient::Plain(tls.into_inner().0)
        };
        io.write_all(b"GO").await?;
        io.flush().await?;
        Ok(hyper_util::rt::TokioIo::new(io))
    }
    fn connector(
        self: Arc<Self>,
    ) -> impl tower::Service<
        tonic::codegen::http::Uri,
        Response = hyper_util::rt::TokioIo<WireClient>,
        Error = io::Error,
        Future = ConnectFuture,
    > + Clone {
        let config = self.clone();
        tower::service_fn(move |uri| -> ConnectFuture {
            let config = config.clone();
            Box::pin(async move { config.connect_socket(uri).await })
        })
    }
    pub(super) fn connect_lazy(self: &Arc<Self>, endpoint: Endpoint) -> Channel {
        endpoint.connect_with_connector_lazy(self.clone().connector())
    }
    pub(super) async fn connect(
        self: &Arc<Self>,
        endpoint: Endpoint,
    ) -> Result<Channel, tonic::transport::Error> {
        endpoint
            .connect_with_connector(self.clone().connector())
            .await
    }
}
type ConnectFuture = Pin<
    Box<dyn std::future::Future<Output = io::Result<hyper_util::rt::TokioIo<WireClient>>> + Send>,
>;
fn log_identity(identity: &AuthenticatedConnection, encrypted: bool) {
    if let Some(leaf) = identity.certificates.first() {
        afs_logging::info!("afs.transport.authenticated"; "encryption_enabled"=>encrypted, "peer"=>identity.remote.to_string(), "peer_der_hash"=>blake3::hash(leaf.as_ref()).to_hex().to_string());
    }
}

enum Wire {
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    Plain(TcpStream),
}
pub struct AuthenticatedIo {
    inner: Wire,
    identity: AuthenticatedConnection,
    prefix: Option<u8>,
}
impl Connected for AuthenticatedIo {
    type ConnectInfo = AuthenticatedConnection;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.identity.clone()
    }
}
impl AsyncRead for AuthenticatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(b) = self.prefix.take() {
            out.put_slice(&[b]);
            return Poll::Ready(Ok(()));
        }
        match &mut self.inner {
            Wire::Tls(s) => Pin::new(s).poll_read(cx, out),
            Wire::Plain(s) => Pin::new(s).poll_read(cx, out),
        }
    }
}
macro_rules! write_io {
    ($ty:ty, $field:ident, $enum:ident) => {
        impl AsyncWrite for $ty {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                b: &[u8],
            ) -> Poll<io::Result<usize>> {
                match &mut self.$field {
                    $enum::Tls(s) => Pin::new(s).poll_write(cx, b),
                    $enum::Plain(s) => Pin::new(s).poll_write(cx, b),
                }
            }
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                match &mut self.$field {
                    $enum::Tls(s) => Pin::new(s).poll_flush(cx),
                    $enum::Plain(s) => Pin::new(s).poll_flush(cx),
                }
            }
            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<()>> {
                match &mut self.$field {
                    $enum::Tls(s) => Pin::new(s).poll_shutdown(cx),
                    $enum::Plain(s) => Pin::new(s).poll_shutdown(cx),
                }
            }
            fn is_write_vectored(&self) -> bool {
                match &self.$field {
                    $enum::Tls(s) => s.is_write_vectored(),
                    $enum::Plain(s) => s.is_write_vectored(),
                }
            }
            fn poll_write_vectored(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                b: &[io::IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                match &mut self.$field {
                    $enum::Tls(s) => Pin::new(s).poll_write_vectored(cx, b),
                    $enum::Plain(s) => Pin::new(s).poll_write_vectored(cx, b),
                }
            }
        }
    };
}
write_io!(AuthenticatedIo, inner, Wire);
enum WireClient {
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    Plain(TcpStream),
}
impl AsyncRead for WireClient {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => Pin::new(s).poll_read(cx, out),
            Self::Plain(s) => Pin::new(s).poll_read(cx, out),
        }
    }
}
impl AsyncWrite for WireClient {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, b: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tls(s) => Pin::new(s).poll_write(cx, b),
            Self::Plain(s) => Pin::new(s).poll_write(cx, b),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tls(s) => s.is_write_vectored(),
            Self::Plain(s) => s.is_write_vectored(),
        }
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tls(s) => Pin::new(s).poll_write_vectored(cx, b),
            Self::Plain(s) => Pin::new(s).poll_write_vectored(cx, b),
        }
    }
}

impl AuthenticatedIo {
    pub(super) fn plain(socket: TcpStream) -> io::Result<Self> {
        let remote = socket.peer_addr()?;
        Ok(Self {
            inner: Wire::Plain(socket),
            identity: AuthenticatedConnection {
                certificates: Arc::new(Vec::new()),
                remote,
            },
            prefix: None,
        })
    }
}
/// Certificate identity from this actual accepted connection, never request metadata.
pub fn peer_certificates<T>(
    request: &tonic::Request<T>,
) -> Option<Arc<Vec<CertificateDer<'static>>>> {
    request.peer_certs().or_else(|| {
        request
            .extensions()
            .get::<AuthenticatedConnection>()
            .filter(|identity| !identity.certificates.is_empty())
            .map(|identity| identity.certificates.clone())
    })
}
pub fn peer_remote_addr<T>(request: &tonic::Request<T>) -> Option<SocketAddr> {
    request.remote_addr().or_else(|| {
        request
            .extensions()
            .get::<AuthenticatedConnection>()
            .map(|identity| identity.remote)
    })
}

#[cfg(test)]
#[path = "test_tls.rs"]
mod test_tls;
#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    fn policy(dir: &Path, encrypted: bool, server: bool) -> Arc<AuthenticatedTransport> {
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, super::test_tls::CA_PEM).unwrap();
        let cert = dir.join(if server { "server.pem" } else { "client.pem" });
        let key = dir.join(if server { "server.key" } else { "client.key" });
        std::fs::write(
            &cert,
            if server {
                super::test_tls::SERVER_CERT_PEM
            } else {
                super::test_tls::CLIENT_CERT_PEM
            },
        )
        .unwrap();
        std::fs::write(
            &key,
            if server {
                super::test_tls::SERVER_KEY_PEM
            } else {
                super::test_tls::CLIENT_KEY_PEM
            },
        )
        .unwrap();
        Arc::new(
            AuthenticatedTransport::new(&TlsConfig::MutualTls {
                encryption_enabled: encrypted,
                ca_certificate: ca,
                identity_certificate: cert,
                identity_private_key: key,
                server_name: "localhost".into(),
            })
            .unwrap(),
        )
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_transport_modes_authenticate_and_transfer_full_body() {
        for encrypted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let server = policy(dir.path(), encrypted, true);
            let client = policy(dir.path(), encrypted, false);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                let mut io = server
                    .accept(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();
                assert!(!io.identity.certificates.is_empty());
                let mut request = tonic::Request::new(());
                request.extensions_mut().insert(io.connect_info());
                assert!(peer_certificates(&request).is_some());
                let mut body = vec![0; 131072];
                io.read_exact(&mut body).await.unwrap();
                assert!(body.iter().all(|&b| b == 71));
                io.write_all(&body).await.unwrap();
                io.flush().await.unwrap();
            });
            let uri = format!("http://localhost:{}", addr.port()).parse().unwrap();
            let mut io = client.connect_socket(uri).await.unwrap().into_inner();
            io.write_all(&vec![71; 131072]).await.unwrap();
            io.flush().await.unwrap();
            let mut body = vec![0; 131072];
            io.read_exact(&mut body).await.unwrap();
            assert!(body.iter().all(|&b| b == 71));
            task.await.unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_transport_mode_mismatch_does_not_downgrade() {
        for encrypted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let server = policy(dir.path(), encrypted, true);
            let client = policy(dir.path(), !encrypted, false);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                assert!(
                    server
                        .accept(listener.accept().await.unwrap().0)
                        .await
                        .is_err()
                );
            });
            assert!(
                client
                    .connect_socket(format!("http://localhost:{}", addr.port()).parse().unwrap())
                    .await
                    .is_err()
            );
            task.await.unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_transport_invalid_identity_is_rejected_in_both_modes() {
        for encrypted in [false, true] {
            for fault in ["missing-client", "wrong-server", "wrong-ca"] {
                let dir = tempfile::tempdir().unwrap();
                let server = policy(dir.path(), encrypted, true);
                let mut client = (*policy(dir.path(), encrypted, false)).clone();
                if fault == "wrong-server" {
                    client.name = ServerName::try_from("untrusted.invalid")
                        .unwrap()
                        .to_owned();
                } else {
                    let mut roots = rustls::RootCertStore::empty();
                    let pem = if fault == "wrong-ca" {
                        super::test_tls::CLIENT_CERT_PEM
                    } else {
                        super::test_tls::CA_PEM
                    };
                    for c in rustls_pemfile::certs(&mut std::io::BufReader::new(pem.as_bytes())) {
                        roots.add(c.unwrap()).unwrap();
                    }
                    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
                        rustls::crypto::ring::default_provider(),
                    ))
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_root_certificates(roots);
                    let mut config = if fault == "missing-client" {
                        builder.with_no_client_auth()
                    } else {
                        let certs = certificates(&dir.path().join("client.pem")).unwrap();
                        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
                            std::fs::File::open(dir.path().join("client.key")).unwrap(),
                        ))
                        .unwrap()
                        .unwrap();
                        builder.with_client_auth_cert(certs, key).unwrap()
                    };
                    config.alpn_protocols = vec![b"h2".to_vec()];
                    client.client = Arc::new(config);
                }
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let task = tokio::spawn(async move {
                    assert!(
                        server
                            .accept(listener.accept().await.unwrap().0)
                            .await
                            .is_err()
                    );
                });
                assert!(
                    client
                        .connect_socket(
                            format!("http://localhost:{}", addr.port()).parse().unwrap()
                        )
                        .await
                        .is_err(),
                    "{fault}"
                );
                task.await.unwrap();
            }
        }
    }
    #[test]
    fn unified_transport_auth_only_requires_real_credentials() {
        assert!(AuthenticatedTransport::new(&TlsConfig::Disabled).is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_transport_slow_handshake_does_not_block_another_connection() {
        let dir = tempfile::tempdir().unwrap();
        let server = policy(dir.path(), false, true);
        let client = policy(dir.path(), false, false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = TlsConfig::MutualTls {
            encryption_enabled: false,
            ca_certificate: dir.path().join("ca.pem"),
            identity_certificate: dir.path().join("server.pem"),
            identity_private_key: dir.path().join("server.key"),
            server_name: "localhost".into(),
        };
        let incoming = super::super::SecurityManager::new(cfg)
            .unwrap()
            .incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .unwrap();
        let task = tokio::spawn(async move {
            tokio::pin!(incoming);
            let first = tokio::time::timeout(Duration::from_secs(2), incoming.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(!first.identity.certificates.is_empty());
        });
        let _slow = TcpStream::connect(addr).await.unwrap();
        client
            .connect_socket(format!("http://localhost:{}", addr.port()).parse().unwrap())
            .await
            .unwrap();
        task.await.unwrap();
        drop(server);
    }
}
