use super::*;
use crate::meta::{Meta, rpc::MetaRpc};
use afs_protocol::meta::{NodeDescriptor, NodeEndpoint};
use std::{fs, path::Path, process::Command, sync::Arc};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

#[tokio::test]
async fn registration_rejects_known_permanent_tls_transport_error() {
    let tls = openssl_tls_fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    let server_tls = ServerTlsConfig::new()
        .client_ca_root(Certificate::from_pem(
            fs::read(&tls.ca).expect("read CA certificate"),
        ))
        .identity(Identity::from_pem(
            fs::read(&tls.server_cert).expect("read server certificate"),
            fs::read(&tls.server_key).expect("read server key"),
        ));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server TLS")
            .add_service(afs_protocol::meta::meta_server::MetaServer::new(MetaRpc(
                Arc::new(Meta::new(
                    "meta-test".into(),
                    crate::runtime::Observability::new().unwrap(),
                )),
            )))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("test Meta TLS server");
    });

    let endpoint = format!("https://{addr}");
    let error = register_node_with_readiness(
        &endpoint,
        node_descriptor(&endpoint),
        Duration::from_secs(2),
        TlsConfig::MutualTls {
            encryption_enabled: true,
            ca_certificate: tls.ca.clone(),
            identity_certificate: tls.client_cert.clone(),
            identity_private_key: tls.client_key.clone(),
            server_name: "wrong.local".into(),
        },
    )
    .await
    .expect_err("server_name mismatch must fail during TLS handshake");

    server.abort();
    let _ = server.await;
    assert!(error.message().contains("wrong.local"), "{error}");
    assert_eq!(error.code(), CLIENT_ARGUMENT_INVALID);
    assert_eq!(error.kind(), afs_error::ErrorKind::InvalidArgument);
}

#[tokio::test]
async fn registration_connect_refused_remains_retryable_unavailable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    drop(listener);

    let error = register_node_with_readiness(
        &format!("http://{addr}"),
        NodeDescriptor {
            node_id: "node-a".into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: format!("http://{addr}"),
                data_addr: format!("http://{addr}"),
                rest_addr: "http://127.0.0.1:0".into(),
                nfs_addr: String::new(),
            }),
            labels: Default::default(),
            capabilities: Vec::new(),
            session_id: "session-a".into(),
            storage_devices: Vec::new(),
        },
        Duration::from_millis(50),
        TlsConfig::Disabled,
    )
    .await
    .expect_err("closed listener must fail to connect");

    assert_eq!(error.code(), CLIENT_CONNECTION_UNAVAILABLE);
    assert_eq!(error.kind(), afs_error::ErrorKind::Unavailable);
}

struct TlsFixture {
    _temp: tempfile::TempDir,
    ca: std::path::PathBuf,
    server_cert: std::path::PathBuf,
    server_key: std::path::PathBuf,
    client_cert: std::path::PathBuf,
    client_key: std::path::PathBuf,
}

fn openssl_tls_fixture() -> TlsFixture {
    let temp = tempfile::tempdir().expect("create TLS fixture directory");
    let dir = temp.path();
    run_openssl(
        dir,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-sha256",
            "-subj",
            "/CN=AFS Test CA",
            "-keyout",
            "ca-key.pem",
            "-out",
            "ca.pem",
        ],
    );
    write_ext(
        dir,
        "server.ext",
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    );
    signed_cert(dir, "server", "/CN=localhost", "server.ext");
    write_ext(dir, "client.ext", "extendedKeyUsage=clientAuth\n");
    signed_cert(dir, "client", "/CN=node-a", "client.ext");

    TlsFixture {
        ca: dir.join("ca.pem"),
        server_cert: dir.join("server.pem"),
        server_key: dir.join("server-key.pem"),
        client_cert: dir.join("client.pem"),
        client_key: dir.join("client-key.pem"),
        _temp: temp,
    }
}

fn signed_cert(dir: &Path, name: &str, subject: &str, ext: &str) {
    let key = format!("{name}-key.pem");
    let csr = format!("{name}.csr");
    let cert = format!("{name}.pem");
    run_openssl(
        dir,
        &[
            "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256", "-subj", subject, "-keyout",
            &key, "-out", &csr,
        ],
    );
    run_openssl(
        dir,
        &[
            "x509",
            "-req",
            "-days",
            "1",
            "-sha256",
            "-in",
            &csr,
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca-key.pem",
            "-CAcreateserial",
            "-out",
            &cert,
            "-extfile",
            ext,
        ],
    );
}

fn write_ext(dir: &Path, name: &str, content: &str) {
    fs::write(dir.join(name), content).expect("write certificate extension file");
}

fn run_openssl(dir: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run openssl");
    assert!(
        output.status.success(),
        "openssl {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn node_descriptor(endpoint: &str) -> NodeDescriptor {
    NodeDescriptor {
        node_id: "node-a".into(),
        endpoint: Some(NodeEndpoint {
            grpc_addr: endpoint.into(),
            data_addr: endpoint.into(),
            rest_addr: "http://127.0.0.1:0".into(),
            nfs_addr: String::new(),
        }),
        labels: Default::default(),
        capabilities: Vec::new(),
        session_id: "session-a".into(),
        storage_devices: Vec::new(),
    }
}
