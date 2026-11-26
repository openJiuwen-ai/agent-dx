#[test]
fn deployment_ca_load_rejects_missing_empty_and_invalid_pem() {
    assert!(crate::tls::Trust::load(Some("/definitely-missing-console-ca.pem")).is_err());
    let path = std::env::temp_dir().join(format!("console-ca-{}",uuid::Uuid::new_v4()));
    for bytes in [b"".as_slice(),b"not a certificate",b"-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----"] {
        std::fs::write(&path,bytes).unwrap();
        assert!(crate::tls::Trust::load(path.to_str()).is_err());
    }
    std::fs::remove_file(path).unwrap();
    assert!(crate::tls::Trust::load(None).is_ok());
}

async fn server(websocket: bool) -> (u16,tokio::task::JoinHandle<()>) {
    let cert = include_bytes!("../../../gateway/tests/fixtures/ingress-cert.pem");
    let key = include_bytes!("../../../gateway/tests/fixtures/ingress-key.pem");
    let certs = rustls_pemfile::certs(&mut cert.as_slice()).collect::<Result<Vec<_>,_>>().unwrap();
    let key = rustls_pemfile::private_key(&mut key.as_slice()).unwrap().unwrap();
    let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(certs,key).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let worker = tokio::spawn(async move {
        let (socket,_) = listener.accept().await.unwrap();
        if let Ok(mut tls) = acceptor.accept(socket).await {
            if websocket { let _ = tokio_tungstenite::accept_async(tls).await; }
            else {
                use tokio::io::{AsyncReadExt,AsyncWriteExt};
                let mut bytes = [0;4096];
                let _ = tls.read(&mut bytes).await;
                let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await;
            }
        }
    });
    (port,worker)
}

#[tokio::test]
async fn deployment_ca_http_and_ws_verify_trust_and_hostname() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../gateway/tests/fixtures/ingress-cert.pem");
    for websocket in [false,true] {
        for (trusted,host,expected) in [(true,"localhost",true),(false,"localhost",false),(true,"wrong-host.test",false)] {
            let trust = crate::tls::Trust::load(if trusted { path.to_str() } else { None }).unwrap();
            let (port,worker) = server(websocket).await;
            let result = if websocket {
                let socket = tokio::net::TcpStream::connect(("127.0.0.1",port)).await.unwrap();
                tokio_tungstenite::client_async_tls_with_config(format!("wss://{host}:{port}"),socket,None,Some(tokio_tungstenite::Connector::Rustls(trust.websocket))).await.is_ok()
            } else {
                trust.http_builder().unwrap().no_proxy().resolve(host,([127,0,0,1],port).into()).build().unwrap().get(format!("https://{host}:{port}")).send().await.is_ok()
            };
            assert_eq!(result,expected,"websocket={websocket} trusted={trusted} host={host}");
            worker.abort();
        }
    }
}
