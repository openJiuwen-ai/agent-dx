//! Real E2B SDK interoperability through the TLS Ingress and Relay.
//!
//! This is intentionally a component test: it publishes a fixture route and
//! runs envd directly, rather than asserting Coordinator or lifecycle behavior.
//! It needs the pinned native envd and Python SDK supplied by the CCE builder.

use data_plane_gateway::common::protocol::GatewayPolicy;
use data_plane_gateway::common::route::{EnvironmentStatus, RouteInfo};
use data_plane_gateway::ingress::{
    DataPlaneL4Connector, H2PoolConfig, Ingress, IngressAuthenticator, IngressRouteResolver,
    RouteStore,
};
use data_plane_gateway::node::Relay;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::net::{IpAddr, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};
use tokio_rustls::TlsAcceptor;

const ENVD_PORT: u16 = 49_983;
const SANDBOX_ID: &str = "e2b-component-sandbox";
const ENVD_TOKEN: &str = "component-envd-token-0123456789abcdef";
const TRAFFIC_TOKEN: &str = "component-traffic-token-0123456789";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires ADX_TEST_ENVD_BIN and ADX_TEST_E2B_PYTHON from the CCE builder"]
async fn python_sdk_reaches_real_envd_only_through_tls_ingress_and_relay() {
    let envd = std::env::var("ADX_TEST_ENVD_BIN").expect("ADX_TEST_ENVD_BIN is required");
    let python = std::env::var("ADX_TEST_E2B_PYTHON").expect("ADX_TEST_E2B_PYTHON is required");
    let mut envd = start_envd(&envd).await;
    initialize_envd().await;
    let sandbox_ip = sandbox_ip();

    let relay_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_address = relay_listener.local_addr().unwrap();
    let relay = Arc::new(
        Relay::new(GatewayPolicy::new(vec!["0.0.0.0/0".parse().unwrap()])).with_route_enforcement(),
    );
    relay
        .activate_route(SANDBOX_ID.into(), SANDBOX_ID.into(), sandbox_ip)
        .await;
    let relay_task = serve_relay(relay_listener, relay.clone());

    let store = Arc::new(RouteStore::new());
    let route = e2b_route(relay_address.to_string(), sandbox_ip, future_ms());
    store.put(route.clone());
    store.set_ready(true);
    let ingress = Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store.clone())),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::disabled(),
            50_090,
            8765,
            "127.0.0.1:1",
            Vec::new(),
        )
        .with_port_host_domain(Some("sandbox.test".into()))
        .with_e2b_enabled(true)
        .with_client_acl(Vec::new(), true),
    );
    let ingress_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingress_address = ingress_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let reconciler = tokio::spawn(ingress.clone().run_route_reconciler(store.subscribe()));
    let ingress_task = tokio::spawn(ingress.clone().serve_http_tls(
        ingress_listener,
        tls_acceptor(),
        shutdown_rx,
    ));

    // The SDK defaults to HTTP/2. Keep that default so this test proves the
    // public compatibility path rather than a client-side downgrade.
    let script = python_script(ingress_address.port(), ENVD_TOKEN, false, None);
    run_python(&python, &script).await;
    run_python(
        &python,
        &python_script(ingress_address.port(), ENVD_TOKEN, false, Some("1.1")),
    )
    .await;

    // The gateway, rather than envd, must reject expired routes before a new
    // SDK operation can reach the guest.
    let mut expired = route;
    expired.e2b.as_mut().unwrap().expires_at_unix_ms = now_ms().saturating_sub(1);
    expired.e2b.as_mut().unwrap().control_revision += 1;
    store.put(expired);
    sleep(Duration::from_millis(25)).await;
    run_python(
        &python,
        &python_script(ingress_address.port(), ENVD_TOKEN, true, None),
    )
    .await;

    let _ = shutdown_tx.send(true);
    ingress_task.await.unwrap().unwrap();
    reconciler.abort();
    relay_task.abort();
    envd.stop();
}

fn e2b_route(relay_address: String, sandbox_ip: IpAddr, expires_at_unix_ms: u64) -> RouteInfo {
    RouteInfo {
        instance_id: SANDBOX_ID.into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "component-test".into(),
        environment_id: SANDBOX_ID.into(),
        relay_address,
        environment_ip: sandbox_ip.to_string(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        port_forward_routes: Vec::new(),
        e2b: Some(data_plane_gateway::ingress::e2b_route::E2bRouteConfig {
            envd_token_sha256: sha256(ENVD_TOKEN),
            traffic_token_sha256: sha256(TRAFFIC_TOKEN),
            allow_public_traffic: false,
            envd_ready: true,
            expires_at_unix_ms,
            control_revision: 1,
        }),
    }
}

fn sandbox_ip() -> IpAddr {
    // UDP connect only selects the pod's default-route source address; it
    // sends no packet. Relay policy intentionally rejects loopback targets.
    let socket = UdpSocket::bind("0.0.0.0:0").expect("bind source-address probe socket");
    socket
        .connect("192.0.2.1:9")
        .expect("select non-loopback sandbox source address");
    let ip = socket
        .local_addr()
        .expect("read source-address probe socket")
        .ip();
    assert!(!ip.is_loopback() && !ip.is_unspecified());
    ip
}

fn tls_acceptor() -> TlsAcceptor {
    adx_transport::install_crypto_provider();
    let certs = rustls_pemfile::certs(&mut &include_bytes!("fixtures/ingress-cert.pem")[..])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut &include_bytes!("fixtures/ingress-key.pem")[..])
        .unwrap()
        .unwrap();
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TlsAcceptor::from(Arc::new(config))
}

fn serve_relay(listener: TcpListener, relay: Arc<Relay>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let relay = relay.clone();
            tokio::spawn(async move {
                let _ = relay.serve_h2(stream).await;
            });
        }
    })
}

struct EnvdProcess {
    child: Child,
}

impl EnvdProcess {
    fn stop(&mut self) {
        if self.child.try_wait().unwrap_or(None).is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Drop for EnvdProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn start_envd(path: &str) -> EnvdProcess {
    let port = ENVD_PORT.to_string();
    let mut process = EnvdProcess {
        child: Command::new(path)
            .args(["--isnotfc", "--no-cgroups", "--port", port.as_str()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start pinned envd"),
    };
    for _ in 0..100 {
        if raw_envd("GET", "/health", None, None)
            .await
            .starts_with("HTTP/1.1 204")
        {
            return process;
        }
        sleep(Duration::from_millis(50)).await;
    }
    process.stop();
    panic!("envd did not become healthy");
}

async fn initialize_envd() {
    let body = serde_json::json!({
        "accessToken": ENVD_TOKEN,
        "defaultUser": "root",
        "defaultWorkdir": "/tmp",
        "lifecycleID": "e2b-gateway-component-test",
        "envVars": BTreeMap::<String, String>::new(),
    })
    .to_string();
    assert!(raw_envd("POST", "/init", Some(&body), None)
        .await
        .starts_with("HTTP/1.1 204"));
}

async fn raw_envd(method: &str, path: &str, body: Option<&str>, token: Option<&str>) -> String {
    let mut stream = match TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, ENVD_PORT)).await {
        Ok(stream) => stream,
        Err(_) => return String::new(),
    };
    let body = body.unwrap_or_default();
    let token = token
        .map(|token| format!("X-Access-Token: {token}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{token}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

async fn run_python(python: &str, script: &str) {
    let certificate =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/ingress-cert.pem");
    let python = python.to_owned();
    let script = script.to_owned();
    let output = timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || {
            Command::new(python)
                .arg("-c")
                .arg(script)
                .env("SSL_CERT_FILE", certificate)
                .env_remove("E2B_DEBUG")
                .output()
        }),
    )
    .await
    .expect("Python SDK test timed out")
    .expect("Python SDK task panicked")
    .expect("run Python SDK");
    assert!(
        output.status.success(),
        "Python SDK failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn python_script(
    port: u16,
    token: &str,
    expect_expired: bool,
    http_version: Option<&str>,
) -> String {
    let expiry_assertion = if expect_expired {
        "\ntry:\n    sandbox.commands.run('printf must-not-run')\nexcept TimeoutException:\n    pass\nelse:\n    raise AssertionError('expired route did not map the gateway 503 to TimeoutException')\n"
    } else {
        "\nresult = sandbox.commands.run('printf command-ok')\nassert result.stdout == 'command-ok', result.stdout\nsandbox.files.write('/tmp/e2b-sdk.txt', 'file-ok')\nassert sandbox.files.read('/tmp/e2b-sdk.txt') == 'file-ok'\nterminal = sandbox.pty.create(PtySize(cols=80, rows=24), cwd='/tmp', timeout=5)\nsandbox.pty.send_stdin(terminal.pid, b'printf pty-ok\\nexit\\n')\npty_output = []\npty_result = terminal.wait(on_pty=lambda data: pty_output.append(data.decode('utf-8')))\nassert pty_result.exit_code == 0, pty_result\nassert 'pty-ok' in ''.join(pty_output), pty_output\nwrong = make_sandbox('wrong-token')\ntry:\n    wrong.commands.run('printf must-not-run')\nexcept AuthenticationException:\n    pass\nelse:\n    raise AssertionError('wrong envd token did not map gateway 401 to AuthenticationException')\nresponse = httpx.post(base + '/init', headers=headers, verify=cert, json={'accessToken': 'forbidden'})\nassert response.status_code == 403, response.text\n"
    };
    let http_version = http_version
        .map(|version| format!(", http_version='{version}'"))
        .unwrap_or_default();
    format!(
        r#"import httpx
from e2b import AuthenticationException, PtySize, Sandbox, TimeoutException
from e2b.connection_config import ConnectionConfig
from packaging.version import Version

base = 'https://localhost:{port}'
cert = __import__('os').environ['SSL_CERT_FILE']
headers = {{'E2b-Sandbox-Id': '{SANDBOX_ID}', 'E2b-Sandbox-Port': '49983', 'X-Access-Token': '{token}'}}
def make_sandbox(access_token):
    config = ConnectionConfig(sandbox_url=base, extra_sandbox_headers={{**headers, 'X-Access-Token': access_token}}, retries=0, debug=False{http_version})
    return Sandbox(sandbox_id='{SANDBOX_ID}', sandbox_domain='sandbox.test', envd_version=Version('0.9.0'), envd_access_token=access_token, traffic_access_token='{TRAFFIC_TOKEN}', connection_config=config)
sandbox = make_sandbox('{token}')
{expiry_assertion}
"#,
    )
}

fn sha256(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn future_ms() -> u64 {
    now_ms().saturating_add(Duration::from_secs(60).as_millis().try_into().unwrap())
}
