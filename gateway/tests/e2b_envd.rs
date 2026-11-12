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
    real_envd_interop(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires ADX_TEST_ENVD_BIN and ADX_TEST_E2B_PYTHON from the CCE builder"]
async fn gateway_auth_and_signed_file_urls_reach_tokenless_envd() {
    real_envd_interop(true).await;
}

async fn real_envd_interop(gateway_auth: bool) {
    let envd = std::env::var("ADX_TEST_ENVD_BIN").expect("ADX_TEST_ENVD_BIN is required");
    let python = std::env::var("ADX_TEST_E2B_PYTHON").expect("ADX_TEST_E2B_PYTHON is required");
    let mut envd = start_envd(&envd).await;
    initialize_envd(gateway_auth).await;
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
    let mut route = e2b_route(relay_address.to_string(), sandbox_ip);
    let policy = route.e2b.as_mut().unwrap();
    policy.envd_gateway_auth = gateway_auth;
    if gateway_auth {
        policy.envd_signing_key = ENVD_TOKEN.into();
    }
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

    if gateway_auth {
        run_python(&python, &signed_file_script(ingress_address.port())).await;
        if let Ok(legacy_python) = std::env::var("ADX_TEST_E2B_LEGACY_PYTHON") {
            run_python(&legacy_python, &signed_file_script(ingress_address.port())).await;
        }
        // This fixture runs envd directly in the test Pod, rather than creating
        // a sandbox with the production ACL. It proves the accepted in-guest
        // tokenless behavior, not cross-sandbox network isolation.
        assert!(raw_envd("GET", "/files?path=/tmp/e2b-sdk.txt", None, None)
            .await
            .starts_with("HTTP/1.1 200"));
    }

    // Service readiness comes from the lifecycle owner. A no-longer-ready
    // route must reject new SDK operations before they reach the guest.
    let mut unavailable = route;
    unavailable.e2b.as_mut().unwrap().envd_ready = false;
    store.put(unavailable);
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

fn e2b_route(relay_address: String, sandbox_ip: IpAddr) -> RouteInfo {
    RouteInfo {
        private_service_ports: Vec::new(),
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
            envd_gateway_auth: false,
            envd_signing_key: String::new(),
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

async fn initialize_envd(gateway_auth: bool) {
    let mut payload = serde_json::json!({
        "accessToken": ENVD_TOKEN,
        "defaultUser": "root",
        "defaultWorkdir": "/tmp",
        "lifecycleID": "e2b-gateway-component-test",
        "envVars": BTreeMap::<String, String>::new(),
    });
    if gateway_auth {
        payload.as_object_mut().unwrap().remove("accessToken");
    }
    let body = payload.to_string();
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
                .env_remove("E2B_SANDBOX_URL")
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
    expect_unavailable: bool,
    http_version: Option<&str>,
) -> String {
    let availability_assertion = if expect_unavailable {
        "\ntry:\n    sandbox.commands.run('printf must-not-run')\nexcept TimeoutException:\n    pass\nelse:\n    raise AssertionError('unavailable route did not map the gateway 503 to TimeoutException')\n"
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
    config = ConnectionConfig(sandbox_url=base, extra_sandbox_headers={{**headers, 'X-Access-Token': access_token}}, debug=False{http_version})
    return Sandbox(sandbox_id='{SANDBOX_ID}', sandbox_domain='sandbox.test', envd_version=Version('0.9.0'), envd_access_token=access_token, traffic_access_token='{TRAFFIC_TOKEN}', connection_config=config)
sandbox = make_sandbox('{token}')
{availability_assertion}
"#,
    )
}

fn sha256(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn signed_file_script(port: u16) -> String {
    let preamble = python_script(port, ENVD_TOKEN, false, None);
    let (preamble, _) = preamble
        .split_once("\nresult = sandbox.commands.run")
        .unwrap();
    format!(
        r#"{preamble}
from urllib.parse import urlsplit, parse_qsl, urlencode
import socket

# Keep the SDK-generated path/query/signature intact; only map its authority to
# the local TLS fixture, as the existing command/file/PTY test does.
selectors = {{'E2b-Sandbox-Id': '{SANDBOX_ID}', 'E2b-Sandbox-Port': '49983'}}
def local_url(url):
    parsed = urlsplit(url)
    return base + parsed.path + ('?' + parsed.query if parsed.query else '')

download = local_url(sandbox.download_url('/tmp/e2b-sdk.txt', use_signature_expiration=60))
response = httpx.get(download, headers=selectors, verify=cert)
assert response.status_code == 200 and response.text == 'file-ok', response.text
upload = local_url(sandbox.upload_url('/tmp/e2b-signed-upload.txt', use_signature_expiration=60))
response = httpx.post(upload, headers=selectors, verify=cert, files={{'file': ('file.txt', b'signed-upload-ok')}})
assert response.is_success, (response.status_code, response.text)
assert sandbox.files.read('/tmp/e2b-signed-upload.txt') == 'signed-upload-ok'

with httpx.Client(http2=True, verify=cert) as http2:
    for path in ('/{SANDBOX_ID}/49983/files', '/tunnel/{SANDBOX_ID}/49983/files'):
        response = http2.get(base + path + '?' + urlsplit(download).query)
        assert response.status_code == 200 and response.text == 'file-ok', (path, response.status_code)

expired = local_url(sandbox.download_url('/tmp/e2b-sdk.txt', use_signature_expiration=-10))
other = local_url(make_sandbox('another-sandbox-token').download_url('/tmp/e2b-sdk.txt', use_signature_expiration=60))
tampered = download.replace('e2b-sdk.txt', 'other.txt')
for url in (expired, other, tampered, download + '&path=/tmp/other', download + '&signature=bad'):
    response = httpx.get(url, headers=selectors, verify=cert)
    assert response.status_code == 401, (response.status_code, response.text)
response = httpx.get(download, headers={{**selectors, 'X-Access-Token': 'wrong-token'}}, verify=cert)
assert response.status_code == 401, response.text
response = httpx.post(download, headers=selectors, verify=cert, files={{'file': ('x', b'bad')}})
assert response.status_code == 401, response.text
response = httpx.get(base + '/files?path=/tmp/e2b-sdk.txt', headers=selectors, verify=cert)
assert response.status_code == 401, response.text
response = httpx.post(base + '/process.Process/List?' + urlsplit(download).query, headers=selectors, json={{}}, verify=cert)
assert response.status_code == 401, response.text

# Model wildcard DNS in the test transport; keep SDK URLs, Host and TLS SNI intact.
host = '49983-{SANDBOX_ID}.sandbox.test'
resolve = socket.getaddrinfo
def resolve_test_host(name, *args, **kwargs):
    return resolve('127.0.0.1' if name == host else name, *args, **kwargs)
socket.getaddrinfo = resolve_test_host
standard = Sandbox(sandbox_id='{SANDBOX_ID}', sandbox_domain='sandbox.test:{port}',
    envd_version=Version('0.9.0'), envd_access_token='{ENVD_TOKEN}',
    traffic_access_token='{TRAFFIC_TOKEN}', connection_config=ConnectionConfig(debug=False))
for http2 in (False, True):
    with httpx.Client(http2=http2, verify=cert, trust_env=False) as client:
        for endpoint in (base, base + '/unused-prefix'):
            fixed = Sandbox(sandbox_id='{SANDBOX_ID}', sandbox_domain='sandbox.test',
                envd_version=Version('0.9.0'), envd_access_token='{ENVD_TOKEN}',
                connection_config=ConnectionConfig(sandbox_url=endpoint, debug=False))
            fixed_url = fixed.download_url('/tmp/e2b-sdk.txt', use_signature_expiration=60)
            assert urlsplit(fixed_url).path == '/files'
            response = client.get(fixed_url)
            assert response.status_code == 404 and response.text == 'route not found', response.text
        download = standard.download_url('/tmp/e2b-sdk.txt', use_signature_expiration=60)
        assert urlsplit(download).hostname == host
        response = client.get(download)
        assert response.status_code == 200 and response.text == 'file-ok', response.text
        assert response.http_version == ('HTTP/2' if http2 else 'HTTP/1.1')
        upload = standard.upload_url('/tmp/e2b-standalone.txt', use_signature_expiration=60)
        response = client.post(upload, files={{'file': ('standalone.txt', b'standalone-ok')}})
        assert response.status_code == 200, response.text
        response = client.get(standard.download_url('/tmp/e2b-standalone.txt', use_signature_expiration=60))
        assert response.text == 'standalone-ok', response.text
        for invalid in (
            standard.download_url('/tmp/e2b-sdk.txt', use_signature_expiration=-10),
            download.replace('e2b-sdk.txt', 'other.txt'),
            download + '&signature=bad',
        ):
            response = client.get(invalid)
            assert response.status_code == 401, response.text
"#
    )
}
