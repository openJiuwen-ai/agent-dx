use super::*;
use crate::common::route::{DataPlaneAuthMode, EnvironmentStatus, PortForwardRoute, RouteInfo};
use crate::ingress::auth::{AuthError, AuthenticatedIdentity, CredentialVerifier};
use crate::ingress::{e2b_route::E2bRouteConfig, H2PoolConfig, RouteStore};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn ingress(public: bool) -> Arc<Ingress> {
    ingress_with_policy(Some(public))
}

fn ingress_with_policy(public: Option<bool>) -> Arc<Ingress> {
    configured_ingress(public, false)
}

#[derive(Debug)]
struct NativeKeys;

#[async_trait::async_trait]
impl CredentialVerifier for NativeKeys {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError> {
        let tenant_id = match key {
            "native-key" => "tenant-a",
            "other-key" => "tenant-b",
            _ => return Err(AuthError::Invalid("unknown test key".into())),
        };
        Ok(AuthenticatedIdentity {
            tenant_id: tenant_id.into(),
            expires_at_unix: None,
        })
    }
}

fn configured_ingress(public: Option<bool>, native_private: bool) -> Arc<Ingress> {
    configured_named_ingress(public, native_private, "sandbox-a", true, None)
}

fn configured_named_ingress(
    public: Option<bool>,
    native_private: bool,
    instance_id: &str,
    e2b_enabled: bool,
    additional_route: Option<RouteInfo>,
) -> Arc<Ingress> {
    let store = Arc::new(RouteStore::new());
    store.put(published_route(public, native_private, instance_id));
    if let Some(route) = additional_route {
        store.put(route);
    }
    store.set_ready(true);
    Arc::new(
        Ingress::new(
            Arc::new(IngressRouteResolver::new(store)),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::with_verifier(Arc::new(NativeKeys)),
            50090,
            8765,
            "127.0.0.1:1",
            Vec::new(),
        )
        .with_port_host_domain(Some("sandbox.test".into()))
        .with_e2b_enabled(e2b_enabled),
    )
}

fn published_route(public: Option<bool>, native_private: bool, instance_id: &str) -> RouteInfo {
    RouteInfo {
        instance_id: instance_id.into(),
        environment_status: EnvironmentStatus {
            code: 3,
            ..Default::default()
        },
        tenant_id: "tenant-a".into(),
        environment_id: "runtime-a".into(),
        environment_ip: "192.0.2.1".into(),
        // Reaching this unavailable Relay returns 502. Authorization failures
        // must instead be returned before any Relay connection is attempted.
        relay_address: "127.0.0.1:0".into(),
        tunnel_security_mode: Default::default(),
        port_forward_security_mode: Default::default(),
        private_service_ports: if public.is_some() {
            vec![49_983]
        } else {
            Vec::new()
        },
        port_forward_routes: if native_private {
            vec![PortForwardRoute {
                target_port: 8080,
                auth_mode: DataPlaneAuthMode::Token,
            }]
        } else {
            Vec::new()
        },
        e2b: public.map(|public| E2bRouteConfig {
            envd_token_sha256: format!("{:x}", Sha256::digest(b"envd")),
            traffic_token_sha256: format!("{:x}", Sha256::digest(b"traffic")),
            allow_public_traffic: public,
            envd_ready: true,
            envd_gateway_auth: false,
            envd_signing_key: String::new(),
        }),
    }
}

async fn request(ingress: Arc<Ingress>, target: &str, host: &str, headers: &str) -> u16 {
    request_method(ingress, "GET", target, host, headers).await
}

async fn request_method(
    ingress: Arc<Ingress>,
    method: &str,
    target: &str,
    host: &str,
    headers: &str,
) -> u16 {
    request_with_options(ingress, method, target, host, headers, None).await
}

async fn request_with_options(
    ingress: Arc<Ingress>,
    method: &str,
    target: &str,
    host: &str,
    headers: &str,
    options: Option<super::super::ListenerConfig>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.unwrap();
        let security = if options.is_some() {
            IngressSecurity::AuthenticatedHttp
        } else {
            IngressSecurity::Tls
        };
        ingress
            .serve_http_connection_with_options(socket, peer, security, options.map(Arc::new))
            .await;
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!(
                "{method} {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n{headers}\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    String::from_utf8(response)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn configured_http_listener_keeps_authentication_and_denies_unlisted_routes() {
    let ingress = configured_named_ingress(None, true, "sandbox-a", false, None);
    let options = super::super::ListenerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: false,
        allow_http_credentials: true,
        allowed_routes: Some(vec![super::super::ListenerRoute {
            method: Some("GET".into()),
            path: "prefix:/sandbox-a".into(),
        }]),
    };
    for (target, headers, expected) in [
        (
            "/api/admin/v1/keys",
            "Authorization: Bearer native-key\r\n",
            403,
        ),
        ("/sandbox-a/8080/hello", "", 401),
        (
            "/sandbox-a/8080/hello",
            "Authorization: Bearer other-key\r\n",
            403,
        ),
        (
            "/sandbox-a/8080/hello",
            "Authorization: Bearer native-key\r\n",
            502,
        ),
    ] {
        assert_eq!(
            request_with_options(
                ingress.clone(),
                "GET",
                target,
                "localhost",
                headers,
                Some(options.clone())
            )
            .await,
            expected
        );
    }
    assert_eq!(
        request_with_options(
            ingress,
            "POST",
            "/sandbox-a/8080/hello",
            "localhost",
            "Authorization: Bearer native-key\r\n",
            Some(options)
        )
        .await,
        403
    );
}

#[tokio::test]
async fn private_dynamic_port_has_identical_policy_across_http_addresses() {
    let ingress = ingress(false);
    for (path, host) in [
        ("/hello", "8080-sandbox-a.sandbox.test"),
        ("/hello", "sandbox-a-8080.sandbox.test"),
        ("/sandbox-a/8080/hello", "localhost"),
        ("/tunnel/sandbox-a/8080/hello", "localhost"),
    ] {
        for headers in [
            "",
            "E2b-Traffic-Access-Token: wrong\r\n",
            "X-Access-Token: envd\r\n",
        ] {
            assert_eq!(
                request(ingress.clone(), path, host, headers).await,
                401,
                "{path} {host}"
            );
        }
        assert_eq!(
            request(
                ingress.clone(),
                path,
                host,
                "E2b-Traffic-Access-Token: traffic\r\n"
            )
            .await,
            502,
            "authorized request must reach the unavailable Relay: {path} {host}"
        );
    }
}

#[tokio::test]
async fn native_envd_paths_and_control_ports_cannot_bypass_policy() {
    let ingress = ingress(true);
    for (path, host) in [
        ("/%69nit", "sandbox-a-49983.sandbox.test"),
        ("/sandbox-a/49983/freeze", "localhost"),
        ("/tunnel/sandbox-a/49983/unfreeze", "localhost"),
        ("/sandbox-a/50090/health", "localhost"),
        ("/tunnel/sandbox-a/8765/", "localhost"),
    ] {
        assert_eq!(
            request(ingress.clone(), path, host, "X-Access-Token: envd\r\n").await,
            403,
            "{path} {host}"
        );
    }
    assert_eq!(
        request(ingress.clone(), "/sandbox-a/49983/files", "localhost", "").await,
        401
    );
    assert_eq!(
        request(
            ingress.clone(),
            "/sandbox-a/49983/files",
            "localhost",
            "X-Access-Token: envd\r\n"
        )
        .await,
        502
    );
    assert_eq!(
        request(ingress, "/sandbox-a/8080/", "localhost", "").await,
        502
    );
}

#[tokio::test]
async fn native_routes_without_service_policy_keep_anonymous_port_access() {
    let ingress = ingress_with_policy(None);
    for (path, host) in [
        ("/hello", "sandbox-a-8080.sandbox.test"),
        ("/sandbox-a/8080/hello", "localhost"),
    ] {
        assert_eq!(
            request(ingress.clone(), path, host, "").await,
            502,
            "a native anonymous port must still reach its Relay: {path} {host}"
        );
    }
}

#[tokio::test]
async fn opaque_l4_cannot_bypass_service_policy_even_with_platform_auth_disabled() {
    let mut ingress = (*ingress(false)).clone();
    ingress.authenticator = IngressAuthenticator::disabled();
    let ingress = Arc::new(ingress);
    for port in [8080, 49983, 50090, 8765] {
        assert!(matches!(
            ingress
                .open_l4_stream(
                    "sandbox-a",
                    port,
                    AccessKind::PortForwarding,
                    "",
                    "test".into()
                )
                .await,
            Err(IngressOpenError::Forbidden)
        ));
    }
    assert!(
        matches!(
            ingress
                .open_l4_stream("sandbox-a", 50090, AccessKind::Direct, "", "test".into())
                .await,
            Err(IngressOpenError::Forbidden)
        ),
        "protected Execd must use the native HTTP execution entry"
    );
}

#[test]
fn service_credentials_are_consumed_independently_of_address_format() {
    for port in [8080, 49983] {
        let mut request = Request::builder()
            .header("e2b-traffic-access-token", "traffic")
            .header("x-access-token", "envd")
            .header(H_TARGET_IP, "untrusted")
            .body(())
            .unwrap();
        strip_service_credentials(&mut request, port);
        assert!(!request.headers().contains_key("e2b-traffic-access-token"));
        assert!(!request.headers().contains_key(H_TARGET_IP));
        assert_eq!(
            request.headers().contains_key("x-access-token"),
            port == 49983
        );
    }
}

#[test]
fn gateway_envd_credentials_are_not_forwarded_to_the_guest() {
    let mut request = Request::builder()
        .uri("/files?path=%2Ftmp%2Ffile&username=root&signature=bearer&signature_expiration=100")
        .header("x-access-token", "sdk-secret")
        .body(())
        .unwrap();
    strip_gateway_envd_credentials(&mut request).unwrap();
    assert!(!request.headers().contains_key("x-access-token"));
    assert_eq!(request.uri(), "/files?path=%2Ftmp%2Ffile&username=root");
}

#[test]
fn envd_credential_cleanup_preserves_http2_authority() {
    let mut request = Request::builder()
        .version(http::Version::HTTP_2)
        .uri("https://49983-sandbox-a.sandbox.test/files?path=%2Ftmp%2Ffile&signature=bearer")
        .body(())
        .unwrap();
    strip_gateway_envd_credentials(&mut request).unwrap();
    assert_eq!(request.uri().scheme_str(), Some("https"));
    assert_eq!(
        request.uri().authority().unwrap().as_str(),
        "49983-sandbox-a.sandbox.test"
    );
    assert_eq!(
        request.uri().path_and_query().unwrap().as_str(),
        "/files?path=%2Ftmp%2Ffile"
    );
}

#[tokio::test]
async fn native_private_ports_keep_native_key_and_tenant_authorization() {
    let ingress = configured_ingress(None, true);
    for (path, host) in [
        ("/hello", "sandbox-a-8080.sandbox.test"),
        ("/sandbox-a/8080/hello", "localhost"),
    ] {
        assert_eq!(request(ingress.clone(), path, host, "").await, 401);
        assert_eq!(
            request(
                ingress.clone(),
                path,
                host,
                "E2b-Traffic-Access-Token: traffic\r\n"
            )
            .await,
            401
        );
        assert_eq!(
            request(
                ingress.clone(),
                path,
                host,
                "Authorization: Bearer other-key\r\n"
            )
            .await,
            403
        );
        assert_eq!(
            request(
                ingress.clone(),
                path,
                host,
                "Authorization: Bearer native-key\r\n"
            )
            .await,
            502,
            "native private access must accept its original key without a traffic token"
        );
    }
}

#[tokio::test]
async fn connect_cannot_carry_unchecked_requests_to_protected_services() {
    for public in [false, true] {
        let ingress = ingress(public);
        for target in ["sandbox-a:8080", "sandbox-a:49983", "sandbox-a:50090"] {
            assert_eq!(
                request_method(
                    ingress.clone(),
                    "CONNECT",
                    target,
                    target,
                    "E2b-Traffic-Access-Token: traffic\r\nX-Access-Token: envd\r\nAuthorization: Bearer native-key\r\n"
                )
                .await,
                403
            );
        }
    }
    let native = configured_ingress(None, true);
    assert_eq!(
        request_method(
            native.clone(),
            "CONNECT",
            "sandbox-a:8080",
            "sandbox-a:8080",
            ""
        )
        .await,
        401
    );
    assert_eq!(
        request_method(
            native,
            "CONNECT",
            "sandbox-a:8080",
            "sandbox-a:8080",
            "Authorization: Bearer native-key\r\n"
        )
        .await,
        502
    );
}

#[tokio::test]
async fn native_execd_entry_keeps_platform_key_authorization_on_e2b_sandbox() {
    let ingress = ingress(false);
    let path = "/direct/sandbox-a/health";
    assert_eq!(
        request(
            ingress.clone(),
            path,
            "localhost",
            "E2b-Traffic-Access-Token: traffic\r\n"
        )
        .await,
        401
    );
    assert_eq!(
        request(
            ingress.clone(),
            path,
            "localhost",
            "Authorization: Bearer other-key\r\n"
        )
        .await,
        403
    );
    assert_eq!(
        request(
            ingress,
            path,
            "localhost",
            "Authorization: Bearer native-key\r\n"
        )
        .await,
        502
    );
}

#[tokio::test]
async fn protected_native_execution_keeps_authorization_without_a_public_lease() {
    let ingress = ingress(false);
    let route = ingress
        .resolver
        .resolve("sandbox-a", 50090, AccessKind::Direct, "test")
        .await
        .unwrap();
    assert!(ingress.authorize(&route, "tenant-a").is_ok());
    assert!(matches!(
        ingress.authorize(&route, "tenant-b"),
        Err(IngressOpenError::Forbidden)
    ));
    assert!(matches!(
        ingress
            .open_l4_stream(
                "sandbox-a",
                50090,
                AccessKind::Direct,
                "native-key",
                "test".into()
            )
            .await,
        Err(IngressOpenError::Forbidden)
    ));
}

#[tokio::test]
async fn numeric_native_host_keeps_native_authentication_with_e2b_enabled_or_disabled() {
    for e2b_enabled in [false, true] {
        for id in ["123-app", "70000-app"] {
            let ingress = configured_named_ingress(None, true, id, e2b_enabled, None);
            let host = format!("{id}-8080.sandbox.test");
            assert_eq!(request(ingress.clone(), "/hello", &host, "").await, 401);
            assert_eq!(
                request(
                    ingress.clone(),
                    "/hello",
                    &host,
                    "Authorization: Bearer other-key\r\n"
                )
                .await,
                403
            );
            assert_eq!(
                request(
                    ingress,
                    "/hello",
                    &host,
                    "Authorization: Bearer native-key\r\n"
                )
                .await,
                502
            );
        }
    }
}

#[tokio::test]
async fn overlapping_host_defaults_to_native_target_and_keeps_its_service_policy() {
    let competing = published_route(Some(true), false, "app-8080");
    let ingress = configured_named_ingress(Some(false), false, "123-app", true, Some(competing));
    let host = "123-app-8080.sandbox.test";
    assert_eq!(request(ingress.clone(), "/hello", host, "").await, 401);
    assert_eq!(
        request(
            ingress.clone(),
            "/hello",
            host,
            "E2b-Traffic-Access-Token: traffic\r\n"
        )
        .await,
        502
    );
    assert_eq!(
        request(
            ingress.clone(),
            "/hello",
            host,
            "E2b-Sandbox-Id: app-8080\r\nE2b-Sandbox-Port: 123\r\n"
        )
        .await,
        502,
        "consistent explicit selectors choose the public E2B target"
    );
    for headers in [
        "E2b-Sandbox-Id: other\r\nE2b-Sandbox-Port: 123\r\n",
        "E2b-Sandbox-Id: app-8080\r\n",
        "E2b-Sandbox-Port: 123\r\n",
    ] {
        assert_eq!(request(ingress.clone(), "/hello", host, headers).await, 400);
    }
}

#[tokio::test]
async fn overlapping_host_requires_consistent_headers_to_select_e2b_target() {
    let ingress = configured_named_ingress(Some(false), false, "app-8080", true, None);
    let host = "123-app-8080.sandbox.test";
    assert_eq!(request(ingress.clone(), "/hello", host, "").await, 404);
    let selector = "E2b-Sandbox-Id: app-8080\r\nE2b-Sandbox-Port: 123\r\n";
    assert_eq!(
        request(ingress.clone(), "/hello", host, selector).await,
        401
    );
    assert_eq!(
        request(
            ingress,
            "/hello",
            host,
            &format!("{selector}E2b-Traffic-Access-Token: traffic\r\n")
        )
        .await,
        502
    );
}

#[tokio::test]
async fn disabled_e2b_does_not_select_header_targets() {
    let ingress = configured_named_ingress(Some(true), false, "sandbox-a", false, None);
    assert_eq!(
        request(
            ingress,
            "/hello",
            "localhost",
            "E2b-Sandbox-Id: sandbox-a\r\nE2b-Sandbox-Port: 8080\r\n"
        )
        .await,
        404
    );
    let native = configured_named_ingress(None, true, "123-app", false, None);
    assert_eq!(
        request(
            native,
            "/hello",
            "123-app-8080.sandbox.test",
            "Authorization: Bearer native-key\r\nE2b-Sandbox-Port: invalid\r\n"
        )
        .await,
        502,
        "disabled E2B selectors must not intercept authenticated native traffic"
    );
}

#[tokio::test]
async fn native_clone_private_service_cannot_be_forwarded_without_protocol_policy() {
    let mut route = published_route(None, false, "native-clone");
    route.private_service_ports = vec![49_983];
    let ingress = configured_named_ingress(None, false, "ordinary", true, Some(route));
    for (path, host) in [
        ("/files", "native-clone-49983.sandbox.test"),
        ("/native-clone/49983/files", "localhost"),
        ("/tunnel/native-clone/49983/files", "localhost"),
    ] {
        assert_eq!(request(ingress.clone(), path, host, "").await, 403);
        assert_eq!(
            request(
                ingress.clone(),
                path,
                host,
                "Authorization: Bearer native-key\r\n"
            )
            .await,
            403
        );
    }
    assert_eq!(
        request_method(
            ingress.clone(),
            "CONNECT",
            "native-clone:49983",
            "native-clone:49983",
            "Authorization: Bearer native-key\r\n"
        )
        .await,
        403
    );
    assert!(matches!(
        ingress
            .open_l4_stream(
                "native-clone",
                49_983,
                AccessKind::PortForwarding,
                "native-key",
                String::new()
            )
            .await,
        Err(IngressOpenError::Forbidden)
    ));
    let opaque = ingress
        .resolve_route("native-clone", 49_983, AccessKind::Ssh, String::new())
        .await
        .unwrap();
    assert_eq!(
        ingress
            .open_authorized_stream(opaque)
            .await
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        request(ingress, "/native-clone/8080/hello", "localhost", "").await,
        502
    );
}

#[tokio::test]
async fn route_refresh_keeps_streams_but_pause_delete_and_identity_changes_cancel_them() {
    for public in [None, Some(false)] {
        for change in ["refresh", "pause", "delete", "identity", "credentials"] {
            let ingress = ingress_with_policy(public);
            let route = ingress
                .resolver
                .resolve("sandbox-a", 8080, AccessKind::PortForwarding, "test")
                .await
                .unwrap();
            let revision = ingress.resolver.current_route_revision(&route).unwrap();
            let session_id = Uuid::new_v4();
            let (cancel, cancelled) = watch::channel(false);
            ingress.sessions.lock().unwrap().insert(
                session_id,
                ActiveSession {
                    revision,
                    instance_id: route.target.instance_id.clone(),
                    workload_id: route.target.workload_id.clone(),
                    relay_address: route.relay_address.clone(),
                    target_ip: route.target.target_ip,
                    route,
                    cancel,
                },
            );
            let mut current = published_route(public, false, "sandbox-a");
            match change {
                "pause" => current.environment_status.code = 8,
                "identity" => current.environment_id = "replacement-runtime".into(),
                "credentials" => {
                    if let Some(policy) = &mut current.e2b {
                        policy.envd_token_sha256 = "c".repeat(64);
                    } else {
                        current.private_service_ports = vec![8080];
                    }
                }
                _ => {}
            }
            let update = super::super::route_store::RouteChange {
                revision: revision + 1,
                instance_id: "sandbox-a".into(),
                previous: None,
                current: (change != "delete").then_some(current),
            };
            let mut delayed = update.clone();
            delayed.revision = revision;
            delayed.current = None;
            ingress.apply_route_change(delayed);
            assert!(!*cancelled.borrow());
            ingress.apply_route_change(update);
            assert_eq!(*cancelled.borrow(), change != "refresh", "{change}");
        }
    }
}

#[tokio::test]
async fn authenticated_http_does_not_implicitly_enable_e2b_http() {
    let options = super::super::ListenerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: false,
        allow_http_credentials: true,
        allowed_routes: None,
    };
    assert_eq!(
        request_with_options(
            ingress(false),
            "GET",
            "/sandboxes",
            "localhost",
            "Authorization: Bearer native-key\r\n",
            Some(options)
        )
        .await,
        426
    );
}
