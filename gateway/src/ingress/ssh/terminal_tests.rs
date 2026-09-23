//! Real SSH and Relay sockets with a fake Platform lifecycle backend.
use super::*;
use crate::{
    common::protocol::GatewayPolicy,
    ingress::ssh::{KeyGrant, SshConfig, SshListener},
    node::Relay,
};
use adx_agent_core::{
    target::{SshRoute, Target},
    Scope,
};
use russh::server::Server as _;
use russh::{
    client,
    keys::{Algorithm, PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate},
    server, ChannelMsg,
};
use std::time::Duration;

fn key() -> PrivateKey {
    PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()
}
#[derive(Clone)]
struct Echo {
    credential: PublicKey,
}
impl server::Server for Echo {
    type Handler = Self;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self {
        self.clone()
    }
}
impl server::Handler for Echo {
    type Error = russh::Error;
    async fn auth_publickey(
        &mut self,
        user: &str,
        key: &PublicKey,
    ) -> Result<server::Auth, Self::Error> {
        Ok(
            if user == "harness" && key.key_data() == self.credential.key_data() {
                server::Auth::Accept
            } else {
                server::Auth::reject()
            },
        )
    }
    async fn channel_open_session(
        &mut self,
        mut channel: russh::Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        session: &mut server::Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await;
        let handle = session.handle();
        tokio::spawn(async move {
            while let Some(message) = channel.wait().await {
                match message {
                    ChannelMsg::RequestPty {
                        term,
                        col_width,
                        row_height,
                        ..
                    } => {
                        assert_eq!(term, "xterm");
                        assert!(matches!((col_width, row_height), (80, 24) | (0, 0)));
                        handle.channel_success(channel.id()).await.unwrap();
                    }
                    ChannelMsg::RequestShell { .. } => {
                        handle.channel_success(channel.id()).await.unwrap();
                        channel.data(&b"backend-ready\r\n"[..]).await.unwrap();
                    }
                    ChannelMsg::WindowChange {
                        col_width,
                        row_height,
                        ..
                    } => {
                        channel
                            .data(format!("resize:{col_width}:{row_height}\r\n").as_bytes())
                            .await
                            .unwrap();
                    }
                    ChannelMsg::Data { data } if data == b"exit\r"[..] => {
                        channel.exit_status(17).await.unwrap();
                        channel.eof().await.unwrap();
                        channel.close().await.unwrap();
                        break;
                    }
                    ChannelMsg::Data { data } => {
                        channel.data(&data[..]).await.unwrap();
                    }
                    ChannelMsg::Close | ChannelMsg::Eof => break,
                    _ => {}
                }
            }
        });
        Ok(())
    }
}
struct Verify(PublicKey);
impl client::Handler for Verify {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(
            matches!(key, PublicKeyOrCertificate::PublicKey { key, .. } if key.key_data() == self.0.key_data()),
        )
    }
}
struct RoutingBackend {
    inner: Backend,
    routes: Arc<RouteStore>,
    node: Arc<Relay>,
    node_address: std::net::SocketAddr,
}
impl RoutingBackend {
    async fn install(&self, tenant: &str, id: &str) {
        let route: crate::common::route::RouteInfo = serde_json::from_value(serde_json::json!({"instanceID":id,"instanceStatus":{"code":3},"tenantID":tenant,"sandboxID":format!("runtime-{id}"),"sandboxIP":"127.0.0.1","nodeProxyAddress":self.node_address.to_string()})).unwrap();
        self.node
            .activate_route(
                route.instance_id.clone(),
                route.sandbox_id.clone(),
                route.sandbox_ip.parse().unwrap(),
            )
            .await;
        self.routes.put(route);
    }
}
#[async_trait::async_trait]
impl Sandbox for RoutingBackend {
    async fn create(&self, req: &CreateSandbox) -> Result<SandboxObservation, SandboxError> {
        let result = self.inner.create(req).await?;
        self.install(&req.tenant, &req.id).await;
        Ok(result)
    }
    async fn get(
        &self,
        tenant: &str,
        id: &str,
    ) -> Result<Option<SandboxObservation>, SandboxError> {
        self.inner.get(tenant, id).await
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation, SandboxError> {
        self.inner.delete(tenant, id).await
    }
}

async fn read_until(channel: &mut russh::Channel<client::Msg>, text: &str) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut result = String::new();
        loop {
            match channel.wait().await.unwrap() {
                ChannelMsg::Data { data } => result.push_str(std::str::from_utf8(&data).unwrap()),
                ChannelMsg::ExtendedData { data, .. } => {
                    panic!("gateway error: {}", String::from_utf8_lossy(&data))
                }
                ChannelMsg::Close => panic!("closed before {text}: {result}"),
                _ => {}
            }
            if result.contains(text) {
                return result;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn ssh_terminal_activates_one_environment_and_proxies_input_resize_and_exit() {
    terminal_case(false).await;
}

#[tokio::test]
#[ignore = "requires system OpenSSH client"]
async fn native_openssh_receives_environment_on_terminal_stdout() {
    terminal_case(true).await;
}

async fn terminal_case(native: bool) {
    let host = key();
    let backend_host = key();
    let backend_key = key();
    let user = key();
    let inline_user = key();
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = backend_listener.local_addr().unwrap().port();
    let mut echo = Echo {
        credential: backend_key.public_key().clone(),
    };
    let backend_server = echo.run_on_socket(
        Arc::new(server::Config {
            keys: vec![backend_host.clone()],
            ..Default::default()
        }),
        &backend_listener,
    );
    let backend_shutdown = backend_server.handle();
    let node = Arc::new(
        Relay::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = node_listener.local_addr().unwrap();
    let node_server = {
        let node = node.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = node_listener.accept().await.unwrap();
                let node = node.clone();
                tokio::spawn(async move {
                    let _ = node.serve_h2(stream).await;
                });
            }
        })
    };
    let (gateway, _, routes) = fixture_with_routes().await;
    let sandbox = Arc::new(RoutingBackend {
        inner: Backend::default(),
        routes,
        node,
        node_address,
    });
    sandbox.install("tenant", "inline-instance").await;
    sandbox.install("other", "other-instance").await;
    let state = AgentState::new(Arc::new(MemoryRepository::default()));
    let template = serde_json::from_value(serde_json::json!({"name":"demo","version":"1","image":"app:1","isolation_runtime":"runc","entrypoint":["/start"],"resources":{"cpu_millis":1000,"memory_mib":512},"service":[{"protocol":"ssh","port":port}]})).unwrap();
    state.publish("tenant", &template).await.unwrap();
    let api = Arc::new(AgentApi {
        managed: Arc::new(adx_agent_api::managed::ManagedService::new(Arc::new(
            local_control(adx_activator::Activator::new(state.clone(), sandbox)),
        ))),
        request_timeout: adx_agent_core::limits::AGENT_REQUEST_TIMEOUT,
    });
    let gateway = Arc::new(
        Arc::try_unwrap(gateway)
            .ok()
            .unwrap()
            .with_agent_api(api)
            .with_client_acl(vec!["127.0.0.0/8".parse().unwrap()], false),
    );
    let directory = tempfile::tempdir().unwrap();
    let host_path = directory.path().join("host");
    let backend_path = directory.path().join("backend");
    std::fs::write(
        &host_path,
        host.to_openssh(russh::keys::ssh_key::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    std::fs::write(
        &backend_path,
        backend_key
            .to_openssh(russh::keys::ssh_key::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let listener = SshListener::bind(
        SshConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            host_key: host_path,
            backend_key: backend_path,
            backend_user: "harness".into(),
            backend_host_keys: vec![backend_host.public_key().to_openssh().unwrap()],
            inline_authorized_keys: vec![KeyGrant {
                public_key: inline_user.public_key().to_openssh().unwrap(),
                tenant_id: "tenant".into(),
            }],
            agent_authorized_keys: vec![KeyGrant {
                public_key: user.public_key().to_openssh().unwrap(),
                tenant_id: "tenant".into(),
            }],
            connect_timeout_seconds: 5,
            auth_timeout_seconds: 5,
            max_connections: 8,
        },
        gateway.clone(),
    )
    .await
    .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let ingress = tokio::spawn(listener.serve(stopped));
    let checks = async {
        let target = SshRoute {
            target: Target::Template {
                name: "demo".into(),
                version: "1".into(),
            },
            port: None,
            trace: None,
        };
        let mut client = client::connect(
            Arc::new(client::Config::default()),
            address,
            Verify(host.public_key().clone()),
        )
        .await
        .unwrap();
        assert!(!client
            .authenticate_publickey(
                target.to_string(),
                PrivateKeyWithHashAlg::new(Arc::new(inline_user.clone()), None)
            )
            .await
            .unwrap()
            .success());
        assert!(client
            .authenticate_publickey(
                target.to_string(),
                PrivateKeyWithHashAlg::new(Arc::new(user.clone()), None)
            )
            .await
            .unwrap()
            .success());
        let query = adx_agent_core::activator::EnvironmentList {
            tenant: "tenant".into(),
            template: "demo".into(),
            version: "1".into(),
            page_size: 10,
            page_token: None,
        };
        assert!(state
            .list_environments(&query)
            .await
            .unwrap()
            .environments
            .is_empty());
        let mut channel = client.channel_open_session().await.unwrap();
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .unwrap();
        assert!(matches!(channel.wait().await, Some(ChannelMsg::Success)));
        channel.request_shell(true).await.unwrap();
        let output = read_until(&mut channel, "backend-ready").await;
        let id = output
            .lines()
            .find_map(|v| v.strip_prefix("Environment ID: "))
            .unwrap();
        let urn = output
            .lines()
            .find_map(|v| v.strip_prefix("Environment URN: "))
            .unwrap();
        assert_eq!(
            urn.parse::<Target>().unwrap(),
            Target::Environment {
                name: "demo".into(),
                version: "1".into(),
                id: id.into()
            }
        );
        let environment = state
            .environment(&Scope {
                tenant: "tenant".into(),
                template: "demo".into(),
                version: "1".into(),
                environment_id: id.into(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(environment.scope.environment_id, id);
        assert!(output.find("Environment ID:").unwrap() < output.find("backend-ready").unwrap());
        channel.data(&b"hello\r"[..]).await.unwrap();
        assert_eq!(read_until(&mut channel, "hello\r").await, "hello\r");
        channel.window_change(132, 43, 0, 0).await.unwrap();
        assert_eq!(
            read_until(&mut channel, "resize:132:43").await,
            "resize:132:43\r\n"
        );
        channel.data(&b"exit\r"[..]).await.unwrap();
        let mut status = None;
        while let Some(message) = channel.wait().await {
            match message {
                ChannelMsg::ExitStatus { exit_status } => status = Some(exit_status),
                ChannelMsg::Close => break,
                _ => {}
            }
        }
        assert_eq!(status, Some(17));
        client
            .disconnect(russh::Disconnect::ByApplication, "done", "")
            .await
            .unwrap();
        let mut reconnect = client::connect(
            Arc::new(client::Config::default()),
            address,
            Verify(host.public_key().clone()),
        )
        .await
        .unwrap();
        let route = SshRoute {
            target: urn.parse().unwrap(),
            port: None,
            trace: None,
        };
        assert!(reconnect
            .authenticate_publickey(
                route.to_string(),
                PrivateKeyWithHashAlg::new(Arc::new(user.clone()), None)
            )
            .await
            .unwrap()
            .success());
        let mut channel = reconnect.channel_open_session().await.unwrap();
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .unwrap();
        channel.request_shell(true).await.unwrap();
        let next = read_until(&mut channel, "backend-ready").await;
        assert!(next.contains(&format!("Environment ID: {id}\r\n")));
        assert_eq!(
            state
                .list_environments(&query)
                .await
                .unwrap()
                .environments
                .len(),
            1
        );
        reconnect
            .disconnect(russh::Disconnect::ByApplication, "done", "")
            .await
            .unwrap();
        let mut client = client::connect(
            Arc::new(client::Config::default()),
            address,
            Verify(host.public_key().clone()),
        )
        .await
        .unwrap();
        assert!(client
            .authenticate_publickey(
                format!("yr:instance:inline-instance:port={port}"),
                PrivateKeyWithHashAlg::new(Arc::new(inline_user.clone()), None)
            )
            .await
            .unwrap()
            .success());
        let mut channel = client.channel_open_session().await.unwrap();
        // OpenSSH may pipeline environment, PTY and shell requests. Each requested reply must keep its order.
        channel.set_env(true, "LANG", "C.UTF-8").await.unwrap();
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .unwrap();
        channel.request_shell(true).await.unwrap();
        let mut replies = Vec::new();
        let mut terminal_output = String::new();
        while replies.len() < 3 || !terminal_output.contains("backend-ready") {
            match channel.wait().await.unwrap() {
                ChannelMsg::Failure => replies.push(false),
                ChannelMsg::Success => replies.push(true),
                ChannelMsg::Data { data } => {
                    terminal_output.push_str(std::str::from_utf8(&data).unwrap())
                }
                other => panic!("unexpected terminal message: {other:?}"),
            }
        }
        assert_eq!(replies, [false, true, true]);
        assert_eq!(terminal_output, "backend-ready\r\n");
        client
            .disconnect(russh::Disconnect::ByApplication, "done", "")
            .await
            .unwrap();
        let mut denied = client::connect(
            Arc::new(client::Config::default()),
            address,
            Verify(host.public_key().clone()),
        )
        .await
        .unwrap();
        assert!(denied
            .authenticate_publickey(
                format!("yr:instance:other-instance:port={port}"),
                PrivateKeyWithHashAlg::new(Arc::new(inline_user), None)
            )
            .await
            .unwrap()
            .success());
        let mut channel = denied.channel_open_session().await.unwrap();
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .unwrap();
        channel.request_shell(true).await.unwrap();
        let mut diagnostic = String::new();
        while let Some(message) = channel.wait().await {
            match message {
                ChannelMsg::ExtendedData { data, .. } => {
                    diagnostic.push_str(std::str::from_utf8(&data).unwrap())
                }
                ChannelMsg::Close => break,
                _ => {}
            }
        }
        assert!(diagnostic.contains("access denied"), "{diagnostic}");
        denied
            .disconnect(russh::Disconnect::ByApplication, "done", "")
            .await
            .unwrap();
        if native {
            use std::os::unix::fs::PermissionsExt;
            let user_path = directory.path().join("user");
            std::fs::write(
                &user_path,
                user.to_openssh(russh::keys::ssh_key::LineEnding::LF)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            std::fs::set_permissions(&user_path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let known_hosts = directory.path().join("known_hosts");
            std::fs::write(
                &known_hosts,
                format!(
                    "[127.0.0.1]:{} {}\n",
                    address.port(),
                    host.public_key().to_openssh().unwrap()
                ),
            )
            .unwrap();
            let username = target.to_string();
            let output = tokio::task::spawn_blocking(move || {
                use std::io::Write;
                use std::process::{Command, Stdio};
                let mut child = Command::new("ssh")
                    .args([
                        "-F",
                        "/dev/null",
                        "-tt",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "StrictHostKeyChecking=yes",
                        "-o",
                        "IdentitiesOnly=yes",
                        "-o",
                        "ConnectTimeout=5",
                    ])
                    .arg("-o")
                    .arg(format!("UserKnownHostsFile={}", known_hosts.display()))
                    .arg("-p")
                    .arg(address.port().to_string())
                    .arg("-l")
                    .arg(username)
                    .arg("-i")
                    .arg(user_path)
                    .arg("127.0.0.1")
                    .env("TERM", "xterm")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                child.stdin.take().unwrap().write_all(b"exit\r").unwrap();
                child.wait_with_output().unwrap()
            })
            .await
            .unwrap();
            assert_eq!(
                output.status.code(),
                Some(17),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = std::str::from_utf8(&output.stdout).unwrap();
            assert!(text.contains("Environment ID: "), "{text}");
            assert!(
                text.contains("Environment URN: urn:adx:environment:demo:1:"),
                "{text}"
            );
            assert!(text.contains("backend-ready"), "{text}");
        }
    };
    tokio::pin!(backend_server);
    tokio::select! { result = tokio::time::timeout(Duration::from_secs(30), checks) => { result.unwrap(); }, result = &mut backend_server => panic!("backend stopped: {result:?}") }
    stop.send(true).unwrap();
    ingress.await.unwrap().unwrap();
    backend_shutdown.shutdown("done".into());
    backend_server.await.unwrap();
    node_server.abort();
    tokio::time::timeout(Duration::from_secs(3), async {
        while gateway.active_sessions() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
