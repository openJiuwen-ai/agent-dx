use adx_protocol::relay::{self as pb, relay_service_client::RelayServiceClient};
use data_plane_gateway::{
    config::{IngressNodeSecurityMode, RelayConfig},
    node::RelayService,
};
use hyper_util::rt::TokioIo;
use std::time::Duration;
use tonic::transport::Endpoint;
use tower::service_fn;

#[tokio::test]
#[cfg(feature = "mock-e2e")]
async fn standalone_upgrade_fences_identity_and_confirms_released_ports() {
    use data_plane_gateway::node::route_control::proto::{
        relay_upgrade_service_client::RelayUpgradeServiceClient, RelayUpgradeRequest,
    };
    use data_plane_gateway::{
        common::protocol::ConnectTarget,
        ingress::{DataPlaneL4Connector, H2PoolConfig},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = tempfile::Builder::new()
        .prefix("adx-upgrade-")
        .tempdir_in("/tmp")
        .unwrap();
    // UDP connect selects a local source address without sending a datagram.
    let probe = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    probe.connect("192.0.2.1:9").unwrap();
    let local_ip = probe.local_addr().unwrap().ip();
    assert!(!local_ip.is_loopback() && !local_ip.is_unspecified());
    let mut relay_config = config(root.path());
    relay_config.allowed_target_networks = vec![format!("{local_ip}/32").parse().unwrap()];
    let old_socket = root.path().join("old.sock");
    let service = RelayService::bind_standalone(relay_config, Some(&old_socket))
        .await
        .unwrap();
    let business = service.local_addr().unwrap();
    let health = service.health_addr().unwrap();
    let task = tokio::spawn(service.serve_standalone(std::future::pending::<()>()));
    let path = old_socket;
    let channel = Endpoint::from_static("http://local")
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .unwrap();
    let mut bindings = RelayServiceClient::new(channel.clone());
    let session = bindings
        .get_binding_state(pb::GetBindingStateRequest {})
        .await
        .unwrap()
        .into_inner()
        .proxy_session_id;
    let mut upgrade = RelayUpgradeServiceClient::new(channel);
    let request = RelayUpgradeRequest {
        task_id: "release-a".into(),
        proxy_session_id: session,
    };
    let state = bindings
        .begin_bindings(pb::BindingState {
            proxy_session_id: request.proxy_session_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    bindings
        .replace_bindings(pb::BindingSnapshot {
            proxy_session_id: state.proxy_session_id.clone(),
            sync_epoch: state.sync_epoch,
            bindings: vec![pb::UpdateBindingRequest {
                proxy_session_id: state.proxy_session_id.clone(),
                sync_epoch: state.sync_epoch,
                environment_id: "old-stream".into(),
                ownership_generation: 1,
                binding_revision: 1,
                binding: Some(pb::update_binding_request::Binding::Active(
                    pb::RuntimeTarget {
                        runtime_id: "runtime".into(),
                        ip: local_ip.to_string(),
                    },
                )),
            }],
        })
        .await
        .unwrap();
    let backend = tokio::net::TcpListener::bind((local_ip, 0)).await.unwrap();
    let backend_address = backend.local_addr().unwrap();
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.unwrap();
        let mut byte = [0; 1];
        while stream.read_exact(&mut byte).await.is_ok() {
            stream.write_all(&byte).await.unwrap();
        }
    });
    let connector = DataPlaneL4Connector::new(H2PoolConfig::default());
    let (_cancel, cancelled) = tokio::sync::watch::channel(false);
    let mut old = connector
        .connect_stream(
            &business.to_string(),
            &ConnectTarget {
                instance_id: "old-stream".into(),
                workload_id: "runtime".into(),
                target_ip: backend_address.ip(),
                target_port: backend_address.port(),
                request_id: "upgrade-old-stream".into(),
            },
            cancelled,
        )
        .await
        .unwrap();
    old.write_all(b"a").await.unwrap();
    let mut byte = [0; 1];
    tokio::time::timeout(Duration::from_secs(2), old.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(byte, *b"a");
    // Keep a long-lived control RPC open too: it must not hold up final shutdown.
    let _activity = bindings
        .watch_activity(pb::GetBindingStateRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        upgrade.query(request.clone()).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    let mut wrong = request.clone();
    wrong.proxy_session_id = "wrong".into();
    assert_eq!(
        upgrade.begin(wrong.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    // Query must not release either listener.
    assert!(tokio::net::TcpListener::bind(business).await.is_err());
    assert!(tokio::net::TcpListener::bind(health).await.is_err());
    let accepted = upgrade.begin(request.clone()).await.unwrap().into_inner();
    assert_eq!(accepted.task_id, request.task_id);
    assert_eq!(accepted.proxy_session_id, request.proxy_session_id);
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = upgrade.query(request.clone()).await.unwrap().into_inner();
            if state.listeners_released {
                break state;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut next_config = config(root.path());
    next_config.bind = business;
    next_config.health_bind = health;
    let next_socket = root.path().join("new.sock");
    let next = RelayService::bind_standalone(next_config, Some(&next_socket))
        .await
        .unwrap();
    assert_eq!(next.local_addr().unwrap(), business);
    assert_eq!(next.health_addr().unwrap(), health);
    let replay = upgrade.begin(request.clone()).await.unwrap().into_inner();
    assert_eq!(
        replay.drain_deadline_unix_ms,
        released.drain_deadline_unix_ms
    );
    assert!(replay.listeners_released);
    assert_eq!(
        upgrade.query(wrong).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let mut other = request;
    other.task_id = "release-b".into();
    assert_eq!(
        upgrade.begin(other).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    // The original binding service and its session remain available after TCP release.
    assert_eq!(
        bindings
            .get_binding_state(pb::GetBindingStateRequest {})
            .await
            .unwrap()
            .into_inner()
            .proxy_session_id,
        released.proxy_session_id
    );
    // GOAWAY/listener release must not terminate this already accepted stream.
    old.write_all(b"b").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), old.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(byte, *b"b");
    old.shutdown().await.unwrap();
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), old.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    backend_task.await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(next);
}

#[tokio::test]
async fn explicit_route_paths_never_replace_existing_entries() {
    use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
    let root = tempfile::Builder::new()
        .prefix("adx-route-")
        .tempdir_in("/tmp")
        .unwrap();
    for path in [
        std::path::Path::new(""),
        std::path::Path::new("relative.sock"),
    ] {
        assert!(
            RelayService::bind_standalone(config(root.path()), Some(path))
                .await
                .is_err()
        );
    }
    let file = root.path().join("file");
    std::fs::write(&file, b"keep").unwrap();
    let directory = root.path().join("directory");
    std::fs::create_dir(&directory).unwrap();
    let link = root.path().join("link");
    symlink(&file, &link).unwrap();
    let active = root.path().join("active.sock");
    let listener = tokio::net::UnixListener::bind(&active).unwrap();
    let stale = root.path().join("stale.sock");
    drop(tokio::net::UnixListener::bind(&stale).unwrap());
    for path in [&file, &directory, &link, &active, &stale] {
        let before = std::fs::symlink_metadata(path).unwrap();
        assert!(
            RelayService::bind_standalone(config(root.path()), Some(path))
                .await
                .is_err()
        );
        let after = std::fs::symlink_metadata(path).unwrap();
        assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    }
    assert_eq!(std::fs::read(&file).unwrap(), b"keep");
    drop(listener);
    let explicit = root.path().join("generation.sock");
    let service = RelayService::bind_standalone(config(root.path()), Some(&explicit))
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(&explicit).unwrap().permissions().mode() & 0o777,
        0o660
    );
    assert!(!root.path().join("route.sock").exists());
    // The activity endpoint remains independently owned by Adxlet.
    let _activity = tokio::net::UnixListener::bind(root.path().join("adxlet.sock")).unwrap();
    let default = RelayService::bind_standalone(config(root.path()), None)
        .await
        .unwrap();
    assert!(root.path().join("route.sock").exists());
    let mut conflict = config(root.path());
    conflict.bind = service.local_addr().unwrap();
    assert!(
        RelayService::bind_standalone(conflict, Some(&root.path().join("busy.sock")))
            .await
            .is_err()
    );
    let mut conflict = config(root.path());
    conflict.health_bind = service.health_addr().unwrap();
    assert!(
        RelayService::bind_standalone(conflict, Some(&root.path().join("busy-health.sock")))
            .await
            .is_err()
    );
    drop(default);
    drop(service);
    assert!(explicit.exists()); // Explicit generation cleanup belongs to the supervisor.
}

fn config(root: &std::path::Path) -> RelayConfig {
    RelayConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        advertise_address: String::new(),
        health_bind: "127.0.0.1:0".parse().unwrap(),
        allowed_target_networks: vec!["127.0.0.0/8".parse().unwrap()],
        allowed_ingress_networks: vec!["127.0.0.0/8".parse().unwrap()],
        allow_any_ingress: false,
        max_streams: 32,
        ingress_security_mode: IngressNodeSecurityMode::Network,
        tls_cert: String::new(),
        tls_key: String::new(),
        mtls_client_ca: String::new(),
        activity_uds_dir: Some(root.to_string_lossy().into_owned()),
        activity_interval: Duration::from_secs(30),
        gateway_epoch: "test".into(),
        drain_timeout: Duration::from_millis(10),
    }
}

#[tokio::test]
async fn shared_node_service_requires_sync_and_closes_owned_listeners() {
    let root = tempfile::Builder::new()
        .prefix("adx-node-")
        .tempdir_in("/tmp")
        .unwrap();
    let service = RelayService::bind(config(root.path())).await.unwrap();
    let address = service.local_addr().unwrap();
    let (stop, done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(service.serve(async {
        let _ = done.await;
    }));
    let socket = root.path().join("route.sock");
    let path = socket.clone();
    let channel = Endpoint::from_static("http://local")
        .connect_with_connector(service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .unwrap();
    let mut client = RelayServiceClient::new(channel);
    let state = client
        .get_binding_state(pb::GetBindingStateRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(!state.ready);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    assert!(client
        .get_binding_state(pb::GetBindingStateRequest {})
        .await
        .is_err());
}
