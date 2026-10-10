#![cfg(feature = "rdma")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use afs::node::{
    rpc::{
        control::{RDMA_HANDSHAKE_VERSION, RdmaSessionRegistry, make_control_server},
        data::make_data_server,
        peer::{DataClientOptions, DataMode, connect_data_client},
    },
    storage::Storage,
};
use afs_protocol::{
    node_control::{
        CloseDataRequest, NegotiateDataRequest, node_control_client::NodeControlClient,
    },
    node_data::{
        DataReadReply, DataReadRequest, DataTransfer, DataWriteReply, DataWriteRequest,
        node_data_client::NodeDataClient,
        node_data_server::{NodeData, NodeDataServer},
    },
};
use afs_transport::rdma::{CAPACITY, RdmaEndpoint};
use tokio::{net::TcpListener, sync::Notify};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

fn rdma_device() -> Option<String> {
    std::env::var("AFS_TEST_RDMA_DEVICE")
        .ok()
        .filter(|value| !value.is_empty())
}

async fn spawn_real_rdma_node(
    device: String,
    ttl: Duration,
) -> (
    tempfile::TempDir,
    RdmaSessionRegistry,
    String,
    tokio::task::JoinHandle<()>,
) {
    let temp = tempfile::tempdir().expect("temp dir");
    let storage = Arc::new(Storage::new(temp.path()).expect("storage"));
    let registry = RdmaSessionRegistry::with_ttl(Some(device), ttl);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let server = tokio::spawn({
        let registry = registry.clone();
        async move {
            Server::builder()
                .add_service(make_control_server(registry.clone()))
                .add_service(make_data_server(storage, registry))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("server");
        }
    });
    (temp, registry, endpoint, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn real_negotiated_rdma_write_then_read_roundtrip() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (_temp, _registry, endpoint, server) =
        spawn_real_rdma_node(device.clone(), Duration::from_secs(60)).await;

    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Rdma,
        rdma_device: Some(device),
        timeout: Duration::from_secs(5),
    })
    .await
    .expect("rdma client");

    assert_eq!(client.mode(), "rdma");
    assert_eq!(
        client
            .write("eight.bin", 0, b"abcdefgh".to_vec())
            .await
            .expect("write"),
        8
    );
    assert_eq!(
        client.read("eight.bin", 0, 8).await.expect("read"),
        b"abcdefgh"
    );
    assert_eq!(
        client
            .write("eight.bin", 0, b"ABCDEFGH".to_vec())
            .await
            .expect("rewrite"),
        8
    );
    assert_eq!(
        client.read("eight.bin", 0, 8).await.expect("read again"),
        b"ABCDEFGH"
    );
    client.close().await.expect("close");
    server.abort();
}

/// The external runner stops only the native data worker after ibv_post_send
/// succeeds, before it consumes CQ. No production pause hooks are compiled in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real RXE and scripts/check-rdma-cancellation.py under Linux GDB"]
async fn posted_rdma_cancel_keeps_endpoint_until_worker_drains() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let checkpoint = std::path::PathBuf::from(
        std::env::var_os("AFS_TEST_RDMA_CHECKPOINT").expect("external GDB checkpoint directory"),
    );
    save_rdma_resources(&checkpoint, "baseline");
    let (temp, registry, endpoint, server) =
        spawn_real_rdma_node(device.clone(), Duration::from_secs(60)).await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint: endpoint.clone(),
        mode: DataMode::Rdma,
        rdma_device: Some(device),
        timeout: Duration::from_secs(30),
    })
    .await
    .expect("real RDMA client");
    // This fresh registry contains exactly one negotiated session (id 1).
    let session = registry.session(1).await.expect("complete real probe");
    let retained_endpoint = Arc::downgrade(&session.endpoint);
    drop(session);
    save_rdma_resources(&checkpoint, "connected");

    let payload: Vec<u8> = (0..4096).map(|i| (i * 37 + 19) as u8).collect();
    let posted = checkpoint.join("posted");
    {
        let write = client.write("cancelled.bin", 0, payload.clone());
        tokio::pin!(write);
        tokio::select! {
            result = &mut write => panic!("write finished before posted checkpoint: {result:?}"),
            () = wait_for_rdma_checkpoint(&posted) => {}
        }
        // Dropping the caller future poisons it; its detached operation owns
        // the client MR and operation lock until the real command completes.
    }
    let error = client
        .write("must-not-replay.bin", 0, b"reuse".to_vec())
        .await
        .expect_err("cancelled client must reject buffer reuse");
    assert!(error.to_string().contains("poisoned"));
    assert_eq!(client.mode(), "rdma");
    let mut control = NodeControlClient::connect(endpoint).await.expect("control");
    control
        .close_data(CloseDataRequest { session_id: 1 })
        .await
        .expect("remove old lookup independently of blocked client operation");
    assert_eq!(
        registry
            .session(1)
            .await
            .err()
            .expect("stale lookup")
            .code(),
        tonic::Code::FailedPrecondition
    );
    {
        let endpoint = retained_endpoint
            .upgrade()
            .expect("worker retains endpoint");
        assert!(endpoint.try_lock().is_err(), "native worker still owns MR");
    }
    assert!(!temp.path().join("cancelled.bin").exists());
    assert!(!temp.path().join("must-not-replay.bin").exists());
    save_rdma_resources(&checkpoint, "closed-paused");
    eprintln!("AFS_RDMA_CANCEL lookup=STALE client=POISONED endpoint=RETAINED storage=UNTOUCHED");
    std::fs::write(checkpoint.join("resume"), b"resume").expect("resume native worker");

    // Cancellation is an unknown outcome, not rollback. This diagnostic
    // handler is allowed to finish the already admitted write after close.
    tokio::time::timeout(Duration::from_secs(15), client.close())
        .await
        .expect("detached client command drains")
        .expect("idempotent close");
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), async {
        while retained_endpoint.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("last worker releases server endpoint");
    assert_eq!(
        std::fs::read(temp.path().join("cancelled.bin")).unwrap(),
        payload
    );
    assert!(!temp.path().join("must-not-replay.bin").exists());
    save_rdma_resources(&checkpoint, "drained");
    eprintln!("AFS_RDMA_CANCEL drain=COMPLETE content=EXACT endpoint=RELEASED replay=ABSENT");
    server.abort();
    let _ = server.await;
}

async fn wait_for_rdma_checkpoint(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("GDB must prove data WQE posted before cancellation");
}

fn save_rdma_resources(directory: &std::path::Path, phase: &str) {
    let tids: Vec<u32> = std::fs::read_dir("/proc/self/task")
        .expect("Linux thread inventory")
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .parse()
                .unwrap()
        })
        .collect();
    std::fs::write(
        directory.join(format!("{phase}-process.json")),
        serde_json::to_vec(&serde_json::json!({"pid": std::process::id(), "tids": tids})).unwrap(),
    )
    .expect("save process identity");
    for kind in ["qp", "mr", "cq", "pd", "ctx"] {
        let output = std::process::Command::new("rdma")
            .args(["-j", "resource", "show", kind])
            .output()
            .expect("rdma resource inventory");
        assert!(output.status.success(), "RDMA {kind} inventory failed");
        std::fs::write(
            directory.join(format!("{phase}-{kind}.json")),
            output.stdout,
        )
        .expect("save exact RDMA resources");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_inline_data_server_does_not_require_control_service() {
    let (_temp, endpoint, server) = spawn_grpc_data_only_node().await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(5),
    })
    .await
    .expect("grpc client");

    assert_eq!(client.mode(), "grpc");
    assert_eq!(
        client
            .write("grpc-only.bin", 0, b"abcdefgh".to_vec())
            .await
            .expect("write"),
        8
    );
    assert_eq!(
        client.read("grpc-only.bin", 0, 8).await.expect("read"),
        b"abcdefgh"
    );
    client.close().await.expect("close");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn cancelled_rdma_call_poisons_same_client_without_grpc_replay() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (endpoint, service, server) = spawn_delayed_rdma_node(device.clone()).await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Rdma,
        rdma_device: Some(device),
        timeout: Duration::from_secs(5),
    })
    .await
    .expect("rdma client");

    {
        let write = client.write("cancel.bin", 0, b"abcdefgh".to_vec());
        tokio::pin!(write);
        tokio::select! {
            () = service.started.notified() => {}
            result = &mut write => panic!("write completed before test could cancel it: {result:?}"),
            () = tokio::time::sleep(Duration::from_secs(1)) => panic!("server did not observe RDMA write command"),
        }
    }

    let reuse = client.write("cancel.bin", 0, b"ABCDEFGH".to_vec()).await;
    assert!(
        reuse
            .expect_err("poisoned client must reject reuse")
            .to_string()
            .contains("poisoned")
    );
    assert_eq!(service.rdma_commands.load(Ordering::SeqCst), 1);
    assert_eq!(service.grpc_commands.load(Ordering::SeqCst), 0);
    assert_eq!(client.mode(), "rdma");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn ttl_cleanup_makes_old_rdma_session_stale_for_data_rpc() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (_temp, registry, endpoint, server) =
        spawn_real_rdma_node(device.clone(), Duration::from_millis(1)).await;
    let mut client_endpoint = RdmaEndpoint::open(&device).expect("client endpoint");
    let client_info = client_endpoint.info().expect("client info");
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut control = NodeControlClient::new(channel.clone());
    let negotiate = control
        .negotiate_data(Request::new(NegotiateDataRequest {
            client_info: client_info.to_vec(),
            capacity: CAPACITY as u32,
            handshake_version: RDMA_HANDSHAKE_VERSION,
        }))
        .await
        .expect("negotiate")
        .into_inner();
    assert!(negotiate.rdma_supported);
    client_endpoint
        .connect(&negotiate.server_info)
        .expect("client connect");
    client_endpoint.send_probe(5000).expect("probe");

    tokio::time::sleep(Duration::from_millis(5)).await;
    registry.cleanup_expired().await;

    let mut data = NodeDataClient::new(channel);
    let error = data
        .read(Request::new(DataReadRequest {
            session_id: negotiate.session_id,
            transfer: DataTransfer::RdmaOneSided.into(),
            name: "missing.bin".into(),
            offset: 0,
            length: 8,
        }))
        .await
        .expect_err("stale session must be rejected");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn missing_rdma_probe_rejects_first_write_before_storage_and_poisons_session() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let (temp, _registry, endpoint, server) =
        spawn_real_rdma_node(device.clone(), Duration::from_secs(60)).await;

    let mut client_endpoint = RdmaEndpoint::open(&device).expect("client endpoint");
    let client_info = client_endpoint.info().expect("client info");
    client_endpoint
        .put_local(b"abcdefgh")
        .expect("stage client buffer");
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut control = NodeControlClient::new(channel.clone());
    let negotiate = control
        .negotiate_data(Request::new(NegotiateDataRequest {
            client_info: client_info.to_vec(),
            capacity: CAPACITY as u32,
            handshake_version: RDMA_HANDSHAKE_VERSION,
        }))
        .await
        .expect("negotiate")
        .into_inner();
    assert!(negotiate.rdma_supported);
    assert_eq!(negotiate.handshake_version, RDMA_HANDSHAKE_VERSION);
    client_endpoint
        .connect(&negotiate.server_info)
        .expect("client connect");

    let mut data = NodeDataClient::new(channel);
    let error = data
        .write(Request::new(DataWriteRequest {
            session_id: negotiate.session_id,
            transfer: DataTransfer::RdmaOneSided.into(),
            name: "no-probe.bin".into(),
            offset: 0,
            data: Vec::new(),
            length: 8,
        }))
        .await
        .expect_err("missing probe must reject first data command");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(
        !temp.path().join("no-probe.bin").exists(),
        "storage must not be touched before RDMA probe is proven"
    );

    let poisoned = data
        .read(Request::new(DataReadRequest {
            session_id: negotiate.session_id,
            transfer: DataTransfer::RdmaOneSided.into(),
            name: "no-probe.bin".into(),
            offset: 0,
            length: 8,
        }))
        .await
        .expect_err("failed probe must poison the session");
    assert_eq!(poisoned.code(), tonic::Code::FailedPrecondition);
    server.abort();
}

#[derive(Clone)]
struct DelayedDataService {
    started: Arc<Notify>,
    rdma_commands: Arc<AtomicUsize>,
    grpc_commands: Arc<AtomicUsize>,
}

impl DelayedDataService {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            rdma_commands: Arc::new(AtomicUsize::new(0)),
            grpc_commands: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[tonic::async_trait]
impl NodeData for DelayedDataService {
    async fn read(
        &self,
        request: Request<DataReadRequest>,
    ) -> Result<Response<DataReadReply>, Status> {
        let request = request.into_inner();
        if request.transfer == i32::from(DataTransfer::GrpcInline) {
            self.grpc_commands.fetch_add(1, Ordering::SeqCst);
        }
        if request.transfer == i32::from(DataTransfer::RdmaOneSided) {
            self.rdma_commands.fetch_add(1, Ordering::SeqCst);
        }
        self.started.notify_waiters();
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(Response::new(DataReadReply {
            length: request.length,
            data: Vec::new(),
        }))
    }

    async fn write(
        &self,
        request: Request<DataWriteRequest>,
    ) -> Result<Response<DataWriteReply>, Status> {
        let request = request.into_inner();
        if request.transfer == i32::from(DataTransfer::GrpcInline) {
            self.grpc_commands.fetch_add(1, Ordering::SeqCst);
        }
        if request.transfer == i32::from(DataTransfer::RdmaOneSided) {
            self.rdma_commands.fetch_add(1, Ordering::SeqCst);
        }
        self.started.notify_waiters();
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(Response::new(DataWriteReply {
            written: request.length,
        }))
    }
}

async fn spawn_delayed_rdma_node(
    device: String,
) -> (String, DelayedDataService, tokio::task::JoinHandle<()>) {
    let registry = RdmaSessionRegistry::new(Some(device));
    let service = DelayedDataService::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let server = tokio::spawn({
        let registry = registry.clone();
        let service = service.clone();
        async move {
            Server::builder()
                .add_service(make_control_server(registry))
                .add_service(NodeDataServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("server");
        }
    });
    (endpoint, service, server)
}

async fn spawn_grpc_data_only_node() -> (tempfile::TempDir, String, tokio::task::JoinHandle<()>) {
    let temp = tempfile::tempdir().expect("temp dir");
    let storage = Arc::new(Storage::new(temp.path()).expect("storage"));
    let registry = RdmaSessionRegistry::new(None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(make_data_server(storage, registry))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server");
    });
    (temp, endpoint, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn rdma_adapter_preserves_remote_error_before_poisoning_session() {
    let device = rdma_device().expect("explicit device");
    let (_temp, _registry, endpoint, server) =
        spawn_real_rdma_node(device.clone(), Duration::from_secs(60)).await;
    let mut client = connect_data_client(DataClientOptions {
        endpoint,
        mode: DataMode::Rdma,
        rdma_device: Some(device),
        timeout: Duration::from_secs(2),
    })
    .await
    .unwrap();
    let remote = client.read("missing", 0, 1).await.unwrap_err();
    assert_eq!(remote.code(), afs_error::NODE_STORAGE_NOT_FOUND);
    assert_eq!(remote.kind(), afs_error::ErrorKind::NotFound);
    // Preserve the existing conservative failure policy; structured errors must not revive MR reuse.
    let next = client.read("missing", 0, 1).await.unwrap_err();
    assert_eq!(next.code(), afs_error::NODE_RDMA_SESSION_POISONED);
    client.close().await.unwrap();
    server.abort();
}
