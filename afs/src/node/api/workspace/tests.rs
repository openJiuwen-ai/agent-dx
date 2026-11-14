use super::*;
use crate::{
    config::{Cli, Config, DataMode, OutletConfig, Role},
    meta::{
        Meta,
        rpc::{MetaRpc, OwnerRootsRpc},
        store::{MetaStore, NodeSessionLease, RequestKey, Store, journal::MemoryLog},
    },
    node::{NodeReadiness, storage::LocalFs, vfs::ownerfs::root::RootManager},
    runtime::Observability,
};
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use clap::Parser;
use tokio_stream::wrappers::TcpListenerStream;
use tower::ServiceExt;

use crate::node::vfs::ownerfs::{catalog::LocalRootRecord, root};
use afs_protocol::meta::meta_server::Meta as MetaService;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

#[derive(Default)]
struct LookupFault {
    calls: AtomicUsize,
    delay_ms: AtomicU64,
    initial_delay_ms: AtomicU64,
    fail: AtomicBool,
}
struct FaultMeta {
    inner: MetaRpc,
    fault: Arc<LookupFault>,
}
#[tonic::async_trait]
impl MetaService for FaultMeta {
    async fn ping(
        &self,
        request: tonic::Request<afs_protocol::meta::PingRequest>,
    ) -> Result<tonic::Response<afs_protocol::meta::PingReply>, tonic::Status> {
        self.inner.ping(request).await
    }
    async fn register_node(
        &self,
        request: tonic::Request<afs_protocol::meta::RegisterNodeRequest>,
    ) -> Result<tonic::Response<afs_protocol::meta::RegisterNodeReply>, tonic::Status> {
        self.inner.register_node(request).await
    }
    async fn lookup_node(
        &self,
        request: tonic::Request<afs_protocol::meta::LookupNodeRequest>,
    ) -> Result<tonic::Response<LookupNodeReply>, tonic::Status> {
        if self.fault.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(
                self.fault.initial_delay_ms.load(Ordering::SeqCst),
            ))
            .await;
        } else {
            if self.fault.fail.load(Ordering::SeqCst) {
                return Err(tonic::Status::unavailable("injected node lookup failure"));
            }
            tokio::time::sleep(Duration::from_millis(
                self.fault.delay_ms.load(Ordering::SeqCst),
            ))
            .await;
        }
        self.inner.lookup_node(request).await
    }
}

struct DeleteGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

struct LossyActivation {
    inner: Arc<GrpcRootMeta>,
    lose: Arc<AtomicBool>,
    fail_recovery: Arc<AtomicBool>,
    delete_gate: Arc<Mutex<Option<DeleteGate>>>,
    fail_deactivate: Arc<AtomicBool>,
    lose_deactivate_reply: Arc<AtomicBool>,
    lose_begin_reply: Arc<AtomicBool>,
}
impl RootMeta for LossyActivation {
    fn begin_root_deletion(
        &self,
        record: &LocalRootRecord,
        session: &str,
    ) -> afs_error::Result<()> {
        if self.fail_recovery.swap(false, Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected deletion preflight failure",
            ));
        }
        self.inner.begin_root_deletion(record, session)?;
        if self.lose_begin_reply.swap(false, Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected lost Begin reply",
            ));
        }
        Ok(())
    }
    fn current_node_session(&self, node: &str) -> afs_error::Result<Option<String>> {
        self.inner.current_node_session(node)
    }
    fn reserve_root(&self, id: &root::RootId, intent: &str) -> afs_error::Result<RootReservation> {
        self.inner.reserve_root(id, intent)
    }
    fn activate_root(&self, prepared: &root::PreparedRoot) -> afs_error::Result<root::RootGrant> {
        let grant = self.inner.activate_root(prepared)?;
        if self.lose.swap(false, Ordering::SeqCst) {
            self.fail_recovery.store(true, Ordering::SeqCst);
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected lost Activate reply",
            ));
        }
        Ok(grant)
    }
    fn abort_root(&self, r: &RootReservation) -> afs_error::Result<()> {
        self.inner.abort_root(r)
    }
    fn deactivate_root(
        &self,
        id: &root::RootId,
        epoch: u64,
        home: &str,
        session: &str,
    ) -> afs_error::Result<()> {
        if let Some(gate) = self.delete_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            let _ = gate.release.recv();
        }
        if self.fail_deactivate.swap(false, Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected deactivation failure",
            ));
        }
        self.inner.deactivate_root(id, epoch, home, session)?;
        if self.lose_deactivate_reply.swap(false, Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected lost deletion reply",
            ));
        }
        Ok(())
    }
    fn lookup_root(&self, id: &root::RootId) -> afs_error::Result<Option<RootLocation>> {
        self.inner.lookup_root(id)
    }
    fn lookup_root_state(&self, id: &root::RootId) -> afs_error::Result<root::RootLookupState> {
        self.inner.lookup_root_state(id)
    }
    fn list_owner_roots(&self, home: &str) -> afs_error::Result<root::OwnerRootInventory> {
        self.inner.list_owner_roots(home)
    }
    fn acquire_root(
        &self,
        id: &root::RootId,
        right: root::RootRight,
    ) -> afs_error::Result<root::RootGrant> {
        self.inner.acquire_root(id, right)
    }
    fn validate_root_access(
        &self,
        access: &root::PresentedRootAccess,
        peer: &str,
    ) -> afs_error::Result<root::RootGrant> {
        self.inner.validate_root_access(access, peer)
    }
    fn recover_root(
        &self,
        record: &LocalRootRecord,
        session: &str,
    ) -> afs_error::Result<root::RootGrant> {
        if self.fail_recovery.swap(false, Ordering::SeqCst) {
            return Err(Error::coded(
                afs_error::IO_UNAVAILABLE,
                "injected first recovery failure",
            ));
        }
        self.inner.recover_root(record, session)
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    api: Arc<WorkspaceApi>,
    roots: Arc<RootManager>,
    store: Arc<Store>,
    server: tokio::task::JoinHandle<()>,
    lookup_fault: Arc<LookupFault>,
    delete_gate: Arc<Mutex<Option<DeleteGate>>>,
    lose_activation: Arc<std::sync::atomic::AtomicBool>,
    fail_deactivate: Arc<AtomicBool>,
    lose_deactivate_reply: Arc<AtomicBool>,
    lose_begin_reply: Arc<AtomicBool>,
    fail_recovery: Arc<AtomicBool>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        Self::with_node_address("http://127.0.0.1:9").await
    }

    async fn with_node_address(address: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let tls_path = directory.path().join("tls");
        std::fs::create_dir(&tls_path).unwrap();
        let generate = |args: &[&str]| {
            let output = std::process::Command::new("openssl")
                .current_dir(&tls_path)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        generate(&[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-subj",
            "/CN=workspace-ca",
            "-days",
            "1",
        ]);
        generate(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
            "entry.key",
            "-out",
            "entry.csr",
            "-subj",
            "/CN=entry",
        ]);
        std::fs::write(tls_path.join("leaf.ext"), "subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth,clientAuth\n").unwrap();
        generate(&[
            "x509",
            "-req",
            "-in",
            "entry.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "entry.pem",
            "-days",
            "1",
            "-extfile",
            "leaf.ext",
        ]);
        let tls = afs_transport::TlsConfig::MutualTls {
            encryption_enabled: true,
            ca_certificate: tls_path.join("ca.pem"),
            identity_certificate: tls_path.join("entry.pem"),
            identity_private_key: tls_path.join("entry.key"),
            server_name: "localhost".into(),
        };
        let trusted = std::collections::HashMap::from([(
            crate::config::read_certificate_der(&tls_path.join("entry.pem")).unwrap(),
            "entry".into(),
        )]);
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        store
            .register_node_session(
                RequestKey::new("entry", "register"),
                NodeSessionLease {
                    node_id: "entry".into(),
                    session_id: "session".into(),
                    grpc_addr: address.into(),
                    data_addr: String::new(),
                    rest_addr: String::new(),
                    nfs_addr: "127.0.0.1".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let meta = Arc::new(Meta::with_store_and_peer_identity(
            "workspace-test".into(),
            Observability::new().unwrap(),
            store.clone(),
            trusted,
            true,
        ));
        let security = afs_transport::SecurityManager::new(tls.clone()).unwrap();
        let lookup_fault = Arc::new(LookupFault::default());
        let fault = lookup_fault.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(afs_protocol::meta::meta_server::MetaServer::new(
                    FaultMeta {
                        inner: MetaRpc(meta.clone()),
                        fault,
                    },
                ))
                .add_service(
                    afs_protocol::meta::owner_roots_server::OwnerRootsServer::new(OwnerRootsRpc(
                        meta,
                    )),
                )
                .serve_with_incoming(security.incoming(TcpListenerStream::new(listener)).unwrap())
                .await
                .unwrap();
        });
        let mut config = Config::resolve(Role::Node, Cli::parse_from(["afs-node"])).unwrap();
        config.id = "entry".into();
        config.data_dir = directory.path().into();
        config.timeout_ms = 500;
        config.outlet = Some(OutletConfig {
            public_root: directory.path().join("public"),
            import_root: directory.path().join("imports"),
            users_root: directory.path().join("export"),
            nfs_addr: "127.0.0.1".into(),
            mirror_interval_ms: 25,
            mount_options: String::new(),
        });
        let meta = Arc::new(
            GrpcRootMeta::new(
                &endpoint,
                "entry".into(),
                "session".into(),
                Duration::from_millis(500),
                tls.clone(),
            )
            .unwrap(),
        );
        let lose_activation = Arc::new(AtomicBool::new(false));
        let delete_gate = Arc::new(Mutex::new(None));
        let fail_deactivate = Arc::new(AtomicBool::new(false));
        let lose_deactivate_reply = Arc::new(AtomicBool::new(false));
        let lose_begin_reply = Arc::new(AtomicBool::new(false));
        let fail_recovery = Arc::new(AtomicBool::new(false));
        let root_meta = Arc::new(LossyActivation {
            inner: meta.clone(),
            lose: lose_activation.clone(),
            fail_recovery: fail_recovery.clone(),
            delete_gate: delete_gate.clone(),
            fail_deactivate: fail_deactivate.clone(),
            lose_deactivate_reply: lose_deactivate_reply.clone(),
            lose_begin_reply: lose_begin_reply.clone(),
        });
        let data = directory.path().join("ownerfs");
        let (owner, roots) = bridge::spawn_blocking(move || {
            let disk = Arc::new(LocalFs::open(data).unwrap());
            let roots = Arc::new(
                RootManager::open("entry".into(), "session".into(), root_meta, disk.clone())
                    .unwrap(),
            );
            (Arc::new(OwnerFs::new_local(roots.clone(), disk)), roots)
        })
        .await
        .unwrap();
        let observability = Observability::new().unwrap();
        let node = Arc::new(Node {
            config,
            session_id: "session".into(),
            readiness: Arc::new(NodeReadiness::new(
                true,
                Some(1),
                Some(Instant::now()),
                None,
                None,
                DataMode::Grpc,
                None,
            )),
            #[cfg(feature = "dfs")]
            read_work_metrics: crate::node::metrics::work::ReadWorkMetrics::register(
                &observability.registry,
            )
            .unwrap(),
            observability,
            ownerfs: Some(owner),
            #[cfg(feature = "dfs")]
            dfs: None,
        });
        let peers = Arc::new(PeerConnectionPool::new(Default::default(), tls, 4).unwrap());
        let api = WorkspaceApi::new(node, meta, peers).unwrap();
        Self {
            directory,
            api,
            roots,
            store,
            server,
            lookup_fault,
            delete_gate,
            lose_activation,
            fail_deactivate,
            lose_deactivate_reply,
            lose_begin_reply,
            fail_recovery,
        }
    }

    async fn prepare_workspace(&self, owner: &str) -> RootReservation {
        let api = self.api.clone();
        let owner = owner.to_owned();
        let uid = self.directory.path().metadata().unwrap().uid();
        let gid = self.directory.path().metadata().unwrap().gid();
        bridge::spawn_blocking(move || {
            let reservation = api.owner.reserve_workspace(OsStr::new(&owner)).unwrap();
            api.prepare(
                &reservation,
                &CreateWorkspace {
                    owner,
                    uid: Some(uid),
                    gid: Some(gid),
                    mode: None,
                },
            )
            .unwrap();
            reservation
        })
        .await
        .unwrap()
    }

    async fn request(&self, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = self.api.clone().router().oneshot(request).await.unwrap();
        let status = response.status();
        let value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                .unwrap();
        (status, value)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_home_preparation_reports_only_one_creation() {
    let fixture = Fixture::new().await;
    let api = fixture.api.clone();
    let uid = fixture.directory.path().metadata().unwrap().uid();
    let gid = fixture.directory.path().metadata().unwrap().gid();
    bridge::spawn_blocking(move || {
        let request = CreateWorkspace {
            owner: "concurrent".into(),
            uid: Some(uid),
            gid: Some(gid),
            mode: None,
        };
        let reservation = api
            .owner
            .reserve_workspace(OsStr::new(&request.owner))
            .unwrap();
        assert!(api.prepare(&reservation, &request).unwrap());
        assert!(!api.prepare(&reservation, &request).unwrap());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_http_refuses_unready_outlet_and_recursively_deletes_active_data() {
    let fixture = Fixture::new().await;
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request("POST", "/v1/workspaces", json!({"owner":"../bad"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let uid = fixture.directory.path().metadata().unwrap().uid();
    for method in ["GET", "DELETE"] {
        let (status, error) = fixture
            .request(method, "/v1/workspaces/%FF", Value::Null)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["code"], "invalid_request");
    }
    let gid = fixture.directory.path().metadata().unwrap().gid();
    let create = fixture
        .request(
            "POST",
            "/v1/workspaces",
            json!({"owner":"alice","uid":uid,"gid":gid}),
        )
        .await;
    assert_eq!(create.0, StatusCode::SERVICE_UNAVAILABLE);
    let (status, record) = fixture
        .request("GET", "/v1/workspaces/alice", Value::Null)
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "create={create:?}, lookup={record:?}"
    );
    assert_eq!(record["node_id"], "entry");
    assert_eq!(record["mode"], "0700");
    assert_eq!(record["state"], "active");
    assert!(record["access_path"].is_null());
    assert_eq!(
        fixture
            .request(
                "POST",
                "/v1/workspaces",
                json!({"owner":"alice","uid":uid + 1,"gid":gid})
            )
            .await
            .0,
        StatusCode::CONFLICT
    );
    let data = fixture.directory.path().join("ownerfs/root-616c696365-e1");
    assert!(data.is_dir());
    std::fs::create_dir_all(data.join("nested/child")).unwrap();
    std::fs::write(data.join("nested/child/file"), b"data").unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("keep"), b"keep").unwrap();
    std::os::unix::fs::symlink(outside.path(), data.join("link")).unwrap();
    let (_, list) = fixture
        .request("GET", "/v1/workspaces?node_ip=127.0.0.1", Value::Null)
        .await;
    assert_eq!(list["total"], 1);
    let (status, deleted) = fixture
        .request(
            "DELETE",
            "/v1/workspaces/alice?force=true&expected_epoch=1",
            Value::Null,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        deleted,
        json!({"owner":"alice","deleted":true,"data_lost":false})
    );
    assert!(!data.exists());
    assert_eq!(std::fs::read(outside.path().join("keep")).unwrap(), b"keep");
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/alice?expected_epoch=1",
                Value::Null
            )
            .await
            .1["deleted"],
        false
    );
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/alice?force=true&expected_epoch=1",
                Value::Null
            )
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_timeout_keeps_the_blocking_io_permit_until_io_finishes() {
    let fixture = Fixture::new().await;
    let (send, receive) = std::sync::mpsc::channel::<()>();
    let response = fixture
        .api
        .clone()
        .execute("lookup", move |_| {
            receive.recv().map_err(ApiError::home)?;
            Ok((StatusCode::OK, Value::Null))
        })
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(fixture.api.pending.available_permits(), 15);
    send.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.api.pending.available_permits() != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let all = fixture
        .api
        .pending
        .clone()
        .acquire_many_owned(16)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .request("GET", "/v1/workspaces/missing", Value::Null)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(all);
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_list_keeps_all_matching_roots_when_attributes_are_unavailable() {
    for (address, filter) in [
        ("http://127.0.0.1:9", "127.0.0.1"),
        ("http://[0:0:0:0:0:0:0:1]:9", "::1"),
        ("http://[::1]:9", "0:0:0:0:0:0:0:1"),
    ] {
        let fixture = Fixture::with_node_address(address).await;
        let api = fixture.api.clone();
        let uid = fixture.directory.path().metadata().unwrap().uid();
        let gid = fixture.directory.path().metadata().unwrap().gid();
        bridge::spawn_blocking(move || {
            for owner in ["alice", "bob", "legacy name"] {
                let request = CreateWorkspace {
                    owner: owner.into(),
                    uid: Some(uid),
                    gid: Some(gid),
                    mode: None,
                };
                let reservation = api.owner.reserve_workspace(OsStr::new(owner)).unwrap();
                assert!(api.prepare(&reservation, &request).unwrap());
            }
        })
        .await
        .unwrap();
        std::fs::remove_dir(fixture.directory.path().join("ownerfs/root-616c696365-e1")).unwrap();

        let (status, list) = fixture
            .request(
                "GET",
                &format!("/v1/workspaces?node_ip={filter}"),
                Value::Null,
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["total"], 2, "address={address}, filter={filter}");
        let items = list["items"].as_array().unwrap();
        let alice = items.iter().find(|item| item["owner"] == "alice").unwrap();
        assert!(alice["uid"].is_null());
        assert!(alice["gid"].is_null());
        assert!(alice["mode"].is_null());
        assert!(items.iter().any(|item| item["owner"] == "bob"));
        assert_eq!(
            fixture
                .request("GET", "/v1/workspaces?node_ip=127.0.0.2", Value::Null)
                .await
                .1["total"],
            0
        );
    }
}

#[test]
fn permanent_workspace_errors_do_not_advertise_retry() {
    for (code, status) in [
        (afs_error::IO_PERMISSION_DENIED, StatusCode::FORBIDDEN),
        (afs_error::IO_DIRECTORY_NOT_EMPTY, StatusCode::CONFLICT),
        (afs_error::IO_TOO_MANY_SYMLINKS, StatusCode::CONFLICT),
        (afs_error::NODE_OWNER_STALE_HANDLE, StatusCode::CONFLICT),
        (afs_error::IO_OTHER, StatusCode::INTERNAL_SERVER_ERROR),
    ] {
        let response = ApiError::operation(Error::coded(code, "failure")).into_response();
        assert_eq!(response.status(), status);
        assert!(!response.headers().contains_key(RETRY_AFTER));
    }
    let response =
        ApiError::operation(Error::coded(afs_error::IO_UNAVAILABLE, "offline")).into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[RETRY_AFTER], "1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_cleanup_failure_does_not_block_logical_deletion_or_recreation() {
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("deep").await;
    let old = fixture.directory.path().join("ownerfs/root-64656570-e1");
    let mut deep = old.clone();
    for _ in 0..260 {
        deep.push("d");
        std::fs::create_dir(&deep).unwrap();
    }
    let uri = "/v1/workspaces/deep?expected_epoch=1";
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::OK
    );
    assert!(!old.exists());
    let trash = fixture
        .directory
        .path()
        .join("ownerfs.trash/root-64656570-e1");
    assert!(trash.is_dir());
    let api = fixture.api.clone();
    bridge::spawn_blocking(move || api.owner.maintain_workspace_deletions())
        .await
        .unwrap()
        .unwrap();
    assert!(trash.is_dir(), "failed GC must retain its durable entry");
    assert_eq!(fixture.prepare_workspace("deep").await.epoch, 2);
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::CONFLICT
    );
    let mut deep = trash.clone();
    for _ in 0..260 {
        deep.push("d");
    }
    for _ in 0..6 {
        std::fs::remove_dir(&deep).unwrap();
        deep.pop();
    }
    let api = fixture.api.clone();
    bridge::spawn_blocking(move || api.owner.maintain_workspace_deletions())
        .await
        .unwrap()
        .unwrap();
    assert!(!trash.exists());
    assert!(old.with_file_name("root-64656570-e2").is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_rejects_generation_replaced_after_preparation() {
    let fixture = Fixture::new().await;
    let uid = fixture.directory.path().metadata().unwrap().uid();
    let gid = fixture.directory.path().metadata().unwrap().gid();
    let api = fixture.api.clone();
    *fixture.api.after_prepare.lock().unwrap() = Some(Box::new(move || {
        let id = root_id_from_name(OsStr::new("replaced")).unwrap();
        let first = api.meta.lookup_root(&id).unwrap().unwrap();
        api.owner.delete_workspace(&first).unwrap();
        let second = api.owner.reserve_workspace(OsStr::new("replaced")).unwrap();
        assert!(second.epoch > first.epoch);
        api.prepare(
            &second,
            &CreateWorkspace {
                owner: "replaced".into(),
                uid: Some(uid),
                gid: Some(gid),
                mode: None,
            },
        )
        .unwrap();
    }));
    let (status, _) = fixture
        .request(
            "POST",
            "/v1/workspaces",
            json!({"owner":"replaced", "uid":uid, "gid":gid}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, current) = fixture
        .request("GET", "/v1/workspaces/replaced", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(current["epoch"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_after_lost_activation_reply_recovers_without_other_requests() {
    let fixture = Fixture::new().await;
    fixture.lose_activation.store(true, Ordering::SeqCst);
    let body = json!({"owner":"lost-delete", "uid":fixture.directory.path().metadata().unwrap().uid(), "gid":fixture.directory.path().metadata().unwrap().gid()});
    assert_eq!(
        fixture.request("POST", "/v1/workspaces", body).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(fixture.roots.cached_local_roots().unwrap().is_empty());
    let (status, deleted) = fixture
        .request(
            "DELETE",
            "/v1/workspaces/lost-delete?expected_epoch=1",
            Value::Null,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["deleted"], true);
    assert!(
        !fixture
            .directory
            .path()
            .join("ownerfs/root-6c6f73742d64656c657465-e1")
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_retry_is_bound_to_the_original_epoch() {
    let fixture = Fixture::new().await;
    let first = fixture.prepare_workspace("retry").await;
    let uri = format!("/v1/workspaces/retry?expected_epoch={}", first.epoch);
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::OK
    );
    let second = fixture.prepare_workspace("retry").await;
    assert!(second.epoch > first.epoch);
    for query in ["", "&force=true"] {
        assert_eq!(
            fixture
                .request("DELETE", &(uri.clone() + query), Value::Null)
                .await
                .0,
            StatusCode::CONFLICT
        );
    }
    for query in [
        "",
        "?expected_epoch=0",
        "?expected_epoch=-1",
        "?expected_epoch=abc",
    ] {
        assert_eq!(
            fixture
                .request(
                    "DELETE",
                    &("/v1/workspaces/retry".to_owned() + query),
                    Value::Null
                )
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, record) = fixture
        .request("GET", "/v1/workspaces/retry", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["epoch"], second.epoch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_after_lost_activation_reply_recovers_without_home_restart() {
    let fixture = Fixture::new().await;
    fixture.lose_activation.store(true, Ordering::SeqCst);
    let body = json!({"owner":"lost", "uid":fixture.directory.path().metadata().unwrap().uid(), "gid":fixture.directory.path().metadata().unwrap().gid()});
    // The fixture has no mounted outlet, so create must still fail its readiness check.
    assert_eq!(
        fixture
            .request("POST", "/v1/workspaces", body.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(fixture.roots.cached_local_roots().unwrap().is_empty());
    assert_eq!(
        fixture.request("POST", "/v1/workspaces", body).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(fixture.roots.cached_local_roots().unwrap().len(), 1);
    assert_eq!(
        fixture
            .roots
            .local_root_data_dir(&root::root_id_from_name(OsStr::new("lost")).unwrap())
            .unwrap()
            .unwrap()
            .1,
        1
    );
    let (status, record) = fixture
        .request("GET", "/v1/workspaces/lost", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        record["uid"],
        fixture.directory.path().metadata().unwrap().uid()
    );
    assert_eq!(record["mode"], "0700");
    assert_eq!(record["epoch"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_and_force_cleanup_keep_capacity_when_regular_requests_are_full() {
    let fixture = Fixture::new().await;
    let _all = fixture
        .api
        .pending
        .clone()
        .acquire_many_owned(16)
        .await
        .unwrap();
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/missing?force=true&expected_epoch=1",
                Value::Null
            )
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let _force = fixture
        .api
        .control
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_returns_snapshot_when_node_details_fail_or_exhaust_budget() {
    for (initial_delay, delay) in [(0, 0), (0, 1000), (350, 300)] {
        let fixture = Fixture::new().await;
        fixture.prepare_workspace("alice").await;
        fixture.prepare_workspace("bob").await;
        for filter in [false, true] {
            fixture
                .lookup_fault
                .initial_delay_ms
                .store(initial_delay, Ordering::SeqCst);
            fixture.lookup_fault.calls.store(0, Ordering::SeqCst);
            fixture.lookup_fault.delay_ms.store(delay, Ordering::SeqCst);
            fixture
                .lookup_fault
                .fail
                .store(delay == 0, Ordering::SeqCst);
            let uri = if filter {
                "/v1/workspaces?node_ip=127.0.0.1"
            } else {
                "/v1/workspaces"
            };
            let (status, list) = fixture.request("GET", uri, Value::Null).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "delay={delay}, filter={filter}, list={list}"
            );
            assert_eq!(list["complete"], !filter);
            assert_eq!(list["total"], if filter { 0 } else { 2 });
            if !filter {
                assert!(
                    list["items"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|record| record["node_ip"].is_null())
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_probes_are_isolated_by_home() {
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("healthy").await;
    let mut release = Vec::new();
    let mut requests = Vec::new();
    for _ in 0..2 {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        release.push(tx);
        let (entered, ready) = tokio::sync::oneshot::channel();
        let api = fixture.api.clone();
        requests.push(tokio::spawn(async move {
            api.execute("create", move |api| {
                let _probe = api.probe_slot("faulty-home")?;
                let _ = entered.send(());
                let _ = rx.recv();
                Ok((StatusCode::OK, Value::Null))
            })
            .await
        }));
        ready.await.unwrap();
    }
    for request in requests {
        assert_eq!(
            request.await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(fixture.api.pending.available_permits(), 14);
    assert!(fixture.api.probe_slot("faulty-home").is_err());
    assert!(fixture.api.probe_slot("healthy-home").is_ok());
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request("GET", "/v1/workspaces/healthy", Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/missing?force=true&expected_epoch=1",
                Value::Null
            )
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    drop(release);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.api.pending.available_permits() != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.api.probe_slot("faulty-home").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_returns_snapshot_while_local_home_holds_workspace_lock() {
    let fixture = Fixture::new().await;
    let reservation = fixture.prepare_workspace("locked").await;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    *fixture.delete_gate.lock().unwrap() = Some(DeleteGate {
        entered,
        release: held,
    });
    let roots = fixture.roots.clone();
    let deletion = bridge::spawn_blocking(move || {
        roots.delete_root_with(
            &reservation.id,
            Some(reservation.epoch),
            false,
            || Ok(()),
            || Ok(()),
        )
    });
    ready.await.unwrap();
    let mut results = Vec::new();
    for uri in [
        "/v1/workspaces",
        "/v1/workspaces",
        "/v1/workspaces?node_ip=127.0.0.1",
    ] {
        results.push(fixture.request("GET", uri, Value::Null).await);
    }
    let checks_bounded = fixture.api.probe_slot("entry").is_err();
    let ordinary_capacity = fixture.api.pending.available_permits();
    drop(release);
    deletion.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.api.probe_slot("entry").is_err()
            || fixture.api.pending.available_permits() != 16
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for (status, list) in results {
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["total"], 1);
        assert_eq!(list["complete"], true);
        assert_eq!(list["items"][0]["owner"], "locked");
        assert!(list["items"][0]["uid"].is_null());
        assert!(list["items"][0]["gid"].is_null());
        assert!(list["items"][0]["mode"].is_null());
    }
    assert!(checks_bounded);
    assert_eq!(ordinary_capacity, 16);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_marks_hostname_ip_filter_as_incomplete() {
    for host in ["localhost", "home.invalid"] {
        let fixture = Fixture::with_node_address(&format!("http://{host}:9")).await;
        fixture.prepare_workspace("alice").await;
        let (status, list) = fixture.request("GET", "/v1/workspaces", Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["total"], 1);
        assert_eq!(list["complete"], true);
        assert_eq!(list["items"][0]["node_ip"], host);
        for ip in ["127.0.0.1", "127.0.0.2", "::1"] {
            let (status, list) = fixture
                .request("GET", &format!("/v1/workspaces?node_ip={ip}"), Value::Null)
                .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(list["total"], 0);
            assert_eq!(list["complete"], false, "host={host}, filter={ip}");
        }
    }
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("alice").await;
    let (status, list) = fixture
        .request("GET", "/v1/workspaces?node_ip=127.0.0.2", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["total"], 0);
    assert_eq!(list["complete"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_retries_meta_after_physical_directory_is_gone() {
    let fixture = Fixture::new().await;
    let reservation = fixture.prepare_workspace("meta-retry").await;
    fixture.fail_deactivate.store(true, Ordering::SeqCst);
    let uri = format!(
        "/v1/workspaces/meta-retry?expected_epoch={}",
        reservation.epoch
    );
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        !fixture
            .directory
            .path()
            .join("ownerfs/root-6d6574612d7265747279-e1")
            .exists()
    );
    let (status, deleted) = fixture.request("DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["deleted"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_delete_retries_cleanup_and_meta_without_directory_lookup() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let mut fixture = Fixture::with_node_address(&endpoint).await;
    fixture.prepare_workspace("remote-retry").await;
    let tls_path = fixture.directory.path().join("tls");
    let security = afs_transport::SecurityManager::new(afs_transport::TlsConfig::MutualTls {
        encryption_enabled: true,
        ca_certificate: tls_path.join("ca.pem"),
        identity_certificate: tls_path.join("entry.pem"),
        identity_private_key: tls_path.join("entry.key"),
        server_name: "localhost".into(),
    })
    .unwrap();
    let authenticator = Arc::new(
        crate::node::rpc::data::MtlsPeerAuthenticator::new([(
            "entry".into(),
            crate::config::read_certificate_der(&tls_path.join("entry.pem")).unwrap(),
        )])
        .unwrap(),
    );
    let service = crate::node::rpc::control::OwnerControlService::new(Default::default())
        .with_owner_locks(fixture.api.owner.peer_executor().unwrap(), authenticator)
        .into_server();
    let home = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(security.incoming(TcpListenerStream::new(listener)).unwrap())
            .await
            .unwrap();
    });
    let api = fixture.api.clone();
    let stale_session_error = bridge::spawn_blocking(move || {
        let info = api.node_info("entry").unwrap();
        let mut client = OwnerControlClient::new(api.channel("entry", &info).unwrap());
        bridge::block_on(&api.runtime, async {
            client
                .delete_workspace(afs_protocol::node_control::DeleteWorkspaceRequest {
                    root_id: root_id_from_name(OsStr::new("remote-retry")).unwrap().0,
                    expected_epoch: 1,
                    requester_session_id: "expired-session".into(),
                })
                .await
                .map_err(afs_transport::grpc::error_status::status_to_error)
        })
        .unwrap_err()
    })
    .await
    .unwrap();
    assert!(
        stale_session_error
            .message()
            .contains("requester Meta session")
    );
    // Exercise entry-to-Home gRPC against the same durable authority.
    Arc::get_mut(&mut Arc::get_mut(&mut fixture.api).unwrap().node)
        .unwrap()
        .config
        .id = "remote-entry".into();
    let old = fixture
        .directory
        .path()
        .join("ownerfs/root-72656d6f74652d7265747279-e1");
    let delete = || {
        let api = fixture.api.clone();
        async move {
            match bridge::spawn_blocking(move || api.delete("remote-retry".into(), false, 1))
                .await
                .unwrap()
            {
                Ok(result) => result,
                Err(error) => (
                    error.status,
                    json!({"code":error.code, "message":error.message}),
                ),
            }
        }
    };
    fixture.fail_deactivate.store(true, Ordering::SeqCst);
    let failure = delete().await;
    assert_eq!(failure.0, StatusCode::SERVICE_UNAVAILABLE, "{failure:?}");
    assert!(!old.exists());
    assert!(
        fixture
            .directory
            .path()
            .join("ownerfs.trash/root-72656d6f74652d7265747279-e1")
            .is_dir()
    );
    let (status, deleted) = delete().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["deleted"], true);
    let second = fixture.prepare_workspace("remote-retry").await;
    assert_eq!(second.epoch, 2);
    assert_eq!(delete().await.0, StatusCode::CONFLICT);
    let api = fixture.api.clone();
    let error = bridge::spawn_blocking(move || {
        let info = api.node_info("entry").unwrap();
        let mut client = OwnerControlClient::new(api.channel("entry", &info).unwrap());
        bridge::block_on(&api.runtime, async {
            client
                .delete_workspace(afs_protocol::node_control::DeleteWorkspaceRequest {
                    root_id: second.id.0,
                    expected_epoch: 1,
                    requester_session_id: api.node.session_id.clone(),
                })
                .await
                .map_err(afs_transport::grpc::error_status::status_to_error)
        })
        .unwrap_err()
    })
    .await
    .unwrap();
    assert_eq!(error.code(), afs_error::NODE_OWNER_STALE_HANDLE);
    assert!(
        fixture
            .directory
            .path()
            .join("ownerfs/root-72656d6f74652d7265747279-e2")
            .is_dir()
    );
    home.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_checks_home_authority_before_removing_data() {
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("authority").await;
    let payload = fixture
        .directory
        .path()
        .join("ownerfs/root-617574686f72697479-e1/payload");
    std::fs::write(&payload, b"keep").unwrap();
    fixture.fail_recovery.store(true, Ordering::SeqCst);
    let uri = "/v1/workspaces/authority?expected_epoch=1";
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(std::fs::read(&payload).unwrap(), b"keep");
    let id = root_id_from_name(OsStr::new("authority")).unwrap();
    assert!(fixture.roots.enter_root(&id, root::RootRight::Read).is_ok());
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_workspace_leaves_outlet_snapshot_before_cleanup() {
    let fixture = Fixture::new().await;
    let reservation = fixture.prepare_workspace("detach").await;
    let old = fixture
        .directory
        .path()
        .join("ownerfs/root-646574616368-e1");
    std::fs::create_dir_all(old.join("nested")).unwrap();
    std::fs::write(old.join("nested/payload"), b"keep").unwrap();
    let public = fixture.api.public_path("detach");
    std::fs::create_dir_all(public.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&old, &public).unwrap();
    fixture.fail_deactivate.store(true, Ordering::SeqCst);
    let uri = format!("/v1/workspaces/detach?expected_epoch={}", reservation.epoch);
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        !public.exists(),
        "published old generation must no longer resolve"
    );
    let api = fixture.api.clone();
    let snapshot = bridge::spawn_blocking(move || api.meta.list_active_outlet_roots(None))
        .await
        .unwrap()
        .unwrap();
    assert!(
        snapshot.roots.is_empty(),
        "deleting generations must never be republished"
    );
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_deletion_uses_the_same_home_capacity() {
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("capacity").await;
    let permits: Vec<_> = (0..4)
        .map(|_| fixture.roots.try_delete_permit().unwrap())
        .collect();
    assert!(
        fixture
            .api
            .owner
            .peer_executor()
            .unwrap()
            .try_delete_permit()
            .is_err()
    );
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/capacity?expected_epoch=1",
                Value::Null
            )
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        fixture
            .request("GET", "/v1/workspaces/capacity", Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture.request("GET", "/healthz", Value::Null).await.0,
        StatusCode::OK
    );
    drop(permits);
    assert_eq!(
        fixture
            .request(
                "DELETE",
                "/v1/workspaces/capacity?expected_epoch=1",
                Value::Null
            )
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_delete_reply_is_reconciled_before_same_home_recreation() {
    let fixture = Fixture::new().await;
    fixture.prepare_workspace("lost-delete").await;
    fixture.lose_deactivate_reply.store(true, Ordering::SeqCst);
    let uri = "/v1/workspaces/lost-delete?expected_epoch=1";
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let api = fixture.api.clone();
    bridge::spawn_blocking(move || api.owner.maintain_workspace_deletions())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.prepare_workspace("lost-delete").await.epoch, 2);
    assert_eq!(
        fixture.request("DELETE", uri, Value::Null).await.0,
        StatusCode::CONFLICT
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ip_filtered_list_is_incomplete_when_home_session_expired() {
    let fixture = Fixture::new().await;
    let reservation = fixture.prepare_workspace("offline").await;
    use crate::meta::store::{
        MetaEntity, MetaRead, MetaTxn, OperationResult, RequestOutcome, StoreOperation,
        TxnCondition, TxnMutation,
    };
    let state = fixture
        .store
        .read(MetaRead::Root {
            root_id: reservation.id.0,
        })
        .await
        .unwrap();
    let Some(MetaEntity::Root(mut root)) = state.entity else {
        panic!("missing root");
    };
    let original = root.clone();
    root.home_node_id = "expired-home".into();
    fixture
        .store
        .register_node_session(
            RequestKey::new("expired-home", "register"),
            NodeSessionLease {
                node_id: "expired-home".into(),
                session_id: "old".into(),
                grpc_addr: "127.0.0.2:1".into(),
                data_addr: String::new(),
                rest_addr: String::new(),
                nfs_addr: "127.0.0.2".into(),
                ownerfs_enabled: true,
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_millis(1),
            },
        )
        .await
        .unwrap();
    let mut txn = MetaTxn::new(
        RequestKey::new("test", "move-home"),
        StoreOperation::RecoverRoot,
    );
    txn.conditions
        .push(TxnCondition::RequestAbsent(txn.request.clone()));
    txn.conditions
        .push(TxnCondition::EntityEquals(MetaEntity::Root(original)));
    txn.mutations.push(TxnMutation::Put(MetaEntity::Root(root)));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: txn.request.clone(),
            operation: txn.operation,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        fixture.store.compare_and_commit(txn).await.unwrap(),
        crate::meta::store::TxnOutcome::Committed { .. }
    ));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let (status, list) = fixture
        .request("GET", "/v1/workspaces?node_ip=127.0.0.2", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["total"], 0);
    assert_eq!(list["complete"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_begin_reply_keeps_the_epoch_fenced_and_retryable() {
    let fixture = Fixture::new().await;
    let root = fixture.prepare_workspace("lost-begin").await;
    fixture.lose_begin_reply.store(true, Ordering::SeqCst);
    let uri = format!("/v1/workspaces/lost-begin?expected_epoch={}", root.epoch);
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        fixture
            .roots
            .enter_root(&root.id, root::RootRight::Read)
            .is_err()
    );
    let (status, item) = fixture
        .request("GET", "/v1/workspaces/lost-begin", Value::Null)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item["state"], "deleting");
    let api = fixture.api.clone();
    assert!(
        bridge::spawn_blocking(move || api.meta.list_active_outlet_roots(None))
            .await
            .unwrap()
            .unwrap()
            .roots
            .is_empty()
    );
    assert_eq!(
        fixture
            .request("POST", "/v1/workspaces", json!({"owner":"lost-begin"}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        fixture.request("DELETE", &uri, Value::Null).await.0,
        StatusCode::OK
    );
}
