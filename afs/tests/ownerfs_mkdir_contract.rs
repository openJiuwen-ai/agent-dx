#![cfg(feature = "ownerfs")]

//! Persistent Meta + authenticated node-control contracts, without FUSE.

use std::{
    collections::HashMap, ffi::OsStr, os::unix::fs::MetadataExt, path::PathBuf, process::Command,
    sync::Arc, time::Duration,
};

use afs::{
    meta::{
        Meta,
        owner_roots::StoreOwnerRootAuthority,
        rpc::{MetaRpc, OwnerRootsRpc},
        store::{Store, local_log::LocalLog},
    },
    node::{
        rpc::{
            control::{NodeControlService, RdmaSessionRegistry},
            data::MtlsPeerAuthenticator,
            meta::GrpcRootMeta,
        },
        storage::LocalFs,
        vfs::ownerfs::{
            OwnerFs,
            root::{RootManager, root_id_from_name},
        },
    },
    runtime::Observability,
};
use afs_protocol::{
    meta::{
        AbortRootRequest, ActivateRootRequest, LookupRootReply, LookupRootRequest, NodeDescriptor,
        NodeEndpoint, RegisterNodeRequest, ReserveRootRequest, RootReservation, RootRight,
        meta_client::MetaClient, meta_server::MetaServer, owner_roots_client::OwnerRootsClient,
        owner_roots_server::OwnerRootsServer,
    },
    node_control::{PrepareOwnerRootRequest, node_control_client::NodeControlClient},
};
use afs_transport::{SecurityManager, TlsConfig};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Code,
    transport::{Channel, Endpoint, Server},
};

struct Certificates {
    dir: PathBuf,
    trusted: HashMap<Vec<u8>, String>,
}

impl Certificates {
    fn generate(dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let openssl = |args: &[&str]| {
            let output = Command::new("openssl")
                .current_dir(&dir)
                .args(args)
                .output()
                .expect("mkdir integration contracts require openssl");
            assert!(
                output.status.success(),
                "openssl {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        // A private config avoids duplicate CA extensions from the host config plus -addext.
        std::fs::write(
            dir.join("ca.cnf"),
            concat!(
                "[req]\ndistinguished_name=dn\nx509_extensions=ca\nprompt=no\n",
                "[dn]\nCN=AFS mkdir test CA\n[ca]\n",
                "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n",
            ),
        )
        .unwrap();
        openssl(&[
            "req",
            "-config",
            "ca.cnf",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=AFS mkdir test CA",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ]);
        let ca = Command::new("openssl")
            .current_dir(&dir)
            .args(["x509", "-in", "ca.pem", "-noout", "-text"])
            .output()
            .unwrap();
        assert!(ca.status.success());
        let ca_text = String::from_utf8_lossy(&ca.stdout);
        assert_eq!(ca_text.matches("X509v3 Basic Constraints").count(), 1);
        assert_eq!(ca_text.matches("X509v3 Key Usage").count(), 1);
        assert!(ca_text.contains("CA:TRUE"));
        std::fs::write(dir.join("leaf.ext"),
            "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n").unwrap();
        let mut trusted = HashMap::new();
        for (serial, node) in ["meta", "node-a", "node-b", "node-c", "unknown-node"]
            .into_iter()
            .enumerate()
        {
            openssl(&[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                &format!("/CN={node}"),
                "-keyout",
                &format!("{node}.key"),
                "-out",
                &format!("{node}.csr"),
            ]);
            openssl(&[
                "x509",
                "-req",
                "-in",
                &format!("{node}.csr"),
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-set_serial",
                &(serial + 1).to_string(),
                "-days",
                "2",
                "-extfile",
                "leaf.ext",
                "-out",
                &format!("{node}.pem"),
            ]);
            openssl(&[
                "x509",
                "-in",
                &format!("{node}.pem"),
                "-outform",
                "DER",
                "-out",
                &format!("{node}.der"),
            ]);
            if node != "meta" && node != "unknown-node" {
                trusted.insert(
                    std::fs::read(dir.join(format!("{node}.der"))).unwrap(),
                    node.into(),
                );
            }
        }
        Self { dir, trusted }
    }

    fn root_meta_tls(&self, node: &str) -> TlsConfig {
        TlsConfig::MutualTls {
            encryption_enabled: true,
            ca_certificate: self.dir.join("ca.pem"),
            identity_certificate: self.dir.join(format!("{node}.pem")),
            identity_private_key: self.dir.join(format!("{node}.key")),
            server_name: "localhost".into(),
        }
    }

    async fn channel(&self, endpoint: &str, node: &str) -> Channel {
        let security = SecurityManager::new(self.root_meta_tls(node)).unwrap();
        let endpoint = Endpoint::from_shared(security.endpoint_uri(endpoint))
            .unwrap()
            .timeout(Duration::from_secs(5));
        security.connect(endpoint).await.unwrap()
    }
}

fn root_name(root_id: &str) -> Vec<u8> {
    root_id
        .strip_prefix("root-")
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

// Pick fixture names that reach the intended Home with all three sessions eligible.
fn name_for_home(prefix: &str, home: &str) -> String {
    fn score(root_id: &str, node_id: &str) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"afs-ownerfs-placement-v1");
        for value in [root_id, node_id] {
            hasher.update(&u64::try_from(value.len()).unwrap().to_be_bytes());
            hasher.update(value.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }

    (0..1000)
        .map(|suffix| format!("{prefix}-{suffix}"))
        .find(|name| {
            let root_id = root_id_from_name(OsStr::new(name)).unwrap().0;
            let home_score = score(&root_id, home);
            ["node-a", "node-b", "node-c"]
                .into_iter()
                .filter(|node| *node != home)
                .all(|node| home_score > score(&root_id, node))
        })
        .expect("fixture name must select the intended Home")
}

struct Fixture {
    dir: TempDir,
    certs: Certificates,
    backend: Arc<LocalLog>,
    endpoint: String,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Home {
    endpoint: String,
    server: JoinHandle<()>,
}

impl Home {
    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
        // Dropping the final RootManager releases the catalog process lock.
    }
}

impl Fixture {
    async fn serve_meta(
        backend: Arc<LocalLog>,
        certs: &Certificates,
        listener: TcpListener,
    ) -> JoinHandle<()> {
        let store = Arc::new(Store::open_log(backend).await.unwrap());
        let mut meta = Meta::with_store_and_peer_identity(
            "mkdir-test".into(),
            Observability::new().unwrap(),
            store.clone(),
            certs.trusted.clone(),
            true,
        );
        meta.owner_roots = Arc::new(StoreOwnerRootAuthority::with_load_balancing(store, true));
        let meta = Arc::new(meta);
        let security = SecurityManager::new(certs.root_meta_tls("meta")).unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetaServer::new(MetaRpc(meta.clone())))
                .add_service(OwnerRootsServer::new(OwnerRootsRpc(meta)))
                .serve_with_incoming(security.incoming(TcpListenerStream::new(listener)).unwrap())
                .await
                .unwrap();
        })
    }

    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let tls_dir = dir.path().join("tls");
        let certs = tokio::task::spawn_blocking(move || Certificates::generate(tls_dir))
            .await
            .unwrap();
        let backend =
            Arc::new(LocalLog::open(dir.path().join("meta"), Default::default()).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let server = Self::serve_meta(backend.clone(), &certs, listener).await;
        let fixture = Self {
            dir,
            certs,
            backend,
            endpoint,
            server,
        };
        for node in ["node-a", "node-c"] {
            fixture
                .register(node, &format!("{node}-session"), "https://localhost:1")
                .await;
        }
        fixture
    }

    async fn restart_meta(&mut self) {
        self.server.abort();
        let _ = (&mut self.server).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        // Store::open_log loads durable bytes again; no acknowledged in-memory
        // Store snapshot or OwnerRootAuthority is reused by the new service.
        self.server = Self::serve_meta(self.backend.clone(), &self.certs, listener).await;
    }

    async fn owner_client(&self, node: &str) -> OwnerRootsClient<Channel> {
        OwnerRootsClient::new(self.certs.channel(&self.endpoint, node).await)
    }

    async fn register(&self, node: &str, session: &str, endpoint: &str) {
        MetaClient::new(self.certs.channel(&self.endpoint, node).await)
            .register_node(RegisterNodeRequest {
                request_id: format!("register-{node}-{session}"),
                node: Some(NodeDescriptor {
                    node_id: node.into(),
                    session_id: session.into(),
                    capabilities: vec!["ownerfs".into()],
                    endpoint: Some(NodeEndpoint {
                        grpc_addr: endpoint.into(),
                        data_addr: endpoint.into(),
                        rest_addr: "http://localhost:1".into(),
                    }),
                    ..Default::default()
                }),
                lease_seconds: 300,
            })
            .await
            .unwrap();
    }

    async fn home(&self, session: &str) -> Home {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        self.register("node-b", session, &endpoint).await;
        let meta = Arc::new(
            GrpcRootMeta::new(
                &self.endpoint,
                "node-b".into(),
                session.into(),
                Duration::from_secs(5),
                self.certs.root_meta_tls("node-b"),
            )
            .unwrap(),
        );
        let disk = Arc::new(LocalFs::open(self.dir.path().join("home-b")).unwrap());
        let session = session.to_owned();
        let open_disk = disk.clone();
        let roots = Arc::new(
            tokio::task::spawn_blocking(move || {
                RootManager::open("node-b".into(), session, meta, open_disk)
            })
            .await
            .unwrap()
            .unwrap(),
        );
        let fs = OwnerFs::new_local(roots, disk);
        let authenticator = Arc::new(
            MtlsPeerAuthenticator::new(
                self.certs
                    .trusted
                    .iter()
                    .map(|(cert, node)| (node.clone(), cert.clone())),
            )
            .unwrap(),
        );
        let service = NodeControlService::new(RdmaSessionRegistry::new(None))
            .with_owner_locks(fs.peer_executor().unwrap(), authenticator)
            .into_server();
        let security = SecurityManager::new(self.certs.root_meta_tls("node-b")).unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(security.incoming(TcpListenerStream::new(listener)).unwrap())
                .await
                .unwrap();
        });
        Home { endpoint, server }
    }

    fn reserve_request(&self, node: &str, name: &str, request_id: &str) -> ReserveRootRequest {
        let name = name_for_home(name, "node-b");
        let root_id = root_id_from_name(OsStr::new(&name)).unwrap().0;
        let requester_session_id = format!("{node}-session");
        ReserveRootRequest {
            request_id: request_id.into(),
            create_intent_id: format!("{node}:{requester_session_id}:{root_id}"),
            root_id,
            preferred_home_node_id: node.into(),
            session_id: requester_session_id,
            rights: vec![RootRight::Lookup as i32, RootRight::Write as i32],
            ..Default::default()
        }
    }

    async fn lookup(&self, node: &str, root: &str) -> LookupRootReply {
        self.owner_client(node)
            .await
            .lookup_root(LookupRootRequest {
                request_id: format!("lookup-{root}"),
                root_id: root.into(),
            })
            .await
            .unwrap()
            .into_inner()
    }

    fn prepare_request(&self, reservation: &RootReservation) -> PrepareOwnerRootRequest {
        let owner = self.dir.path().metadata().unwrap();
        PrepareOwnerRootRequest {
            reservation: Some(reservation.clone()),
            name: root_name(&reservation.root_id),
            uid: owner.uid(),
            gid: owner.gid(),
            mode: 0o750,
        }
    }

    async fn prepare(&self, home: &Home, node: &str, reservation: &RootReservation) {
        NodeControlClient::new(self.certs.channel(&home.endpoint, node).await)
            .prepare_owner_root(self.prepare_request(reservation))
            .await
            .unwrap();
    }

    async fn reject_prepare(&self, home: &Home, node: &str, reservation: &RootReservation) {
        let error = NodeControlClient::new(self.certs.channel(&home.endpoint, node).await)
            .prepare_owner_root(self.prepare_request(reservation))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    fn abort_request(reservation: &RootReservation, request_id: &str) -> AbortRootRequest {
        AbortRootRequest {
            request_id: request_id.into(),
            root_id: reservation.root_id.clone(),
            root_epoch: reservation.root_epoch,
            session_id: reservation.session_id.clone(),
            create_intent_id: reservation.create_intent_id.clone(),
            prepare_token: reservation.prepare_token.clone(),
            reason: "mkdir contract".into(),
        }
    }

    fn data_dir(&self, reservation: &RootReservation) -> PathBuf {
        self.dir.path().join("home-b").join(format!(
            "{}-e{}",
            reservation.root_id, reservation.root_epoch
        ))
    }

    fn catalog_count(&self) -> usize {
        std::fs::read_dir(self.dir.path().join("home-b/.ownerfs-roots"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension() == Some(OsStr::new("json")))
            .count()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_mtls_mkdir_has_one_winner_and_distinct_pending_active_errors() {
    let fixture = Fixture::new().await;
    let home = fixture.home("home-session").await;
    let mut a = fixture.owner_client("node-a").await;
    let mut c = fixture.owner_client("node-c").await;
    let a_request = fixture.reserve_request("node-a", "race", "a-race");
    let c_request = fixture.reserve_request("node-c", "race", "c-race");
    let (a, c) = tokio::join!(a.reserve_root(a_request), c.reserve_root(c_request));
    let (node, reservation, error) = match (a, c) {
        (Ok(reply), Err(error)) => ("node-a", reply.into_inner().reservation.unwrap(), error),
        (Err(error), Ok(reply)) => ("node-c", reply.into_inner().reservation.unwrap(), error),
        results => panic!("exactly one requester must win: {results:?}"),
    };
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(reservation.home_node_id, "node-b");
    fixture.prepare(&home, node, &reservation).await;
    assert_eq!(fixture.catalog_count(), 1);
    let state = fixture.lookup("node-b", &reservation.root_id).await;
    assert!(state.found);
    assert!(state.reservation.is_none());
    let error = fixture
        .owner_client("node-a")
        .await
        .reserve_root(fixture.reserve_request("node-a", "race", "after-activation"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    home.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_mtls_prepare_replay_preserves_identity_and_permissions() {
    let fixture = Fixture::new().await;
    let home = fixture.home("home-session").await;
    let mut a = fixture.owner_client("node-a").await;
    let reservation = a
        .reserve_root(fixture.reserve_request("node-a", "replay", "first"))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    for node in ["node-a", "node-b"] {
        let pending = fixture.lookup(node, &reservation.root_id).await;
        assert_eq!(pending.reservation.as_ref(), Some(&reservation));
        assert!(!pending.found);
    }
    assert!(
        fixture
            .lookup("node-c", &reservation.root_id)
            .await
            .reservation
            .is_none()
    );
    let unauthorized =
        NodeControlClient::new(fixture.certs.channel(&home.endpoint, "unknown-node").await)
            .prepare_owner_root(fixture.prepare_request(&reservation))
            .await
            .unwrap_err();
    assert_eq!(
        afs_transport::grpc::error_status::status_to_error(unauthorized).code(),
        afs_error::NODE_OWNER_INVALID_GRANT
    );
    assert!(!fixture.data_dir(&reservation).exists());
    let reused = a
        .reserve_root(fixture.reserve_request("node-a", "replay", "retry"))
        .await
        .unwrap()
        .into_inner();
    assert!(reused.already_reserved_by_same_intent);
    assert_eq!(reused.reservation.as_ref(), Some(&reservation));
    fixture.prepare(&home, "node-a", &reservation).await;
    let duplicate = NodeControlClient::new(fixture.certs.channel(&home.endpoint, "node-a").await)
        .prepare_owner_root(PrepareOwnerRootRequest {
            reservation: Some(reservation.clone()),
            name: root_name(&reservation.root_id),
            uid: 1,
            gid: 1,
            mode: 0o777,
        })
        .await
        .unwrap_err();
    assert_eq!(duplicate.code(), Code::AlreadyExists);
    assert_eq!(fixture.catalog_count(), 1);
    let stat = fixture.data_dir(&reservation).metadata().unwrap();
    let owner = fixture.dir.path().metadata().unwrap();
    assert_eq!(
        (stat.uid(), stat.gid(), stat.mode() & 0o777),
        (owner.uid(), owner.gid(), 0o750)
    );
    home.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_mtls_activate_and_abort_cannot_both_commit() {
    let fixture = Fixture::new().await;
    let home = fixture.home("home-session").await;
    let reservation = fixture
        .owner_client("node-a")
        .await
        .reserve_root(fixture.reserve_request("node-a", "activate-abort", "reserve"))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    let mut activate = fixture.owner_client("node-b").await;
    let mut abort = fixture.owner_client("node-b").await;
    let activate_request = ActivateRootRequest {
        request_id: "activate".into(),
        reservation: Some(reservation.clone()),
        local_prepare_id: "persisted-prepare-evidence".into(),
        parent_fsync_generation: 1,
        parent_fsync_complete: true,
    };
    let abort_request = Fixture::abort_request(&reservation, "abort");
    let (activated, aborted) = tokio::join!(
        activate.activate_root(activate_request),
        abort.abort_root(abort_request)
    );
    assert_ne!(activated.is_ok(), aborted.is_ok());
    let state = fixture.lookup("node-b", &reservation.root_id).await;
    assert!(state.reservation.is_none());
    assert_eq!(state.found, activated.is_ok());
    home.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_mtls_meta_reopen_preserves_active_pending_and_reserve_replay() {
    let mut fixture = Fixture::new().await;
    let home = fixture.home("home-session").await;
    let active_request = fixture.reserve_request("node-a", "active-reopen", "active");
    let active = fixture
        .owner_client("node-a")
        .await
        .reserve_root(active_request)
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    fixture.prepare(&home, "node-a", &active).await;
    let before = fixture.lookup("node-b", &active.root_id).await;
    let pending_request = fixture.reserve_request("node-a", "pending-reopen", "pending");
    let pending = fixture
        .owner_client("node-a")
        .await
        .reserve_root(pending_request.clone())
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    home.stop().await;

    fixture.restart_meta().await;

    let recovered = fixture.lookup("node-b", &active.root_id).await;
    assert_eq!(recovered.location, before.location);
    let recovered_pending = fixture.lookup("node-b", &pending.root_id).await;
    assert_eq!(recovered_pending.reservation.as_ref(), Some(&pending));
    assert!(!recovered_pending.found);
    let replay = fixture
        .owner_client("node-a")
        .await
        .reserve_root(pending_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.reservation.as_ref(), Some(&pending));
    assert!(replay.already_reserved_by_same_intent);
    assert!(fixture.data_dir(&active).is_dir());
    assert!(!fixture.data_dir(&pending).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_mtls_expired_requester_takeover_rejects_late_prepare() {
    let fixture = Fixture::new().await;
    let home = fixture.home("home-session").await;
    let old = fixture
        .owner_client("node-a")
        .await
        .reserve_root(fixture.reserve_request("node-a", "takeover", "old"))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    let error = fixture
        .owner_client("node-c")
        .await
        .reserve_root(fixture.reserve_request("node-c", "takeover", "while-a-live"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    // A new process fences the prior session without timing-dependent sleeps.
    fixture
        .register("node-a", "node-a-restarted", "https://localhost:1")
        .await;
    let replacement = fixture
        .owner_client("node-c")
        .await
        .reserve_root(fixture.reserve_request("node-c", "takeover", "after-a-fenced"))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    assert!(replacement.root_epoch > old.root_epoch);
    assert_ne!(replacement.prepare_token, old.prepare_token);
    fixture.reject_prepare(&home, "node-a", &old).await;
    assert!(!fixture.data_dir(&old).exists());
    assert_eq!(
        fixture
            .lookup("node-b", &old.root_id)
            .await
            .reservation
            .as_ref(),
        Some(&replacement)
    );
    fixture.prepare(&home, "node-c", &replacement).await;
    let active = fixture.lookup("node-b", &replacement.root_id).await;
    assert!(active.found);
    assert_eq!(active.location.unwrap().root_epoch, replacement.root_epoch);
    let error = fixture
        .owner_client("node-b")
        .await
        .abort_root(Fixture::abort_request(&old, "late-old-abort"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        fixture
            .lookup("node-b", &old.root_id)
            .await
            .location
            .unwrap()
            .root_epoch,
        replacement.root_epoch
    );
    assert_eq!(fixture.catalog_count(), 1);
    home.stop().await;
}
