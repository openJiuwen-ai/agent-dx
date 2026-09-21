mod common;
use adx_core::{scheduling::DeviceAllocation, CapsuleRecord, CapsuleSpec, Resources, Result};
use adx_master::{routes::RoutePublisher, rpc::MasterRpc, storage::Session, Placement};
use adx_node_manager::{
    rpc::{MasterStateSink, NodeRpc},
    NodeManager, Readiness, Routes, RuntimeDriver,
};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Identity, Server, ServerTlsConfig},
    Request,
};

async fn scrape_metrics(address: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            if response.starts_with("HTTP/1.1 200") {
                return response;
            }
            assert!(response.starts_with("HTTP/1.1 503"), "{response}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

fn file(name: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(
            std::env::var("ADX_TEST_TLS_DIR")
                .expect("run control-rpc suite to generate test certificates"),
        )
        .join(name),
    )
    .unwrap()
}
fn identity(name: &str) -> Identity {
    Identity::from_pem(file(&format!("{name}.pem")), file(&format!("{name}.key")))
}
fn client_tls(name: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(file("ca.pem")))
        .identity(identity(name))
        .domain_name("localhost")
}
fn server_tls(name: &str) -> ServerTlsConfig {
    ServerTlsConfig::new()
        .identity(identity(name))
        .client_ca_root(Certificate::from_pem(file("ca.pem")))
}
fn peers() -> Peers {
    Peers::new(
        [
            ("master", Principal::Master),
            ("node", Principal::Node("node".into())),
            ("api-server", Principal::ApiServer),
            ("edge", Principal::Edge),
        ]
        .map(|(name, p)| (file(&format!("{name}.der")), p)),
    )
}
async fn channel(address: std::net::SocketAddr, name: &str) -> Channel {
    tonic::transport::Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .tls_config(client_tls(name))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap()
}
fn spec(id: &str) -> CapsuleSpec {
    CapsuleSpec {
        environment: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        sandbox: Default::default(),
    }
}
fn caller() -> Option<pb::CallerContext> {
    Some(pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    })
}
fn create(id: &str) -> pb::CreateCapsuleRequest {
    pb::CreateCapsuleRequest {
        spec: Some(spec(id).into()),
        caller: caller(),
        schedule_timeout_seconds: 30,
        create_timeout_seconds: 90,
    }
}
struct Backend {
    session: Session,
    started: AtomicUsize,
    removed: AtomicUsize,
    running: Mutex<BTreeSet<String>>,
}
#[async_trait::async_trait]
impl RuntimeDriver for Backend {
    async fn inventory(&self) -> Result<Vec<adx_node_manager::RuntimeObservation>> {
        Ok(self
            .running
            .lock()
            .unwrap()
            .iter()
            .map(|id| {
                let (capsule, generation) = id.rsplit_once('-').unwrap();
                adx_node_manager::RuntimeObservation {
                    capsule_id: capsule.into(),
                    runtime_id: id.clone(),
                    generation: generation.parse().unwrap(),
                    tenant_id: "tenant".into(),
                    running: true,
                }
            })
            .collect())
    }

    async fn start(
        &self,
        spec: &CapsuleSpec,
        id: &str,
        generation: u64,
        _: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        // Observe real Redis at the instant the execution boundary is crossed.
        let allocation = self.session.get(&spec.id).await?;
        assert_eq!(allocation.assignment.generation, generation);
        assert_eq!(allocation.spec, *spec);
        self.started.fetch_add(1, Ordering::SeqCst);
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, id: &str, path: &std::path::Path, _: Duration) -> Result<()> {
        std::fs::write(path.join("memory"), b"saved runtime state").unwrap();
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
    async fn restore_from(
        &self,
        spec: &CapsuleSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &std::path::Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        if let Some(origin) = origin {
            assert_ne!(origin.capsule_id, spec.id);
            let snapshot = self
                .session
                .get_snapshot(spec.snapshot_id.as_deref().unwrap())
                .await?;
            assert_eq!(&snapshot.origin()?, origin);
            assert!(snapshot
                .references
                .contains(&adx_core::snapshots::Reference::Restore {
                    capsule_id: spec.id.clone()
                }));
        }
        self.restore(spec, id, generation, devices, path).await
    }
    async fn restore(
        &self,
        spec: &CapsuleSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &std::path::Path,
    ) -> Result<std::net::IpAddr> {
        assert_eq!(
            std::fs::read(path.join("memory")).unwrap(),
            b"saved runtime state"
        );
        self.start(spec, id, generation, devices).await
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains(id))
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.removed.fetch_add(1, Ordering::SeqCst);
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
}
struct LocalChecks;
#[async_trait::async_trait]
impl Readiness for LocalChecks {
    async fn wait_ready(&self, _: &CapsuleRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl Routes for LocalChecks {
    async fn retire_orphan(&self, _: &adx_node_manager::RuntimeObservation) -> Result<()> {
        Ok(())
    }

    async fn activate(&self, _: &CapsuleRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &CapsuleRecord) -> Result<()> {
        Ok(())
    }
}
struct Servers(Vec<tokio::task::JoinHandle<()>>);
impl Drop for Servers {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn lifecycle_rpc_persists_before_execution_and_retries_only_the_result() {
    let mut redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let auth = adx_master::auth::AuthRpc::new(session.clone(), peers());
    let master = MasterRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let metrics_rpc = master.clone();
    let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_address = metrics_listener.local_addr().unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let publication = RoutePublisher::new(session.clone(), peers());
    publication.refresh().await.unwrap();
    let updater = publication.clone();
    let mut servers = Servers(vec![
        tokio::spawn(updater.run(Duration::from_millis(20))),
        tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls("master"))
                .unwrap()
                .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
                    master.clone(),
                ))
                .add_service(pb::master_service_server::MasterServiceServer::new(master))
                .add_service(pb::credential_service_server::CredentialServiceServer::new(
                    auth.clone(),
                ))
                .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                .add_service(
                    pb::capsule_directory_service_server::CapsuleDirectoryServiceServer::new(
                        publication,
                    ),
                )
                .serve_with_incoming(TcpListenerStream::new(ml))
                .await
                .unwrap();
        }),
    ]);
    servers.0.push(tokio::spawn(async move {
        adx_master::metrics::serve(metrics_listener, metrics_rpc)
            .await
            .unwrap();
    }));
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let sink = Arc::new(
        MasterStateSink::new(channel(ma, "node").await, Duration::from_secs(2))
            .unwrap()
            .with_session("boot-1".into()),
    );
    let checkpoint_root = tempfile::tempdir().unwrap();
    let checkpoint_store = Arc::new(
        adx_node_manager::checkpoint::LocalCheckpointStore::new(checkpoint_root.path().into())
            .unwrap(),
    );
    let manager = Arc::new(
        NodeManager::new(
            "node".into(),
            backend.clone(),
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            sink.clone(),
        )
        .with_checkpointing(checkpoint_store, Arc::new(LocalChecks))
        .unwrap()
        .with_snapshot_catalog(sink)
        .unwrap(),
    );
    manager
        .update_capacity(spec("i").resources, Duration::from_secs(60))
        .unwrap();
    let node = NodeRpc::new(manager.clone(), peers(), "boot-1".into());
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    let mut node_master =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "node").await);
    let mut register = pb::RegisterNodeRequest {
        session_id: "boot-1".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        node_id: "node".into(),
        node_address: na.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("i").resources.into()),
        accepting_allocations: false,
        labels: Default::default(),
        devices: vec![],
    };
    let mut forged = register.clone();
    forged.node_id = "other".into();
    assert_eq!(
        node_master.register_node(forged).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    node_master.register_node(register.clone()).await.unwrap();
    node_master
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot-1".into(),
        })
        .await
        .unwrap();
    register.heartbeat_sequence += 1;
    register.reconciling = false;
    register.accepting_allocations = true;
    node_master.register_node(register.clone()).await.unwrap();
    let mut frontend =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "api-server").await);
    let mut duplicate = frontend.clone();
    let (a, b) = tokio::join!(
        frontend.create_capsule(create("first")),
        duplicate.create_capsule(create("first"))
    );
    let first = a.unwrap().into_inner();
    assert_eq!(first, b.unwrap().into_inner());
    assert_eq!(first.durability, pb::Durability::Published as i32);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    let first_record = first.record.unwrap();
    let metrics = scrape_metrics(metrics_address).await;
    assert!(metrics
        .contains("adx_master_capsules{shard_id=\"0\",node_id=\"node\",state=\"Running\"} 1\n"));
    assert!(metrics
        .contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"node\"} 100\n"));

    let mut stale_master =
        pb::node_service_client::NodeServiceClient::new(channel(na, "master").await);
    assert_eq!(
        stale_master
            .create_capsule(pb::StartAssignedCapsuleRequest {
                snapshot: None,
                spec: Some(spec("late").into()),
                assignment: first_record.assignment.clone(),
                node_session_id: "old-boot".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    let assignment = first_record.assignment.clone().unwrap();
    let get = pb::GetCapsuleRequest {
        capsule_id: "first".into(),
        caller: caller(),
    };
    assert_eq!(
        frontend
            .get_capsule(get.clone())
            .await
            .unwrap()
            .into_inner()
            .node_address,
        na.to_string()
    );
    let mut other = get.clone();
    other.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        frontend.get_capsule(other).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut unknown =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "unknown").await);
    assert_eq!(
        unknown.get_capsule(get).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        frontend
            .commit_capsule(pb::CommitCapsuleRequest {
                node_session_id: "boot-1".into(),
                record: Some(first_record.clone())
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut node_frontend =
        pb::node_service_client::NodeServiceClient::new(channel(na, "api-server").await);
    assert_eq!(
        node_frontend
            .create_capsule(pb::StartAssignedCapsuleRequest {
                snapshot: None,
                node_session_id: "boot-1".into(),
                spec: Some(spec("first").into()),
                assignment: Some(assignment.clone())
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut pause = pb::PauseCapsuleRequest {
        assignment: Some(assignment.clone()),
        caller: caller(),
        operation_id: "pause-auth".into(),
        expected_revision: 2,
        ttl_seconds: 600,
        timeout_seconds: 60,
    };
    assert_eq!(
        stale_master
            .pause_capsule(pause.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    pause.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        node_frontend.pause_capsule(pause).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut resume = pb::ResumeCapsuleRequest {
        assignment: Some(assignment.clone()),
        caller: caller(),
        operation_id: "resume-auth".into(),
        expected_revision: 2,
    };
    assert_eq!(
        stale_master
            .resume_capsule(resume.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    resume.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        node_frontend
            .resume_capsule(resume)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut bad_delete = pb::DeleteCapsuleRequest {
        assignment: Some(assignment.clone()),
        caller: caller(),
    };
    bad_delete.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        node_frontend
            .delete_capsule(bad_delete)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut stale = assignment.clone();
    stale.generation += 1;
    assert_eq!(
        node_frontend
            .delete_capsule(pb::DeleteCapsuleRequest {
                assignment: Some(stale),
                caller: caller()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut expires = create("queue-deadline");
    expires.schedule_timeout_seconds = 1;
    let started = tokio::time::Instant::now();
    assert_eq!(
        frontend.create_capsule(expires).await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert_eq!(
        session.get("queue-deadline").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    assert!(scrape_metrics(metrics_address)
        .await
        .contains("adx_master_queued_requests{shard_id=\"0\"} 0\n"));

    let mut pending = Request::new(create("second"));
    pending.set_timeout(Duration::from_millis(80));
    assert!(frontend.create_capsule(pending).await.is_err());
    assert!(scrape_metrics(metrics_address)
        .await
        .contains("adx_master_queued_requests{shard_id=\"0\"} 1\n"));

    assert_eq!(
        session.get("second").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    let delete = pb::DeleteCapsuleRequest {
        assignment: Some(assignment),
        caller: caller(),
    };
    node_frontend.delete_capsule(delete.clone()).await.unwrap();
    node_frontend.delete_capsule(delete).await.unwrap();
    // The caller timed out, but the accepted creation continues after release.
    let second = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Ok(record) = session.get("second").await {
                if let Some(result) = record.result {
                    break result;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(backend.started.load(Ordering::SeqCst), 2);
    assert_eq!(backend.removed.load(Ordering::SeqCst), 1);
    assert_eq!(
        node_master
            .commit_capsule(pb::CommitCapsuleRequest {
                node_session_id: "boot-1".into(),
                record: Some(first_record)
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    redis.crash();
    let delete = pb::DeleteCapsuleRequest {
        assignment: Some(second.assignment.try_into().unwrap()),
        caller: caller(),
    };
    assert!(node_frontend.delete_capsule(delete.clone()).await.is_err());
    assert_eq!(backend.removed.load(Ordering::SeqCst), 2);
    redis.start().await;
    node_frontend.delete_capsule(delete).await.unwrap();
    assert_eq!(
        backend.removed.load(Ordering::SeqCst),
        2,
        "commit retry must not delete runtime twice"
    );
    assert!(session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
    assert!(backend.running.lock().unwrap().is_empty());
    let metrics = scrape_metrics(metrics_address).await;
    assert!(metrics
        .contains("adx_master_capsules{shard_id=\"0\",node_id=\"node\",state=\"Running\"} 0\n"));
    assert!(metrics.contains("adx_master_deleted_records 2\n"));
    assert!(metrics
        .contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"node\"} 0\n"));

    if let Ok(api_binary) = std::env::var("ADX_TEST_API_SERVER") {
        use adx_master::auth::Credential;
        for (key, tenant) in [("a".repeat(40), "tenant"), ("b".repeat(40), "other")] {
            session
                .bootstrap_credential(
                    &key,
                    &Credential {
                        tenant_id: tenant.into(),
                        administrator: false,
                        expires_at_unix_seconds: 0,
                    },
                )
                .await
                .unwrap();
        }
        session
            .bootstrap_credential(
                &"c".repeat(40),
                &Credential {
                    tenant_id: "tenant".into(),
                    administrator: false,
                    expires_at_unix_seconds: 1,
                },
            )
            .await
            .unwrap();
        session
            .bootstrap_credential(
                &"d".repeat(40),
                &Credential {
                    tenant_id: "admin".into(),
                    administrator: true,
                    expires_at_unix_seconds: 0,
                },
            )
            .await
            .unwrap();
        let capacity = Resources {
            cpu_millis: 1000,
            memory_bytes: 8 * 1024 * 1024,
            disk_bytes: 0,
        };
        manager
            .update_capacity(capacity, Duration::from_secs(60))
            .unwrap();
        let mut update = register.clone();
        update.heartbeat_sequence += 1;
        update.capacity = Some(capacity.into());
        node_master.register_node(update).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
        session
            .advertise("test", &format!("https://{ma}"), Duration::from_secs(60))
            .await
            .unwrap();
        let agent_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let agent_address = agent_listener.local_addr().unwrap();
        drop(agent_listener);
        let config = serde_json::json!({"agent_address":format!("http://{agent_address}"),"listen":address.to_string(),"discovery":{"redis_url":redis.url,"namespace":"test","poll_seconds":1},"ca":tls.join("ca.pem"),"certificate":tls.join("api-server.pem"),"private_key":tls.join("api-server.key"),"server_name":"localhost","rpc_timeout_seconds":3,"cache_entries":128,"auth_cache_ttl_seconds":1});
        let path = directory.path().join("api.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let evidence = PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap());
        let log = std::fs::File::create(evidence.join("api-server.log")).unwrap();
        let mut process = tokio::process::Command::new(api_binary)
            .args(["--config", path.to_str().unwrap()])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../build/ci/api_http.py");
        let result = tokio::process::Command::new("python3")
            .arg(script)
            .env("ADX_TEST_API_ENDPOINT", format!("https://{address}"))
            .env("ADX_TEST_AGENT_PORT", agent_address.port().to_string())
            .output()
            .await
            .unwrap();
        std::fs::write(
            evidence.join("api-http.log"),
            [result.stdout.clone(), result.stderr.clone()].concat(),
        )
        .unwrap();
        process.kill().await.unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let record = session.get("t-http-case").await.unwrap();
        assert_eq!(record.spec.tenant_id, "tenant");
        assert_eq!(record.spec.env["USER_VALUE"], "propagated");
        let peer = session.get("t-http-peer").await.unwrap();
        assert_eq!(peer.spec.scheduling.labels["app"], "db");
        let client = session.get("t-http-affinity").await.unwrap();
        assert_eq!(client.spec.scheduling.placement_groups.len(), 3);
        assert!(client
            .spec
            .scheduling
            .placement_groups
            .iter()
            .any(|g| g.ordered));
        assert!(client
            .spec
            .scheduling
            .placement_groups
            .iter()
            .any(|g| g.required && g.terms.len() == 2));
        assert!(client
            .spec
            .scheduling
            .placement_groups
            .iter()
            .any(|g| g.anti && g.terms[0].weight == 7));
        assert!(backend.running.lock().unwrap().is_empty());
        assert!(session
            .snapshot()
            .await
            .unwrap()
            .routes()
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn master_process_loads_configuration_and_restores_bootstrap_credentials() {
    let redis = common::Redis::new().await;
    let directory = tempfile::tempdir().unwrap();
    let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_address = metrics_listener.local_addr().unwrap();
    drop(metrics_listener);
    let key = "master-process-test-key-".repeat(3);
    let key_file = directory.path().join("bootstrap.key");
    std::fs::write(&key_file, &key).unwrap();
    let config = serde_json::json!({
        "listen":address.to_string(),"metrics_listen":metrics_address.to_string(),"advertised_address":format!("https://{address}"),"discovery_ttl_seconds":3,"redis_url":redis.url,"namespace":"process","scheduler_shards":1,"placement":"pack","rpc_timeout_seconds":2,
        "tls":{"ca":tls.join("ca.pem"),"certificate":tls.join("master.pem"),"private_key":tls.join("master.key"),"server_name":"localhost","peers":{"api-server":tls.join("api-server.der"),"node:node":tls.join("node.der")}},
        "bootstrap_credentials":[{"key_file":key_file,"tenant_id":"admin","administrator":true,"expires_at_unix_seconds":0}]
    });
    let path = directory.path().join("master.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    for index in 0..2 {
        let log = std::fs::File::create(
            PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap())
                .join(format!("master-process-{index}.log")),
        )
        .unwrap();
        let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_adx-master"))
            .args(["--config", path.to_str().unwrap()])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let connection = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    process.try_wait().unwrap().is_none(),
                    "Master startup failed; inspect process log"
                );
                let endpoint =
                    tonic::transport::Endpoint::from_shared(format!("https://{address}"))
                        .unwrap()
                        .tls_config(client_tls("api-server"))
                        .unwrap();
                if let Ok(channel) = endpoint.connect().await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(scrape_metrics(metrics_address)
            .await
            .contains("adx_master_deleted_records 0\n"));
        let discovery =
            adx_discovery::RedisDiscovery::new(&redis.url, "process", Duration::from_secs(1))
                .unwrap();
        let endpoint = discovery.lookup().await.unwrap();
        assert_eq!(endpoint.address, format!("https://{address}"));
        assert_eq!(endpoint.epoch, index + 1);
        let mut auth = pb::auth_service_client::AuthServiceClient::new(connection.clone());
        let credential = auth
            .verify_api_key(pb::VerifyApiKeyRequest {
                api_key: key.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            credential.caller.unwrap(),
            pb::CallerContext {
                tenant_id: "admin".into(),
                administrator: true
            }
        );
        let mut keys = pb::credential_service_client::CredentialServiceClient::new(connection);
        let page = keys
            .list_tenant_keys(pb::ListTenantKeysRequest {
                caller: Some(pb::CallerContext {
                    tenant_id: "admin".into(),
                    administrator: true,
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            page.keys.is_empty(),
            "administrator bootstrap key is not a tenant key"
        );
        process.kill().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates; run control-rpc suite"]
async fn heartbeat_expiry_reconciliation_and_old_session_fencing() {
    use adx_core::{Assignment, CapsuleState};
    let redis = common::Redis::new().await;
    let store = redis.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = MasterRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
        Duration::from_millis(250),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = rpc.clone();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::master_service_server::MasterServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut node =
        pb::master_service_client::MasterServiceClient::new(channel(address, "node").await);
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: "127.0.0.1:9001".into(),
        proxy_address: "127.0.0.1:9002".into(),
        capacity: Some(spec("held").resources.into()),
        accepting_allocations: true,
        session_id: "first-boot".into(),
        heartbeat_sequence: 1,
        ..Default::default()
    };
    assert_eq!(
        node.register_node(report.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    report.accepting_allocations = false;
    report.reconciling = true;
    node.register_node(report.clone()).await.unwrap();
    assert!(node
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "wrong".into()
        })
        .await
        .is_err());
    let catalog = node
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(catalog.records.is_empty());
    assert_eq!(catalog.master_epoch, session.epoch());
    report.heartbeat_sequence = 2;
    report.accepting_allocations = true;
    report.reconciling = false;
    node.register_node(report.clone()).await.unwrap();
    let assigned = Assignment {
        capsule_id: "held".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assigned.clone())
        .await
        .unwrap();
    let running = CapsuleRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: spec("held"),
        assignment: assigned,
        state: CapsuleState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: "held-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    };
    session.commit(running.clone()).await.unwrap();
    assert_eq!(session.snapshot().await.unwrap().routes().unwrap().len(), 1);
    // Duplicate packets acknowledge the same request but cannot extend liveness.
    tokio::time::sleep(Duration::from_millis(180)).await;
    node.register_node(report.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(rpc.expire_nodes().await.unwrap(), 1);
    let snapshot = session.snapshot().await.unwrap();
    assert!(!snapshot.nodes["node"].node.available);
    assert!(snapshot.routes().unwrap().is_empty());
    assert!(!snapshot.capsules["held"].resources_held());
    let metrics = rpc.metrics().await.unwrap();
    assert!(metrics.contains(
        "adx_master_capsules{shard_id=\"0\",node_id=\"node\",state=\"Invalidated\"} 1\n"
    ));
    assert!(metrics
        .contains("adx_master_node_available_cpu_millis{shard_id=\"0\",node_id=\"node\"} 0\n"));

    assert_eq!(
        snapshot.capsules["held"].result.as_ref().unwrap().state,
        CapsuleState::Failed
    );
    report.heartbeat_sequence = 3;
    assert_eq!(
        node.register_node(report.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    report.session_id = "second-boot".into();
    report.heartbeat_sequence = 1;
    report.accepting_allocations = false;
    report.reconciling = true;
    node.register_node(report.clone()).await.unwrap();
    let snapshot = node
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(snapshot.records.len(), 1);
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::new(BTreeSet::from(["held-1".into()])),
    });
    let manager = NodeManager::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            MasterStateSink::new(channel(address, "node").await, Duration::from_secs(2))
                .unwrap()
                .with_session("second-boot".into()),
        ),
    );
    manager
        .reconcile(
            snapshot
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(manager.used(), adx_core::Resources::default());
    assert!(backend.running.lock().unwrap().is_empty());
    assert_eq!(backend.started.load(Ordering::SeqCst), 0);

    assert_eq!(
        node.commit_capsule(pb::CommitCapsuleRequest {
            record: Some(running.clone().try_into().unwrap()),
            node_session_id: "first-boot".into()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        node.commit_capsule(pb::CommitCapsuleRequest {
            record: Some(running.try_into().unwrap()),
            node_session_id: "second-boot".into(),
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    report.heartbeat_sequence = 2;
    report.accepting_allocations = true;
    report.reconciling = false;
    node.register_node(report.clone()).await.unwrap();
    assert!(session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
    let new_session = store.begin(1).await.unwrap();
    let _new = MasterRpc::new(
        new_session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert!(new_session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates; run control-rpc suite"]
async fn node_restart_before_expiry_requires_new_session_at_registered_endpoint() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let master = MasterRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::master_service_server::MasterServiceServer::new(master))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let mut node_master =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "node").await);
    let manager = |boot: &str, channel| {
        Arc::new(NodeManager::new(
            "node".into(),
            backend.clone(),
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            Arc::new(
                MasterStateSink::new(channel, Duration::from_secs(2))
                    .unwrap()
                    .with_session(boot.into()),
            ),
        ))
    };
    let serve_node = |listener: TcpListener, manager: Arc<NodeManager>, boot: &str| {
        let node = NodeRpc::new(manager, peers(), boot.into());
        tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls("node"))
                .unwrap()
                .add_service(pb::node_service_server::NodeServiceServer::new(node))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        })
    };
    let first = manager("boot-1", channel(ma, "node").await);
    first
        .update_capacity(spec("held").resources, Duration::from_secs(60))
        .unwrap();
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    servers.0.push(serve_node(nl, first.clone(), "boot-1"));
    let mut probe = pb::node_service_client::NodeServiceClient::new(channel(na, "master").await);
    let identity = probe
        .get_session(pb::GetNodeSessionRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(identity.node_id, "node");
    assert_eq!(identity.session_id, "boot-1");
    let mut untrusted =
        pb::node_service_client::NodeServiceClient::new(channel(na, "api-server").await);
    assert_eq!(
        untrusted
            .get_session(pb::GetNodeSessionRequest {})
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: na.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("held").resources.into()),
        session_id: "boot-1".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        ..Default::default()
    };
    node_master.register_node(report.clone()).await.unwrap();
    node_master
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot-1".into(),
        })
        .await
        .unwrap();
    report.heartbeat_sequence = 2;
    report.reconciling = false;
    report.accepting_allocations = true;
    node_master.register_node(report.clone()).await.unwrap();
    let mut frontend =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "api-server").await);
    let running = frontend
        .create_capsule(create("held"))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let old_report = report.clone();
    report.session_id = "boot-2".into();
    report.heartbeat_sequence = 1;
    report.reconciling = true;
    report.accepting_allocations = false;
    // A second process claiming the live endpoint cannot replace its actual owner.
    assert_eq!(
        node_master
            .register_node(report.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(session.snapshot().await.unwrap().routes().unwrap().len(), 1);

    let second = manager("boot-2", channel(ma, "node").await);
    second
        .update_capacity(spec("held").resources, Duration::from_secs(60))
        .unwrap();
    let alternate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let alternate_address = alternate.local_addr().unwrap();
    servers
        .0
        .push(serve_node(alternate, second.clone(), "boot-2"));
    let mut moved = report.clone();
    moved.node_address = alternate_address.to_string();
    assert_eq!(
        node_master.register_node(moved).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );

    // Restart on the registered endpoint, without aging out the original heartbeat.
    servers.0[1].abort();
    assert!((&mut servers.0[1]).await.as_ref().is_err());
    let nl = TcpListener::bind(na).await.unwrap();
    servers.0.push(serve_node(nl, second.clone(), "boot-2"));
    node_master
        .register_node(report.clone())
        .await
        .expect("live endpoint now belongs to restarted process");
    assert!(session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
    assert_eq!(
        node_master
            .register_node(old_report)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        node_master
            .commit_capsule(pb::CommitCapsuleRequest {
                record: Some(running.clone()),
                node_session_id: "boot-1".into(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let catalog = node_master
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot-2".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(catalog.records, vec![running]);
    second
        .reconcile(
            catalog
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        )
        .await
        .unwrap();
    report.heartbeat_sequence = 2;
    report.reconciling = false;
    report.accepting_allocations = true;
    node_master.register_node(report).await.unwrap();
    assert_eq!(session.snapshot().await.unwrap().routes().unwrap().len(), 1);
    assert!(!session.get("held").await.unwrap().invalidated);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.removed.load(Ordering::SeqCst), 0);
    assert_eq!(
        *backend.running.lock().unwrap(),
        BTreeSet::from(["held-1".into()])
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates; run control-rpc suite"]
async fn master_restart_bounds_re_registration_grace() {
    use adx_core::{Assignment, CapsuleState};
    for mode in ["timer", "late-register", "timely-register"] {
        let redis = common::Redis::new().await;
        let store = redis.store().await;
        let session = store.begin(1).await.unwrap();
        let report = pb::RegisterNodeRequest {
            node_id: "node".into(),
            node_address: "127.0.0.1:9001".into(),
            proxy_address: "127.0.0.1:9002".into(),
            capacity: Some(spec("held").resources.into()),
            session_id: "boot".into(),
            heartbeat_sequence: 1,
            reconciling: true,
            ..Default::default()
        };
        session
            .register_session(
                adx_master::Node::try_from(report.clone()).unwrap(),
                report.node_address.clone(),
                report.proxy_address.clone(),
                Some(adx_master::storage::NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let assignment = Assignment {
            capsule_id: "held".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        session
            .reserve(spec("held"), assignment.clone())
            .await
            .unwrap();
        let running = CapsuleRecord {
            spec: spec("held"),
            assignment,
            state: CapsuleState::Running,
            revision: 2,
            runtime: adx_core::Runtime {
                id: "held-1".into(),
                ip: Some("10.0.0.2".parse().unwrap()),
            },
            resources_held: true,
            checkpoint: None,
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
        };
        session.commit(running.clone()).await.unwrap();
        let session = store.begin(1).await.unwrap();
        let rpc = MasterRpc::with_heartbeat_timeout(
            session.clone(),
            Placement::Pack,
            peers(),
            client_tls("master"),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(session
            .snapshot()
            .await
            .unwrap()
            .routes()
            .unwrap()
            .is_empty());
        assert!(!session.get("held").await.unwrap().invalidated);
        assert_eq!(rpc.expire_nodes().await.unwrap(), 0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = rpc.clone();
        let _servers = Servers(vec![tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls("master"))
                .unwrap()
                .add_service(pb::master_service_server::MasterServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        })]);
        let mut node =
            pb::master_service_client::MasterServiceClient::new(channel(address, "node").await);
        if mode != "timely-register" {
            tokio::time::sleep(Duration::from_millis(1100)).await;
        }
        if mode == "timer" {
            assert_eq!(
                rpc.expire_nodes().await.unwrap(),
                1,
                "unregistered node must expire after Master recovery grace"
            );
            assert_eq!(rpc.expire_nodes().await.unwrap(), 0);
        }
        node.register_node(report).await.unwrap();
        let catalog = node
            .inspect_node(pb::InspectNodeRequest {
                node_id: "node".into(),
                session_id: "boot".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let stored = session.get("held").await.unwrap();
        assert_eq!(stored.invalidated, mode != "timely-register", "{mode}");
        assert_eq!(catalog.records.len(), 1);
        let record: CapsuleRecord = catalog.records[0].clone().try_into().unwrap();
        if mode == "timely-register" {
            assert_eq!(record, running);
        } else {
            assert_eq!(record.state, CapsuleState::Failed, "{mode}");
            assert!(!record.resources_held);
            assert!(record.runtime.ip.is_none());
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates; run control-rpc suite"]
async fn published_routes_drive_real_gateway_streams_and_reconnect_to_new_master() {
    use adx_core::{Assignment, CapsuleState};
    use adx_master::{
        auth::{AuthRpc, Credential},
        routes::RoutePublisher,
    };
    use adx_node_manager::{routes::UdsRoutes, Routes as _};
    use data_plane_gateway::{
        common::protocol::GatewayPolicy,
        edge::{
            master_routes::{ControlConfig, MasterConnection},
            AccessKind, DataPlaneL4Connector, EdgeAuthenticator, EdgeFrontend, EdgeRouteResolver,
            H2PoolConfig, RouteStore,
        },
        node::NodeProxy,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn wait_route(store: &RouteStore, present: bool) {
        tokio::time::timeout(Duration::from_secs(6), async {
            while !store.ready() || store.get("routed").is_some() != present {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    let redis = common::Redis::new().await;
    let db = redis.store().await;
    let session = db.begin(1).await.unwrap();
    let key = "route-test-api-key-".repeat(3);
    session
        .bootstrap_credential(
            &key,
            &Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
            },
        )
        .await
        .unwrap();
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_port = echo.local_addr().unwrap().port();
    let mut tasks = Servers(vec![tokio::spawn(async move {
        loop {
            let (mut socket, _) = echo.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut read, mut write) = socket.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    })]);
    let proxy = Arc::new(
        NodeProxy::new(GatewayPolicy::for_local_mock(vec!["127.0.0.0/8"
            .parse()
            .unwrap()]))
        .with_route_enforcement(),
    );
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    let node = proxy.clone();
    tasks.0.push(tokio::spawn(async move {
        loop {
            let (stream, _) = nl.accept().await.unwrap();
            let node = node.clone();
            tokio::spawn(async move {
                let _ = node.serve_h2(stream).await;
            });
        }
    }));
    let temp = tempfile::Builder::new()
        .prefix("adx-routes-")
        .tempdir_in("/tmp")
        .unwrap();
    let socket = temp.path().join("route.sock");
    let listener = data_plane_gateway::node::bind_route_control(socket.to_str().unwrap())
        .await
        .unwrap();
    let node = proxy.clone();
    tasks.0.push(tokio::spawn(async move {
        data_plane_gateway::node::serve_route_control(node, listener)
            .await
            .unwrap();
    }));
    let routes = UdsRoutes::new(socket, Duration::from_secs(2)).unwrap();
    routes.begin_reconcile().await.unwrap();
    session
        .register(
            adx_master::Node {
                id: "node".into(),
                capacity: spec("routed").resources,
                available: true,
                labels: Default::default(),
                devices: vec![],
            },
            "127.0.0.1:9001".into(),
            na.to_string(),
        )
        .await
        .unwrap();
    let assignment = Assignment {
        capsule_id: "routed".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("routed"), assignment.clone())
        .await
        .unwrap();
    let mut record = CapsuleRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: spec("routed"),
        assignment,
        state: CapsuleState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: "routed-1".into(),
            ip: Some("127.0.0.1".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    };
    routes.activate(&record).await.unwrap();
    routes.finish_reconcile().await.unwrap();
    session.commit(record.clone()).await.unwrap();
    let publisher = RoutePublisher::new(session.clone(), peers());
    publisher.refresh().await.unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let route_service = publisher.clone();
    let auth = AuthRpc::new(session.clone(), peers());
    tasks.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::route_service_server::RouteServiceServer::new(
                route_service,
            ))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    }));
    session
        .advertise("test", &format!("https://{ma}"), Duration::from_secs(30))
        .await
        .unwrap();
    let mut forbidden =
        pb::route_service_client::RouteServiceClient::new(channel(ma, "api-server").await);
    assert_eq!(
        forbidden
            .watch_routes(pb::WatchRoutesRequest {})
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
    let config:ControlConfig=serde_json::from_value(serde_json::json!({"redis_url":redis.url,"namespace":"test","rpc_timeout_seconds":2,"refresh_seconds":1,"auth_cache_seconds":10,"auth_cache_entries":16,"tls":{"ca":tls.join("ca.pem"),"certificate":tls.join("edge.pem"),"private_key":tls.join("edge.key"),"server_name":"localhost","peers":{"master":tls.join("master.der")}}})).unwrap();
    let connection = Arc::new(MasterConnection::new(config).unwrap());
    let store = Arc::new(RouteStore::new());
    let conn = connection.clone();
    let cache = store.clone();
    tasks.0.push(tokio::spawn(conn.run(cache)));
    wait_route(&store, true).await;
    let edge = EdgeFrontend::new(
        Arc::new(EdgeRouteResolver::new(store.clone()).stream_only()),
        DataPlaneL4Connector::new(H2PoolConfig::default()),
        EdgeAuthenticator::with_verifier(connection),
        target_port,
        target_port,
        "127.0.0.1:1",
        vec![],
    );
    assert!(edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::PortForwarding,
            "",
            "missing".into()
        )
        .await
        .is_err());
    assert!(edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::Direct,
            &"x".repeat(40),
            "invalid".into()
        )
        .await
        .is_err());
    let mut stream = edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::Direct,
            &key,
            "valid".into(),
        )
        .await
        .unwrap();
    stream.write_all(b"through-master-routes").await.unwrap();
    let mut echoed = vec![0; 21];
    tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(echoed, b"through-master-routes");
    drop(stream);
    // Exercise an incremental frame on the same stream, including its base cursor.
    record.revision = 3;
    session.commit(record.clone()).await.unwrap();
    let revision = session.revision().await.unwrap();
    publisher.refresh().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.watch_revision() < revision as i64 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Stop the original publisher endpoint. A synchronized route and cached credential remain usable.
    tasks.0[3].abort();
    let mut cached = edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::Direct,
            &key,
            "cached".into(),
        )
        .await
        .unwrap();
    cached.write_all(b"cached").await.unwrap();
    let mut bytes = [0; 6];
    cached.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"cached");
    drop(cached);
    // Local retirement protects stale Edge cache before Master publishes the deletion.
    routes.retire(&record).await.unwrap();
    assert!(store.get("routed").is_some());
    assert!(edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::Direct,
            &key,
            "retired".into()
        )
        .await
        .is_err());
    let next = db.begin(1).await.unwrap();
    record.state = CapsuleState::Deleted;
    record.resources_held = false;
    record.revision = 4;
    next.commit(record).await.unwrap();
    let new_publisher = RoutePublisher::new(next.clone(), peers());
    new_publisher.refresh().await.unwrap();
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = nl.local_addr().unwrap();
    tasks.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::route_service_server::RouteServiceServer::new(
                new_publisher,
            ))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    next.advertise(
        "test",
        &format!("https://{address}"),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    wait_route(&store, false).await;
    assert!(publisher.refresh().await.is_err());
    assert!(edge
        .open_l4_stream(
            "routed",
            target_port,
            AccessKind::Direct,
            &key,
            "gone".into()
        )
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn snapshot_rpc_enforces_component_tenant_and_deferred_deletion() {
    use adx_core::{
        snapshots::{Reference, Snapshot, SnapshotState},
        CheckpointArtifact,
    };
    let mut redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let snapshot = Snapshot::new(
        "snapshot-a".into(),
        vec!["base".into()],
        spec("source"),
        "node".into(),
        "source-1".into(),
        CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 100,
        },
    )
    .unwrap();
    session.publish_snapshot(snapshot.clone()).await.unwrap();
    let mut second = snapshot.clone();
    second.id = "snapshot-b".into();
    session.publish_snapshot(second).await.unwrap();
    session
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Template {
                node_id: "node".into(),
                template_id: "base".into(),
            },
        )
        .await
        .unwrap();
    let rpc = MasterRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut client = pb::snapshot_service_client::SnapshotServiceClient::new(
        channel(address, "api-server").await,
    );
    let get = pb::GetSnapshotRequest {
        node_session_id: String::new(),
        id: snapshot.id.clone(),
        caller: caller(),
    };
    assert_eq!(
        client
            .get_snapshot(get.clone())
            .await
            .unwrap()
            .into_inner()
            .id,
        snapshot.id
    );
    let mut foreign = get.clone();
    foreign.caller.as_mut().unwrap().tenant_id = "foreign".into();
    assert_eq!(
        client.get_snapshot(foreign).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut missing = get.clone();
    missing.caller = None;
    assert_eq!(
        client.get_snapshot(missing).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let mut edge =
        pb::snapshot_service_client::SnapshotServiceClient::new(channel(address, "edge").await);
    assert_eq!(
        edge.get_snapshot(get).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        client
            .publish_snapshot(pb::PublishSnapshotRequest {
                snapshot: Some(snapshot.clone().try_into().unwrap()),
                node_session_id: "boot".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let list = pb::ListSnapshotsRequest {
        caller: caller(),
        name: "base".into(),
        page_token: String::new(),
        page_size: 1,
    };
    let page = client
        .list_snapshots(list.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.snapshots.len(), 1);
    assert_eq!(page.snapshots[0].id, "snapshot-a");
    assert!(!page.next_page_token.is_empty());
    let mut next = list;
    next.page_token = page.next_page_token;
    let page = client.list_snapshots(next).await.unwrap().into_inner();
    assert_eq!(page.snapshots[0].id, "snapshot-b");
    assert!(page.next_page_token.is_empty());
    let deleted = client
        .delete_snapshot(pb::DeleteSnapshotRequest {
            id: snapshot.id.clone(),
            caller: caller(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.state, pb::SnapshotState::Deleting as i32);
    assert_eq!(
        session.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert!(!session
        .get_snapshot(&snapshot.id)
        .await
        .unwrap()
        .collectable());
    drop(servers);
    redis.crash();
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn tenant_key_rpc_requires_frontend_admin_and_revokes_verification() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let auth = adx_master::auth::AuthRpc::new(session, peers());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::credential_service_server::CredentialServiceServer::new(
                auth.clone(),
            ))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let admin = Some(pb::CallerContext {
        tenant_id: "admin".into(),
        administrator: true,
    });
    let request = pb::CreateTenantKeyRequest {
        caller: admin.clone(),
        tenant_id: "tenant".into(),
        expires_at_unix_seconds: 0,
    };
    let mut edge =
        pb::credential_service_client::CredentialServiceClient::new(channel(addr, "edge").await);
    assert_eq!(
        edge.create_tenant_key(request.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut client = pb::credential_service_client::CredentialServiceClient::new(
        channel(addr, "api-server").await,
    );
    assert_eq!(
        client
            .create_tenant_key(pb::CreateTenantKeyRequest {
                caller: caller(),
                ..request.clone()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        client
            .create_tenant_key(pb::CreateTenantKeyRequest {
                expires_at_unix_seconds: 1,
                ..request.clone()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let created = client
        .create_tenant_key(request)
        .await
        .unwrap()
        .into_inner();
    let id = created.key.as_ref().unwrap().id.clone();
    assert_ne!(id, created.api_key);
    let mut verify = pb::auth_service_client::AuthServiceClient::new(channel(addr, "edge").await);
    let identity = verify
        .verify_api_key(pb::VerifyApiKeyRequest {
            api_key: created.api_key.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .caller
        .unwrap();
    assert_eq!(identity.tenant_id, "tenant");
    assert!(!identity.administrator);
    let page = client
        .list_tenant_keys(pb::ListTenantKeysRequest {
            caller: admin.clone(),
            tenant_id: "tenant".into(),
            page_size: 1,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.keys.len(), 1);
    assert_eq!(page.keys[0].id, id);
    for _ in 0..2 {
        client
            .revoke_tenant_key(pb::RevokeTenantKeyRequest {
                caller: admin.clone(),
                id: id.clone(),
            })
            .await
            .unwrap();
    }
    assert_eq!(
        verify
            .verify_api_key(pb::VerifyApiKeyRequest {
                api_key: created.api_key
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}

#[async_trait::async_trait]
impl adx_node_manager::checkpoint::CheckpointCooperation for LocalChecks {
    async fn prepare(&self, _: &CapsuleRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn abort_unstarted(&self, _: &CapsuleRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn snapshot_gc_waits_for_node_ack_and_recovers_from_lost_ack() {
    use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
    use adx_node_manager::checkpoint::{CheckpointStore, LocalCheckpointStore};
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalCheckpointStore::new(root.path().into()).unwrap());
    let path = store.allocate().await.unwrap();
    std::fs::write(path.join("memory"), b"preserve snapshot memory").unwrap();
    let snapshot = Snapshot::new(
        "snapshot-gc".into(),
        vec![],
        spec("source"),
        "node".into(),
        "source-1".into(),
        store.publish(&path).await.unwrap(),
    )
    .unwrap();
    session.publish_snapshot(snapshot.clone()).await.unwrap();
    let master = MasterRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let serving = master.clone();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
                serving.clone(),
            ))
            .add_service(pb::master_service_server::MasterServiceServer::new(serving))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let sink = Arc::new(
        MasterStateSink::new(channel(ma, "node").await, Duration::from_secs(2))
            .unwrap()
            .with_session("gc-boot".into()),
    );
    let manager = Arc::new(
        NodeManager::new(
            "node".into(),
            backend,
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            sink.clone(),
        )
        .with_checkpointing(store.clone(), Arc::new(LocalChecks))
        .unwrap()
        .with_snapshot_catalog(sink.clone())
        .unwrap(),
    );
    manager
        .update_capacity(spec("i").resources, Duration::from_secs(60))
        .unwrap();
    let node = NodeRpc::new(manager.clone(), peers(), "gc-boot".into())
        .with_local_creation((*sink).clone());
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    let mut client = pb::master_service_client::MasterServiceClient::new(channel(ma, "node").await);
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        session_id: "gc-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        node_address: na.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("i").resources.into()),
        labels: Default::default(),
        devices: vec![],
    };
    client.register_node(report.clone()).await.unwrap();
    let catalog = client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "gc-boot".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(catalog.records.is_empty());
    assert_eq!(catalog.snapshots.len(), 1);
    manager
        .reconcile_catalog(
            vec![],
            catalog
                .snapshots
                .into_iter()
                .map(|s| s.try_into().unwrap())
                .collect(),
        )
        .await
        .unwrap();
    assert!(path.exists());
    report.heartbeat_sequence += 1;
    report.reconciling = false;
    report.accepting_allocations = true;
    client.register_node(report).await.unwrap();
    let reference = Reference::Restore {
        capsule_id: "clone".into(),
    };
    session
        .acquire_snapshot(&snapshot.id, "tenant", reference.clone())
        .await
        .unwrap();
    session
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    assert_eq!(master.collect_snapshots().await.unwrap(), 0);
    assert!(path.exists());
    let deleting = session
        .release_snapshot(&snapshot.id, reference)
        .await
        .unwrap();
    let request = pb::CollectSnapshotRequest {
        snapshot: Some(deleting.clone().try_into().unwrap()),
        node_session_id: "gc-boot".into(),
    };
    let mut frontend =
        pb::node_service_client::NodeServiceClient::new(channel(na, "api-server").await);
    assert_eq!(
        frontend
            .collect_snapshot(request.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut trusted = pb::node_service_client::NodeServiceClient::new(channel(na, "master").await);
    let mut stale = request.clone();
    stale.node_session_id = "old".into();
    assert_eq!(
        trusted.collect_snapshot(stale).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    manager.pause_lifecycle().await;
    assert!(master.collect_snapshots().await.is_err());
    assert_eq!(
        session.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert!(path.exists());
    manager
        .reconcile_catalog(vec![], vec![deleting.clone()])
        .await
        .unwrap();
    // Node completed physical deletion but Master did not receive/commit the ack.
    trusted.collect_snapshot(request).await.unwrap();
    assert!(!path.exists());
    assert_eq!(
        session.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert_eq!(master.collect_snapshots().await.unwrap(), 1);
    assert_eq!(
        session.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Deleted
    );
    assert_eq!(master.collect_snapshots().await.unwrap(), 0);

    let mut api =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "api-server").await);
    let source = api
        .create_capsule(create("snapshot-source"))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let request = pb::CreateSnapshotRequest {
        assignment: source.assignment.clone(),
        caller: caller(),
        operation_id: "save-op".into(),
        expected_revision: source.revision,
        names: vec!["saved".into()],
        timeout_seconds: 60,
    };
    let saved = frontend
        .create_snapshot(request.clone())
        .await
        .unwrap()
        .into_inner();
    let reusable = saved.snapshot.unwrap();
    assert_eq!(
        saved.capsule.unwrap().record.unwrap().state,
        pb::CapsuleState::Running as i32
    );
    assert!(std::path::Path::new(&reusable.artifact.as_ref().unwrap().location).exists());
    let again = frontend
        .create_snapshot(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(again.snapshot.as_ref().unwrap().id, reusable.id);
    frontend
        .delete_capsule(pb::DeleteCapsuleRequest {
            assignment: source.assignment,
            caller: caller(),
        })
        .await
        .unwrap();
    assert!(std::path::Path::new(&reusable.artifact.as_ref().unwrap().location).exists());
    let mut request = create("snapshot-clone");
    let raw = request.spec.as_mut().unwrap();
    raw.snapshot_id = Some(reusable.id.clone());
    raw.image.clear();
    raw.runtime_class.clear();
    raw.resources = None;
    let mut denied = request.clone();
    denied.spec.as_mut().unwrap().resources = Some(pb::Resources {
        cpu_millis: 1,
        memory_bytes: 1,
        disk_bytes: 1,
    });
    assert_eq!(
        api.create_capsule(denied).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let created = frontend
        .create_local_capsule(pb::LocalCapsuleCreateRequest {
            create: Some(request.clone()),
            node_session_id: "gc-boot".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(created.spec.as_ref().unwrap().id, "snapshot-clone");
    assert_eq!(
        created.spec.as_ref().unwrap().resources,
        source.spec.as_ref().unwrap().resources
    );
    let clone_artifact = created
        .checkpoint
        .as_ref()
        .unwrap()
        .artifact
        .as_ref()
        .unwrap();
    assert_ne!(clone_artifact, reusable.artifact.as_ref().unwrap());
    assert!(session
        .get_snapshot(&reusable.id)
        .await
        .unwrap()
        .references
        .contains(&Reference::Restore {
            capsule_id: "snapshot-clone".into()
        }));
    assert_eq!(master.collect_snapshots().await.unwrap(), 0);
    assert!(session
        .get_snapshot(&reusable.id)
        .await
        .unwrap()
        .references
        .is_empty());
    session
        .delete_snapshot(&reusable.id, "tenant")
        .await
        .unwrap();
    assert_eq!(master.collect_snapshots().await.unwrap(), 1);
    assert!(!std::path::Path::new(&reusable.artifact.as_ref().unwrap().location).exists());
    assert!(std::path::Path::new(&clone_artifact.location).exists());
    // Retry a completed create even after its source was collected.
    assert_eq!(
        frontend
            .create_local_capsule(pb::LocalCapsuleCreateRequest {
                create: Some(request.clone()),
                node_session_id: "gc-boot".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap(),
        created
    );
    assert_eq!(
        api.create_capsule(request)
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap(),
        created
    );
    frontend
        .delete_capsule(pb::DeleteCapsuleRequest {
            assignment: created.assignment,
            caller: caller(),
        })
        .await
        .unwrap();
    assert!(!std::path::Path::new(&clone_artifact.location).exists());
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn master_restart_releases_snapshot_pins_for_lost_memory_queue_only() {
    use adx_core::snapshots::{Reference, Snapshot};
    let redis = common::Redis::new().await;
    let store = redis.store().await;
    let first = store.begin(1).await.unwrap();
    let snapshot = Snapshot::new(
        "queued-source".into(),
        vec![],
        spec("source"),
        "node".into(),
        "source-1".into(),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "immutable".into(),
            size_bytes: 10,
        },
    )
    .unwrap();
    first.publish_snapshot(snapshot.clone()).await.unwrap();
    first
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Restore {
                capsule_id: "memory-queue-only".into(),
            },
        )
        .await
        .unwrap();
    first
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Template {
                node_id: "node".into(),
                template_id: "warm".into(),
            },
        )
        .await
        .unwrap();
    first.delete_snapshot(&snapshot.id, "tenant").await.unwrap();
    let second = store.begin(1).await.unwrap();
    let _master = MasterRpc::new(
        second.clone(),
        Placement::Pack,
        peers(),
        client_tls("master"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let after = second.get_snapshot(&snapshot.id).await.unwrap();
    assert_eq!(
        after.references,
        BTreeSet::from([Reference::Template {
            node_id: "node".into(),
            template_id: "warm".into()
        }])
    );
    assert!(first
        .release_snapshot(
            &snapshot.id,
            Reference::Template {
                node_id: "node".into(),
                template_id: "warm".into()
            }
        )
        .await
        .is_err());
}

struct RecoveryBackend(Backend);
#[async_trait::async_trait]
impl RuntimeDriver for RecoveryBackend {
    async fn inventory(&self) -> Result<Vec<adx_node_manager::RuntimeObservation>> {
        self.0.inventory().await
    }
    async fn start(
        &self,
        _: &CapsuleSpec,
        _: &str,
        _: u64,
        _: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        panic!("fault recovery must never fall back to image start")
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        self.0.is_running(id).await
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.0.remove(id).await
    }
    async fn restore_from(
        &self,
        spec: &CapsuleSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &std::path::Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        let origin = origin.expect("source checkpoint identity required");
        assert_eq!(origin.capsule_id, spec.id);
        assert!(origin.ownership_generation < generation);
        assert_eq!(
            std::fs::read(path.join("memory")).unwrap(),
            b"saved runtime state"
        );
        self.0.start(spec, id, generation, devices).await
    }
}
#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates"]
async fn shared_checkpoint_moves_to_new_node_and_old_node_cleans_without_deleting_artifact() {
    use adx_core::{Assignment, CapsuleState, RestorePoint};
    use adx_node_manager::checkpoint::{CheckpointStore, ObjectCheckpointStore, RemoteGcConfig};
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(2).await.unwrap();
    let test_peers = || {
        Peers::new([
            (file("master.der"), Principal::Master),
            (file("node.der"), Principal::Node("source".into())),
            (file("api-server.der"), Principal::Node("target".into())),
        ])
    };
    let rpc = MasterRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        test_peers(),
        client_tls("master"),
        Duration::from_secs(3),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let service = rpc.clone();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::master_service_server::MasterServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let remote = Arc::new(object_store::memory::InMemory::new());
    let src_dir = tempfile::tempdir().unwrap();
    let source_store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        src_dir.path().into(),
        1024,
    )
    .unwrap()
    .with_owner(
        "source".into(),
        "old-upload-session".into(),
        RemoteGcConfig::default(),
    )
    .unwrap();
    let staged = source_store.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), b"saved runtime state").unwrap();
    let artifact = source_store.publish(&staged).await.unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let target_store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            target_dir.path().into(),
            1024,
        )
        .unwrap(),
    );
    let backend = Arc::new(RecoveryBackend(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    }));
    let manager = Arc::new(
        NodeManager::new(
            "target".into(),
            backend.clone(),
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            Arc::new(
                MasterStateSink::new(channel(ma, "api-server").await, Duration::from_secs(3))
                    .unwrap()
                    .with_session("target-boot".into()),
            ),
        )
        .with_checkpointing(target_store.clone(), Arc::new(LocalChecks))
        .unwrap(),
    );
    manager
        .update_capacity(spec("held").resources, Duration::from_secs(60))
        .unwrap();
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    let node = NodeRpc::new(manager.clone(), test_peers(), "target-boot".into());
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    let mut source = pb::master_service_client::MasterServiceClient::new(channel(ma, "node").await);
    let mut target =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "api-server").await);
    let mut source_report = pb::RegisterNodeRequest {
        node_id: "source".into(),
        node_address: "127.0.0.1:9001".into(),
        proxy_address: "source:9002".into(),
        session_id: "source-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        capacity: Some(spec("held").resources.into()),
        ..Default::default()
    };
    let mut target_report = pb::RegisterNodeRequest {
        node_id: "target".into(),
        node_address: na.to_string(),
        proxy_address: "target:9002".into(),
        session_id: "target-boot".into(),
        ..source_report.clone()
    };
    for (client, report) in [
        (&mut source, &mut source_report),
        (&mut target, &mut target_report),
    ] {
        client.register_node(report.clone()).await.unwrap();
        client
            .inspect_node(pb::InspectNodeRequest {
                node_id: report.node_id.clone(),
                session_id: report.session_id.clone(),
            })
            .await
            .unwrap();
        report.heartbeat_sequence = 2;
        report.reconciling = false;
        report.accepting_allocations = true;
        client.register_node(report.clone()).await.unwrap();
    }
    let previous = Assignment {
        capsule_id: "held".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), previous.clone())
        .await
        .unwrap();
    let old = CapsuleRecord {
        spec: spec("held"),
        assignment: previous,
        state: CapsuleState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: "held-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: Some(RestorePoint {
            id: "pause".into(),
            artifact: artifact.clone(),
            expires_at_unix_seconds: u64::MAX,
            source_runtime_id: "held-1".into(),
            origin: None,
        }),
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    };
    session.commit(old.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(550)).await;
    // Keep only the target healthy; the source heartbeat expires.
    target_report.heartbeat_sequence += 1;
    // Target was also idle in this fixture; re-enter reconciliation before opening.
    target_report.reconciling = true;
    target_report.accepting_allocations = false;
    target.register_node(target_report.clone()).await.unwrap();
    target
        .inspect_node(pb::InspectNodeRequest {
            node_id: "target".into(),
            session_id: "target-boot".into(),
        })
        .await
        .unwrap();
    target_report.heartbeat_sequence += 1;
    target_report.reconciling = false;
    target_report.accepting_allocations = true;
    target.register_node(target_report.clone()).await.unwrap();
    assert_eq!(rpc.expire_nodes().await.unwrap(), 1);
    assert_eq!(rpc.recover_capsules().await.unwrap(), 1);
    let restored = session.get("held").await.unwrap();
    let result = restored.result.clone().unwrap();
    assert_eq!(result.state, CapsuleState::Running);
    assert_eq!(result.assignment.node_id, "target");
    assert_eq!(result.assignment.shard_id, 1);
    assert!(result.assignment.generation > old.assignment.generation);
    assert_eq!(result.spec, old.spec);
    assert!(!restored.recovery.as_ref().unwrap().pending);
    assert_eq!(rpc.recover_capsules().await.unwrap(), 0);
    assert_eq!(backend.0.started.load(Ordering::SeqCst), 1);
    let mut duplicate = result.clone();
    duplicate.state = CapsuleState::Paused;
    duplicate.revision = 1;
    duplicate.resources_held = false;
    duplicate.runtime.ip = None;
    duplicate.runtime.id = format!("held-{}", result.assignment.generation);
    duplicate.last_operation = None;
    let mut master_node =
        pb::node_service_client::NodeServiceClient::new(channel(na, "master").await);
    master_node
        .recover_capsule(pb::RecoverCapsuleRequest {
            record: Some(duplicate.try_into().unwrap()),
            node_session_id: "target-boot".into(),
        })
        .await
        .unwrap();
    assert_eq!(backend.0.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.snapshot().await.unwrap().routes().unwrap()[0].node_id,
        "target"
    );
    assert!(source
        .commit_capsule(pb::CommitCapsuleRequest {
            record: Some(old.clone().try_into().unwrap()),
            node_session_id: "source-boot".into()
        })
        .await
        .is_err());
    source_report.heartbeat_sequence += 1;
    source_report.reconciling = true;
    source_report.accepting_allocations = false;
    source.register_node(source_report).await.unwrap();
    let catalog = source
        .inspect_node(pb::InspectNodeRequest {
            node_id: "source".into(),
            session_id: "source-boot".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(catalog.records.is_empty());
    assert_eq!(catalog.retained_checkpoints.len(), 1);
    let returned_dir = tempfile::tempdir().unwrap();
    let returned_store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            returned_dir.path().into(),
            1024,
        )
        .unwrap()
        .with_owner(
            "source".into(),
            "new-upload-session".into(),
            RemoteGcConfig {
                min_age_seconds: 0,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let old_backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::new(BTreeSet::from(["held-1".into()])),
    });
    let returned = NodeManager::new(
        "source".into(),
        old_backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            MasterStateSink::new(channel(ma, "node").await, Duration::from_secs(3))
                .unwrap()
                .with_session("source-boot".into()),
        ),
    )
    .with_checkpointing(returned_store.clone(), Arc::new(LocalChecks))
    .unwrap();
    let retained = catalog
        .retained_checkpoints
        .into_iter()
        .map(|cp| RestorePoint::try_from(cp).unwrap().artifact)
        .collect();
    returned
        .reconcile_retained(vec![], vec![], retained)
        .await
        .unwrap();
    assert!(old_backend.running.lock().unwrap().is_empty());
    assert_eq!(returned.collect_remote_orphans().await.unwrap(), 0);
    assert!(returned_store.materialize(&artifact).await.is_ok());
}

#[path = "rpc/local_first.rs"]
mod local_first;
