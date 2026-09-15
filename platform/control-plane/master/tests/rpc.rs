mod common;
use adx_core::{scheduling::DeviceAllocation, InstanceRecord, InstanceSpec, Resources, Result};
use adx_master::{rpc::MasterRpc, storage::Session, Placement};
use adx_node_manager::{
    rpc::{MasterStateSink, NodeRpc},
    NodeManager, Readiness, Routes, RuntimeBackend,
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
            ("frontend", Principal::Frontend),
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
fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        env: Default::default(),
        id: id.into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
    }
}
fn caller() -> Option<pb::CallerContext> {
    Some(pb::CallerContext {
        tenant_id: "tenant".into(),
        administrator: false,
    })
}
fn create(id: &str) -> pb::CreateInstanceRequest {
    pb::CreateInstanceRequest {
        spec: Some(spec(id).into()),
        caller: caller(),
    }
}
struct Backend {
    session: Session,
    started: AtomicUsize,
    removed: AtomicUsize,
    running: Mutex<BTreeSet<String>>,
}
#[async_trait::async_trait]
impl RuntimeBackend for Backend {
    async fn inventory(&self) -> Result<Vec<adx_node_manager::RuntimeObservation>> {
        Ok(self
            .running
            .lock()
            .unwrap()
            .iter()
            .map(|id| {
                let (instance, generation) = id.rsplit_once('-').unwrap();
                adx_node_manager::RuntimeObservation {
                    instance_id: instance.into(),
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
        spec: &InstanceSpec,
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
    async fn wait_ready(&self, _: &InstanceRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait::async_trait]
impl Routes for LocalChecks {
    async fn activate(&self, _: &InstanceRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &InstanceRecord) -> Result<()> {
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
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("master"))
            .unwrap()
            .add_service(pb::master_service_server::MasterServiceServer::new(master))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
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
            .with_session("boot-1".into()),
    );
    let manager = Arc::new(NodeManager::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        sink,
    ));
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
        pb::master_service_client::MasterServiceClient::new(channel(ma, "frontend").await);
    let mut duplicate = frontend.clone();
    let (a, b) = tokio::join!(
        frontend.create_instance(create("first")),
        duplicate.create_instance(create("first"))
    );
    let first = a.unwrap().into_inner();
    assert_eq!(first, b.unwrap().into_inner());
    assert_eq!(first.durability, pb::Durability::Published as i32);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    let first_record = first.record.unwrap();
    let mut stale_master =
        pb::node_service_client::NodeServiceClient::new(channel(na, "master").await);
    assert_eq!(
        stale_master
            .create_instance(pb::StartAssignedInstanceRequest {
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
    let get = pb::GetInstanceRequest {
        instance_id: "first".into(),
        caller: caller(),
    };
    assert_eq!(
        frontend
            .get_instance(get.clone())
            .await
            .unwrap()
            .into_inner()
            .node_address,
        na.to_string()
    );
    let mut other = get.clone();
    other.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        frontend.get_instance(other).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let mut unknown =
        pb::master_service_client::MasterServiceClient::new(channel(ma, "unknown").await);
    assert_eq!(
        unknown.get_instance(get).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        frontend
            .commit_instance(pb::CommitInstanceRequest {
                node_session_id: "boot-1".into(),
                record: Some(first_record.clone())
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut node_frontend =
        pb::node_service_client::NodeServiceClient::new(channel(na, "frontend").await);
    assert_eq!(
        node_frontend
            .create_instance(pb::StartAssignedInstanceRequest {
                node_session_id: "boot-1".into(),
                spec: Some(spec("first").into()),
                assignment: Some(assignment.clone())
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut bad_delete = pb::DeleteInstanceRequest {
        assignment: Some(assignment.clone()),
        caller: caller(),
    };
    bad_delete.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        node_frontend
            .delete_instance(bad_delete)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut stale = assignment.clone();
    stale.generation += 1;
    assert_eq!(
        node_frontend
            .delete_instance(pb::DeleteInstanceRequest {
                assignment: Some(stale),
                caller: caller()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut pending = Request::new(create("second"));
    pending.set_timeout(Duration::from_millis(80));
    assert!(frontend.create_instance(pending).await.is_err());
    assert_eq!(
        session.get("second").await.unwrap_err(),
        adx_core::Error::NotFound
    );
    let delete = pb::DeleteInstanceRequest {
        assignment: Some(assignment),
        caller: caller(),
    };
    node_frontend.delete_instance(delete.clone()).await.unwrap();
    node_frontend.delete_instance(delete).await.unwrap();
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
            .commit_instance(pb::CommitInstanceRequest {
                node_session_id: "boot-1".into(),
                record: Some(first_record)
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    redis.crash();
    let delete = pb::DeleteInstanceRequest {
        assignment: Some(second.assignment.try_into().unwrap()),
        caller: caller(),
    };
    assert!(node_frontend.delete_instance(delete.clone()).await.is_err());
    assert_eq!(backend.removed.load(Ordering::SeqCst), 2);
    redis.start().await;
    node_frontend.delete_instance(delete).await.unwrap();
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
    if let Ok(api_binary) = std::env::var("ADX_TEST_SANDBOX_API") {
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
        let config = serde_json::json!({"listen":address.to_string(),"discovery":{"redis_url":redis.url,"namespace":"test","poll_seconds":1},"ca":tls.join("ca.pem"),"certificate":tls.join("frontend.pem"),"private_key":tls.join("frontend.key"),"server_name":"localhost","rpc_timeout_seconds":3,"cache_ttl_seconds":60,"cache_entries":128,"auth_cache_ttl_seconds":10});
        let path = directory.path().join("api.json");
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let evidence = PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap());
        let log = std::fs::File::create(evidence.join("sandbox-api.log")).unwrap();
        let mut process = tokio::process::Command::new(api_binary)
            .args(["--config", path.to_str().unwrap()])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../build/ci/frontend_http.py");
        let result = tokio::process::Command::new("python3")
            .arg(script)
            .env("ADX_TEST_API_ENDPOINT", format!("https://{address}"))
            .output()
            .await
            .unwrap();
        std::fs::write(
            evidence.join("frontend-http.log"),
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
    let key = "master-process-test-key-".repeat(3);
    let key_file = directory.path().join("bootstrap.key");
    std::fs::write(&key_file, &key).unwrap();
    let config = serde_json::json!({
        "listen":address.to_string(),"advertised_address":format!("https://{address}"),"discovery_ttl_seconds":3,"redis_url":redis.url,"namespace":"process","domains":1,"placement":"pack","rpc_timeout_seconds":2,
        "tls":{"ca":tls.join("ca.pem"),"certificate":tls.join("master.pem"),"private_key":tls.join("master.key"),"server_name":"localhost","peers":{"frontend":tls.join("frontend.der"),"node:node":tls.join("node.der")}},
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
                        .tls_config(client_tls("frontend"))
                        .unwrap();
                if let Ok(channel) = endpoint.connect().await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let discovery =
            adx_discovery::RedisDiscovery::new(&redis.url, "process", Duration::from_secs(1))
                .unwrap();
        let endpoint = discovery.lookup().await.unwrap();
        assert_eq!(endpoint.address, format!("https://{address}"));
        assert_eq!(endpoint.epoch, index + 1);
        let mut auth = pb::auth_service_client::AuthServiceClient::new(connection);
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
        process.kill().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and mTLS certificates; run control-rpc suite"]
async fn heartbeat_expiry_reconciliation_and_old_session_fencing() {
    use adx_core::{Assignment, InstanceState};
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
        instance_id: "held".into(),
        node_id: "node".into(),
        domain_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assigned.clone())
        .await
        .unwrap();
    let running = InstanceRecord {
        spec: spec("held"),
        assignment: assigned,
        state: InstanceState::Running,
        revision: 2,
        runtime_id: "held-1".into(),
        resources_held: true,
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
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
    assert!(snapshot.instances["held"].resources_held());
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
    assert_eq!(manager.used(), spec("held").resources);
    assert_eq!(backend.started.load(Ordering::SeqCst), 0);

    assert_eq!(
        node.commit_instance(pb::CommitInstanceRequest {
            record: Some(running.clone().try_into().unwrap()),
            node_session_id: "first-boot".into()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::FailedPrecondition
    );
    node.commit_instance(pb::CommitInstanceRequest {
        record: Some(running.try_into().unwrap()),
        node_session_id: "second-boot".into(),
    })
    .await
    .unwrap();
    report.heartbeat_sequence = 2;
    report.accepting_allocations = true;
    report.reconciling = false;
    node.register_node(report.clone()).await.unwrap();
    assert_eq!(session.snapshot().await.unwrap().routes().unwrap().len(), 1);
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
async fn published_routes_drive_real_gateway_streams_and_reconnect_to_new_master() {
    use adx_core::{Assignment, InstanceState};
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
        instance_id: "routed".into(),
        node_id: "node".into(),
        domain_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("routed"), assignment.clone())
        .await
        .unwrap();
    let mut record = InstanceRecord {
        spec: spec("routed"),
        assignment,
        state: InstanceState::Running,
        revision: 2,
        runtime_id: "routed-1".into(),
        resources_held: true,
        runtime_ip: Some("127.0.0.1".parse().unwrap()),
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
        pb::route_service_client::RouteServiceClient::new(channel(ma, "frontend").await);
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
    record.state = InstanceState::Deleted;
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
