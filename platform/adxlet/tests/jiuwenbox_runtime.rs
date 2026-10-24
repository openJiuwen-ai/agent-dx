//! Real HTTP/UDS contract tests; host mounts and VMs are replaced explicitly.
#[path = "common/authority.rs"]
mod authority;
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error};
use adxlet::{
    jiuwenbox::{host::HostOperations, Bootstrap, Config, Jiuwenbox},
    sandboxd::Config as LaunchConfig,
    RuntimeDriver,
};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::UnixListener, sync::Semaphore, task::JoinHandle, time::Instant};

const CREATE_BUDGET: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Host {
    prepares: AtomicUsize,
    releases: AtomicUsize,
    supervisors: Mutex<Vec<String>>,
    process_alive: AtomicBool,
    mounted: AtomicBool,
}
#[async_trait]
impl HostOperations for Host {
    async fn prepare(&self, _: &Bootstrap, root: &Path) -> adx_core::Result<PathBuf> {
        self.prepares.fetch_add(1, Ordering::SeqCst);
        self.mounted.store(true, Ordering::SeqCst);
        Ok(root.into())
    }
    async fn supervisor_inventory(&self) -> adx_core::Result<Vec<String>> {
        Ok(self.supervisors.lock().unwrap().clone())
    }
    async fn cleanup_supervisor(&self, id: &str) -> adx_core::Result<()> {
        self.supervisors.lock().unwrap().retain(|v| v != id);
        self.process_alive.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn supervisor_absent(&self, _: &str) -> adx_core::Result<bool> {
        Ok(!self.process_alive.load(Ordering::SeqCst))
    }
}

struct Backend {
    refs: Mutex<BTreeMap<String, Value>>,
    physical: Arc<Mutex<BTreeMap<String, Value>>>,
    calls: Mutex<Vec<(String, String, Value)>>,
    entered: Semaphore,
    release: Semaphore,
    reject_create: AtomicBool,
    reject_delete: AtomicBool,
    block_delete: AtomicBool,
    delete_entered: Semaphore,
    delete_release: Semaphore,
    block_running_list: AtomicBool,
    running_list_entered: Semaphore,
    running_list_release: Semaphore,
    conch: bool,
}
impl Backend {
    fn new(physical: Arc<Mutex<BTreeMap<String, Value>>>, conch: bool) -> Arc<Self> {
        Arc::new(Self {
            refs: Mutex::default(),
            physical,
            calls: Mutex::default(),
            entered: Semaphore::new(0),
            release: Semaphore::new(1),
            reject_create: AtomicBool::new(false),
            reject_delete: AtomicBool::new(false),
            block_delete: AtomicBool::new(false),
            delete_entered: Semaphore::new(0),
            delete_release: Semaphore::new(0),
            block_running_list: AtomicBool::new(false),
            running_list_entered: Semaphore::new(0),
            running_list_release: Semaphore::new(0),
            conch,
        })
    }
    async fn serve(
        self: Arc<Self>,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let method = request.method().to_string();
        let path = request.uri().path().to_owned();
        let query = request.uri().query().unwrap_or_default().to_owned();
        let bytes = request.into_body().collect().await.unwrap().to_bytes();
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        self.calls
            .lock()
            .unwrap()
            .push((method.clone(), path.clone(), body.clone()));
        let mut code = StatusCode::OK;
        let result = if path == "/health" {
            json!({"status":"ok"})
        } else if path == "/api/v1/timeout" {
            json!({"idle_timeout":null})
        } else if path == "/api/v1/sandboxes" && method == "POST" {
            self.entered.add_permits(1);
            if self.reject_create.load(Ordering::SeqCst) {
                code = StatusCode::SERVICE_UNAVAILABLE;
                json!({})
            } else {
                let id = body["sandbox_id"].as_str().unwrap().to_owned();
                self.refs
                    .lock()
                    .unwrap()
                    .insert(id.clone(), json!({"id":id,"phase":"provisioning"}));
                self.release.acquire().await.unwrap().forget();
                let value = json!({"id":id,"phase":"ready","ip_address":"192.0.2.2","pid":12345,"sandbox_runtime":body["sandbox_runtime"]});
                self.refs.lock().unwrap().insert(id.clone(), value.clone());
                if body["sandbox_runtime"] == "conch" {
                    self.physical
                        .lock()
                        .unwrap()
                        .insert(id.clone(), value.clone());
                }
                code = StatusCode::CREATED;
                value
            }
        } else if path == "/api/v1/sandboxes" && method == "GET" {
            if self.conch {
                if query.contains("state=running") && self.block_running_list.load(Ordering::SeqCst)
                {
                    self.running_list_entered.add_permits(1);
                    self.running_list_release.acquire().await.unwrap().forget();
                }
                json!(self
                    .physical
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>())
            } else {
                json!(self
                    .refs
                    .lock()
                    .unwrap()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>())
            }
        } else if path.starts_with("/api/v1/policies/") && method == "PUT" {
            body["policy"].clone()
        } else if path.ends_with("/exec_background") {
            json!({"started":true,"job_id":"adx-execd","pid":5678})
        } else if path.starts_with("/api/v1/sandboxes/") {
            let id = path.trim_start_matches("/api/v1/sandboxes/");
            if method == "DELETE" {
                if self.block_delete.load(Ordering::SeqCst) {
                    self.delete_entered.add_permits(1);
                    self.delete_release.acquire().await.unwrap().forget();
                }
                if self.reject_delete.load(Ordering::SeqCst) {
                    code = StatusCode::SERVICE_UNAVAILABLE;
                    json!({})
                } else {
                    self.refs.lock().unwrap().remove(id);
                    self.physical.lock().unwrap().remove(id);
                    code = StatusCode::NO_CONTENT;
                    Value::Null
                }
            } else {
                let value = if self.conch {
                    self.physical.lock().unwrap().get(id).cloned()
                } else {
                    self.refs.lock().unwrap().get(id).cloned()
                };
                match value {
                    Some(v) => v,
                    None => {
                        code = StatusCode::NOT_FOUND;
                        json!({})
                    }
                }
            }
        } else {
            code = StatusCode::NOT_FOUND;
            json!({})
        };
        let bytes = if code == StatusCode::NO_CONTENT {
            vec![]
        } else {
            serde_json::to_vec(&result).unwrap()
        };
        Ok(Response::builder()
            .status(code)
            .body(Full::new(Bytes::from(bytes)))
            .unwrap())
    }
    fn count(&self, method: &str, path: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, p, _)| m == method && p == path)
            .count()
    }
}
fn server(path: &Path, backend: Arc<Backend>) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let backend = backend.clone();
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |r| backend.clone().serve(r)),
                    )
                    .await;
            });
        }
    })
}
struct Fixture {
    _dir: tempfile::TempDir,
    config: Config,
    backend: Arc<Backend>,
    native: Arc<Backend>,
    host: Arc<Host>,
    listeners: Vec<JoinHandle<()>>,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("j.sock");
        let conch_socket = dir.path().join("c.sock");
        let physical: Arc<Mutex<BTreeMap<String, Value>>> = Arc::default();
        let backend = Backend::new(physical.clone(), false);
        let native = Backend::new(physical, true);
        let listeners = vec![
            server(&socket, backend.clone()),
            server(&conch_socket, native.clone()),
        ];
        let config=serde_json::from_value(json!({"socket":socket,"conch_socket":conch_socket,"state_dir":dir.path().join("state"),"bootstrap":{"image":dir.path().join("execd.erofs"),"sha256":"a".repeat(64),"target":"/__adx","command":["/__adx/usr/local/bin/adx-execd"]},"classes":{"supervisor":{"images":{"agent:v1":{"process":{"args":["agent-server","--port","18092"],"cwd":"/work","user":"1000"}}}},"conch":{"images":{"agent:v1":{"template_name":"agent-v1","process":{"args":["agent-server"],"cwd":"/work","user":"1000"}}}}}})).unwrap();
        Self {
            _dir: dir,
            config,
            backend,
            native,
            host: Arc::default(),
            listeners,
        }
    }
    async fn driver(&self) -> Jiuwenbox {
        self.driver_with_timeout(Duration::from_millis(150)).await
    }
    async fn driver_with_timeout(&self, timeout: Duration) -> Jiuwenbox {
        let mut launch = LaunchConfig {
            rpc_timeout: timeout,
            ..Default::default()
        };
        launch.env.insert("EXECD_HTTP_PORT".into(), "1".into());
        Jiuwenbox::with_host(self.config.clone(), launch, self.host.clone())
            .await
            .unwrap()
    }
    async fn expire_blocked_create(&self, deadline: Instant) {
        tokio::time::timeout(CREATE_BUDGET, self.backend.entered.acquire())
            .await
            .expect("create did not reach the blocked backend")
            .unwrap()
            .forget();
        // Expire the caller only after the real HTTP handler owns the request.
        // Setup includes fsync, so a short wall-clock deadline can expire before
        // reaching the race this test is meant to exercise on a busy CI host.
        tokio::time::pause();
        tokio::time::advance(deadline.saturating_duration_since(Instant::now())).await;
        tokio::time::resume();
    }
    fn restart(&mut self) {
        self.listeners[0].abort();
        std::fs::remove_file(&self.config.socket).unwrap();
        self.backend = Backend::new(self.native.physical.clone(), false);
        self.listeners[0] = server(&self.config.socket, self.backend.clone());
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for h in &self.listeners {
            h.abort();
        }
    }
}
fn spec(class: &str) -> EnvironmentSpec {
    serde_json::from_value(json!({"id":"tenant-agent","tenant_id":"t","image":"agent:v1","runtime_class":class,"resources":{"cpu_millis":1000,"memory_bytes":134217728,"disk_bytes":0},"priority":0,"execution":{"failover":false,"inherit_entrypoint":true,"rootfs":{"readonly":false,"source":{"Image":"agent:v1"}}}})).unwrap()
}

#[tokio::test]
async fn native_runtime_response_field_keeps_reconciliation_and_deletion_available() {
    for class in ["supervisor", "conch"] {
        let fixture = Fixture::new();
        let driver = fixture.driver().await;
        driver
            .start_with_deadline(
                &spec(class),
                "tenant-agent-1",
                1,
                &[],
                Instant::now() + CREATE_BUDGET,
            )
            .await
            .unwrap();
        {
            let mut references = fixture.backend.refs.lock().unwrap();
            for reference in references.values_mut() {
                let runtime = reference
                    .as_object_mut()
                    .unwrap()
                    .remove("sandbox_runtime")
                    .unwrap();
                reference["runtime"] = runtime;
            }
        }
        // Native GET/list uses `runtime`; `sandbox_runtime` belongs to create.
        // A ready backend must not close node admission at the next inventory.
        assert_eq!(driver.inventory().await.unwrap().len(), 1);
        driver.remove("tenant-agent-1").await.unwrap();
        assert!(driver.inventory().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn inventory_keeps_active_deletion_visible_without_blocking_other_runtimes() {
    for class in ["supervisor", "conch"] {
        let fixture = Fixture::new();
        fixture.host.process_alive.store(true, Ordering::SeqCst);
        let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
        driver
            .start(&spec(class), "tenant-agent-1", 1, &[])
            .await
            .unwrap();
        let mut other = spec(class);
        other.id = "tenant-other".into();
        fixture.backend.release.add_permits(1);
        driver
            .start(&other, "tenant-other-1", 1, &[])
            .await
            .unwrap();
        fixture.backend.block_delete.store(true, Ordering::SeqCst);
        let removing = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.remove("tenant-agent-1").await })
        };
        fixture
            .backend
            .delete_entered
            .acquire()
            .await
            .unwrap()
            .forget();
        let observed = tokio::time::timeout(Duration::from_secs(1), driver.inventory()).await;
        let deletes = fixture
            .backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _, _)| method == "DELETE")
            .count();
        // Always release the real HTTP handler, including when inventory fails.
        fixture.backend.block_delete.store(false, Ordering::SeqCst);
        fixture.backend.delete_release.add_permits(1);
        removing.await.unwrap().unwrap();
        let observed = observed
            .expect("inventory waited for the unrelated delete")
            .unwrap();
        assert_eq!(observed.len(), 2);
        assert!(
            !observed
                .iter()
                .find(|r| r.runtime_id == "tenant-agent-1")
                .unwrap()
                .running
        );
        assert!(
            observed
                .iter()
                .find(|r| r.runtime_id == "tenant-other-1")
                .unwrap()
                .running
        );
        assert_eq!(deletes, 1, "inventory must not issue a duplicate delete");
        let observed = driver.inventory().await.unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].runtime_id, "tenant-other-1");
        driver.remove("tenant-other-1").await.unwrap();
        assert!(driver.inventory().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn inventory_keeps_timed_out_creation_tracked_until_its_worker_finishes() {
    let fixture = Fixture::new();
    let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
    fixture.backend.release.acquire().await.unwrap().forget();
    let deadline = Instant::now() + CREATE_BUDGET;
    let creating = {
        let driver = driver.clone();
        tokio::spawn(async move {
            driver
                .start_with_deadline(&spec("conch"), "tenant-agent-1", 1, &[], deadline)
                .await
        })
    };
    fixture.expire_blocked_create(deadline).await;
    assert!(matches!(
        creating.await.unwrap(),
        Err(Error::DeadlineExceeded(_))
    ));
    let observed = tokio::time::timeout(Duration::from_secs(1), driver.inventory()).await;
    fixture.backend.release.add_permits(1);
    driver.remove("tenant-agent-1").await.unwrap();
    let observed = observed
        .expect("inventory waited for a late create")
        .unwrap();
    assert_eq!(observed.len(), 1);
    assert!(!observed[0].running);
    assert!(fixture.native.physical.lock().unwrap().is_empty());
    assert!(driver.inventory().await.unwrap().is_empty());
}

#[tokio::test]
async fn inventory_only_reports_failed_deletion_without_starting_a_retry() {
    for class in ["supervisor", "conch"] {
        let fixture = Fixture::new();
        let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
        driver
            .start(&spec(class), "tenant-agent-1", 1, &[])
            .await
            .unwrap();
        fixture.backend.reject_delete.store(true, Ordering::SeqCst);
        assert!(driver.remove("tenant-agent-1").await.is_err());
        fixture.backend.reject_delete.store(false, Ordering::SeqCst);
        fixture.backend.block_delete.store(true, Ordering::SeqCst);
        let observed = tokio::time::timeout(Duration::from_millis(500), driver.inventory()).await;
        let retried = tokio::time::timeout(
            Duration::from_millis(100),
            fixture.backend.delete_entered.acquire(),
        )
        .await
        .is_ok();
        fixture.backend.block_delete.store(false, Ordering::SeqCst);
        fixture.backend.delete_release.add_permits(1);
        driver.remove("tenant-agent-1").await.unwrap();
        let observed = observed
            .expect("inventory waited for cleanup retry")
            .unwrap();
        assert_eq!(observed.len(), 1);
        assert!(!observed[0].running);
        assert!(
            !retried,
            "inventory must not start cleanup outside the controller queue"
        );
        assert!(driver.inventory().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn cancelling_delete_keeps_cleanup_owned_and_retry_waits_for_it() {
    let fixture = Fixture::new();
    let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
    driver
        .start(&spec("conch"), "tenant-agent-1", 1, &[])
        .await
        .unwrap();
    fixture.backend.block_delete.store(true, Ordering::SeqCst);
    let deleting = {
        let driver = driver.clone();
        tokio::spawn(async move { driver.remove("tenant-agent-1").await })
    };
    fixture
        .backend
        .delete_entered
        .acquire()
        .await
        .unwrap()
        .forget();
    deleting.abort();
    assert!(deleting.await.unwrap_err().is_cancelled());
    let retry = {
        let driver = driver.clone();
        tokio::spawn(async move { driver.remove("tenant-agent-1").await })
    };
    for _ in 0..3 {
        let observed = driver.inventory().await.unwrap();
        assert_eq!(observed.len(), 1);
        assert!(!observed[0].running);
        assert!(!retry.is_finished());
    }
    let deletes = fixture
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _, _)| method == "DELETE")
        .count();
    fixture.backend.block_delete.store(false, Ordering::SeqCst);
    fixture.backend.delete_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), retry)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        deletes, 1,
        "cancelled HTTP callers must not release the execution gate"
    );
    assert!(driver.inventory().await.unwrap().is_empty());
    assert!(driver
        .start(&spec("conch"), "tenant-agent-1", 1, &[])
        .await
        .is_err());
}

#[tokio::test]
async fn inventory_cannot_clean_creation_that_completed_after_its_snapshot() {
    let fixture = Fixture::new();
    let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
    driver
        .start(&spec("conch"), "tenant-agent-1", 1, &[])
        .await
        .unwrap();
    fixture.backend.entered.acquire().await.unwrap().forget();
    let creating = {
        let driver = driver.clone();
        let mut other = spec("conch");
        other.id = "tenant-other".into();
        tokio::spawn(async move { driver.start(&other, "tenant-other-1", 1, &[]).await })
    };
    fixture.backend.entered.acquire().await.unwrap().forget();
    fixture
        .native
        .block_running_list
        .store(true, Ordering::SeqCst);
    let inventory = {
        let driver = driver.clone();
        tokio::spawn(async move { driver.inventory().await })
    };
    fixture
        .native
        .running_list_entered
        .acquire()
        .await
        .unwrap()
        .forget();
    fixture.backend.release.add_permits(1);
    creating.await.unwrap().unwrap();
    fixture
        .native
        .block_running_list
        .store(false, Ordering::SeqCst);
    fixture.native.running_list_release.add_permits(1);
    let observed = inventory.await.unwrap().unwrap();
    assert_eq!(
        observed.len(),
        2,
        "the newly completed execution remains owned"
    );
    assert!(driver.is_running("tenant-other-1").await.unwrap());
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 2);
    for id in ["tenant-agent-1", "tenant-other-1"] {
        driver.remove(id).await.unwrap();
    }
}

#[tokio::test]
async fn periodic_native_cleanup_keeps_other_environment_lifecycle_available() {
    for class in ["supervisor", "conch"] {
        let fixture = Fixture::new();
        fixture.host.process_alive.store(true, Ordering::SeqCst);
        let driver = fixture.driver_with_timeout(Duration::from_secs(5)).await;
        let first = running_record(class);
        let mut other = running_record(class);
        other.spec.id = "tenant-other".into();
        other.assignment.environment_id = other.spec.id.clone();
        other.runtime.id = "tenant-other-1".into();
        fixture.backend.release.add_permits(1);
        for record in [&first, &other] {
            driver
                .start(&record.spec, &record.runtime.id, 1, &[])
                .await
                .unwrap();
        }
        let node = authority::manager(Arc::new(driver.clone()));
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = [&first, &other]
            .into_iter()
            .map(|r| r.clone().try_into().unwrap())
            .collect();
        authority.cycle(&node).await.unwrap();
        let deleting = {
            let handle = node.environment(first.spec, first.assignment).unwrap();
            fixture.backend.block_delete.store(true, Ordering::SeqCst);
            tokio::spawn(async move { handle.delete().await })
        };
        fixture
            .backend
            .delete_entered
            .acquire()
            .await
            .unwrap()
            .forget();
        for _ in 0..3 {
            tokio::time::timeout(Duration::from_millis(500), authority.cycle(&node))
                .await
                .expect("native cleanup stalled the whole node")
                .unwrap();
            assert!(node.accepting_allocations());
            let peer = node
                .environment(other.spec.clone(), other.assignment.clone())
                .unwrap();
            assert_eq!(
                peer.ensure_running("keep-peer".into())
                    .await
                    .unwrap()
                    .record
                    .state,
                EnvironmentState::Running
            );
        }
        let mut new = running_record(class);
        new.spec.id = "tenant-new".into();
        new.assignment.environment_id = new.spec.id.clone();
        fixture.backend.release.add_permits(1);
        let new_handle = node.environment(new.spec, new.assignment).unwrap();
        assert_eq!(
            new_handle.create().await.unwrap().record.state,
            EnvironmentState::Running
        );
        fixture.backend.block_delete.store(false, Ordering::SeqCst);
        fixture.backend.delete_release.add_permits(1);
        deleting.await.unwrap().unwrap();
        node.environment(other.spec, other.assignment)
            .unwrap()
            .delete()
            .await
            .unwrap();
        new_handle.delete().await.unwrap();
        assert!(driver.inventory().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn recovery_reclaims_failed_execution_without_a_retained_controller() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    let node = authority::manager(Arc::new(driver.clone()));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + CREATE_BUDGET,
        )
        .await
        .unwrap();
    fixture.backend.reject_delete.store(true, Ordering::SeqCst);
    assert!(driver.remove("tenant-agent-1").await.is_err());
    let deletes = fixture
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _, _)| method == "DELETE")
        .count();
    assert_eq!(driver.inventory().await.unwrap().len(), 1);
    assert_eq!(
        fixture
            .backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _, _)| method == "DELETE")
            .count(),
        deletes
    );
    // An external execution has no lifecycle controller; recovery owns cleanup.
    fixture.backend.reject_delete.store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert_eq!(driver.inventory().await.unwrap().len(), 1);
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
    assert!(driver.inventory().await.unwrap().is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
}

#[tokio::test]
async fn backend_epoch_change_is_cleaned_during_session_recovery() {
    let mut fixture = Fixture::new();
    let driver = fixture.driver().await;
    let record = running_record("conch");
    driver
        .start_with_deadline(
            &record.spec,
            &record.runtime.id,
            1,
            &[],
            Instant::now() + CREATE_BUDGET,
        )
        .await
        .unwrap();
    let node = authority::manager(Arc::new(driver.clone()));
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![record.clone().try_into().unwrap()];
    authority.cycle(&node).await.unwrap();
    fixture.restart();
    let observed = driver.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert!(!observed[0].running);
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
    authority.cycle(&node).await.unwrap();
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
    assert!(driver.inventory().await.unwrap().is_empty());
}

#[tokio::test]
async fn creates_conch_with_erofs_volume_and_fenced_execd_identity() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    {
        let calls = fixture.backend.calls.lock().unwrap();
        let body = &calls
            .iter()
            .find(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes")
            .unwrap()
            .2;
        assert_eq!(body["sandbox_runtime"], "conch");
        assert_eq!(body["sandbox_id"].as_str().unwrap().len(), 32);
        assert_eq!(body["policy"]["conch"]["vcpu_num"], 1);
        let mounts = body["policy"]["conch"]["filesystem_policy"]["bind_mounts"]
            .as_array()
            .unwrap();
        assert!(mounts
            .iter()
            .any(|m| m["sandbox_path"] == "/__adx" && m["mode"] == "ro"));
        assert!(mounts
            .iter()
            .any(|m| m["sandbox_path"] == "/__adx_launch" && m["mode"] == "ro"));
        let exec = &calls
            .iter()
            .find(|(_, p, _)| p.ends_with("/exec_background"))
            .unwrap()
            .2;
        assert_eq!(exec["command"][0], "/__adx/usr/local/bin/adx-execd");
        assert_eq!(exec["env"]["ADX_RUNTIME_ID"], "tenant-agent-1");
        assert_eq!(exec["env"]["ADX_OWNERSHIP_GENERATION"], "1");
        assert_eq!(
            exec["env"]["ADX_IMAGE_PROCESS_CONFIG"],
            "/__adx_launch/process.json"
        );
        assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    }
    driver.remove("tenant-agent-1").await.unwrap();
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn supervisor_uses_native_cgroup_and_same_operation_is_not_replayed() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    let wanted = spec("supervisor");
    let deadline = Instant::now() + Duration::from_secs(3);
    let ip = driver
        .start_with_deadline(&wanted, "tenant-agent-1", 1, &[], deadline)
        .await
        .unwrap();
    assert_eq!(
        driver
            .start_with_deadline(&wanted, "tenant-agent-1", 1, &[], deadline)
            .await
            .unwrap(),
        ip
    );
    assert_eq!(fixture.backend.count("POST", "/api/v1/sandboxes"), 1);
    let body = fixture
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes")
        .unwrap()
        .2
        .clone();
    assert_eq!(body["sandbox_runtime"], "bwrap");
    assert_eq!(body["policy"]["cgroup"]["cpu_max"], json!("100000 100000"));
    assert_eq!(body["policy"]["cgroup"]["memory_max"], 134217728);
    let mut changed = wanted.clone();
    changed.env.insert("changed".into(), "value".into());
    assert!(matches!(
        driver
            .start_with_deadline(&changed, "tenant-agent-1", 1, &[], deadline)
            .await,
        Err(Error::Conflict)
    ));
}

#[tokio::test]
async fn deadline_keeps_late_create_owned_and_delete_waits_for_completion() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    fixture.backend.release.acquire().await.unwrap().forget();
    let deadline = Instant::now() + CREATE_BUDGET;
    let worker = driver.clone();
    let create = tokio::spawn(async move {
        worker
            .start_with_deadline(&spec("conch"), "tenant-agent-1", 1, &[], deadline)
            .await
    });
    fixture.expire_blocked_create(deadline).await;
    assert!(matches!(
        create.await.unwrap(),
        Err(Error::DeadlineExceeded(_))
    ));
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    let worker = driver.clone();
    let delete = tokio::spawn(async move { worker.remove("tenant-agent-1").await });
    assert!(!delete.is_finished());
    fixture.backend.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), delete)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(fixture.native.physical.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p, _)| p.ends_with("/exec_background"))
            .count(),
        0
    );
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    assert!(driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(1)
        )
        .await
        .is_err());
}

#[tokio::test]
async fn jiuwenbox_restart_cleans_conch_via_native_api_and_keeps_mount() {
    let mut fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
    drop(driver);
    fixture.restart();
    let recovered = fixture.driver().await;
    assert!(recovered.reconcile_inventory(&[]).await.unwrap().is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
    assert!(fixture
        .native
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(m, _, _)| m == "DELETE"));
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn delete_failure_retains_mount_and_remains_retryable() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    fixture.backend.reject_delete.store(true, Ordering::SeqCst);
    assert!(driver.remove("tenant-agent-1").await.is_err());
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    fixture.backend.reject_delete.store(false, Ordering::SeqCst);
    driver.remove("tenant-agent-1").await.unwrap();
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn execd_transport_failure_keeps_live_vm_available_to_readiness_policy() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(driver.is_running("tenant-agent-1").await.unwrap());
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
    fixture.native.physical.lock().unwrap().clear();
    assert!(!driver.is_running("tenant-agent-1").await.unwrap());
}

#[tokio::test]
async fn delete_before_start_fences_current_process_without_local_persistence() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver.remove("tenant-agent-1").await.unwrap();
    assert!(matches!(
        driver.start(&spec("conch"), "tenant-agent-1", 1, &[]).await,
        Err(Error::Conflict)
    ));
    assert_eq!(fixture.backend.count("POST", "/api/v1/sandboxes"), 0);
}

#[tokio::test]
async fn unsupported_limits_fail_before_mount_or_create() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    let mut wanted = spec("conch");
    wanted.resources.cpu_millis = 500;
    assert!(matches!(
        driver.start(&wanted, "tenant-agent-1", 1, &[]).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(fixture.host.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.backend.count("POST", "/api/v1/sandboxes"), 0);
}

#[tokio::test]
async fn checkpoint_is_explicitly_unsupported() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    assert!(matches!(
        driver.checkpoint_supported("conch").await,
        Err(Error::Invalid(_))
    ));
}

#[tokio::test]
async fn rejected_create_can_be_cleaned_without_restarting_backend() {
    let mut fixture = Fixture::new();
    let driver = fixture.driver().await;
    fixture.backend.reject_create.store(true, Ordering::SeqCst);
    assert!(driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3)
        )
        .await
        .is_err());
    driver.remove("tenant-agent-1").await.unwrap();
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    drop(driver);
    fixture.restart();
    let recovered = fixture.driver().await;
    assert!(recovered.reconcile_inventory(&[]).await.unwrap().is_empty());
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unknown_managed_vm_is_deleted_after_authority_while_foreign_vm_is_preserved() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    fixture.native.physical.lock().unwrap().insert(
        "adx-unknown-owner".into(),
        json!({"sandboxID":"adx-unknown-owner"}),
    );
    fixture
        .native
        .physical
        .lock()
        .unwrap()
        .insert("foreign-vm".into(), json!({"sandboxID":"foreign-vm"}));
    assert!(driver.inventory().await.is_err());
    assert!(driver.available_runtime_classes().await.is_ok());
    assert!(driver.reconcile_inventory(&[]).await.unwrap().is_empty());
    let physical = fixture.native.physical.lock().unwrap();
    assert!(!physical.contains_key("adx-unknown-owner"));
    assert!(physical.contains_key("foreign-vm"));
}

#[tokio::test]
async fn inventory_only_reports_unknown_executions_after_authority_is_loaded() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver.reconcile_inventory(&[]).await.unwrap();
    fixture.native.physical.lock().unwrap().insert(
        "adx-late-unowned".into(),
        json!({"sandboxID":"adx-late-unowned"}),
    );
    assert!(driver.inventory().await.is_err());
    assert!(fixture
        .native
        .physical
        .lock()
        .unwrap()
        .contains_key("adx-late-unowned"));
    assert_eq!(
        fixture
            .native
            .count("DELETE", "/api/v1/sandboxes/adx-late-unowned"),
        0
    );
    assert!(driver.reconcile_inventory(&[]).await.unwrap().is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
}

#[tokio::test]
async fn erofs_is_mounted_once_and_retained_after_all_generations_are_deleted() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    fixture.backend.release.add_permits(1);
    for generation in [1, 2] {
        driver
            .start_with_deadline(
                &spec("conch"),
                &format!("tenant-agent-{generation}"),
                generation,
                &[],
                Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
    }
    driver.remove("tenant-agent-1").await.unwrap();
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
    driver.remove("tenant-agent-2").await.unwrap();
    assert_eq!(fixture.host.prepares.load(Ordering::SeqCst), 1);
    assert!(fixture.host.mounted.load(Ordering::SeqCst));
}

#[tokio::test]
async fn late_physical_runtime_for_deleted_id_is_cleaned_not_adopted() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    let (id, record) = fixture
        .native
        .physical
        .lock()
        .unwrap()
        .iter()
        .next()
        .map(|(k, v)| (k.clone(), v.clone()))
        .unwrap();
    driver.remove("tenant-agent-1").await.unwrap();
    fixture.native.physical.lock().unwrap().insert(id, record);
    let observed = driver.inventory().await.unwrap();
    assert_eq!(observed.len(), 1);
    assert!(!observed[0].running);
    driver.remove("tenant-agent-1").await.unwrap();
    assert!(driver.inventory().await.unwrap().is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
}

#[tokio::test]
async fn only_one_driver_can_own_a_state_directory() {
    let fixture = Fixture::new();
    let _driver = fixture.driver().await;
    let second = Jiuwenbox::with_host(
        fixture.config.clone(),
        LaunchConfig::default(),
        fixture.host.clone(),
    )
    .await;
    assert!(second.is_err());
}

#[tokio::test]
async fn truncated_native_inventory_closes_admission() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    {
        let mut physical = fixture.native.physical.lock().unwrap();
        for index in 0..5000 {
            let id = format!("unmanaged-{index}");
            physical.insert(id.clone(), json!({"sandboxID":id}));
        }
    }
    assert!(driver
        .available_runtime_classes()
        .await
        .unwrap_err()
        .to_string()
        .contains("truncated"));
}

#[tokio::test]
async fn reconciliation_deletes_unknown_and_later_arriving_managed_vms() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    for id in ["adx-first-orphan", "adx-later-orphan"] {
        fixture
            .native
            .physical
            .lock()
            .unwrap()
            .insert(id.into(), json!({"sandboxID":id}));
        assert!(driver.reconcile_inventory(&[]).await.unwrap().is_empty());
        assert!(fixture.native.physical.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn driver_has_no_ownership_database_and_retains_shared_mount_after_delete() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    assert!(!fixture.config.state_dir.join("ownership.sqlite").exists());
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    driver.remove("tenant-agent-1").await.unwrap();
    assert!(fixture.host.mounted.load(Ordering::SeqCst));
    assert_eq!(fixture.host.releases.load(Ordering::SeqCst), 0);
}

fn running_record(class: &str) -> EnvironmentRecord {
    EnvironmentRecord {
        spec: spec(class),
        assignment: Assignment {
            environment_id: "tenant-agent".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        state: EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: "tenant-agent-1".into(),
            ip: Some("192.0.2.2".parse().unwrap()),
        },
        checkpoint: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        last_operation: None,
        resources_held: true,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
    }
}

#[tokio::test]
async fn router_recovers_live_execution_from_authority_without_replaying_create() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    drop(driver);
    let recovered = fixture.driver().await;
    assert!(!recovered.owns("tenant-agent-1").await.unwrap());
    let router = adxlet::runtime_router::RuntimeRouter::new(None, Some(recovered.clone())).unwrap();
    let actual = router
        .reconcile_inventory(&[running_record("conch")])
        .await
        .unwrap();
    assert_eq!(actual.len(), 1);
    assert!(actual[0].running);
    assert_eq!(actual[0].tenant_id, "t");
    assert!(recovered.owns("tenant-agent-1").await.unwrap());
    assert!(router.is_running("tenant-agent-1").await.unwrap());
    assert_eq!(fixture.backend.count("POST", "/api/v1/sandboxes"), 1);
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unpublished_backend_is_deleted_after_driver_restart() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    drop(driver);
    let recovered = fixture.driver().await;
    let mut pending = running_record("conch");
    pending.state = EnvironmentState::Pending;
    pending.revision = 0;
    assert!(recovered
        .reconcile_inventory(&[pending])
        .await
        .unwrap()
        .is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
}

#[tokio::test]
async fn orphan_delete_failure_is_retried_by_later_reconciliation() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    fixture
        .native
        .physical
        .lock()
        .unwrap()
        .insert("adx-orphan".into(), json!({"sandboxID":"adx-orphan"}));
    fixture.native.reject_delete.store(true, Ordering::SeqCst);
    assert!(driver.reconcile_inventory(&[]).await.is_err());
    assert_eq!(fixture.native.physical.lock().unwrap().len(), 1);
    fixture.native.reject_delete.store(false, Ordering::SeqCst);
    assert!(driver.reconcile_inventory(&[]).await.unwrap().is_empty());
    assert!(fixture.native.physical.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_orphans_are_discovered_without_registry_or_driver_state() {
    let fixture = Fixture::new();
    fixture
        .host
        .supervisors
        .lock()
        .unwrap()
        .push("adx-supervisor-orphan".into());
    fixture.host.process_alive.store(true, Ordering::SeqCst);
    let driver = fixture.driver().await;
    assert!(driver.reconcile_inventory(&[]).await.unwrap().is_empty());
    assert!(fixture.host.supervisors.lock().unwrap().is_empty());
    assert!(!fixture.host.process_alive.load(Ordering::SeqCst));
}

#[tokio::test]
async fn policy_updates_use_jiuwenbox_api_and_reject_unsupported_before_mutation() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    driver
        .set_network_policy("tenant-agent-1", None, &[], None)
        .await
        .unwrap();
    let puts = fixture
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(m, _, _)| m == "PUT")
        .count();
    assert_eq!(puts, 1);
    let invalid = NetworkPolicy {
        dns: None,
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Allow,
            mode: TrafficMode::Stateful,
            rules: vec![],
        }),
    };
    assert!(matches!(
        driver
            .set_network_policy("tenant-agent-1", Some(&invalid), &[], None)
            .await,
        Err(Error::Invalid(_))
    ));
    let calls = fixture.backend.calls.lock().unwrap();
    let updates: Vec<_> = calls.iter().filter(|(m, _, _)| m == "PUT").collect();
    assert_eq!(updates.len(), puts);
    assert_eq!(updates[0].2["policy_mode"], "override");
    assert_eq!(
        updates[0].2["policy"]["conch"]["network"]["egress"]["blocked_ips"],
        json!([])
    );
}

#[tokio::test]
async fn recovery_reaps_late_supervisor_and_conch_without_local_records() {
    for class in ["supervisor", "conch"] {
        let fixture = Fixture::new();
        let driver = fixture.driver().await;
        let router = adxlet::runtime_router::RuntimeRouter::new(None, Some(driver)).unwrap();
        let node = authority::manager(Arc::new(router));
        let mut authority = authority::Fixture::new().await;
        authority.cycle(&node).await.unwrap();
        let id = format!("adx-late-{class}");
        let physical = json!({"id":id,"phase":"ready","sandbox_runtime":if class=="conch" {"conch"} else {"bwrap"},"ip_address":"192.0.2.2"});
        fixture
            .backend
            .refs
            .lock()
            .unwrap()
            .insert(id.clone(), physical.clone());
        if class == "conch" {
            fixture
                .native
                .physical
                .lock()
                .unwrap()
                .insert(id.clone(), physical);
        } else {
            fixture.host.supervisors.lock().unwrap().push(id);
            fixture.host.process_alive.store(true, Ordering::SeqCst);
        }
        // Ordinary data reconciliation does not scan backend execution state.
        authority.cycle(&node).await.unwrap();
        assert!(!fixture.backend.refs.lock().unwrap().is_empty(), "{class}");
        // A new recovery handshake performs complete orphan cleanup.
        authority.registration.reconciling = true;
        authority.cycle(&node).await.unwrap();
        assert!(fixture.backend.refs.lock().unwrap().is_empty(), "{class}");
        assert!(
            fixture.native.physical.lock().unwrap().is_empty(),
            "{class}"
        );
        assert!(
            fixture.host.supervisors.lock().unwrap().is_empty(),
            "{class}"
        );
        assert!(node.accepting_allocations());
        assert!(
            authority
                .authority
                .reports
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .accepting_allocations
        );
        assert!(!fixture.config.state_dir.join("ownership.sqlite").exists());
    }
}

#[tokio::test]
async fn conch_reserved_mount_targets_are_rejected_before_side_effects() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    for target in ["/run/custom", "/proc/custom", "/dev/custom", "/sys/custom"] {
        let mut wanted = spec("conch");
        wanted.execution.mounts.push(adx_core::execution::Mount {
            kind: "bind".into(),
            target: target.into(),
            options: vec!["ro".into()],
            source: adx_core::execution::StorageSource::Local("/tmp".into()),
            create_if_missing: false,
            create_options: None,
        });
        assert!(matches!(
            driver.start(&wanted, "tenant-agent-1", 1, &[]).await,
            Err(Error::Invalid(_))
        ));
    }
    assert_eq!(fixture.host.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.backend.count("POST", "/api/v1/sandboxes"), 0);
}

#[tokio::test]
async fn conch_content_id_binding_sends_only_template_id() {
    let mut fixture = Fixture::new();
    let template_id = format!("sha256:{}", "b".repeat(64));
    let mut config = serde_json::to_value(&fixture.config).unwrap();
    let image = config["classes"]["conch"]["images"]["agent:v1"]
        .as_object_mut()
        .unwrap();
    image.remove("template_name");
    image.insert("template_id".into(), json!(template_id));
    fixture.config = serde_json::from_value(config).unwrap();
    fixture.config.validate().unwrap();
    let driver = fixture.driver().await;
    driver
        .start_with_deadline(
            &spec("conch"),
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    {
        let calls = fixture.backend.calls.lock().unwrap();
        let body = &calls
            .iter()
            .find(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes")
            .unwrap()
            .2;
        assert_eq!(body["policy"]["conch"]["template_id"], template_id);
        assert!(body["policy"]["conch"].get("template_name").is_none());
    }
    driver.remove("tenant-agent-1").await.unwrap();
    let mut config = serde_json::to_value(&fixture.config).unwrap();
    config["classes"]["conch"]["images"]["agent:v1"]["template_name"] = json!("also-selected");
    let ambiguous: Config = serde_json::from_value(config).unwrap();
    assert!(ambiguous.validate().is_err());
}

#[tokio::test]
async fn supervisor_tunnel_acl_survives_create_and_routed_policy_update() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    // A create operation may outlive the fixture's short per-RPC timeout.
    fixture.backend.release.acquire().await.unwrap().forget();
    let backend = fixture.backend.clone();
    let release = tokio::spawn(async move {
        backend.entered.acquire().await.unwrap().forget();
        tokio::time::sleep(Duration::from_millis(250)).await;
        backend.release.add_permits(1);
    });
    let mut spec = spec("supervisor");
    let policy = NetworkPolicy {
        dns: None,
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Allow,
            mode: TrafficMode::Stateful,
            rules: vec![],
        }),
    };
    spec.execution.network = Some(policy.clone());
    spec.execution.tunnel_http_port = Some(18766);
    spec.env
        .insert("EXECD_TUNNEL_WS_PORT".into(), "12345".into());
    driver
        .start_with_deadline(
            &spec,
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    release.await.unwrap();
    let router = adxlet::runtime_router::RuntimeRouter::new(None, Some(driver)).unwrap();
    router
        .set_network_policy("tenant-agent-1", Some(&policy), &[], Some(18766))
        .await
        .unwrap();
    let calls = fixture.backend.calls.lock().unwrap();
    let create = &calls
        .iter()
        .find(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes")
        .unwrap()
        .2;
    let update = &calls.iter().find(|(m, _, _)| m == "PUT").unwrap().2;
    assert_eq!(create["env"]["EXECD_TUNNEL_WS_PORT"], "18765");
    for request in [create, update] {
        let network = &request["policy"]["network"];
        assert_eq!(network["ingress"]["allowed_ports"], json!([1, 18765]));
        assert_eq!(network["egress"]["allowed_ports"], json!([]));
        assert_eq!(network["egress"]["blocked_ports"], json!([]));
    }
}

#[tokio::test]
async fn conch_environment_identity_reaches_native_policy() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    let mut raw = serde_json::to_value(spec("conch")).unwrap();
    raw["execution"]["user"] = json!("1001");
    raw["execution"]["group"] = json!("1002");
    raw["execution"]["inherit_entrypoint"] = json!(false);
    let wanted: EnvironmentSpec = serde_json::from_value(raw).unwrap();
    driver
        .start_with_deadline(
            &wanted,
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    {
        let calls = fixture.backend.calls.lock().unwrap();
        let body = &calls
            .iter()
            .find(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes")
            .unwrap()
            .2;
        assert_eq!(body["policy"]["conch"]["run_as_user"], "1001");
        assert_eq!(body["policy"]["conch"]["run_as_group"], "1002");
    }
    driver.remove("tenant-agent-1").await.unwrap();
}

#[tokio::test]
async fn identity_override_is_not_silently_ignored_by_supervisor() {
    let fixture = Fixture::new();
    let driver = fixture.driver().await;
    let mut raw = serde_json::to_value(spec("supervisor")).unwrap();
    raw["execution"]["user"] = json!("1001");
    raw["execution"]["group"] = json!("1002");
    raw["execution"]["inherit_entrypoint"] = json!(false);
    let wanted: EnvironmentSpec = serde_json::from_value(raw).unwrap();
    assert!(driver
        .start_with_deadline(
            &wanted,
            "tenant-agent-1",
            1,
            &[],
            Instant::now() + Duration::from_secs(3)
        )
        .await
        .is_err());
    assert!(!fixture
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(m, p, _)| m == "POST" && p == "/api/v1/sandboxes"));
}
