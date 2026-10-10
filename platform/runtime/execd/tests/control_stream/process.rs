//! Boot the production Execd binary and exercise its local Unix checkpoint API.
//! FIFO simulates sandboxd's handoff; this is not a kernel checkpoint E2E.
use super::*;
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources};
use adxlet::{runtime_control::RuntimeControlClient, Readiness, RuntimeDriver};
use async_trait::async_trait;
use std::{
    ffi::CString,
    fs::OpenOptions,
    io::Write,
    net::IpAddr,
    process::{Child, Command, Stdio},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};
struct Process {
    child: Child,
    temp: tempfile::TempDir,
    writer: Option<std::fs::File>,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn start(address: &str) -> Process {
    let temp = tempfile::tempdir().unwrap();
    let fifo = temp.path().join("handoff");
    let name = CString::new(fifo.to_str().unwrap()).unwrap();
    // SAFETY: NUL-terminated name is valid throughout mkfifo and is not retained.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fifo)
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::fs::write(
        temp.path().join("env"),
        "ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-1\nADX_OWNERSHIP_GENERATION=1\n",
    )
    .unwrap();
    let log = std::fs::File::create(temp.path().join("execd.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_adx-execd"))
        .env_remove("ADX_SEED_FILE")
        .env_remove("ADX_IMAGE_PROCESS_CONFIG")
        .env_remove("EXECD_TUNNEL_ONLY")
        .env_remove("EXECD_TUNNEL_WS_PORT")
        .env("EXECD_HTTP_ONLY", "1")
        .env("EXECD_HTTP_PORT", port.to_string())
        .env("EXECD_HTTP_TOKEN", "test-http-token")
        .env("ADX_ENVIRONMENT_ID", "i")
        .env("ADX_RUNTIME_ID", "i-1")
        .env("ADX_OWNERSHIP_GENERATION", "1")
        .env("ADX_RUNTIME_CONTROL_ADDRESS", address)
        .env(
            "ADX_RUNTIME_CONTROL_TOKEN",
            execution_token(SECRET, &identity()).unwrap(),
        )
        .env("ADX_EXECD_CONTROL_SOCKET_PATH", temp.path())
        .env("ADX_ENV_FILE", temp.path().join("env"))
        .env("ADX_CHECKPOINT_HANDOFF_FILE", fifo)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    Process {
        child,
        temp,
        writer: Some(writer),
    }
}
fn record() -> EnvironmentRecord {
    EnvironmentRecord {
        spec: EnvironmentSpec {
            id: "i".into(),
            tenant_id: "t".into(),
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            image: "execd".into(),
            runtime_class: "runsc".into(),
            resources: Resources::default(),
            priority: 0,
            sandbox: Default::default(),
        },
        assignment: Assignment {
            environment_id: "i".into(),
            node_id: "n".into(),
            generation: 1,
            shard_id: 0,
            devices: vec![],
        },
        state: EnvironmentState::Starting,
        revision: 1,
        runtime: adx_core::Runtime {
            id: "i-1".into(),
            ip: None,
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    }
}
struct Backend;
#[async_trait]
impl RuntimeDriver for Backend {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> adx_core::Result<IpAddr> {
        unreachable!()
    }
    async fn is_running(&self, _: &str) -> adx_core::Result<bool> {
        Ok(true)
    }
    async fn remove(&self, _: &str) -> adx_core::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn binary_boot_ready_and_unix_checkpoint_complete_over_grpc_without_http_probe() {
    let test = setup(None, true).await;
    test.client.abort();
    test.hub.disconnect(&identity()).unwrap();
    let mut process = start(&test.address);
    let control = RuntimeControlClient::new(1, Duration::from_secs(2))
        .unwrap()
        .with_stream(test.hub.clone());
    let readiness = adxlet::readiness::ExecdReadiness::with_client(
        Arc::new(Backend),
        control.clone(),
        Duration::from_secs(60),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    let record = record();
    readiness.expect_runtime(&record).unwrap();
    readiness.wait_ready(&record).await.unwrap(); // record has no IP; HTTP cannot be used.
    let mut events = test.hub.take_events().unwrap();
    let path = process.temp.path().join("execd.sock");
    let caller = tokio::spawn(async move {
        let mut socket = UnixStream::connect(path).await.unwrap();
        socket.write_all(b"POST /checkpoint HTTP/1.1\r\nHost: localhost\r\nContent-Length: 20\r\n\r\n{\"timeoutSeconds\":5}").await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    let status = control.status(&record).await.unwrap();
    let id = status.requested_checkpoint.clone().unwrap();
    assert!(status.requested_checkpoint_deadline_unix_millis.is_some());
    control
        .prepare(&record, &id, status.revision)
        .await
        .unwrap();
    assert!(!caller.is_finished());
    let mut writer = process.writer.take().unwrap();
    writer.write_all(b"resume").unwrap();
    drop(writer);
    test.hub.wait_resumed(&identity(), &id).await.unwrap();
    // Completion must remain deliverable after the original stream is gone.
    test.hub.disconnect(&identity()).unwrap();
    control.finish_checkpoint(&record, &id, None).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), caller)
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("completed"));
    assert!(control
        .status(&record)
        .await
        .unwrap()
        .requested_checkpoint
        .is_none());
}

struct ManagedRuntime {
    address: String,
    hub: RuntimeControlHub,
    process: Mutex<Option<Process>>,
}
#[async_trait]
impl RuntimeDriver for ManagedRuntime {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> adx_core::Result<IpAddr> {
        assert!(
            self.hub.observed(&identity())?.is_none(),
            "expectation must precede Start"
        );
        *self.process.lock().unwrap() = Some(start(&self.address));
        // Deliberately let Ready arrive before the backend Start response.
        self.hub.wait_ready(&identity()).await?;
        Ok("127.0.0.1".parse().unwrap())
    }
    async fn is_running(&self, _: &str) -> adx_core::Result<bool> {
        Ok(self.process.lock().unwrap().is_some())
    }
    async fn remove(&self, _: &str) -> adx_core::Result<()> {
        self.process.lock().unwrap().take();
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> adx_core::Result<()> {
        Ok(())
    }
    async fn checkpoint_running(
        &self,
        _: &str,
        path: &std::path::Path,
        _: Duration,
    ) -> adx_core::Result<()> {
        std::fs::write(path.join("checkpoint"), b"fixture; not a kernel checkpoint").unwrap();
        let mut current = self.process.lock().unwrap();
        let mut writer = current.as_mut().unwrap().writer.take().unwrap();
        writer.write_all(b"resume").unwrap();
        drop(writer);
        Ok(())
    }
}
struct Publication {
    checkpoint_entered: tokio::sync::Semaphore,
    checkpoint_release: tokio::sync::Semaphore,
    record: Mutex<Option<EnvironmentRecord>>,
}
#[async_trait]
impl adxlet::StateSink for Publication {
    async fn commit(&self, record: &EnvironmentRecord) -> adx_core::Result<adxlet::Durability> {
        *self.record.lock().unwrap() = Some(record.clone());
        if record.checkpoint.is_some() {
            self.checkpoint_entered.add_permits(1);
            self.checkpoint_release.acquire().await.unwrap().forget();
        }
        Ok(adxlet::Durability::Published)
    }
}
struct Routes;
#[async_trait]
impl adxlet::Routes for Routes {
    async fn activate(&self, record: &EnvironmentRecord) -> adx_core::Result<()> {
        assert_eq!(record.runtime.id, "i-1");
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn adxlet_creation_and_event_driven_checkpoint_wait_for_state_publication() {
    let test = setup(None, true).await;
    test.client.abort();
    test.hub.disconnect(&identity()).unwrap();
    let runtime = Arc::new(ManagedRuntime {
        address: test.address.clone(),
        hub: test.hub.clone(),
        process: Mutex::default(),
    });
    let publication = Arc::new(Publication {
        checkpoint_entered: tokio::sync::Semaphore::new(0),
        checkpoint_release: tokio::sync::Semaphore::new(0),
        record: Mutex::default(),
    });
    let control = RuntimeControlClient::new(1, Duration::from_secs(2))
        .unwrap()
        .with_stream(test.hub.clone());
    let readiness = adxlet::readiness::ExecdReadiness::with_client(
        runtime.clone(),
        control.clone(),
        Duration::from_secs(60),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let node = adxlet::Adxlet::new(
        "n".into(),
        runtime.clone(),
        Arc::new(readiness),
        Arc::new(Routes),
        publication.clone(),
    )
    .with_runtime_control(control.clone())
    .unwrap()
    .with_checkpointing(
        Arc::new(adxlet::checkpoint::LocalCheckpointStore::new(temp.path().into()).unwrap()),
        Arc::new(control),
    )
    .unwrap();
    let mut record = record();
    record.spec.resources = Resources {
        cpu_millis: 100,
        memory_bytes: 1024,
        disk_bytes: 1024,
    };
    node.update_capacity(record.spec.resources, Duration::from_secs(30))
        .unwrap();
    let handle = node
        .environment(record.spec.clone(), record.assignment.clone())
        .unwrap();
    let created = handle.create().await.unwrap();
    assert_eq!(created.record.state, EnvironmentState::Running);
    assert_eq!(created.durability, adxlet::Durability::Published);
    let node = Arc::new(node);
    let mut events = test.hub.take_events().unwrap();
    let path = runtime
        .process
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .temp
        .path()
        .join("execd.sock");
    let caller = tokio::spawn(async move {
        let mut socket = UnixStream::connect(path).await.unwrap();
        socket.write_all(b"POST /checkpoint HTTP/1.1\r\nHost: localhost\r\nContent-Length: 20\r\n\r\n{\"timeoutSeconds\":5}").await.unwrap();
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    let mut stale_event = event.clone();
    stale_event.ownership_generation += 1;
    node.observe_runtime_event(&stale_event).await.unwrap();
    assert!(publication
        .record
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .checkpoint
        .is_none());
    assert!(!caller.is_finished());
    let current = node.clone();
    let handling = tokio::spawn(async move { current.observe_runtime_event(&event).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        publication.checkpoint_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    assert!(
        !caller.is_finished(),
        "backend resume is not durable completion"
    );
    assert_eq!(node.used(), record.spec.resources);
    let committed = publication.record.lock().unwrap().clone().unwrap();
    assert!(committed.checkpoint.is_some());
    assert_eq!(committed.runtime.id, "i-1");
    publication.checkpoint_release.add_permits(1);
    handling.await.unwrap().unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), caller)
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    publication.checkpoint_release.add_permits(1); // Delete retains the committed recovery reference during publication.
    handle.delete().await.unwrap();
    assert!(test.hub.is_empty());
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn restored_binary_reconnects_with_target_identity_and_scoped_credential() {
    let test = setup(None, true).await;
    test.client.abort();
    test.hub.disconnect(&identity()).unwrap();
    let mut process = start(&test.address);
    let control = RuntimeControlClient::new(1, Duration::from_secs(2))
        .unwrap()
        .with_stream(test.hub.clone());
    test.hub.wait_ready(&identity()).await.unwrap();
    let original = record();
    let status = control.status(&original).await.unwrap();
    control
        .prepare(&original, "restore", status.revision)
        .await
        .unwrap();
    let mut target = original.clone();
    target.assignment.generation = 2;
    target.runtime.id = "i-2".into();
    let target_identity = RuntimeControlClient::identity(&target);
    test.hub.expect(&target_identity).unwrap();
    let environment = process.temp.path().join("restored-env");
    std::fs::write(&environment, format!("ADX_ENVIRONMENT_ID=i\nADX_RUNTIME_ID=i-2\nADX_OWNERSHIP_GENERATION=2\nADX_RUNTIME_CONTROL_ADDRESS={}\nADX_RUNTIME_CONTROL_TOKEN={}\n", test.address, execution_token(SECRET, &target_identity).unwrap())).unwrap();
    // The existing boot-time path is refreshed only after backend handoff.
    std::fs::copy(&environment, process.temp.path().join("env")).unwrap();
    let mut writer = process.writer.take().unwrap();
    writer.write_all(b"restore").unwrap();
    drop(writer);
    test.hub.wait_ready(&target_identity).await.unwrap();
    test.hub.retire(&identity());
    let restored = control.status(&target).await.unwrap();
    assert_eq!(restored.identity, target_identity);
    assert_eq!(
        restored.checkpoint.unwrap().phase,
        CheckpointPhase::Restored
    );
    assert!(control.status(&original).await.is_err());
}
