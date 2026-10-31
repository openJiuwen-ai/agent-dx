//! Lifecycle transitions without a status scan or monitor tick.
use adx_core::{
    runtime::*, Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Result,
};
use adx_protocol::{runtime as pb, runtime_stream::execution_token};
use adxlet::{
    control_stream::RuntimeControlHub, runtime_control::RuntimeControlClient, Adxlet, Durability,
    Readiness, Routes, RuntimeDriver, StateSink,
};
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";
struct Backend {
    live: watch::Sender<bool>,
    records: watch::Sender<Option<EnvironmentRecord>>,
    observations: AtomicUsize,
    stats: AtomicUsize,
    stats_delay: AtomicU64,
    cleanup_failures: AtomicUsize,
    reopens: AtomicUsize,
    activations: AtomicUsize,
    proxy: watch::Sender<u64>,
}
impl Default for Backend {
    fn default() -> Self {
        Self {
            live: watch::channel(false).0,
            records: watch::channel(None).0,
            observations: AtomicUsize::new(0),
            stats: AtomicUsize::new(0),
            stats_delay: AtomicU64::new(0),
            cleanup_failures: AtomicUsize::new(0),
            reopens: AtomicUsize::new(0),
            activations: AtomicUsize::new(0),
            proxy: watch::channel(1).0,
        }
    }
}
#[async_trait]
impl RuntimeDriver for Backend {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.live.send_replace(true);
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        let records = self.records.borrow();
        let Some(record) = records.as_ref().filter(|_| *self.live.borrow()) else {
            return Ok(vec![]);
        };
        Ok(vec![adxlet::RuntimeObservation {
            environment_id: record.spec.id.clone(),
            runtime_id: record.runtime.id.clone(),
            generation: record.assignment.generation,
            tenant_id: record.spec.tenant_id.clone(),
            running: true,
        }])
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        Ok(*self.live.borrow())
    }
    async fn wait_exit(&self, _: &str) -> Result<adx_core::RuntimeFailure> {
        let mut live = self.live.subscribe();
        while *live.borrow_and_update() {
            live.changed().await.unwrap();
        }
        Ok(adx_core::RuntimeFailure {
            reason: adx_core::RuntimeFailureReason::Exited,
            exit_code: Some(137),
            probe_failure: None,
            message: "backend exited".into(),
        })
    }
    async fn remove(&self, _: &str) -> Result<()> {
        if self
            .cleanup_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            return Err(adx_core::Error::Unavailable(
                "transient cleanup failure".into(),
            ));
        }
        self.live.send_replace(false);
        Ok(())
    }
    async fn stats(&self, _: &str) -> Result<adxlet::metrics::RuntimeUsage> {
        self.stats.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.stats_delay.load(Ordering::SeqCst),
        ))
        .await;
        Ok(adxlet::metrics::RuntimeUsage {
            cpu_usage_ns: 10,
            memory_usage_bytes: 20,
            memory_limit_bytes: 30,
        })
    }
}
#[async_trait]
impl Readiness for Backend {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Backend {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        self.activations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn activity_changed(
        &self,
        _: &EnvironmentRecord,
        previous: Option<(String, u64, u64)>,
    ) -> Result<(String, u64, u64)> {
        let mut changes = self.proxy.subscribe();
        loop {
            let current = ("proxy".to_string(), *changes.borrow_and_update(), 0);
            if previous.as_ref() != Some(&current) {
                return Ok(current);
            }
            changes.changed().await.unwrap();
        }
    }
    async fn reopen(&self, _: &EnvironmentRecord) -> Result<()> {
        self.reopens.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn quiesce(&self, _: &EnvironmentRecord, _: &str, revision: u64) -> Result<bool> {
        Ok(revision == *self.proxy.borrow())
    }
}
#[async_trait]
impl StateSink for Backend {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        self.records.send_replace(Some(record.clone()));
        Ok(Durability::Published)
    }
}
struct Fixture {
    node: Adxlet,
    backend: Arc<Backend>,
    hub: RuntimeControlHub,
    identity: RuntimeIdentity,
    address: String,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        Self::with_lifecycle(serde_json::json!({})).await
    }
    async fn with_lifecycle(lifecycle: serde_json::Value) -> Self {
        let backend = Arc::new(Backend::default());
        let hub = RuntimeControlHub::new(SECRET.to_vec(), Duration::from_secs(1)).unwrap();
        let control = RuntimeControlClient::new(50090, Duration::from_secs(1))
            .unwrap()
            .with_stream(hub.clone());
        let node = Adxlet::new(
            "node".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
        )
        .with_runtime_control(control)
        .unwrap();
        node.update_capacity(
            Resources {
                cpu_millis: 100,
                memory_bytes: 4096,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"event", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc", "priority":0, "lifecycle": lifecycle,
            "resources":{"cpu_millis":1,"memory_bytes":1024,"disk_bytes":0}
        })).unwrap();
        hub.expect(&RuntimeIdentity {
            environment_id: "event".into(),
            runtime_id: "event-1".into(),
            ownership_generation: 1,
        })
        .unwrap();
        let handle = node
            .environment(
                spec,
                Assignment {
                    environment_id: "event".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: 1,
                    devices: vec![],
                },
            )
            .unwrap();
        let record = handle.create().await.unwrap().record;
        let identity = RuntimeControlClient::identity(&record);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let server_hub = hub.clone();
        let server = tokio::spawn(async move {
            server_hub.serve(listener).await.unwrap();
        });
        Self {
            node,
            backend,
            hub,
            identity,
            address,
            server,
        }
    }
    fn status(&self) -> RuntimeStatus {
        RuntimeStatus {
            required_services: Default::default(),
            identity: self.identity.clone(),
            revision: 1,
            phase: RuntimePhase::Running,
            failure: None,
            checkpoint: None,
            requested_checkpoint: None,
            requested_checkpoint_deadline_unix_millis: None,
            active_requests: 0,
            active_commands: 0,
            activity_revision: 1,
        }
    }
    async fn connect(
        &self,
        status: RuntimeStatus,
    ) -> (
        mpsc::Sender<pb::RuntimeEvent>,
        tonic::Streaming<pb::RuntimeCommand>,
    ) {
        self.connect_result(status).await.unwrap()
    }
    async fn connect_result(
        &self,
        status: RuntimeStatus,
    ) -> std::result::Result<
        (
            mpsc::Sender<pb::RuntimeEvent>,
            tonic::Streaming<pb::RuntimeCommand>,
        ),
        tonic::Status,
    > {
        let mut client = pb::runtime_control_service_client::RuntimeControlServiceClient::connect(
            self.address.clone(),
        )
        .await
        .unwrap();
        let (send, receive) = mpsc::channel(16);
        send.send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
                token: execution_token(SECRET, &self.identity).unwrap(),
                status_json: serde_json::to_vec(&status).unwrap(),
            })),
        })
        .await
        .unwrap();
        let commands = client
            .open_control(tokio_stream::wrappers::ReceiverStream::new(receive))
            .await?
            .into_inner();
        Ok((send, commands))
    }
    async fn failed(&self) -> EnvironmentRecord {
        let mut records = self.backend.records.subscribe();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(record) = records
                    .borrow_and_update()
                    .as_ref()
                    .filter(|record| record.state == EnvironmentState::Failed)
                {
                    return record.clone();
                }
                records.changed().await.unwrap();
            }
        })
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn metrics_sampling_does_not_observe_or_advance_lifecycle() {
    let test = Fixture::new().await;
    let baseline = test.backend.observations.load(Ordering::SeqCst);
    for _ in 0..3 {
        test.node.sample_metrics().await.unwrap();
    }
    assert_eq!(test.backend.stats.load(Ordering::SeqCst), 3);
    assert_eq!(test.backend.observations.load(Ordering::SeqCst), baseline);
    assert_eq!(
        test.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
}
#[tokio::test]
async fn backend_exit_commits_failure_without_polling() {
    let test = Fixture::new().await;
    test.backend.live.send_replace(false);
    let record = test.failed().await;
    assert_eq!(record.failure.unwrap().exit_code, Some(137));
    assert!(!record.resources_held);
}
#[tokio::test]
async fn explicit_control_failure_commits_without_a_monitor_tick() {
    let test = Fixture::new().await;
    let (send, _commands) = test.connect(test.status()).await;
    let mut status = test.status();
    status.phase = RuntimePhase::Failed;
    status.revision = 2;
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::StatusJson(
            serde_json::to_vec(&status).unwrap(),
        )),
    })
    .await
    .unwrap();
    assert!(!test.failed().await.resources_held);
}
#[tokio::test]
async fn stream_reconnect_keeps_execution_and_replays_latest_failure() {
    let test = Fixture::new().await;
    let (send, commands) = test.connect(test.status()).await;
    test.hub.wait_ready(&test.identity).await.unwrap();
    let baseline = test.backend.observations.load(Ordering::SeqCst);
    drop(send);
    drop(commands);
    tokio::time::timeout(Duration::from_secs(1), async {
        while test.hub.observed(&test.identity).unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        test.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
    let mut status = test.status();
    status.phase = RuntimePhase::Failed;
    status.revision = 2;
    // A terminal Hello can retire its TCP transport before response headers
    // arrive. The replay must still commit failure without a backend poll.
    let _connection = test.connect_result(status).await;
    test.failed().await;
    assert_eq!(test.backend.observations.load(Ordering::SeqCst), baseline);
}

#[tokio::test]
async fn slow_stats_do_not_block_exit_or_restore_a_retired_sample() {
    let test = Fixture::new().await;
    test.backend.stats_delay.store(1000, Ordering::SeqCst);
    let node = test.node.clone();
    let sampling = tokio::spawn(async move {
        node.sample_metrics().await.unwrap();
    });
    while test.backend.stats.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    test.backend.live.send_replace(false);
    test.failed().await;
    assert!(
        !sampling.is_finished(),
        "exit handling must not wait for a slow sample"
    );
    sampling.await.unwrap();
    assert!(!test
        .node
        .environment_metrics()
        .contains("environment_id=\"event\""));
}

#[tokio::test]
async fn idle_deadline_uses_fresh_two_sided_barrier_without_polling() {
    let test = Fixture::with_lifecycle(serde_json::json!({"idle_timeout_seconds":1})).await;
    let (send, mut commands) = test.connect(test.status()).await;
    let baseline = test.backend.observations.load(Ordering::SeqCst);
    let first = tokio::time::timeout(Duration::from_secs(3), commands.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let operation: adx_protocol::runtime_stream::ControlOperation =
        serde_json::from_slice(&first.operation_json).unwrap();
    assert!(matches!(
        operation,
        adx_protocol::runtime_stream::ControlOperation::Quiesce {
            activity_revision: 1,
            ..
        }
    ));
    // New direct activity invalidates the cached idle decision at the runtime barrier.
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
            request_id: first.request_id,
            error_code: "CONFLICT".into(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    assert_eq!(
        test.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
    let reopen = tokio::time::timeout(Duration::from_secs(3), commands.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        serde_json::from_slice::<adx_protocol::runtime_stream::ControlOperation>(
            &reopen.operation_json
        )
        .unwrap(),
        adx_protocol::runtime_stream::ControlOperation::ReopenIdle { .. }
    ));
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
            request_id: reopen.request_id,
            status_json: serde_json::to_vec(&test.status()).unwrap(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(3), commands.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
            request_id: second.request_id,
            status_json: serde_json::to_vec(&test.status()).unwrap(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut records = test.backend.records.subscribe();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if records
                .borrow_and_update()
                .as_ref()
                .is_some_and(|record| record.state == EnvironmentState::Deleted)
            {
                break;
            }
            records.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(test.backend.observations.load(Ordering::SeqCst), baseline);
    assert!(!*test.backend.live.borrow());
}

#[tokio::test]
async fn lost_control_stream_confirms_backend_only_after_recovery_grace() {
    let test = Fixture::new().await;
    let (send, commands) = test.connect(test.status()).await;
    test.hub.wait_ready(&test.identity).await.unwrap();
    let baseline = test.backend.observations.load(Ordering::SeqCst);
    drop(send);
    drop(commands);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(test.backend.observations.load(Ordering::SeqCst), baseline);
    tokio::time::timeout(Duration::from_secs(7), async {
        while test.backend.observations.load(Ordering::SeqCst) == baseline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        test.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
    let (_send, _commands) = test.connect(test.status()).await;
    test.hub.wait_ready(&test.identity).await.unwrap();
    assert_eq!(
        test.backend.observations.load(Ordering::SeqCst),
        baseline + 1
    );
}

#[tokio::test]
async fn backend_failure_retries_cleanup_after_leaving_running() {
    let test = Fixture::new().await;
    test.backend.cleanup_failures.store(1, Ordering::SeqCst);
    test.backend.live.send_replace(false);
    let mut records = test.backend.records.subscribe();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if records.borrow_and_update().as_ref().is_some_and(|record| {
                record.state == EnvironmentState::Failed && !record.resources_held
            }) {
                break;
            }
            records.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(test.node.used(), Resources::default());
}

#[tokio::test]
async fn restart_deadline_is_bounded_and_does_not_require_a_monitor_tick() {
    let test = Fixture::with_lifecycle(serde_json::json!({
        "restart":{"max_attempts":1,"initial_backoff_seconds":1,"max_backoff_seconds":1}
    }))
    .await;
    test.backend.live.send_replace(false);
    let mut records = test.backend.records.subscribe();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if records.borrow_and_update().as_ref().is_some_and(|record| {
                record.state == EnvironmentState::Running && record.restart_attempts == 1
            }) {
                break;
            }
            records.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(*test.backend.live.borrow());
    test.backend.live.send_replace(false);
    let failed = test.failed().await;
    assert_eq!(failed.restart_attempts, 1);
    assert!(!failed.restart_pending);
    assert!(!failed.resources_held);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!*test.backend.live.borrow());
}

#[tokio::test]
async fn passive_commands_do_not_postpone_a_pending_restart() {
    let test = Fixture::with_lifecycle(serde_json::json!({
        "restart":{"max_attempts":1,"initial_backoff_seconds":1,"max_backoff_seconds":1}
    }))
    .await;
    let record = test.backend.records.borrow().as_ref().unwrap().clone();
    let handle = test
        .node
        .environment(record.spec, record.assignment)
        .unwrap();
    test.backend.live.send_replace(false);
    assert!(test.failed().await.restart_pending);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = handle.sync().await.unwrap().record;
            if record.state == EnvironmentState::Running && record.restart_attempts == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("passive commands must not keep moving the restart deadline");
}

#[tokio::test]
async fn lost_idle_barrier_reply_reopens_runtime_before_proxy_after_reconnect() {
    let test = Fixture::with_lifecycle(serde_json::json!({"idle_timeout_seconds":1})).await;
    let (send, mut commands) = test.connect(test.status()).await;
    let command = tokio::time::timeout(Duration::from_secs(3), commands.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        serde_json::from_slice::<adx_protocol::runtime_stream::ControlOperation>(
            &command.operation_json
        )
        .unwrap(),
        adx_protocol::runtime_stream::ControlOperation::Quiesce { .. }
    ));
    // Runtime may have closed admission; drop its reply and reconnect with full state.
    drop(send);
    drop(commands);
    tokio::time::timeout(Duration::from_secs(1), async {
        while test.hub.observed(&test.identity).unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(test.backend.reopens.load(Ordering::SeqCst), 0);
    let (send, mut commands) = test.connect(test.status()).await;
    let reopen = tokio::time::timeout(Duration::from_secs(4), commands.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        serde_json::from_slice::<adx_protocol::runtime_stream::ControlOperation>(
            &reopen.operation_json
        )
        .unwrap(),
        adx_protocol::runtime_stream::ControlOperation::ReopenIdle { .. }
    ));
    assert_eq!(test.backend.reopens.load(Ordering::SeqCst), 0);
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
            request_id: reopen.request_id,
            status_json: serde_json::to_vec(&test.status()).unwrap(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while test.backend.reopens.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        test.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
}

#[tokio::test]
async fn node_recovery_reopens_runtime_admission_before_publishing_route() {
    let test = Fixture::new().await;
    let (send, mut commands) = test.connect(test.status()).await;
    test.hub.wait_ready(&test.identity).await.unwrap();
    let node = test.node.clone();
    let record = test.backend.records.borrow().clone().unwrap();
    let recovering = tokio::spawn(async move { node.reconcile(vec![record]).await });
    let before = test.backend.activations.load(Ordering::SeqCst);
    loop {
        let command = tokio::time::timeout(Duration::from_secs(3), commands.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let operation: adx_protocol::runtime_stream::ControlOperation =
            serde_json::from_slice(&command.operation_json).unwrap();
        let reopened = matches!(
            operation,
            adx_protocol::runtime_stream::ControlOperation::ReopenIdle { .. }
        );
        assert!(
            reopened
                || matches!(
                    operation,
                    adx_protocol::runtime_stream::ControlOperation::Status
                )
        );
        assert_eq!(test.backend.activations.load(Ordering::SeqCst), before);
        send.send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
                request_id: command.request_id,
                status_json: serde_json::to_vec(&test.status()).unwrap(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
        if reopened {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(3), recovering)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(test.backend.activations.load(Ordering::SeqCst), before + 1);
}
