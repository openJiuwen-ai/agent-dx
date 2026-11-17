use super::*;

struct RecoveredReadiness(RuntimeControlHub);

#[async_trait]
impl Readiness for RecoveredReadiness {
    async fn wait_ready(&self, record: &EnvironmentRecord) -> Result<()> {
        self.0
            .wait_ready(&RuntimeControlClient::identity(record))
            .await
    }
    async fn activity(&self, record: &EnvironmentRecord) -> Result<(u64, u64)> {
        let status = self
            .0
            .observed(&RuntimeControlClient::identity(record))?
            .ok_or_else(|| adx_core::Error::Unavailable("runtime disconnected".into()))?;
        Ok((status.activity_revision, status.active_requests))
    }
}

#[tokio::test]
async fn recovered_running_execution_pauses_after_600_idle_seconds_without_a_tick() {
    recovered_pause(false).await;
}

#[tokio::test]
async fn same_execution_takeover_preserves_the_original_idle_deadline() {
    recovered_pause(true).await;
}

async fn recovered_pause(takeover: bool) {
    let previous = Fixture::new().await;
    let backend = previous.backend.clone();
    let mut record = backend.records.borrow().as_ref().unwrap().clone();
    drop(previous);
    record.spec.lifecycle = serde_json::from_value(serde_json::json!({
        "idle_timeout_seconds":600, "idle_action":"pause",
        "pause":{"storage":"local", "retention":{"kind":"forever"}, "timeout_seconds":30}
    }))
    .unwrap();
    backend.records.send_replace(Some(record.clone()));
    backend.proxy_streams.store(1, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let hub = RuntimeControlHub::new(SECRET.to_vec(), Duration::from_secs(2)).unwrap();
    let control = RuntimeControlClient::new(50090, Duration::from_secs(2))
        .unwrap()
        .with_stream(hub.clone());
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        Arc::new(RecoveredReadiness(hub.clone())),
        backend.clone(),
        backend.clone(),
    )
    .with_runtime_control(control)
    .unwrap()
    .with_checkpointing(
        Arc::new(adxlet::checkpoint::LocalCheckpointStore::new(directory.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap();
    node.update_capacity(
        Resources {
            cpu_millis: 100,
            memory_bytes: 4096,
            disk_bytes: 0,
        },
        Duration::from_secs(3600),
    )
    .unwrap();
    let identity = RuntimeControlClient::identity(&record);
    assert!(
        hub.subscribe(&identity).is_err(),
        "new process must start without a pre-registered runtime"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server_hub = hub.clone();
    let server = tokio::spawn(async move {
        server_hub.serve(listener).await.unwrap();
    });
    let fixture = Fixture {
        node,
        backend,
        hub,
        identity,
        address,
        server,
    };
    let resources = record.spec.resources;
    let recovery_node = fixture.node.clone();
    let recovering = tokio::spawn(async move { recovery_node.reconcile(vec![record]).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.hub.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut status = fixture.status();
    status.active_requests = 1;
    let (send, mut commands) = fixture.connect(status.clone()).await;
    loop {
        let command = tokio::time::timeout(Duration::from_secs(2), commands.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let operation = serde_json::from_slice::<adx_protocol::runtime_stream::ControlOperation>(
            &command.operation_json,
        )
        .unwrap();
        let reopened = matches!(
            operation,
            adx_protocol::runtime_stream::ControlOperation::ReopenIdle { .. }
        );
        assert!(
            reopened
                || matches!(
                    operation,
                    adx_protocol::runtime_stream::ControlOperation::Status
                ),
            "unexpected recovery operation: {operation:?}"
        );
        status.revision += 1;
        send.send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
                request_id: command.request_id,
                status_json: serde_json::to_vec(&status).unwrap(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
        if reopened {
            break;
        }
    }
    recovering.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture
            .backend
            .reported_proxy_revision
            .load(Ordering::SeqCst)
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(600)).await;
    assert_eq!(
        fixture.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
    assert_eq!(fixture.node.used(), resources);
    tokio::time::resume();

    // A user disconnects on both sides; the Execd control stream remains connected.
    status.revision += 1;
    let idle_revision = status.revision;
    status.activity_revision = 2;
    status.active_requests = 0;
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::StatusJson(
            serde_json::to_vec(&status).unwrap(),
        )),
    })
    .await
    .unwrap();
    fixture.backend.proxy_streams.store(0, Ordering::SeqCst);
    fixture.backend.proxy.send_replace(2);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture
            .hub
            .observed(&fixture.identity)
            .unwrap()
            .as_ref()
            .unwrap()
            .revision
            != idle_revision
            || fixture
                .backend
                .reported_proxy_revision
                .load(Ordering::SeqCst)
                != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(599)).await;
    assert_eq!(
        fixture.backend.records.borrow().as_ref().unwrap().state,
        EnvironmentState::Running
    );
    assert_eq!(fixture.node.used(), resources);
    if takeover {
        tokio::time::resume();
        let current = fixture.backend.records.borrow().as_ref().unwrap().clone();
        let node = fixture.node.clone();
        let mut recovering = tokio::spawn(async move { node.reconcile(vec![current]).await });
        loop {
            tokio::select! {
                result = &mut recovering => { result.unwrap().unwrap(); break; },
                command = commands.message() => {
                    let command = command.unwrap().unwrap();
                    let operation: adx_protocol::runtime_stream::ControlOperation = serde_json::from_slice(&command.operation_json).unwrap();
                    assert!(matches!(operation, adx_protocol::runtime_stream::ControlOperation::Status
                        | adx_protocol::runtime_stream::ControlOperation::ReopenIdle { .. }));
                    status.revision += 1;
                    send.send(pb::RuntimeEvent {
                        event: Some(pb::runtime_event::Event::Reply(pb::RuntimeReply {
                            request_id: command.request_id,
                            status_json: serde_json::to_vec(&status).unwrap(),
                            ..Default::default()
                        })),
                    }).await.unwrap();
                }
            }
        }
        tokio::time::pause();
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let mut records = fixture.backend.records.subscribe();
    let paused = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(record) = records
                .borrow_and_update()
                .as_ref()
                .filter(|r| r.state == EnvironmentState::Paused)
            {
                break record.clone();
            }
            records.changed().await.unwrap();
        }
    })
    .await
    .expect(
        "restored execution must auto-Pause at the 600-second idle deadline without a monitor tick",
    );
    assert!(!paused.resources_held);
    assert_eq!(fixture.node.used(), Resources::default());
    assert!(!*fixture.backend.live.borrow());
    assert_eq!(
        fixture
            .backend
            .idle_prepared_revision
            .load(Ordering::SeqCst),
        2
    );
    assert!(paused.checkpoint.is_some());
}
