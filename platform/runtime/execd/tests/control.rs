use adx_core::runtime::{CheckpointPhase, PrepareCheckpoint, RuntimeIdentity, RuntimePhase};
use adx_execd::runtime::control::{CheckpointHooks, Controller, Handoff, HandoffOutcome};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::oneshot;
struct Hooks {
    read: Mutex<Option<oneshot::Receiver<HandoffOutcome>>>,
    opens: AtomicUsize,
}
impl CheckpointHooks for Hooks {
    fn open(&self) -> io::Result<Handoff> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let rx = self.read.lock().unwrap().take().unwrap();
        Ok(Box::pin(async { rx.await.map_err(io::Error::other) }))
    }
    fn restore(&self, old: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        Ok(RuntimeIdentity {
            environment_id: old.environment_id.clone(),
            runtime_id: "i-2".into(),
            ownership_generation: 2,
        })
    }
}
fn identity() -> RuntimeIdentity {
    RuntimeIdentity {
        environment_id: "i".into(),
        runtime_id: "i-1".into(),
        ownership_generation: 1,
    }
}
#[tokio::test]
async fn prepare_ack_requires_reader_and_abort_reuses_it() {
    let (tx, rx) = oneshot::channel();
    let hooks = Arc::new(Hooks {
        read: Mutex::new(Some(rx)),
        opens: AtomicUsize::new(0),
    });
    let control = Controller::new(identity(), hooks.clone()).unwrap();
    let request = PrepareCheckpoint {
        identity: identity(),
        operation_id: "a".into(),
        expected_revision: 1,
    };
    let first = control.prepare(request.clone()).await.unwrap();
    assert_eq!(
        first.checkpoint.as_ref().unwrap().phase,
        CheckpointPhase::Prepared
    );
    assert_eq!(control.prepare(request).await.unwrap(), first);
    control
        .abort_unstarted("a", &identity(), first.revision)
        .unwrap();
    let second = control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "b".into(),
            expected_revision: control.status().revision,
        })
        .await
        .unwrap();
    assert_eq!(hooks.opens.load(Ordering::SeqCst), 1);
    tx.send(HandoffOutcome::Resume).unwrap();
    let mut changes = control.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while control.status().phase != RuntimePhase::Running {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(control.status().checkpoint.unwrap().operation_id, "b");
    assert!(second.revision > first.revision);
}
#[tokio::test]
async fn restore_refreshes_identity_and_rejects_old_owner() {
    let (tx, rx) = oneshot::channel();
    let control = Controller::new(
        identity(),
        Arc::new(Hooks {
            read: Mutex::new(Some(rx)),
            opens: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "a".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    tx.send(HandoffOutcome::Restore).unwrap();
    let mut changes = control.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while control.status().identity.ownership_generation != 2 {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(control.status().identity.runtime_id, "i-2");
    assert!(control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "b".into(),
            expected_revision: control.status().revision
        })
        .await
        .is_err());
}

struct MissingHandoff;
impl CheckpointHooks for MissingHandoff {
    fn open(&self) -> io::Result<Handoff> {
        Err(io::Error::other("handoff unavailable"))
    }
    fn restore(&self, _: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        unreachable!()
    }
}
#[tokio::test]
async fn missing_handoff_never_acknowledges_prepared() {
    let control = Controller::new(identity(), Arc::new(MissingHandoff)).unwrap();
    let request = PrepareCheckpoint {
        identity: identity(),
        operation_id: "a".into(),
        expected_revision: 1,
    };
    let status = control.prepare(request.clone()).await.unwrap();
    assert_eq!(status.phase, RuntimePhase::Running);
    assert_eq!(
        status.checkpoint.as_ref().unwrap().phase,
        CheckpointPhase::Failed
    );
    assert_eq!(control.prepare(request).await.unwrap(), status);
    assert!(control
        .abort_unstarted("a", &identity(), status.revision)
        .is_err());
}

struct DelayedOpen {
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    reader: Mutex<Option<oneshot::Receiver<HandoffOutcome>>>,
}
impl CheckpointHooks for DelayedOpen {
    fn open(&self) -> io::Result<Handoff> {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(io::Error::other)?;
        let reader = self.reader.lock().unwrap().take().unwrap();
        Ok(Box::pin(async { reader.await.map_err(io::Error::other) }))
    }
    fn restore(&self, _: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        unreachable!()
    }
}
#[tokio::test]
async fn disconnected_prepare_keeps_operation_and_stale_revision_is_rejected() {
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let (handoff, reader) = oneshot::channel();
    let control = Controller::new(
        identity(),
        Arc::new(DelayedOpen {
            entered,
            release: Mutex::new(released),
            reader: Mutex::new(Some(reader)),
        }),
    )
    .unwrap();
    let request = PrepareCheckpoint {
        identity: identity(),
        operation_id: "a".into(),
        expected_revision: 1,
    };
    let task = {
        let control = control.clone();
        let request = request.clone();
        tokio::spawn(async move { control.prepare(request).await })
    };
    tokio::task::spawn_blocking(move || {
        entering
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(control.status().phase, RuntimePhase::Preparing);
    task.abort();
    release.send(()).unwrap();
    assert_eq!(
        control.prepare(request).await.unwrap().phase,
        RuntimePhase::Prepared
    );
    assert!(control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "b".into(),
            expected_revision: 1
        })
        .await
        .is_err());
    handoff.send(HandoffOutcome::Resume).unwrap();
}

struct CloneHooks;
impl CheckpointHooks for CloneHooks {
    fn open(&self) -> io::Result<Handoff> {
        Ok(Box::pin(async { Ok(HandoffOutcome::Restore) }))
    }
    fn restore(&self, _: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        unreachable!()
    }
    fn restore_context(
        &self,
        old: &RuntimeIdentity,
    ) -> io::Result<adx_core::runtime::RuntimeRestore> {
        Ok(adx_core::runtime::RuntimeRestore {
            target: RuntimeIdentity {
                environment_id: "clone".into(),
                runtime_id: "clone-1".into(),
                ownership_generation: 1,
            },
            origin: Some(old.clone()),
        })
    }
}
#[tokio::test]
async fn clone_handoff_rebinds_identity_and_rejects_source_requests() {
    let control = Controller::new(identity(), Arc::new(CloneHooks)).unwrap();
    control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "clone-source".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    let mut changes = control.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while control.status().identity.environment_id != "clone" {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(control.status().phase, RuntimePhase::Running);
    assert!(control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "old-source-request".into(),
            expected_revision: control.status().revision
        })
        .await
        .is_err());
}

#[tokio::test]
async fn workload_checkpoint_waits_for_handoff_and_durable_completion() {
    use adx_core::runtime::FinishWorkloadCheckpoint;
    let (tx, rx) = oneshot::channel();
    let control = Controller::new(
        identity(),
        Arc::new(Hooks {
            read: Mutex::new(Some(rx)),
            opens: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    let pending = {
        let control = control.clone();
        tokio::spawn(async move { control.request_checkpoint("local-a".into()).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(
        control.status().requested_checkpoint.as_deref(),
        Some("local-a")
    );
    assert!(control.request_checkpoint("local-b".into()).await.is_err());
    let finish = FinishWorkloadCheckpoint {
        identity: identity(),
        operation_id: "local-a".into(),
        error: None,
    };
    assert!(control.finish_checkpoint(finish.clone()).is_err());
    control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "local-a".into(),
            expected_revision: control.status().revision,
        })
        .await
        .unwrap();
    tx.send(HandoffOutcome::Resume).unwrap();
    let mut changes = control.subscribe();
    while control.status().phase != RuntimePhase::Running {
        changes.changed().await.unwrap();
    }
    assert!(
        !pending.is_finished(),
        "handoff alone is not persistence acknowledgement"
    );
    control.finish_checkpoint(finish.clone()).unwrap();
    pending.await.unwrap().unwrap();
    control.finish_checkpoint(finish).unwrap();
    assert!(control.status().requested_checkpoint.is_none());
}

#[tokio::test]
async fn restored_runtime_discards_workload_request() {
    let (tx, rx) = oneshot::channel();
    let control = Controller::new(
        identity(),
        Arc::new(Hooks {
            read: Mutex::new(Some(rx)),
            opens: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    let pending = {
        let control = control.clone();
        tokio::spawn(async move { control.request_checkpoint("source".into()).await })
    };
    tokio::task::yield_now().await;
    control
        .prepare(PrepareCheckpoint {
            identity: identity(),
            operation_id: "source".into(),
            expected_revision: control.status().revision,
        })
        .await
        .unwrap();
    tx.send(HandoffOutcome::Restore).unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(control.status().requested_checkpoint.is_none());
}
