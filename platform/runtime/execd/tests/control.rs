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
        idle_revision: None,
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
        .await
        .unwrap();
    let second = control
        .prepare(PrepareCheckpoint {
            idle_revision: None,
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
            idle_revision: None,
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
            idle_revision: None,
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
        idle_revision: None,
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
        .await
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
        idle_revision: None,
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
            idle_revision: None,
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
            idle_revision: None,
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
            idle_revision: None,
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
        tokio::spawn(async move {
            control
                .request_checkpoint("local-a".into(), Some(std::time::Duration::from_secs(30)))
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(
        control.status().requested_checkpoint.as_deref(),
        Some("local-a")
    );
    assert!(control
        .status()
        .requested_checkpoint_deadline_unix_millis
        .is_some());
    assert!(control
        .request_checkpoint("local-b".into(), Some(std::time::Duration::from_secs(30)))
        .await
        .is_err());
    let finish = FinishWorkloadCheckpoint {
        identity: identity(),
        operation_id: "local-a".into(),
        error: None,
    };
    assert!(control.finish_checkpoint(finish.clone()).is_err());
    control
        .prepare(PrepareCheckpoint {
            idle_revision: None,
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
        tokio::spawn(async move {
            control
                .request_checkpoint("source".into(), Some(std::time::Duration::from_secs(30)))
                .await
        })
    };
    tokio::task::yield_now().await;
    control
        .prepare(PrepareCheckpoint {
            idle_revision: None,
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

#[tokio::test]
async fn workload_checkpoint_timeout_keeps_the_accepted_request_for_reconciliation() {
    let (_tx, rx) = oneshot::channel();
    let control = Controller::new(
        identity(),
        Arc::new(Hooks {
            read: Mutex::new(Some(rx)),
            opens: AtomicUsize::new(0),
        }),
    )
    .unwrap();
    let error = control
        .request_checkpoint("slow".into(), Some(std::time::Duration::from_millis(20)))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    let status = control.status();
    assert_eq!(status.requested_checkpoint.as_deref(), Some("slow"));
    assert!(status.requested_checkpoint_deadline_unix_millis.is_some());
}

struct AsyncRestoreHooks {
    handoff: Mutex<Option<oneshot::Receiver<HandoffOutcome>>>,
    ready: Mutex<Option<oneshot::Receiver<io::Result<()>>>>,
}
impl CheckpointHooks for AsyncRestoreHooks {
    fn open(&self) -> io::Result<Handoff> {
        let rx = self.handoff.lock().unwrap().take().unwrap();
        Ok(Box::pin(async move { rx.await.map_err(io::Error::other) }))
    }
    fn restore_context_async<'a>(
        &'a self,
        old: &'a RuntimeIdentity,
    ) -> adx_execd::runtime::control::Restore<'a> {
        let ready = self.ready.lock().unwrap().take().unwrap();
        Box::pin(async move {
            ready.await.map_err(io::Error::other)??;
            Ok(adx_core::runtime::RuntimeRestore {
                target: RuntimeIdentity {
                    environment_id: old.environment_id.clone(),
                    runtime_id: "restored".into(),
                    ownership_generation: old.ownership_generation + 1,
                },
                origin: None,
            })
        })
    }
}

// The guest uses a current-thread runtime. Pending initialization must yield
// while its identity remains unpublished; errors must never publish Running.
#[tokio::test]
async fn asynchronous_restore_waits_for_guest_services_and_fails_closed() {
    for succeeds in [true, false] {
        let (handoff_tx, handoff_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let control = Controller::new(
            identity(),
            Arc::new(AsyncRestoreHooks {
                handoff: Mutex::new(Some(handoff_rx)),
                ready: Mutex::new(Some(ready_rx)),
            }),
        )
        .unwrap();
        control
            .prepare(PrepareCheckpoint {
                idle_revision: None,
                identity: identity(),
                operation_id: "async-restore".into(),
                expected_revision: 1,
            })
            .await
            .unwrap();
        handoff_tx.send(HandoffOutcome::Restore).unwrap();
        let mut changes = control.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while control.status().phase != RuntimePhase::Restoring {
                changes.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(control.status().identity, identity());
        assert_eq!(control.status().phase, RuntimePhase::Restoring);
        ready_tx
            .send(if succeeds {
                Ok(())
            } else {
                Err(io::Error::other("envd initialization failed"))
            })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while control.status().phase == RuntimePhase::Restoring {
                changes.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        if succeeds {
            assert_eq!(control.status().phase, RuntimePhase::Running);
            assert_eq!(control.status().identity.runtime_id, "restored");
        } else {
            assert_eq!(control.status().phase, RuntimePhase::Failed);
            assert_eq!(control.status().identity, identity());
            assert_eq!(
                control.status().checkpoint.unwrap().error.as_deref(),
                Some("envd initialization failed")
            );
        }
    }
}

struct GuestHooks {
    backend: Hooks,
    prepared: AtomicUsize,
    rolled_back: AtomicUsize,
    resumed: AtomicUsize,
    prepare_gate: tokio::sync::Semaphore,
    release_gate: tokio::sync::Semaphore,
    release_fails: bool,
}
impl CheckpointHooks for GuestHooks {
    fn open(&self) -> io::Result<Handoff> {
        self.backend.open()
    }
    fn restore(&self, previous: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
        self.backend.restore(previous)
    }
    fn prepare_checkpoint(&self) -> adx_execd::runtime::control::Cooperation<'_> {
        Box::pin(async {
            self.prepared.fetch_add(1, Ordering::SeqCst);
            self.prepare_gate.acquire().await.unwrap().forget();
            Ok(())
        })
    }
    fn rollback_checkpoint_prepare(&self) -> adx_execd::runtime::control::Cooperation<'_> {
        Box::pin(async {
            self.rolled_back.fetch_add(1, Ordering::SeqCst);
            self.release_gate.acquire().await.unwrap().forget();
            if self.release_fails {
                Err(io::Error::other("unfreeze failed"))
            } else {
                Ok(())
            }
        })
    }
    fn resume_after_checkpoint(&self) -> adx_execd::runtime::control::Cooperation<'_> {
        Box::pin(async {
            self.resumed.fetch_add(1, Ordering::SeqCst);
            self.release_gate.acquire().await.unwrap().forget();
            if self.release_fails {
                Err(io::Error::other("unfreeze failed"))
            } else {
                Ok(())
            }
        })
    }
}
fn guest_hooks(rx: oneshot::Receiver<HandoffOutcome>, release_fails: bool) -> Arc<GuestHooks> {
    Arc::new(GuestHooks {
        backend: Hooks {
            read: Mutex::new(Some(rx)),
            opens: AtomicUsize::new(0),
        },
        prepared: AtomicUsize::new(0),
        rolled_back: AtomicUsize::new(0),
        resumed: AtomicUsize::new(0),
        prepare_gate: tokio::sync::Semaphore::new(0),
        release_gate: tokio::sync::Semaphore::new(0),
        release_fails,
    })
}
async fn wait_counter(counter: &AtomicUsize, value: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while counter.load(Ordering::SeqCst) != value {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn guest_prepare_and_cancelled_abort_complete_before_acknowledgement() {
    let (_tx, rx) = oneshot::channel();
    let hooks = guest_hooks(rx, false);
    let controller = Controller::new(identity(), hooks.clone()).unwrap();
    let request = PrepareCheckpoint {
        idle_revision: None,
        identity: identity(),
        operation_id: "a".into(),
        expected_revision: 1,
    };
    let prepare = {
        let controller = controller.clone();
        let request = request.clone();
        tokio::spawn(async move { controller.prepare(request).await })
    };
    wait_counter(&hooks.prepared, 1).await;
    assert_eq!(controller.status().phase, RuntimePhase::Preparing);
    prepare.abort();
    hooks.prepare_gate.add_permits(1);
    let prepared = controller.prepare(request).await.unwrap();
    assert_eq!(prepared.phase, RuntimePhase::Prepared);
    let abort = {
        let controller = controller.clone();
        tokio::spawn(async move {
            controller
                .abort_unstarted("a", &identity(), prepared.revision)
                .await
        })
    };
    wait_counter(&hooks.rolled_back, 1).await;
    abort.abort();
    assert_eq!(controller.status().phase, RuntimePhase::Prepared);
    hooks.release_gate.add_permits(1);
    let mut changes = controller.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while controller.status().phase != RuntimePhase::Running {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        controller.status().checkpoint.unwrap().phase,
        CheckpointPhase::Aborted
    );
    controller
        .abort_unstarted("a", &identity(), prepared.revision)
        .await
        .unwrap();
    assert_eq!(hooks.rolled_back.load(Ordering::SeqCst), 1);
    hooks.prepare_gate.add_permits(1);
    controller
        .prepare(PrepareCheckpoint {
            idle_revision: None,
            identity: identity(),
            operation_id: "b".into(),
            expected_revision: controller.status().revision,
        })
        .await
        .unwrap();
    assert_eq!(hooks.prepared.load(Ordering::SeqCst), 2);
    assert_eq!(hooks.backend.opens.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn source_resume_waits_for_unfreeze_and_failure_never_publishes_running() {
    for outcome in [HandoffOutcome::Resume, HandoffOutcome::Error] {
        for fails in [false, true] {
            let (tx, rx) = oneshot::channel();
            let hooks = guest_hooks(rx, fails);
            let controller = Controller::new(identity(), hooks.clone()).unwrap();
            hooks.prepare_gate.add_permits(1);
            controller
                .prepare(PrepareCheckpoint {
                    idle_revision: None,
                    identity: identity(),
                    operation_id: "a".into(),
                    expected_revision: 1,
                })
                .await
                .unwrap();
            tx.send(outcome).unwrap();
            wait_counter(&hooks.resumed, 1).await;
            assert_eq!(controller.status().phase, RuntimePhase::Prepared);
            hooks.release_gate.add_permits(1);
            let mut changes = controller.subscribe();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while controller.status().phase == RuntimePhase::Prepared {
                    changes.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            let status = controller.status();
            assert_eq!(
                status.phase,
                if fails {
                    RuntimePhase::Failed
                } else {
                    RuntimePhase::Running
                }
            );
            assert_eq!(
                status.checkpoint.unwrap().phase,
                if fails || matches!(outcome, HandoffOutcome::Error) {
                    CheckpointPhase::Failed
                } else {
                    CheckpointPhase::Resumed
                }
            );
        }
    }
}

#[tokio::test]
async fn unknown_backend_result_does_not_unfreeze() {
    let (tx, rx) = oneshot::channel();
    let hooks = guest_hooks(rx, false);
    let controller = Controller::new(identity(), hooks.clone()).unwrap();
    hooks.prepare_gate.add_permits(1);
    controller
        .prepare(PrepareCheckpoint {
            idle_revision: None,
            identity: identity(),
            operation_id: "a".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    drop(tx);
    let mut changes = controller.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while controller.status().phase != RuntimePhase::Failed {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(hooks.rolled_back.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.resumed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_abort_never_acknowledges_running_or_attempts_again() {
    let (_tx, rx) = oneshot::channel();
    let hooks = guest_hooks(rx, true);
    let controller = Controller::new(identity(), hooks.clone()).unwrap();
    hooks.prepare_gate.add_permits(1);
    let status = controller
        .prepare(PrepareCheckpoint {
            idle_revision: None,
            identity: identity(),
            operation_id: "a".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    hooks.release_gate.add_permits(1);
    let failed = controller
        .abort_unstarted("a", &identity(), status.revision)
        .await
        .unwrap();
    assert_eq!(failed.phase, RuntimePhase::Failed);
    assert_eq!(failed.checkpoint.unwrap().phase, CheckpointPhase::Failed);
    assert!(controller
        .abort_unstarted("a", &identity(), status.revision)
        .await
        .is_err());
    assert_eq!(hooks.rolled_back.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retained_reader_cannot_publish_running_before_new_guest_prepare_finishes() {
    let (tx, rx) = oneshot::channel();
    let hooks = guest_hooks(rx, false);
    let controller = Controller::new(identity(), hooks.clone()).unwrap();
    hooks.prepare_gate.add_permits(1);
    let first = controller
        .prepare(PrepareCheckpoint {
            idle_revision: None,
            identity: identity(),
            operation_id: "a".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    hooks.release_gate.add_permits(1);
    let aborted = controller
        .abort_unstarted("a", &identity(), first.revision)
        .await
        .unwrap();
    let prepare = {
        let controller = controller.clone();
        tokio::spawn(async move {
            controller
                .prepare(PrepareCheckpoint {
                    idle_revision: None,
                    identity: identity(),
                    operation_id: "b".into(),
                    expected_revision: aborted.revision,
                })
                .await
        })
    };
    wait_counter(&hooks.prepared, 2).await;
    tx.send(HandoffOutcome::Resume).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(controller.status().phase, RuntimePhase::Preparing);
    assert_eq!(hooks.resumed.load(Ordering::SeqCst), 0);
    hooks.prepare_gate.add_permits(1);
    wait_counter(&hooks.resumed, 1).await;
    assert_eq!(controller.status().phase, RuntimePhase::Prepared);
    prepare.await.unwrap().unwrap();
    hooks.release_gate.add_permits(1);
    let mut changes = controller.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while controller.status().phase != RuntimePhase::Running {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(hooks.backend.opens.load(Ordering::SeqCst), 1);
    assert_eq!(controller.status().checkpoint.unwrap().operation_id, "b");
}
#[tokio::test]
async fn restore_uses_reinitialization_hook_without_source_unfreeze() {
    let (tx, rx) = oneshot::channel();
    let hooks = guest_hooks(rx, false);
    let controller = Controller::new(identity(), hooks.clone()).unwrap();
    hooks.prepare_gate.add_permits(1);
    controller
        .prepare(PrepareCheckpoint {
            idle_revision: None,
            identity: identity(),
            operation_id: "a".into(),
            expected_revision: 1,
        })
        .await
        .unwrap();
    tx.send(HandoffOutcome::Restore).unwrap();
    let mut changes = controller.subscribe();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while controller.status().identity.ownership_generation != 2 {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        controller.status().checkpoint.unwrap().phase,
        CheckpointPhase::Restored
    );
    assert_eq!(hooks.rolled_back.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.resumed.load(Ordering::SeqCst), 0);
}
