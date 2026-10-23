use adx_core::{Assignment, CheckpointArtifact, CheckpointCleanup, EnvironmentRecord, Result};
use adx_protocol::{auth::Peers, control as pb};
use adxlet::{
    checkpoint::{CheckpointAccess, CheckpointCooperation, LocalCheckpointStore, RestoreAccess},
    Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink,
};
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tonic::Request;

#[derive(Default)]
struct Noop;

#[async_trait]
impl RuntimeDriver for Noop {
    async fn start(
        &self,
        _: &adx_core::EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        Err(adx_core::Error::Unavailable(
            "test runtime does not start".into(),
        ))
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Readiness for Noop {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Routes for Noop {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }

    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl StateSink for Noop {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        Ok(Durability::Published)
    }
}

#[async_trait]
impl CheckpointCooperation for Noop {
    async fn prepare(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }

    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

struct RecordingStore {
    local: LocalCheckpointStore,
    fail_remove_call: AtomicUsize,
    remove_calls: Mutex<Vec<String>>,
}

#[async_trait]
impl CheckpointAccess for RecordingStore {
    async fn begin_capture(&self) -> Result<PathBuf> {
        self.local.begin_capture().await
    }

    async fn discard_staged(&self, path: &Path) -> Result<()> {
        self.local.discard_staged(path).await
    }

    async fn retain_staged(&self, path: &Path) -> Result<CheckpointArtifact> {
        self.local.retain_staged(path).await
    }

    async fn seal(&self, path: &Path) -> Result<CheckpointArtifact> {
        self.local.seal(path).await
    }

    async fn prepare_restore(&self, artifact: &CheckpointArtifact) -> Result<RestoreAccess> {
        self.local.prepare_restore(artifact).await
    }

    async fn release(&self, artifact: &CheckpointArtifact) -> Result<()> {
        self.remove_calls
            .lock()
            .expect("test state lock is healthy")
            .push(artifact.location.clone());
        let expected = self.fail_remove_call.load(Ordering::SeqCst);
        if expected != 0
            && self
                .remove_calls
                .lock()
                .expect("test state lock is healthy")
                .len()
                == expected
        {
            self.fail_remove_call.store(0, Ordering::SeqCst);
            return Err(adx_core::Error::Unavailable(
                "injected remove failure".into(),
            ));
        }
        Ok(())
    }

    fn shared_scope(&self) -> Option<&str> {
        Some("shared")
    }
}

fn cleanup(node_id: &str, scope: &str) -> CheckpointCleanup {
    CheckpointCleanup {
        assignment: Assignment {
            environment_id: "environment".into(),
            node_id: node_id.into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        revision: 2,
        artifacts: ["one", "two"]
            .into_iter()
            .map(|location| CheckpointArtifact {
                storage: "shared".into(),
                scope: scope.into(),
                location: location.into(),
                size_bytes: 1,
            })
            .collect(),
    }
}

fn rpc_request(
    session: &str,
    cleanup: CheckpointCleanup,
) -> Request<pb::CollectCheckpointCleanupRequest> {
    Request::new(pb::CollectCheckpointCleanupRequest {
        cleanup: Some(cleanup.try_into().expect("test cleanup is valid")),
        node_session_id: session.into(),
        delegated: true,
    })
}

fn coordinator_request(
    session: &str,
    cleanup: CheckpointCleanup,
) -> Request<pb::CollectCheckpointCleanupRequest> {
    let mut request = rpc_request(session, cleanup);
    request.metadata_mut().insert(
        "adx-component",
        "coordinator".parse().expect("static metadata is valid"),
    );
    request
}

#[tokio::test]
async fn cleanup_rejects_foreign_local_and_wrong_shared_scope_before_removal() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(RecordingStore {
        local: LocalCheckpointStore::new(root.path().into()).unwrap(),
        fail_remove_call: AtomicUsize::new(0),
        remove_calls: Mutex::new(vec![]),
    });
    let manager = Adxlet::new(
        "node".into(),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
    )
    .with_checkpointing(store.clone(), Arc::new(Noop))
    .unwrap();

    assert!(manager
        .collect_checkpoint_cleanup(&cleanup("other", "shared"), false)
        .await
        .is_err());
    assert!(manager
        .collect_checkpoint_cleanup(&cleanup("other", "wrong"), true)
        .await
        .is_err());
    let mut local = cleanup("other", "shared");
    local.artifacts[0].storage = "local".into();
    assert!(manager
        .collect_checkpoint_cleanup(&local, true)
        .await
        .is_err());
    assert!(store
        .remove_calls
        .lock()
        .expect("test state lock is healthy")
        .is_empty());
}

#[tokio::test]
async fn cleanup_rpc_requires_coordinator_identity_current_session_and_ready_node() {
    use adx_protocol::control::node_service_server::NodeService;

    let manager = Arc::new(Adxlet::new(
        "node".into(),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
    ));
    let service = adxlet::rpc::NodeRpc::new(manager.clone(), Peers::network(), "session".into());

    assert_eq!(
        service
            .collect_checkpoint_cleanup(rpc_request("session", cleanup("node", "shared")))
            .await
            .expect_err("unauthenticated request must fail")
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        service
            .collect_checkpoint_cleanup(coordinator_request("old", cleanup("node", "shared")))
            .await
            .expect_err("old session must fail")
            .code(),
        tonic::Code::FailedPrecondition
    );
    manager.pause_lifecycle().await;
    assert_eq!(
        service
            .collect_checkpoint_cleanup(coordinator_request("session", cleanup("node", "shared")))
            .await
            .expect_err("reconciling node must fail")
            .code(),
        tonic::Code::Unavailable
    );
}

#[tokio::test]
async fn cleanup_retries_the_full_idempotent_sequence_after_partial_remove_failure() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(RecordingStore {
        local: LocalCheckpointStore::new(root.path().into()).unwrap(),
        fail_remove_call: AtomicUsize::new(2),
        remove_calls: Mutex::new(vec![]),
    });
    let manager = Adxlet::new(
        "node".into(),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
        Arc::new(Noop),
    )
    .with_checkpointing(store.clone(), Arc::new(Noop))
    .unwrap();
    let mut cleanup = cleanup("node", "");
    for artifact in &mut cleanup.artifacts {
        artifact.storage = "local".into();
    }

    assert!(manager
        .collect_checkpoint_cleanup(&cleanup, false)
        .await
        .is_err());
    manager
        .collect_checkpoint_cleanup(&cleanup, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .remove_calls
            .lock()
            .expect("test state lock is healthy")
            .as_slice(),
        ["one", "two", "one", "two"]
    );
}
