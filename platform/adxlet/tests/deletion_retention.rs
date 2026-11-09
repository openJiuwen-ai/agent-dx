use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, Resources, Result};
use adx_protocol::{auth::Peers, control as pb};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tonic::Request;

struct Dependencies {
    durability: Mutex<Durability>,
    block_deleted_commit: AtomicBool,
    deleted_commit_started: tokio::sync::Notify,
    allow_deleted_commit: tokio::sync::Notify,
}

impl Default for Dependencies {
    fn default() -> Self {
        Self {
            durability: Mutex::new(Durability::Published),
            block_deleted_commit: AtomicBool::new(false),
            deleted_commit_started: tokio::sync::Notify::new(),
            allow_deleted_commit: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait]
impl RuntimeDriver for Dependencies {
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }

    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        Ok("10.0.0.2".parse().expect("static test address is valid"))
    }

    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Readiness for Dependencies {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Routes for Dependencies {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }

    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl StateSink for Dependencies {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        if record.state == adx_core::EnvironmentState::Deleted
            && self.block_deleted_commit.load(Ordering::SeqCst)
        {
            self.deleted_commit_started.notify_waiters();
            self.allow_deleted_commit.notified().await;
        }
        // The operation intent is acknowledged before cleanup; this fixture can
        // separately withhold publication of the final Deleted result.
        if record.state == adx_core::EnvironmentState::Running
            && record
                .observation
                .as_ref()
                .is_some_and(|o| o.intent.is_some())
        {
            return Ok(Durability::Published);
        }
        Ok(*self.durability.lock().expect("test state lock is healthy"))
    }
}

fn spec() -> EnvironmentSpec {
    EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: "retired".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runtime".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 1,
        },
        priority: 0,
        execution: Default::default(),
    }
}

fn assignment() -> Assignment {
    Assignment {
        environment_id: "retired".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    }
}

fn delete_request() -> Request<pb::DeleteEnvironmentRequest> {
    let mut request = Request::new(pb::DeleteEnvironmentRequest {
        expected_revision: None,
        failed_revision: None,

        node_session_id: String::new(),
        assignment: Some(assignment().try_into().expect("test assignment is valid")),
        caller: Some(pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        }),
    });
    request.metadata_mut().insert(
        "adx-component",
        "apiserver".parse().expect("static metadata is valid"),
    );
    request
}

fn coordinator_delete_request(
    failed_revision: Option<u64>,
    expected_revision: Option<u64>,
    node_session_id: &str,
) -> Request<pb::DeleteEnvironmentRequest> {
    let mut request = Request::new(pb::DeleteEnvironmentRequest {
        expected_revision,
        failed_revision,
        node_session_id: node_session_id.into(),
        assignment: Some(assignment().try_into().expect("test assignment is valid")),
        caller: None,
    });
    request.metadata_mut().insert(
        "adx-component",
        "coordinator".parse().expect("static metadata is valid"),
    );
    request
}

async fn running_service() -> (Arc<Adxlet>, adxlet::rpc::NodeRpc, u64) {
    let dependencies = Arc::new(Dependencies::default());
    let manager = Arc::new(
        Adxlet::new(
            "node".into(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies,
        )
        .with_operation_timeout(Duration::from_secs(1))
        .expect("test timeout is valid"),
    );
    manager
        .update_capacity(
            Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            Duration::from_secs(30),
        )
        .expect("test capacity is valid");
    let created = manager
        .environment(spec(), assignment())
        .expect("test Environment is admitted")
        .create()
        .await
        .expect("test Environment starts");
    let service = adxlet::rpc::NodeRpc::new(manager.clone(), Peers::network(), "session".into());
    (manager, service, created.record.revision)
}

#[tokio::test]
async fn coordinator_failed_gc_rejects_ambiguous_or_untrusted_fences() {
    use adx_protocol::control::node_service_server::NodeService;

    let (manager, service, revision) = running_service().await;
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(None, None, "session"))
            .await
            .expect_err("Coordinator must select exactly one fence")
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(
                Some(revision),
                Some(revision),
                "session"
            ))
            .await
            .expect_err("Coordinator cannot combine revision and failed fences")
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(Some(revision), None, "other"))
            .await
            .expect_err("Coordinator session must match")
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut api_request = delete_request();
    api_request.get_mut().failed_revision = Some(revision);
    assert_eq!(
        service
            .delete_environment(api_request)
            .await
            .expect_err("API Server cannot supply a Coordinator fence")
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(Some(revision), None, "session"))
            .await
            .expect_err("Failed GC cannot delete a live Running environment")
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(manager
        .metrics()
        .contains("adx_node_managed_environments 1\n"));
}

#[tokio::test]
async fn coordinator_revision_retry_requires_the_original_fence() {
    use adx_protocol::control::node_service_server::NodeService;

    let (_manager, service, revision) = running_service().await;
    let first = service
        .delete_environment(coordinator_delete_request(None, Some(revision), "session"))
        .await
        .expect("revision collection succeeds")
        .into_inner();
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(None, Some(revision), "session"))
            .await
            .expect("lost-response revision retry replays the delete")
            .into_inner(),
        first
    );
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(
                None,
                Some(revision + 1),
                "session"
            ))
            .await
            .expect_err("a different revision revision cannot replay the tombstone")
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(Some(revision), None, "session"))
            .await
            .expect_err("a failed fence cannot replay an revision tombstone")
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn cancelled_coordinator_delete_still_records_its_fence() {
    use adx_protocol::control::node_service_server::NodeService;

    let dependencies = Arc::new(Dependencies::default());
    let manager = Arc::new(
        Adxlet::new(
            "node".into(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
        )
        .with_operation_timeout(Duration::from_secs(1))
        .expect("test timeout is valid"),
    );
    manager
        .update_capacity(
            Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            Duration::from_secs(30),
        )
        .expect("test capacity is valid");
    let created = manager
        .environment(spec(), assignment())
        .expect("test Environment is admitted")
        .create()
        .await
        .expect("test Environment starts");
    let service = adxlet::rpc::NodeRpc::new(manager.clone(), Peers::network(), "session".into());
    dependencies
        .block_deleted_commit
        .store(true, Ordering::SeqCst);
    let deleting = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .delete_environment(coordinator_delete_request(
                    None,
                    Some(created.record.revision),
                    "session",
                ))
                .await
        }
    });
    dependencies.deleted_commit_started.notified().await;
    deleting.abort();
    dependencies.allow_deleted_commit.notify_waiters();
    tokio::task::yield_now().await;
    assert_eq!(
        service
            .delete_environment(coordinator_delete_request(
                None,
                Some(created.record.revision),
                "session",
            ))
            .await
            .expect("retry after a dropped response replays the fenced tombstone")
            .into_inner()
            .record
            .expect("delete result exists")
            .state,
        pb::EnvironmentState::Deleted as i32
    );
}

#[tokio::test(start_paused = true)]
async fn published_delete_releases_controller_and_bounds_retry_tombstone() {
    use adx_protocol::control::node_service_server::NodeService;

    let dependencies = Arc::new(Dependencies::default());
    let manager = Arc::new(
        Adxlet::new(
            "node".into(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies,
        )
        .with_operation_timeout(Duration::from_secs(1))
        .expect("test timeout is valid"),
    );
    manager
        .update_capacity(
            Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            Duration::from_secs(30),
        )
        .expect("test capacity is valid");
    manager
        .environment(spec(), assignment())
        .expect("test Environment is admitted")
        .create()
        .await
        .expect("test Environment starts");

    let service = adxlet::rpc::NodeRpc::new(manager.clone(), Peers::network(), "session".into());
    let first = service
        .delete_environment(delete_request())
        .await
        .expect("first delete succeeds")
        .into_inner();
    assert_eq!(
        first.record.as_ref().expect("delete result exists").state,
        pb::EnvironmentState::Deleted as i32
    );
    let metrics = manager.metrics();
    assert!(metrics.contains("adx_node_managed_environments 0\n"));
    assert!(metrics.contains("adx_node_retired_environment_tombstones 1\n"));

    assert_eq!(
        service
            .delete_environment(delete_request())
            .await
            .expect("lost-response retry replays the delete")
            .into_inner(),
        first
    );

    tokio::time::advance(Duration::from_secs(2)).await;
    manager
        .monitor_environments()
        .await
        .expect("retirement pruning succeeds");
    assert!(manager
        .metrics()
        .contains("adx_node_retired_environment_tombstones 0\n"));
    assert_eq!(
        service
            .delete_environment(delete_request())
            .await
            .expect_err("expired retry state is gone")
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn journaled_delete_keeps_controller_until_cluster_publication() {
    use adx_protocol::control::node_service_server::NodeService;

    let dependencies = Arc::new(Dependencies::default());
    let manager = Arc::new(
        Adxlet::new(
            "node".into(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
            dependencies.clone(),
        )
        .with_operation_timeout(Duration::from_secs(1))
        .expect("test timeout is valid"),
    );
    manager
        .update_capacity(
            Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            Duration::from_secs(30),
        )
        .expect("test capacity is valid");
    manager
        .environment(spec(), assignment())
        .expect("test Environment is admitted")
        .create()
        .await
        .expect("test Environment starts");

    *dependencies
        .durability
        .lock()
        .expect("test state lock is healthy") = Durability::Journaled;
    let service = adxlet::rpc::NodeRpc::new(manager.clone(), Peers::network(), "session".into());
    let journaled = service
        .delete_environment(delete_request())
        .await
        .expect("journal accepts the delete")
        .into_inner();
    assert_eq!(journaled.durability, pb::Durability::Journaled as i32);
    let metrics = manager.metrics();
    assert!(metrics.contains("adx_node_managed_environments 1\n"));
    assert!(metrics.contains("adx_node_retired_environment_tombstones 0\n"));

    *dependencies
        .durability
        .lock()
        .expect("test state lock is healthy") = Durability::Published;
    let published = service
        .delete_environment(delete_request())
        .await
        .expect("retry publishes the retained delete")
        .into_inner();
    assert_eq!(published.durability, pb::Durability::Published as i32);
    let metrics = manager.metrics();
    assert!(metrics.contains("adx_node_managed_environments 0\n"));
    assert!(metrics.contains("adx_node_retired_environment_tombstones 1\n"));
}

#[tokio::test]
async fn preparation_cleanup_uses_exact_revision_and_owner_session() {
    use adx_protocol::control::node_service_server::NodeService;
    let (_manager, service, revision) = running_service().await;
    let request = |expected, session| {
        let mut request = coordinator_delete_request(None, None, session);
        request.get_mut().expected_revision = Some(expected);
        request
    };
    assert_eq!(
        service
            .delete_environment(request(revision, "old-session"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(service
        .delete_environment(request(revision + 1, "session"))
        .await
        .is_err());
    let deleted = service
        .delete_environment(request(revision, "session"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        deleted.record.as_ref().unwrap().state,
        pb::EnvironmentState::Deleted as i32
    );
    assert_eq!(
        service
            .delete_environment(request(revision, "session"))
            .await
            .unwrap()
            .into_inner(),
        deleted
    );
    assert_eq!(
        service
            .delete_environment(request(revision + 1, "session"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}
