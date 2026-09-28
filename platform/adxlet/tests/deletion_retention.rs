use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, Resources, Result};
use adx_protocol::{auth::Peers, control as pb};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::Request;

struct Dependencies(Mutex<Durability>);

impl Default for Dependencies {
    fn default() -> Self {
        Self(Mutex::new(Durability::Published))
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
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        Ok(*self.0.lock().expect("test state lock is healthy"))
    }
}

fn spec() -> EnvironmentSpec {
    EnvironmentSpec {
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
        sandbox: Default::default(),
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

    *dependencies.0.lock().expect("test state lock is healthy") = Durability::Journaled;
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

    *dependencies.0.lock().expect("test state lock is healthy") = Durability::Published;
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
