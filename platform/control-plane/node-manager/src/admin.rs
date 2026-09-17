//! Explicit stop deletes locally owned Instances before their dependencies stop.
use crate::{Durability, NodeManager};
use adx_core::{Error, InstanceState, Result};
use adx_protocol::node_proxy as pb;
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::Path,
    sync::{atomic::Ordering, Arc},
};
impl NodeManager {
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
    pub async fn drain(&self) -> Result<usize> {
        let mut ready = self.lifecycle_ready.write().await;
        if !*ready && !self.is_draining() {
            return Err(Error::Unavailable(
                "authoritative node reconciliation required before cleanup".into(),
            ));
        }
        if !self.local_holds.lock().unwrap().is_empty() {
            return Err(Error::Unavailable(
                "unconfirmed local claims require reconciliation before drain".into(),
            ));
        }
        self.draining.store(true, Ordering::Release);
        self.set_maintenance(true);
        *ready = false;
        let handles: Vec<_> = self
            .instances
            .lock()
            .unwrap()
            .values()
            .map(|(_, _, h)| h.clone())
            .collect();
        let count = handles.len();
        for h in handles {
            let r = h.delete().await?;
            if r.record.state != InstanceState::Deleted
                || r.record.resources_held
                || r.durability != Durability::Published
            {
                return Err(Error::Unavailable(
                    "cleanup is not committed to cluster storage".into(),
                ));
            }
        }
        Ok(count)
    }
}
pub struct NodeAdmin(pub Arc<NodeManager>);
#[tonic::async_trait]
impl pb::node_admin_service_server::NodeAdminService for NodeAdmin {
    async fn drain(
        &self,
        _: tonic::Request<pb::DrainRequest>,
    ) -> std::result::Result<tonic::Response<pb::DrainResponse>, tonic::Status> {
        let count = self.0.drain().await.map_err(adx_protocol::status)?;
        Ok(tonic::Response::new(pb::DrainResponse {
            deleted_instances: count as u64,
        }))
    }
}
pub async fn serve(
    manager: Arc<NodeManager>,
    path: &Path,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() || tokio::net::UnixStream::connect(path).await.is_ok() {
            return Err("node admin path occupied".into());
        }
        std::fs::remove_file(path)?;
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    tonic::transport::Server::builder()
        .add_service(pb::node_admin_service_server::NodeAdminServiceServer::new(
            NodeAdmin(manager),
        ))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
        .await?;
    Ok(())
}
