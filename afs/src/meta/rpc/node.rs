//! The MetaService handlers shared by DFS and OwnerFs: ping and node registration.

use super::*;

#[tonic::async_trait]
impl MetaService for MetaRpc {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        let span = afs_tracing::tracing::info_span!("meta.ping");
        let _entered = span.enter();
        // R41 (model M0): every Meta RPC requires a trusted-node certificate.
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let message = self
            .0
            .ping(&request.into_inner().node_id)
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(PingReply {
            message,
            instance: self.0.id.clone(),
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeReply>, Status> {
        self.0.observability.record("meta", "register_node", true);
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        // K4: protocol admission before anything is read or written. Every
        // Node (DFS and OwnerFs) registers here, so this gates the cluster.
        crate::format::check(
            crate::format::FormatKind::NodeMetaProtocol,
            format_args!(
                "from node {:?}",
                request
                    .node
                    .as_ref()
                    .map_or("", |node| node.node_id.as_str())
            ),
            u64::from(request.protocol_version),
        )
        .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let node = request
            .node
            .ok_or_else(|| invalid("node descriptor is required"))?;
        require_text(&request.request_id, "request_id")?;
        require_text(&node.node_id, "node.node_id")?;
        // Node ids become path segments on every consumer (mount points under
        // the import root, symlink targets under the public root). Reject ids
        // that would escape those roots before the session is stored, so a
        // misconfigured or malicious node cannot pollute the cluster snapshot.
        if !crate::config::is_safe_node_id(&node.node_id) {
            return Err(invalid(
                "node.node_id must contain 1..128 letters, digits, _ or -",
            ));
        }
        if let Some(authenticated) = authenticated
            && authenticated != node.node_id
        {
            return Err(permission_denied(format!(
                "authenticated node {authenticated} does not match node.node_id {}",
                node.node_id
            )));
        }
        require_text(&node.session_id, "node.session_id")?;
        let endpoint = node
            .endpoint
            .ok_or_else(|| invalid("node.endpoint is required"))?;
        let ttl = Duration::from_secs(request.lease_seconds.max(1));
        let dfs_node = node
            .capabilities
            .iter()
            .any(|capability| capability == "dfs");
        let session = self
            .0
            .register_node(
                request_key(
                    format!("{}/{}", node.node_id, node.session_id),
                    request.request_id,
                ),
                NodeSessionLease {
                    node_id: node.node_id,
                    session_id: node.session_id,
                    grpc_addr: endpoint.grpc_addr,
                    data_addr: endpoint.data_addr,
                    rest_addr: endpoint.rest_addr,
                    nfs_addr: endpoint.nfs_addr,
                    ownerfs_enabled: node
                        .capabilities
                        .iter()
                        .any(|capability| capability == "ownerfs"),
                    storage_devices: node
                        .storage_devices
                        .into_iter()
                        .map(|device| crate::dfs::StorageDeviceDescriptor {
                            device_id: device.device_id,
                            device_epoch: device.device_epoch,
                            catalog_revision: device.catalog_revision,
                            failure_domain: device.failure_domain,
                        })
                        .collect(),
                    lease_ttl: ttl,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        // C-1 restart gate: a Node without DFS has no readers to report.
        if let Some(dfs) = &self.0.dfs {
            dfs.note_node_registration(&session.node_id, &session.session_id, dfs_node);
        }
        let readiness = self
            .0
            .store
            .as_deref()
            .ok_or_else(|| {
                afs_transport::grpc::error_status::error_to_status(
                    super::super::store::unavailable_meta_store(),
                )
            })?
            .backend_readiness()
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(register_node_reply(
            session,
            readiness,
            crate::clock::now_unix_ms(),
        )))
    }

    async fn lookup_node(
        &self,
        request: Request<LookupNodeRequest>,
    ) -> Result<Response<LookupNodeReply>, Status> {
        // R41 (model M0): node addresses and lease epochs are served only to
        // trusted nodes, as every other Meta read is.
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.node_id, "node_id")?;
        let session = self
            .0
            .lookup_node(&request.node_id)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let (node, lease_epoch, expires_at_unix_ms) = match session {
            Some(session) => {
                let lease_epoch = session.lease_epoch;
                let expires_at_unix_ms = session.expires_at_unix_ms;
                (Some(wire_node(session)), lease_epoch, expires_at_unix_ms)
            }
            _ => (None, 0, 0),
        };
        Ok(Response::new(LookupNodeReply {
            node,
            lease_epoch,
            expires_at_unix_ms,
            found: lease_epoch != 0,
        }))
    }
}
