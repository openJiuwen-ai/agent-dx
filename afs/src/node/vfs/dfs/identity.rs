use super::*;

/// Who this mount is (namespace, node, node session) and its operation
/// counter. Shared read-only (`Arc`) by the orchestrator, the owner gateway
/// and the write coordinator; the counter is the only mutable part.
pub(super) struct NodeIdentity {
    pub(super) namespace_id: NamespaceId,
    pub(super) node_id: String,
    pub(super) session_id: String,
    /// Source of operation ids, write-session ids and native generations.
    pub(super) next_operation: AtomicU64,
}

impl NodeIdentity {
    pub(super) fn new(namespace_id: NamespaceId, node_id: String, session_id: String) -> Self {
        Self {
            namespace_id,
            node_id,
            session_id,
            next_operation: AtomicU64::new(1),
        }
    }

    pub(super) fn operation_id(&self, prefix: &str) -> OperationId {
        OperationId::new(crate::clock::timed_request_id(format!(
            "{}-{prefix}-{}",
            self.session_id,
            self.next_operation.fetch_add(1, Ordering::Relaxed)
        )))
    }

    /// Rejects an inode record that belongs to another DFS namespace.
    pub(super) fn validate_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        if inode.namespace_id != self.namespace_id {
            return Err(invalid("Meta returned an inode from another DFS namespace"));
        }
        Ok(inode)
    }
}
