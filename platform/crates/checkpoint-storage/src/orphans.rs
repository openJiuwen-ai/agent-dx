//! Upload provenance for diagnostics; shared deletion belongs to Coordinator.
use super::io_error;
use super::object::{invalid, ObjectCheckpointStore};
use adx_core::Result;
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct UploadOwner {
    pub version: u32,
    pub node_id: String,
    pub session_id: String,
}
impl ObjectCheckpointStore {
    pub fn with_owner(mut self, node_id: String, session_id: String) -> Result<Self> {
        for id in [&node_id, &session_id] {
            if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                return Err(invalid("invalid upload owner"));
            }
        }
        self.owner = Some(UploadOwner {
            version: 1,
            node_id,
            session_id,
        });
        Ok(self)
    }
    pub(super) async fn mark_upload(&self, id: &str) -> Result<()> {
        if let Some(owner) = &self.owner {
            let bytes = serde_json::to_vec(owner).map_err(invalid)?;
            self.remote
                .put(&self.key(id, "owner.json"), bytes.into())
                .await
                .map_err(io_error)?;
        }
        Ok(())
    }
}
