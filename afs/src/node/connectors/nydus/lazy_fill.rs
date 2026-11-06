//! Interpret the business binding, fetch bytes, and leave commit entirely to DFS.
use super::registry::{RegistryProvider, RegistrySource};
use crate::node::vfs::lazy_fill::{
    LazyFillBinding, LazyFillFuture, LazyFillHandler, LazyFillRequest,
};
use afs_error::{Error, Result};
use std::sync::Arc;

pub const KIND: &str = "nydus.registry.v1";
pub struct NydusLazyFillHandler {
    provider: Arc<RegistryProvider>,
}
impl NydusLazyFillHandler {
    pub fn new(provider: Arc<RegistryProvider>) -> Self {
        Self { provider }
    }
}
impl LazyFillHandler for NydusLazyFillHandler {
    fn fill(&self, request: LazyFillRequest) -> LazyFillFuture<'_> {
        Box::pin(async move {
            if request.binding.kind != KIND {
                return Err(invalid());
            }
            let source: RegistrySource =
                serde_json::from_slice(&request.binding.payload).map_err(|_| invalid())?;
            self.provider
                .fetch_range(&source, request.offset, request.length, request.deadline)
                .await
        })
    }
}
pub fn binding(source: &RegistrySource) -> Result<LazyFillBinding> {
    Ok(LazyFillBinding {
        kind: KIND.into(),
        payload: serde_json::to_vec(source).map_err(|_| invalid())?,
    })
}
fn invalid() -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, "invalid Nydus source binding")
}
