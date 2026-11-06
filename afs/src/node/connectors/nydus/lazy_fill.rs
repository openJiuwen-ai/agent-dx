//! Interpret the business binding, fetch bytes, and leave commit entirely to DFS.
use super::registry::{RegistryProvider, RegistrySource};
use crate::node::vfs::lazy_fill::{
    LazyFillBinding, LazyFillFuture, LazyFillHandler, LazyFillRequest,
};
use afs_error::{Error, Result};
use std::sync::Arc;

pub const KIND: &str = "nydus.registry.v1";
pub const PLAN_KIND: &str = "nydus.registry-plan.v1";
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
            if request.binding.kind != KIND && request.binding.kind != PLAN_KIND {
                return Err(invalid());
            }
            let source: RegistrySource =
                serde_json::from_slice(&request.binding.payload).map_err(|_| invalid())?;
            if request.binding.kind == PLAN_KIND {
                let plan = self
                    .provider
                    .describe(&source.source_key, &source.blob_digest, request.deadline)
                    .await?;
                if plan.registry != source.registry || plan.repository != source.repository {
                    return Err(invalid());
                }
                let bytes = serde_json::to_vec(&plan).map_err(|_| invalid())?;
                let start = usize::try_from(request.offset).map_err(|_| invalid())?;
                let end = start.checked_add(request.length).ok_or_else(invalid)?;
                if bytes.len() as u64 != source.size || end > bytes.len() {
                    return Err(invalid());
                }
                return Ok(prost::bytes::Bytes::copy_from_slice(&bytes[start..end]));
            }
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

/// Versioned serialization of immutable manifest metadata; no blob body is fetched.
pub fn plan_binding(plan: &super::registry::ImagePlan, length: u64) -> Result<LazyFillBinding> {
    let mut value = binding(&RegistrySource {
        source_key: plan.source_key.clone(),
        registry: plan.registry.clone(),
        repository: plan.repository.clone(),
        blob_digest: plan.manifest_digest.clone(),
        size: length,
    })?;
    value.kind = PLAN_KIND.into();
    Ok(value)
}
