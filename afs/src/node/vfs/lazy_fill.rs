//! Backend contract for immutable files whose bodies are fetched on first read.
//! Binding payloads contain source identity, never credentials or availability.
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Instant};

use afs_error::{Error, Result};
use prost::bytes::Bytes;

#[cfg(feature = "dfs")]
pub(crate) use crate::dfs::LAZY_FILE_DESCRIPTOR_XATTR as DESCRIPTOR_XATTR;
pub use crate::dfs::{LazyFileSpec, LazyFillBinding};
pub const MAX_FILL_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct LazyFillRequest {
    pub binding: LazyFillBinding,
    pub offset: u64,
    pub length: usize,
    pub deadline: Instant,
}

pub type LazyFillFuture<'a> = Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'a>>;

/// Return the complete requested range or an error. Only DFS may commit bytes.
/// Implementations must honor the deadline and must not recursively access AFS.
pub trait LazyFillHandler: Send + Sync {
    fn fill(&self, request: LazyFillRequest) -> LazyFillFuture<'_>;
}

#[derive(Default)]
pub struct LazyFillRegistry {
    handlers: BTreeMap<String, Arc<dyn LazyFillHandler>>,
}

impl LazyFillRegistry {
    /// Construct before sharing the registry with the filesystem.
    pub fn register(&mut self, kind: &str, handler: Arc<dyn LazyFillHandler>) -> Result<()> {
        if self.handlers.contains_key(kind) {
            return Err(invalid("duplicate lazy fill handler kind"));
        }
        LazyFileSpec {
            length: 0,
            binding: LazyFillBinding {
                kind: kind.into(),
                payload: Vec::new(),
            },
        }
        .validate()?;
        self.handlers.insert(kind.into(), handler);
        Ok(())
    }

    pub fn resolve(&self, kind: &str) -> Result<Arc<dyn LazyFillHandler>> {
        self.handlers.get(kind).cloned().ok_or_else(|| {
            Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "no local handler for lazy file kind",
            )
        })
    }
}

fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}
