//! Volatile Store backend for disposable development and tests.

use std::sync::Mutex;

use afs_error::Error;

use super::{BackendPersistence, MetaFuture, StoreBackend};

fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}

/// Explicitly volatile backend for tests and disposable deployments. It is
/// still routed through Store so commit visibility and batching are tested.
#[derive(Default)]
pub struct MemoryBackend {
    snapshot: Mutex<Option<(u64, Vec<u8>)>>,
}

impl StoreBackend for MemoryBackend {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        Box::pin(async move {
            Ok(self
                .snapshot
                .lock()
                .map_err(|_| unavailable("memory store lock poisoned"))?
                .clone())
        })
    }

    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
        Box::pin(async move {
            let mut snapshot = self
                .snapshot
                .lock()
                .map_err(|_| unavailable("memory store lock poisoned"))?;
            if snapshot.as_ref().map_or(0, |(version, _)| *version) != expected_version {
                return Err(unavailable("memory store version changed"));
            }
            let version = expected_version
                .checked_add(1)
                .ok_or_else(|| unavailable("store version exhausted"))?;
            *snapshot = Some((version, bytes));
            Ok(version)
        })
    }
}
