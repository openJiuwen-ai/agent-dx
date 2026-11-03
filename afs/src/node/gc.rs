//! References outlive in-flight handle users. Drops enqueue durable release retries.
use crate::dfs::{ChunkObject, FileVersionId, GcState, InodeId};
use afs_error::{Error, Result};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

pub trait GcControl: Send + Sync {
    fn retain(
        &self,
        id: &str,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        publication: bool,
    ) -> Result<()>;
    /// Fails closed if the authority cannot capture a pre-write GC fence.
    fn retain_fenced(
        &self,
        _id: &str,
        _inode: &InodeId,
        _version: Option<&FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "fenced GC retain is unsupported",
        ))
    }
    fn release(&self, id: &str) -> Result<()>;
    fn reserve_publication(
        &self,
        operation: &crate::dfs::OperationId,
        chunks: Vec<ChunkObject>,
    ) -> Result<()>;
    fn collect(&self, completed: u64, inventory: Vec<ChunkObject>) -> Result<GcState>;
}
pub struct GcReferences {
    pub control: Arc<dyn GcControl>,
    pending: Mutex<Vec<String>>,
    sequence: AtomicU64,
}
pub struct GcPin {
    fence: Option<crate::dfs::GcFence>,
    id: String,
    owner: Arc<GcReferences>,
}
impl GcPin {
    pub(crate) fn fence(&self) -> Option<crate::dfs::GcFence> {
        self.fence
    }
    /// Recover the immutable operation admitted by this generation pin.
    pub(crate) fn write_operation_id(&self) -> Option<crate::dfs::OperationId> {
        self.id
            .strip_prefix("write:")
            .map(crate::dfs::OperationId::new)
    }
}
impl Drop for GcPin {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.owner.pending.lock() {
            pending.push(self.id.clone());
        }
        // Poisoning keeps the durable reference; it never authorizes deletion.
    }
}
impl GcReferences {
    pub fn new(control: Arc<dyn GcControl>) -> Arc<Self> {
        Arc::new(Self {
            control,
            pending: Mutex::new(Vec::new()),
            sequence: AtomicU64::new(1),
        })
    }
    pub fn pin(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        publication: Option<&crate::dfs::OperationId>,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, publication, false, false)
    }
    pub(crate) fn pin_fenced(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        operation: &crate::dfs::OperationId,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, Some(operation), true, false)
    }
    pub(crate) fn pin_generation(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        operation: &crate::dfs::OperationId,
    ) -> Result<Arc<GcPin>> {
        self.pin_mode(inode, version, Some(operation), true, true)
    }
    fn pin_mode(
        self: &Arc<Self>,
        inode: &InodeId,
        version: Option<&FileVersionId>,
        publication: Option<&crate::dfs::OperationId>,
        fenced: bool,
        generation: bool,
    ) -> Result<Arc<GcPin>> {
        let id = match publication {
            Some(operation) => format!(
                "{}:{}",
                if generation { "write" } else { "publish" },
                operation.0
            ),
            None => format!(
                "open:{}",
                crate::meta::store::timed_request_id(format!(
                    "{}-{}",
                    std::process::id(),
                    self.sequence.fetch_add(1, Ordering::Relaxed)
                ))
            ),
        };
        // Queue cleanup even when an admission reply is lost.
        let mut pin = GcPin {
            fence: None,
            id,
            owner: self.clone(),
        };
        pin.fence = if fenced {
            Some(self.control.retain_fenced(&pin.id, inode, version)?)
        } else {
            self.control
                .retain(&pin.id, inode, version, publication.is_some())?;
            None
        };
        if generation && pin.fence != Some(crate::dfs::GcFence::WriteGeneration) {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "Meta did not admit the write generation",
            ));
        }
        Ok(Arc::new(pin))
    }
    pub fn flush(&self) -> Result<()> {
        let pending = {
            let mut queue = self.pending.lock().map_err(|_| {
                Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
            })?;
            let count = queue.len().min(64);
            queue.drain(..count).collect::<Vec<_>>()
        };
        for (index, id) in pending.iter().enumerate() {
            if let Err(error) = self.control.release(id) {
                self.pending
                    .lock()
                    .map_err(|_| {
                        Error::coded(afs_error::IO_UNAVAILABLE, "GC release queue poisoned")
                    })?
                    .extend(pending[index..].iter().cloned());
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Control {
        releases: Mutex<Vec<String>>,
        fail_once: std::sync::atomic::AtomicBool,
    }
    impl GcControl for Control {
        fn retain(&self, _: &str, _: &InodeId, _: Option<&FileVersionId>, _: bool) -> Result<()> {
            Ok(())
        }
        fn release(&self, id: &str) -> Result<()> {
            if self.fail_once.swap(false, Ordering::Relaxed) {
                return Err(Error::coded(afs_error::IO_UNAVAILABLE, "lost reply"));
            }
            self.releases.lock().unwrap().push(id.into());
            Ok(())
        }
        fn reserve_publication(
            &self,
            _: &crate::dfs::OperationId,
            _: Vec<ChunkObject>,
        ) -> Result<()> {
            Ok(())
        }
        fn collect(&self, _: u64, _: Vec<ChunkObject>) -> Result<GcState> {
            Ok(GcState::default())
        }
    }
    #[test]
    fn in_flight_access_retains_pin_and_failed_release_is_retried() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        let handle = references.pin(&InodeId::new("i"), None, None).unwrap();
        let in_flight = handle.clone();
        drop(handle);
        references.flush().unwrap();
        assert!(control.releases.lock().unwrap().is_empty());
        drop(in_flight);
        control.fail_once.store(true, Ordering::Relaxed);
        assert!(references.flush().is_err());
        assert!(control.releases.lock().unwrap().is_empty());
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 1);
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 1);
    }
    #[test]
    fn release_work_is_bounded_without_losing_queued_handles() {
        let control = Arc::new(Control::default());
        let references = GcReferences::new(control.clone());
        for _ in 0..130 {
            drop(references.pin(&InodeId::new("i"), None, None).unwrap());
        }
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 64);
        references.flush().unwrap();
        references.flush().unwrap();
        assert_eq!(control.releases.lock().unwrap().len(), 130);
    }
}
