//! Write-callback ordering, admission and dispatched write execution.

use super::metrics::FuseRequestMetrics;
use super::page_cache::PageCacheIdentities;
use super::reply::{FuseOp, errno_in, fuse_errno};
use super::state::{FuseNode, FuseState};
use super::{AfsFuse, front_write, state};
use crate::node::vfs::types::{RequestContext, WriteOptions};
use crate::node::vfs::{Backend, WriteAdmissionMode};
use afs_tracing::tracing;
use fuser::{OpenFlags, ReplyWrite, WriteFlags};
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FrontOrdering {
    WriteAdmission,
    FixedLocalReply,
    WriteBarrier,
}

pub(super) fn ordinary_queued_write(flags: OpenFlags, write_flags: WriteFlags) -> bool {
    flags.0 & (libc::O_APPEND | libc::O_SYNC | libc::O_DSYNC) == 0
        && !write_flags.contains(WriteFlags::FUSE_WRITE_CACHE)
}

pub(super) fn xattr_front_ordering(backend: &dyn Backend, name: &OsStr) -> FrontOrdering {
    if backend.getxattr_is_fixed_local(name) {
        FrontOrdering::FixedLocalReply
    } else {
        FrontOrdering::WriteBarrier
    }
}

pub(super) fn log_write_admission(
    ino: u64,
    fh: u64,
    offset: u64,
    requested_bytes: u64,
    admission: WriteAdmissionMode,
) {
    afs_logging::debug!(
        "FUSE write admission";
        "event" => "fuse_write_admission",
        "ino" => ino,
        "fh" => fh,
        "offset" => offset,
        "requested_bytes" => requested_bytes,
        "admission" => match admission {
            WriteAdmissionMode::Queued => "queued",
            WriteAdmissionMode::Borrowed => "borrowed",
        },
    );
}

/// An ordinary write eligible for the front queue, before its body is copied.
pub(super) struct FrontQueuedWrite<'a> {
    pub(super) backend: Arc<dyn Backend>,
    pub(super) ino: u64,
    pub(super) fh: u64,
    pub(super) offset: u64,
    pub(super) data: &'a [u8],
    pub(super) write_flags: WriteFlags,
    pub(super) context: RequestContext,
    pub(super) file: state::FuseFileHandle,
}

/// Everything a dispatched non-front-queued write needs on its worker. The
/// callback guard is held until the worker finishes the write.
pub(super) struct DispatchedWrite {
    pub(super) front_guard: Option<front_write::CallbackGuard>,
    pub(super) backend: Arc<dyn Backend>,
    pub(super) state: Arc<Mutex<FuseState>>,
    pub(super) context: RequestContext,
    pub(super) ino: u64,
    pub(super) fh: u64,
    pub(super) offset: u64,
    pub(super) options: WriteOptions,
    pub(super) enqueue_at: std::time::Instant,
    pub(super) source: &'static str,
    pub(super) timing: Option<(FuseRequestMetrics, &'static str)>,
    pub(super) cache_ids: Arc<Mutex<PageCacheIdentities>>,
    pub(super) fuse_span: tracing::Span,
    pub(super) request_span: tracing::Span,
    pub(super) reply: ReplyWrite,
}

impl DispatchedWrite {
    pub(super) fn run(self, data: &[u8]) {
        let _front_guard = self.front_guard;
        let source = self.source;
        if let Some((metrics, filesystem)) = &self.timing {
            metrics
                .write_duration
                .with_label_values(&[filesystem, source, "queue"])
                .observe(self.enqueue_at.elapsed().as_secs_f64());
        }
        let executing = std::time::Instant::now();
        let result = self.fuse_span.in_scope(|| {
            tracing::info_span!("node.fuse.write.execute").in_scope(|| {
                self.fuse_span.record(
                    "queue_wait_us",
                    self.enqueue_at.elapsed().as_micros() as u64,
                );
                let file = self
                    .state
                    .lock()
                    .unwrap()
                    .file_handle(self.fh)
                    .ok_or(libc::ESTALE)?;
                AfsFuse::validate_handle_inode_in(&self.state, self.ino)?;
                Ok(self.backend.as_ref()).and_then(|backend| {
                    backend
                        .write_with_options(
                            &self.context,
                            file.handle,
                            self.offset,
                            data,
                            self.options,
                        )
                        .map_err(errno_in(FuseOp::Write))
                })
            })
        });
        if let Some((metrics, filesystem)) = &self.timing {
            metrics
                .write_duration
                .with_label_values(&[filesystem, source, "execute"])
                .observe(executing.elapsed().as_secs_f64());
        }
        if super::fuse_trace_enabled() {
            afs_logging::warn!(
                "fuse.time";
                "op" => "write",
                "ino" => self.ino,
                "us" => executing.elapsed().as_micros() as u64,
                "q" => self.enqueue_at.elapsed().as_micros() as u64
            );
        }
        let reply = self.reply;
        match result {
            Ok(written) => match u32::try_from(written) {
                Ok(written) => {
                    if let Ok(mut cache) = self.cache_ids.lock() {
                        cache.remove(self.ino);
                    }
                    self.fuse_span.record("returned_bytes", written as u64);
                    tracing::info_span!(parent: &self.request_span, "node.fuse.write.reply", returned_bytes = written as u64)
                        .in_scope(|| reply.written(written))
                }
                Err(_) => reply.error(fuse_errno(libc::EIO)),
            },
            Err(error) => reply.error(fuse_errno(error)),
        }
    }
}

impl AfsFuse {
    pub(super) fn write_front_ordering(
        &self,
        fh: u64,
        flags: OpenFlags,
        write_flags: WriteFlags,
    ) -> FrontOrdering {
        let queued = ordinary_queued_write(flags, write_flags)
            && self
                .state
                .lock()
                .ok()
                .and_then(|state| state.file_handle(fh))
                .is_some_and(|file| self.backend.accepts_queued_writes(file.handle));
        if queued {
            FrontOrdering::WriteAdmission
        } else {
            FrontOrdering::WriteBarrier
        }
    }

    /// Copy the body and admit it to the front queue, which replies once the
    /// bytes are accepted. Ordering among queued writes belongs to the queue.
    pub(super) fn submit_front_queued_write(
        &self,
        queue: &front_write::FrontWriteQueue,
        write: FrontQueuedWrite<'_>,
        reply: ReplyWrite,
    ) {
        let FrontQueuedWrite {
            backend,
            ino,
            fh,
            offset,
            data,
            write_flags,
            context,
            file,
        } = write;
        if let Err(error) = Self::validate_handle_inode_in(&self.state, ino) {
            reply.error(fuse_errno(error));
            return;
        }
        if offset.checked_add(data.len() as u64).is_none() || u32::try_from(data.len()).is_err() {
            reply.error(fuse_errno(libc::EFBIG));
            return;
        }
        let length = data.len();
        let cache_ids = self.page_cache_ids.clone();
        let options = WriteOptions {
            kill_suidgid: write_kill_suidgid(write_flags),
        };
        let timing = self.requests.clone();
        let queued_at = std::time::Instant::now();
        queue.submit_deferred_with(
            ino,
            fh,
            length,
            || {
                let mut owned = Vec::new();
                owned.try_reserve_exact(length).map_err(|_| libc::ENOMEM)?;
                owned.extend_from_slice(data);
                let mut consumed = 0usize;
                Ok(Box::new(move || {
                    let _timer = timing.as_ref().map(|(metrics, filesystem)| {
                        metrics
                            .write_duration
                            .with_label_values(&[filesystem, "write_call", "queue"])
                            .observe(queued_at.elapsed().as_secs_f64());
                        metrics
                            .write_duration
                            .with_label_values(&[filesystem, "write_call", "execute"])
                            .start_timer()
                    });
                    (|| {
                        while consumed < owned.len() {
                            let written = backend
                                .write_with_options(
                                    &context,
                                    file.handle,
                                    offset + consumed as u64,
                                    &owned[consumed..],
                                    options,
                                )
                                .map_err(errno_in(FuseOp::Write))?;
                            if written == 0 || written > owned.len() - consumed {
                                return Err(libc::EIO);
                            }
                            consumed += written;
                        }
                        if let Ok(mut cache) = cache_ids.lock() {
                            cache.remove(ino);
                        }
                        Ok(())
                    })()
                }))
            },
            move |result| match result {
                Ok(()) => reply.written(length as u32),
                Err(error) => reply.error(fuse_errno(error)),
            },
        );
    }

    pub(super) fn validate_handle_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        Self::validate_handle_inode_in(&self.state, ino)
    }

    pub(super) fn validate_handle_inode_in(
        state: &Mutex<FuseState>,
        ino: u64,
    ) -> std::result::Result<(), i32> {
        match state.lock().unwrap().node(ino) {
            Some(FuseNode::Root(_)) | Some(FuseNode::Backend(_)) => Ok(()),
            None => Ok(()),
        }
    }
}

pub(super) fn fuse_write_admission(
    mode: WriteAdmissionMode,
    flags: WriteFlags,
) -> WriteAdmissionMode {
    // Kernel page writeback can wait for uploads/publication. Keep the FUSE
    // receiver available for unrelated page faults; the owned bounded queue
    // retains the same handle FIFO and the original backend acknowledgement.
    if flags.contains(WriteFlags::FUSE_WRITE_CACHE) {
        WriteAdmissionMode::Queued
    } else {
        mode
    }
}

pub(super) fn write_kill_suidgid(write_flags: WriteFlags) -> bool {
    write_flags.contains(WriteFlags::FUSE_WRITE_KILL_SUIDGID)
}
