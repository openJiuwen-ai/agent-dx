//! Deferred replies retain their dispatch turn and inode lease through the
//! actual kernel reply. The reply executor never waits for network or disk.
use super::dispatch::FuseCompletion;
use super::page_cache::InodeCacheGate;
use super::*;
use afs_error::Result;

struct ReadReply {
    reply: ReplyData,
    result: std::result::Result<prost::bytes::Bytes, i32>,
    diagnostic: Option<crate::node::read_work::ReadDiagnosticSpan>,
    read: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    inode: Option<InodeCacheGate>,
    completion: FuseCompletion,
}

impl ReadReply {
    fn finish(self) {
        let Self {
            reply,
            result,
            diagnostic,
            read,
            inode,
            completion,
        } = self;
        complete_read_reply(diagnostic, result.is_ok(), || match result {
            Ok(bytes) => reply.data(&bytes),
            Err(error) => reply.error(fuse_errno(error)),
        });
        drop(read);
        drop(inode);
        drop(completion);
    }
}

pub(super) struct ReadCompletions {
    sender: Option<tokio::sync::mpsc::Sender<ReadReply>>,
    worker: Option<std::thread::JoinHandle<()>>,
    task_timeout: Duration,
}

impl ReadCompletions {
    fn new(capacity: usize, task_timeout: Duration) -> Result<Self> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<ReadReply>(capacity);
        let worker = std::thread::Builder::new()
            .name("afs-read-reply".to_owned())
            .spawn(move || {
                while let Some(reply) = receiver.blocking_recv() {
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reply.finish()))
                        .is_err()
                    {
                        afs_logging::error!("FUSE read reply panicked");
                    }
                }
            })
            .map_err(Error::from)?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            task_timeout,
        })
    }
}

impl Drop for ReadCompletions {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            afs_logging::error!("FUSE read reply executor failed");
        }
    }
}

impl AfsFuse {
    pub(super) fn enable_read_pipeline(
        &mut self,
        limits: crate::node::read::admission::ReadPipelineLimits,
    ) -> Result<()> {
        if !self.backend.supports_async_reads() {
            return Ok(());
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::coded(
                afs_error::CONFIG_INVALID,
                "async FUSE reads require a live node runtime",
            )
        })?;
        let completions = ReadCompletions::new(
            self.mount_options.task_capacity(),
            Duration::from_millis(limits.task_timeout_ms),
        )?;
        self.read_runtime = Some(runtime);
        self.read_completions = Some(completions);
        Ok(())
    }

    pub(super) fn try_start_async_read(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        size: u32,
        reply: ReplyData,
    ) -> Option<ReplyData> {
        let Some(runtime) = self.read_runtime.clone() else {
            return Some(reply);
        };
        let Some((sender, task_timeout)) = self
            .read_completions
            .as_ref()
            .and_then(|c| c.sender.clone().map(|sender| (sender, c.task_timeout)))
        else {
            return Some(reply);
        };
        if !self.backend.supports_async_reads() {
            return Some(reply);
        }
        self.record_io_request("read", size as usize);
        let diagnostic = self.read_work.as_ref().and_then(|metrics| {
            metrics.diagnostic(crate::node::read_work::ReadDiagnosticStage::FuseRead)
        });
        let deadline = std::time::Instant::now() + task_timeout;
        let state = self.state.clone();
        let gates = self.inode_cache_gates.clone();
        let backend = self.backend.clone();
        let context = Self::context(req, 0);
        let self_metrics = self.read_work.clone();
        self.dispatch.submit_read_deferred_until(
            self.file_dispatch_key(ino.0, fh.0),
            deadline,
            move |admitted| {
                let completion = match admitted {
                    Ok(completion) => completion,
                    Err(error) => {
                        // Not admitted: no task slot or data owner exists to retire.
                        complete_read_reply(diagnostic, false, || reply.error(fuse_errno(error)));
                        return;
                    }
                };
                runtime.spawn(async move {
                    let mut inode = None;
                    let mut read = None;
                    let result = async {
                        if std::time::Instant::now() >= deadline {
                            return Err(libc::ETIMEDOUT);
                        }
                        inode = Some(gates.acquire(ino.0)?);
                        if let Some(inode) = inode.as_ref() {
                            read = Some(inode.read_owned_until(deadline).await?);
                        }
                        let file = state
                            .lock()
                            .map_err(|_| libc::EIO)?
                            .file_handle(fh.0)
                            .ok_or(libc::ESTALE)?;
                        AfsFuse::validate_handle_inode_in(&state, ino.0)?;
                        match backend
                            .clone()
                            .read_reply_async(
                                context.clone(),
                                file.handle,
                                offset,
                                size as usize,
                                deadline,
                                completion.hold(),
                            )
                            .map_err(errno_in(FuseOp::Read))?
                        {
                            Some(future) => future.await.map_err(errno_in(FuseOp::Read)),
                            None => Err(libc::ENOTSUP),
                        }
                    }
                    .await;
                    let event = ReadReply {
                        reply,
                        result,
                        diagnostic,
                        read,
                        inode,
                        completion,
                    };
                    let _wait = self_metrics.as_ref().map(|m| {
                        m.activities
                            .enter(crate::node::read::activity::Activity::CompletionWait, 0)
                    });
                    if let Err(error) = sender.send(event).await {
                        // Shutdown must normally drain before closing this lane.
                        // Retain exactly-once completion even if the executor died.
                        error.0.finish();
                    }
                });
            },
        );
        None
    }
}
