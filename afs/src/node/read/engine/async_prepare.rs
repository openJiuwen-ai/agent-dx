//! Async native range preparation; shares cache validation, authority and
//! retry rules with synchronous reads, publishing only a complete private reply.
use super::*;
use crate::node::read::admission::ReadPipelineLimits;

impl DfsReadEngine {
    pub(crate) fn with_read_pipeline(mut self, limits: ReadPipelineLimits) -> Result<Self> {
        let limits = limits.validate()?;
        if self.transfer.supports_async_native_read() {
            self.reply_buffer_pool
                .configure_byte_limit(limits.max_reply_bytes)?;
            if !self.transfer.supports_async_native_read() {
                return Err(invalid(
                    "async read pipeline requires native grpc transport",
                ));
            }
            self.transfer.configure_read_pipeline(limits)?;
        }
        self.reply_admission = Arc::new(tokio::sync::Semaphore::new(limits.max_reply_bytes));
        self.read_pipeline_limits = limits;
        Ok(self)
    }

    pub(crate) fn read_pipeline_timeout(&self) -> Duration {
        Duration::from_millis(self.read_pipeline_limits.task_timeout_ms)
    }

    pub(crate) fn run_sync_boundary(
        &self,
        future: crate::node::read::admission::ReadFuture<'_, ()>,
    ) -> Result<()> {
        self.transfer.block_on_read(future)
    }

    pub(crate) fn native_prefetch_policy(
        &self,
    ) -> Option<(Arc<super::super::direct_prefetch::Budget>, u64)> {
        if self.config.readahead_max_bytes == 0
            || !self.async_read_enabled()
            || self.peer_cache.is_some()
            || self.local.peer_read_crc_mode() != crate::config::DfsReadCrcMode::Off
        {
            return None;
        }
        // Retained speculative replies share the existing reply admission.
        // Leave at least half for ordinary requests; do not enlarge any limit.
        let cap = (self.read_pipeline_limits.max_reply_bytes / 2).min(64 * 1024 * 1024);
        let budget = self
            .native_prefetch_budget
            .get_or_init(|| {
                super::super::direct_prefetch::Budget::new(
                    cap,
                    self.local.read_work_metrics().cloned(),
                )
            })
            .clone();
        Some((
            budget,
            self.config.readahead_max_bytes.min(16 * 1024 * 1024),
        ))
    }

    pub(crate) async fn allocate_native_prefetch_reply(
        &self,
        length: usize,
        deadline: Instant,
    ) -> Result<crate::node::read::admission::NativeReadReply> {
        crate::node::read::admission::NativeReadAllocation {
            length,
            pool: self.reply_buffer_pool.clone(),
            admission: self.reply_admission.clone(),
            local: self.local.clone(),
        }
        .allocate(deadline)
        .await
    }
    pub(crate) fn compose_native_prefetch_reply(
        &self,
        parts: crate::node::read::direct_prefetch::Reply,
        length: usize,
        prepared: Option<crate::node::read::admission::NativeReadReply>,
    ) -> Result<prost::bytes::Bytes> {
        if parts.len() == 1 {
            return parts
                .into_parts()
                .next()
                .ok_or_else(|| invalid("empty native prefetch reply"));
        }
        let mut reply = prepared.ok_or_else(|| invalid("native prefetch assembly not admitted"))?;
        let mut offset = 0usize;
        for bytes in parts.into_parts() {
            let end = offset
                .checked_add(bytes.len())
                .filter(|&end| end <= length)
                .ok_or_else(|| invalid("native prefetch reply overrun"))?;
            reply.buffer[offset..end].copy_from_slice(&bytes);
            if let Some(m) = self.local.read_work_metrics() {
                m.copy("prefetch_parts_to_reply", bytes.len() as u64);
            }
            offset = end;
        }
        if offset != length {
            return Err(invalid("native prefetch reply incomplete"));
        }
        Ok(reply.into_bytes())
    }

    pub(crate) fn async_read_enabled(&self) -> bool {
        self.external_backend.is_none() && self.transfer.supports_async_native_read()
    }

    pub(crate) async fn read_native_unified_async(
        &self,
        batch: ReadBatch,
        output_len: usize,
        deadline: Instant,
        lifetime: crate::node::read::admission::ReadLifetime,
    ) -> Result<prost::bytes::Bytes> {
        let scope = crate::node::read::admission::ReadScope {
            deadline,
            lifetime: lifetime.clone(),
        };
        crate::node::read::admission::in_read_scope(scope, async {
            self.validate_native_read_plan(&batch, output_len)?;
            let authority = self.capture_native_read_authority()?;
            let allocation = crate::node::read::admission::NativeReadAllocation {
                length: output_len,
                pool: self.reply_buffer_pool.clone(),
                admission: self.reply_admission.clone(),
                local: self.local.clone(),
            };
            let mut reply = allocation.allocate(deadline).await?;
            let mut covered = 0usize;
            for op in &batch.ops {
                let start = op.output_offset;
                if start != covered {
                    break;
                }
                covered = covered.saturating_add(op.length as usize);
            }
            if covered != output_len {
                reply.buffer.fill(0);
            }
            self.read_native_parts_async(&batch, &mut reply.buffer, deadline, lifetime)
                .await?;
            self.check_native_read_authority(&authority)?;
            Ok(reply.into_bytes())
        })
        .await
    }

    pub(crate) async fn read_native_overlay_async<F>(
        &self,
        batch: ReadBatch,
        length: usize,
        deadline: Instant,
        lifetime: crate::node::read::admission::ReadLifetime,
        overlay: F,
    ) -> Result<prost::bytes::Bytes>
    where
        F: FnOnce(&mut [u8]) -> Result<()> + Send,
    {
        self.validate_native_read_plan(&batch, length)?;
        let authority = self.capture_native_read_authority()?;
        let scope = crate::node::read::admission::ReadScope {
            deadline,
            lifetime: lifetime.clone(),
        };
        crate::node::read::admission::in_read_scope(scope, async {
            let allocation = crate::node::read::admission::NativeReadAllocation {
                length,
                pool: self.reply_buffer_pool.clone(),
                admission: self.reply_admission.clone(),
                local: self.local.clone(),
            };
            let mut reply = allocation.allocate(deadline).await?;
            reply.buffer.fill(0);
            self.read_native_parts_async(&batch, &mut reply.buffer, deadline, lifetime)
                .await?;
            crate::node::bridge::local_io(|| overlay(&mut reply.buffer))?;
            self.check_native_read_authority(&authority)?;
            Ok(reply.into_bytes())
        })
        .await
    }

    async fn read_native_parts_async(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
        deadline: Instant,
        lifetime: crate::node::read::admission::ReadLifetime,
    ) -> Result<()> {
        let sources = self.replicated_sources();
        for part in self.native_read_batches(batch) {
            sources
                .read_async(&part, out, true, Some(deadline), lifetime.clone())
                .await
                .inspect_err(|error| self.invalidate_native_batch_on_error(&part, error))?;
        }
        Ok(())
    }

    pub(crate) async fn read_owner_payload_async<F>(
        &self,
        length: usize,
        deadline: Instant,
        work: F,
    ) -> Result<prost::bytes::Bytes>
    where
        F: std::future::Future<Output = Result<Vec<u8>>> + Send,
    {
        self.validate_native_read_size(length)?;
        let charge = u32::try_from(length.max(1024 * 1024))
            .map_err(|_| invalid("owner read capacity too large"))?;
        if charge as usize > self.read_pipeline_limits.max_reply_bytes {
            return Err(invalid("owner read exceeds reply budget"));
        }
        let bytes = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.reply_admission.clone().acquire_many_owned(charge),
        )
        .await
        .map_err(|_| unavailable("owner reply byte admission expired"))?
        .map_err(|_| unavailable("owner reply byte admission closed"))?;
        let _rpc = self.transfer.admit_native_read_async().await?;
        let data = work.await?;
        if data.len() > length {
            return Err(invalid("DFS owner read returned more bytes than requested"));
        }
        struct Body {
            data: Vec<u8>,
            _bytes: tokio::sync::OwnedSemaphorePermit,
        }
        impl AsRef<[u8]> for Body {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        Ok(prost::bytes::Bytes::from_owner(Body {
            data,
            _bytes: bytes,
        }))
    }
}
