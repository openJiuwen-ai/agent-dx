//! Fixed-open async range preparation; shares cache validation, authority and
//! retry rules with synchronous reads, publishing only a complete private reply.
use super::*;
use crate::node::read_pipeline::{OwnedNativeRead, ReadPipelineLimits};

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
        future: crate::node::read_pipeline::ReadFuture<'_, ()>,
    ) -> Result<()> {
        self.transfer.block_on_read(future)
    }

    pub(crate) fn native_prefetch_policy(
        &self,
    ) -> Option<(Arc<super::super::native_prefetch::Budget>, u64)> {
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
                super::super::native_prefetch::Budget::new(
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
    ) -> Result<crate::node::read_pipeline::NativeReadReply> {
        crate::node::read_pipeline::NativeReadAllocation {
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
        parts: crate::node::native_prefetch::Reply,
        length: usize,
        prepared: Option<crate::node::read_pipeline::NativeReadReply>,
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

    async fn reserve_owned_native_read(
        &self,
        batch: PeerReadBatch,
        chunk: ChunkObject,
        length: usize,
        deadline: Instant,
        group_handle: u64,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<OwnedNativeRead> {
        Ok(OwnedNativeRead {
            batch,
            chunk,
            buffer: Vec::new().into(),
            bytes: Vec::new(),
            deadline,
            group_handle,
            _lifetime: lifetime,
            activity: None,
            allocation: Some(crate::node::read_pipeline::NativeReadAllocation {
                length,
                pool: self.reply_buffer_pool.clone(),
                admission: self.reply_admission.clone(),
                local: self.local.clone(),
            }),
        })
    }

    pub(crate) async fn read_fixed_open_async(
        &self,
        batch: &ReadBatch,
        output_len: usize,
        reference: String,
        deadline: Instant,
        group_handle: u64,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<Option<prost::bytes::Bytes>> {
        if self.external_backend.is_some()
            || batch.ops.len() != 1
            || batch.ops[0].output_offset != 0
            || batch.ops[0].length != output_len as u64
        {
            return Ok(None);
        }
        let authority = self.capture_native_read_authority()?;
        let op = &batch.ops[0];
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("fixed open version absent"))?;
        let key = (reference.clone(), op.chunk_id.clone());
        let cached = self
            .fixed_open_routes
            .lock()
            .map_err(|_| unavailable("fixed open route cache poisoned"))?
            .entries
            .get(&key)
            .map(|(sources, _)| sources.clone());
        let was_cached = cached.is_some();
        let mut resolved = cached;
        let mut last_error = None;
        for attempt in 0..2 {
            if Instant::now() >= deadline {
                return Err(unavailable("fixed open read expired"));
            }
            if resolved.is_none() {
                resolved = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.sources_for_attempt_async(batch, true, deadline, lifetime.clone()),
                )
                .await
                .map_err(|_| unavailable("fixed source lookup expired"))??
                .into_iter()
                .find(|set| set.chunk_id == op.chunk_id)
                .map(Arc::new);
            }
            let set = resolved
                .take()
                .ok_or_else(|| corrupt("fixed open object source absent"))?;
            let Some(chunk) = set.chunk.as_ref().filter(|chunk| chunk.is_segment_crc32c()) else {
                return Ok(None);
            };
            if set.sources.iter().any(|source| source.state.is_readable()
                && matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id == &self.local_node_id))
            {
                return Ok(None);
            }
            for candidate in set.sources.iter().filter(|source| source.state.is_readable()
                && matches!(source.role, CopyRole::DurableReplica | CopyRole::VerifiedCache)
                && matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id != &self.local_node_id))
            {
                let mut source = candidate.clone();
                source.read_grant.token = format!("dfs-open-v1:{reference}");
                let request = PeerReadBatch { file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations: vec![ResolvedReadOp { op: op.clone(), source, chunk: None }],
                };
                let owned = self.reserve_owned_native_read(request, chunk.clone(), output_len, deadline, group_handle, lifetime.clone()).await?;
                match self.transfer.clone().read_owned_native_async(owned).await {
                    Ok(read) => {
                        self.check_native_read_authority(&authority)?;
                        if !was_cached || attempt != 0 {
                            self.install_fixed_async_route(&key, &version, &batch.layout_root_id, &set)?;
                        }
                        return Ok(Some(read.into_reply()));
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            self.invalidate_native_batch_authority(batch);
            {
                let mut cache = self
                    .fixed_open_routes
                    .lock()
                    .map_err(|_| unavailable("fixed open route cache poisoned"))?;
                if let Some((_, bytes)) = cache.entries.remove(&key) {
                    cache.bytes = cache.bytes.saturating_sub(bytes);
                }
            }
            if attempt == 0 {
                self.capture_native_read_authority()?;
            }
        }
        Err(last_error.unwrap_or_else(|| corrupt("fixed open has no readable peer copy")))
    }

    pub(crate) async fn read_native_unified_async(
        &self,
        batch: ReadBatch,
        output_len: usize,
        reference: Option<String>,
        deadline: Instant,
        group_handle: u64,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<prost::bytes::Bytes> {
        let scope = crate::node::read_pipeline::ReadScope {
            deadline,
            lifetime: lifetime.clone(),
        };
        crate::node::read_pipeline::in_read_scope(scope, async {
            self.validate_native_read_size(output_len)?;
            batch.validate_for_output(output_len, &self.config)?;
            self.validate_private_native_ranges(&batch)?;
            if self.local.peer_read_crc_mode() == crate::config::DfsReadCrcMode::Off
                && self.peer_cache.is_none()
                && let Some(reference) = reference.as_ref()
                && let Some(bytes) = self
                    .read_fixed_open_async(
                        &batch,
                        output_len,
                        reference.clone(),
                        deadline,
                        group_handle,
                        lifetime.clone(),
                    )
                    .await?
            {
                return Ok(bytes);
            }
            let authority = self.capture_native_read_authority()?;
            let allocation = crate::node::read_pipeline::NativeReadAllocation {
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
            self.read_batch_into_private_async(
                &batch,
                &mut reply.buffer,
                true,
                Some(deadline),
                reference.as_deref(),
                lifetime,
            )
            .await
            .inspect_err(|error| self.invalidate_native_batch_on_error(&batch, error))?;
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
        lifetime: crate::node::read_pipeline::ReadLifetime,
        overlay: F,
    ) -> Result<prost::bytes::Bytes>
    where
        F: FnOnce(&mut [u8]) -> Result<()> + Send,
    {
        self.validate_native_read_size(length)?;
        batch.validate_for_output(length, &self.config)?;
        self.validate_private_native_ranges(&batch)?;
        let authority = self.capture_native_read_authority()?;
        let scope = crate::node::read_pipeline::ReadScope {
            deadline,
            lifetime: lifetime.clone(),
        };
        crate::node::read_pipeline::in_read_scope(scope, async {
            let allocation = crate::node::read_pipeline::NativeReadAllocation {
                length,
                pool: self.reply_buffer_pool.clone(),
                admission: self.reply_admission.clone(),
                local: self.local.clone(),
            };
            let mut reply = allocation.allocate(deadline).await?;
            reply.buffer.fill(0);
            self.read_batch_into_private_async(
                &batch,
                &mut reply.buffer,
                true,
                Some(deadline),
                None,
                lifetime,
            )
            .await?;
            crate::node::read_pipeline::local_io(|| overlay(&mut reply.buffer))?;
            self.check_native_read_authority(&authority)?;
            Ok(reply.into_bytes())
        })
        .await
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

    fn install_fixed_async_route(
        &self,
        key: &(String, ChunkId),
        version: &FileVersionId,
        layout: &LayoutRootId,
        set: &Arc<ChunkSources>,
    ) -> Result<()> {
        let charge =
            SourceCache::retained_bytes(&(version.clone(), layout.clone(), key.1.clone()), set)
                .saturating_mul(2)
                .saturating_add(key.0.len() + 1024);
        let mut cache = self
            .fixed_open_routes
            .lock()
            .map_err(|_| unavailable("fixed open route cache poisoned"))?;
        if charge <= MAX_SOURCE_CACHE_BYTES {
            if let Some((_, old)) = cache.entries.remove(key) {
                cache.bytes = cache.bytes.saturating_sub(old);
            }
            while cache.entries.len() >= 512
                || cache.bytes.saturating_add(charge) > MAX_SOURCE_CACHE_BYTES
            {
                let Some(old) = cache.entries.keys().next().cloned() else {
                    break;
                };
                if let Some((_, bytes)) = cache.entries.remove(&old) {
                    cache.bytes = cache.bytes.saturating_sub(bytes);
                }
            }
            cache.bytes = cache.bytes.saturating_add(charge);
            cache.entries.insert(key.clone(), (set.clone(), charge));
        }
        Ok(())
    }
}
