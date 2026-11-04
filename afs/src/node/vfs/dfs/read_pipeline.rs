//! One native asynchronous read entry; local overlays and write barriers keep
//! their original semantics while all native gRPC read waits are futures.
use super::*;

/// Stack-only read context shared by committed and refreshed views.
struct NativeReadRequest<'a> {
    snapshot: &'a DfsFileHandleSnapshot,
    offset: u64,
    size: usize,
    deadline: Instant,
    lifetime: crate::node::read_pipeline::ReadLifetime,
}

impl DistributedFs {
    async fn readonly_version_async(
        &self,
        inode: &InodeRecord,
        deadline: Instant,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<Arc<ReadOnlyVersion>> {
        let engine = self.read_engine.clone();
        let meta = self.meta.clone();
        let cache = self.versions.cache.clone();
        let gates = self.versions.gates.clone();
        let query_timeout = engine.read_pipeline_timeout();
        let empty = self.versions.empty.clone();
        let version = if let Some(id) = inode.head_version.as_ref() {
            let existing = cache
                .lock()
                .map_err(|_| unavailable("readonly version cache poisoned"))?
                .get(id)
                .cloned();
            if let Some(existing) = existing {
                existing
            } else {
                let gate = gates.entry(id.0.clone())?;
                let guard = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    gate.lock_owned(),
                )
                .await
                .map_err(|_| unavailable("immutable layout coordination expired"))?;
                let existing = cache
                    .lock()
                    .map_err(|_| unavailable("readonly version cache poisoned"))?
                    .get(id)
                    .cloned();
                if let Some(existing) = existing {
                    existing
                } else {
                    let id = id.clone();
                    let cache = cache.clone();
                    let meta = meta.clone();
                    let lifetime = lifetime.clone();
                    let query = tokio::spawn(async move {
                        let _guard = guard;
                        let _lifetime = lifetime;
                        let (file, layout) = tokio::time::timeout(
                            query_timeout.min(deadline.saturating_duration_since(Instant::now())),
                            meta.get_file_version_async(id.clone()),
                        )
                        .await
                        .map_err(|_| unavailable("immutable layout query expired"))??;
                        let loaded = Arc::new(ReadOnlyVersion::checked(&id, file, layout)?);
                        let mut cache = cache
                            .lock()
                            .map_err(|_| unavailable("readonly version cache poisoned"))?;
                        if let Some(existing) = cache.get(&id) {
                            if existing.version != loaded.version
                                || existing.layout != loaded.layout
                            {
                                return Err(invalid(
                                    "immutable version changed during async admission",
                                ));
                            }
                            Ok(existing.clone())
                        } else {
                            cache.insert(id, loaded.clone());
                            Ok(loaded)
                        }
                    });
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), query)
                        .await
                        .map_err(|_| unavailable("immutable layout waiter expired"))?
                        .map_err(|e| unavailable(e.to_string()))??
                }
            }
        } else {
            empty
        };
        if version
            .version
            .as_ref()
            .is_some_and(|v| v.inode_id != inode.inode_id)
        {
            return Err(invalid("readonly version belongs to a different inode"));
        }
        Ok(version)
    }

    pub(super) fn prepare_native_async_read(
        self: Arc<Self>,
        handle: FileHandle,
        offset: u64,
        size: usize,
        deadline: Instant,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<Option<crate::node::read_pipeline::ReadReplyFuture>> {
        if !self.read_engine.async_read_enabled() {
            return Ok(None);
        }
        let snapshot = self.handle_snapshot(handle)?;
        Ok(Some(Box::pin(async move {
            self.read_native_bytes_async(handle, &snapshot, offset, size, deadline, lifetime)
                .await
        })))
    }

    pub(super) async fn read_native_bytes_async(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        offset: u64,
        size: usize,
        deadline: Instant,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<prost::bytes::Bytes> {
        self.read_engine.validate_native_read_size(size)?;

        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        let opened = snapshot.handle.clone();
        let operation = self.capture_native_read_view()?;
        let deadline = deadline.min(operation.deadline);
        let lifetime: crate::node::read_pipeline::ReadLifetime =
            Arc::new((opened.clone(), lifetime));
        let scope = crate::node::read_pipeline::ReadScope {
            deadline,
            lifetime: lifetime.clone(),
        };
        crate::node::read_pipeline::in_read_scope(scope, async {
            let result = self
                .native_read_plan_async(handle, snapshot, offset, size, deadline, lifetime)
                .await
                .inspect_err(|e| self.invalidate_native_handle_view_on_error(handle, e))?;
            #[cfg(test)]
            if let Some(hook) = self
                .hooks
                .native_read_before_delivery
                .lock()
                .unwrap()
                .take()
            {
                hook();
            }
            self.check_native_read_view(&operation)?;
            if opened.closed.load(Ordering::Acquire) {
                return Err(stale("DFS file handle is no longer open"));
            }
            if opened
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state poisoned"))?
                .native_denial_serial
                != snapshot.native_denial_serial
            {
                return Err(unavailable(
                    "DFS native read authorization changed before delivery",
                ));
            }
            Ok(result)
        })
        .await
    }

    async fn native_read_plan_async(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        offset: u64,
        size: usize,
        deadline: Instant,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<prost::bytes::Bytes> {
        let request = NativeReadRequest {
            snapshot,
            offset,
            size,
            deadline,
            lifetime: lifetime.clone(),
        };
        let opened = &snapshot.handle;
        if snapshot.native_denial_serial == u64::MAX {
            return Err(unavailable("DFS native read denial serial exhausted"));
        }
        let dynamic = opened.dynamic_read_route.load(Ordering::Acquire);
        if dynamic
            && let Some(provider) = self.remote_provider_visible_for_reads(&snapshot.inode_id)?
        {
            return self
                .remote_read_async(provider, offset, size, deadline)
                .await;
        }
        // This Node's write activity is visible to every handle (4a); without
        // it the handle reads its pinned version below.
        if let Some(state) = self.write_state(&snapshot.inode_id)? {
            let view = {
                let state = state
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state poisoned"))?;
                if state.has_local_write_activity() {
                    Some(ResolvedReadView {
                        committed: state.base_version.as_ref().map(|v| v.id.clone()),
                        length: state.logical_length,
                        layout: state.base_layout.clone(),
                        frozen: state
                            .in_flight
                            .as_ref()
                            .and_then(InFlightCommit::dirty_extents),
                        active: state.dirty_extents.snapshot_range(offset, size)?,
                    })
                } else {
                    None
                }
            };
            if let Some(view) = view {
                if offset >= view.length || size == 0 {
                    return Ok(prost::bytes::Bytes::new());
                }
                let count = (view.length - offset).min(size as u64) as usize;
                let ops = self.layout_read_ops(&view.layout, offset, count)?;
                let batch = ReadBatch {
                    file_version_id: view.committed.clone(),
                    layout_root_id: view.layout.id.clone(),
                    ops,
                    lookahead: Vec::new(),
                };
                let store = self.chunk_store.clone();
                return self
                    .read_engine
                    .read_native_overlay_async(batch, count, deadline, lifetime, move |out| {
                        if let Some(frozen) = view.frozen {
                            frozen.overlay_native(offset, out, Some(store.as_ref()))?;
                        }
                        view.active
                            .overlay_native(offset, out, Some(store.as_ref()))?;
                        Ok(())
                    })
                    .await;
            }
        }
        let (inode, view) = self.native_inode_for_read_async(handle, snapshot).await?;
        let result = self
            .read_committed_async(
                &inode,
                NativeReadRequest {
                    deadline: deadline.min(view.deadline),
                    ..request
                },
                true,
            )
            .await?;
        self.check_native_read_view(&view)?;
        Ok(result)
    }

    async fn read_committed_async(
        &self,
        inode: &InodeRecord,
        request: NativeReadRequest<'_>,
        prefetch: bool,
    ) -> Result<prost::bytes::Bytes> {
        let NativeReadRequest {
            snapshot,
            offset,
            size,
            deadline,
            lifetime,
        } = request;

        let version = self
            .readonly_version_async(inode, deadline, lifetime.clone())
            .await?;
        if offset >= version.layout.file_length || size == 0 {
            return Ok(prost::bytes::Bytes::new());
        }
        let count = (version.layout.file_length - offset).min(size as u64) as usize;
        // Speculative parts are cached per handle and keyed by offset only, so
        // only a handle whose version never moved may use them.
        if !snapshot.handle.dynamic_read_route.load(Ordering::Acquire)
            && crate::node::native_prefetch::eligible(snapshot.flags)
            && let Some((budget, window)) = self.read_engine.native_prefetch_policy()
        {
            let controller = snapshot.handle.native_prefetch.get_or_init(|| {
                crate::node::native_prefetch::Controller::new(budget.clone(), window)
            });
            let fetch = controller.fetch(|| {
                let engine = self.read_engine.clone();
                let version = version.clone();
                Arc::new(move |a, n, read_deadline, life| {
                    let engine = engine.clone();
                    let version = version.clone();
                    Box::pin(async move {
                        let batch =
                            version.native_read_batch(a, n, engine.source_lookahead_chunks())?;
                        engine
                            .read_native_unified_async(batch, n, read_deadline, life)
                            .await
                    })
                })
            });
            // Large internal reads discard speculative parts and use a single
            // owned reply; only small cross-boundary reads need assembly.
            let crossing = count <= crate::node::native_prefetch::UNIT
                && offset % crate::node::native_prefetch::UNIT as u64 + count as u64
                    > crate::node::native_prefetch::UNIT as u64;
            // Serialize rare shape-change assembly before acquiring any parts.
            // Pausing prevents another completion from growing the window here.
            let assembly = if crossing {
                Some(
                    tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        budget.assembly.clone().lock_owned(),
                    )
                    .await
                    .map_err(|_| unavailable("native prefetch assembly expired"))?,
                )
            } else {
                None
            };
            let pause = if crossing {
                Some(controller.pause(offset, count)?)
            } else {
                None
            };
            let prepared = if crossing && controller.overlaps(offset, count)? {
                Some(
                    self.read_engine
                        .allocate_native_prefetch_reply(count, deadline)
                        .await?,
                )
            } else {
                None
            };
            let parts = controller
                .read(
                    offset,
                    count,
                    version.layout.file_length,
                    deadline,
                    fetch,
                    lifetime.clone(),
                )
                .await?;
            let reply = self
                .read_engine
                .compose_native_prefetch_reply(parts, count, prepared)?;
            drop(pause);
            drop(assembly);
            return Ok(reply);
        }
        let batch =
            version.native_read_batch(offset, count, self.read_engine.source_lookahead_chunks())?;
        let hint = if prefetch && self.read_engine.readahead_object_limit() != 0 {
            Some(batch.clone())
        } else {
            None
        };
        let result = self
            .read_engine
            .read_native_unified_async(batch, count, deadline, lifetime)
            .await?;
        #[cfg(test)]
        if let Some(hook) = self
            .hooks
            .native_read_after_committed
            .lock()
            .unwrap()
            .take()
        {
            hook();
        }
        if let Some(batch) = hint {
            self.record_async_prefetch(snapshot, batch, offset, result.len(), deadline)?;
        }
        Ok(result)
    }

    async fn remote_read_async(
        &self,
        provider: RemoteDfsProviderIo,
        offset: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<prost::bytes::Bytes> {
        if length == 0 {
            return Ok(prost::bytes::Bytes::new());
        }
        let remote = &provider.remote;
        let handle = remote.read_handle.as_ref().unwrap_or(&remote.handle);
        let request = afs_protocol::node_data::DfsOwnerReadRequest {
            handle: Some(data_owner_handle(handle)),
            offset,
            length: length as u64,
        };
        self.read_engine
            .read_owner_payload_async(length, deadline, async {
                let result = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    remote.owner.read_async(request),
                )
                .await
                .map_err(|_| unavailable("DFS owner read expired"))??;
                Ok(result.data)
            })
            .await
    }

    fn record_async_prefetch(
        &self,
        snapshot: &DfsFileHandleSnapshot,
        batch: ReadBatch,
        offset: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<()> {
        if self.read_engine.readahead_object_limit() == 0 {
            return Ok(());
        }
        let Some(version) = batch.file_version_id.as_ref() else {
            return Ok(());
        };
        let mut current = snapshot
            .handle
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state poisoned"))?;
        if current.native_denial_serial != snapshot.native_denial_serial {
            return Err(unavailable("DFS read authorization changed"));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(&(version.0.len() as u64).to_le_bytes());
        hash.update(version.0.as_bytes());
        hash.update(batch.layout_root_id.0.as_bytes());
        let window = current.read_sequence.record(
            *hash.finalize().as_bytes(),
            offset,
            length,
            self.read_engine.readahead_object_limit(),
        );
        let sequence = current.read_sequence.generation();
        let generation = current.native_read_generation;
        drop(current);
        let opened = Arc::downgrade(&snapshot.handle);
        let denial = snapshot.native_denial_serial;
        self.read_engine.prefetch_cached(
            &batch,
            window,
            Arc::new(move || {
                let handle = opened
                    .upgrade()
                    .ok_or_else(|| stale("DFS prefetch handle closed"))?;
                if handle.closed.load(Ordering::Acquire) || Instant::now() >= deadline {
                    return Err(stale("DFS prefetch handle view retired"));
                }
                let state = handle
                    .state
                    .lock()
                    .map_err(|_| unavailable("DFS prefetch handle state poisoned"))?;
                if state.native_denial_serial != denial
                    || state.native_read_generation != generation
                    || state.read_sequence.generation() != sequence
                {
                    return Err(unavailable("DFS prefetch handle scope changed"));
                }
                Ok(())
            }),
        );
        Ok(())
    }
}
