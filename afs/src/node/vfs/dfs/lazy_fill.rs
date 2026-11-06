//! Owner-driven lazy fill, using the ordinary file commit and peer protocols.
use super::*;
use crate::node::vfs::lazy_fill::{
    DESCRIPTOR_XATTR, LazyFileSpec, LazyFillRegistry, LazyFillRequest, MAX_FILL_BYTES,
};

pub(super) fn descriptor(inode: &InodeRecord) -> Result<Option<LazyFileSpec>> {
    inode
        .xattrs
        .get(DESCRIPTOR_XATTR)
        .map(|value| {
            let source: LazyFileSpec = serde_json::from_slice(value)
                .map_err(|e| invalid(format!("invalid lazy-file descriptor: {e}")))?;
            source.validate()?;
            Ok(source)
        })
        .transpose()
}

pub(super) fn sealed(inode: &InodeRecord) -> bool {
    inode.xattrs.contains_key(DESCRIPTOR_XATTR)
}

fn covered(layout: &LayoutRoot, offset: u64, length: u64) -> bool {
    let Some(end) = offset.checked_add(length) else {
        return false;
    };
    let mut cursor = offset;
    for extent in &layout.inline_extents {
        if extent.file_offset > cursor {
            break;
        }
        cursor = cursor.max(extent.file_offset.saturating_add(extent.length));
        if cursor >= end {
            return true;
        }
    }
    length == 0
}

/// Find only absent bytes inside the caller's requested range. Do not prefetch
/// neighbors or overwrite already committed bytes.
fn missing_range(layout: &LayoutRoot, offset: u64, end: u64) -> Option<(u64, usize)> {
    let mut cursor = offset;
    for extent in &layout.inline_extents {
        if cursor >= end {
            return None;
        }
        if extent.file_offset > cursor {
            return Some((
                cursor,
                (extent.file_offset.min(end) - cursor).min(MAX_FILL_BYTES as u64) as usize,
            ));
        }
        cursor = cursor.max(extent.file_offset.saturating_add(extent.length));
    }
    (cursor < end).then(|| (cursor, (end - cursor).min(MAX_FILL_BYTES as u64) as usize))
}

impl DistributedFs {
    /// Reject new origin work at Node shutdown; active work retains its permit
    /// through handle release, so normal DFS drain cannot race a late callback.
    pub fn close_lazy_fill_admission(&self) {
        self.lazy_fill_lifetime.close();
        self.lazy_fill_budget.close();
    }

    pub async fn drain_lazy_fills(&self) {
        self.close_lazy_fill_admission();
        while self.lazy_fill_lifetime.available_permits() != tokio::sync::Semaphore::MAX_PERMITS {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Install a local external-system connector before serving requests.
    pub fn with_lazy_fill_registry(mut self, registry: Arc<LazyFillRegistry>) -> Self {
        self.lazy_fill_registry = registry;
        self
    }

    pub(super) fn prepare_lazy(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        source: &LazyFileSpec,
    ) -> Result<Entry> {
        source.validate()?;
        match self.lookup(ctx, parent, name) {
            Ok(entry) => return self.match_lazy_file(entry, source),
            Err(error) if error.kind() == afs_error::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if self.read_engine.has_external_backend() {
            return Err(invalid("lazy fill requires the native DFS backend"));
        }
        // No final-name reader can observe a plain sparse file. Meta's existing
        // no-replace rename arbitrates concurrent preparations of the same name.
        let temporary = OsString::from(format!(".afs-prepare-{}", uuid::Uuid::new_v4()));
        let created = self.create(ctx, parent, &temporary, 0o400, libc::O_RDWR)?;
        let prepared = (|| {
            self.setattr(
                ctx,
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    size: Some(source.length),
                    ..Default::default()
                },
            )?;
            self.fsync(ctx, created.handle, SyncMode::Full)?;
            let inode_id = self.inode_id(created.entry.inode)?;
            let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
            let value = serde_json::to_vec(source).map_err(|e| invalid(e.to_string()))?;
            self.refresh_inode_record(
                self.meta.set_xattr(SetXattrRequest {
                    caller_id: self.node_id.clone(),
                    operation_id: self.operation_id("prepare-lazy"),
                    caller: caller_context(ctx),
                    inode_id,
                    expected_inode_revision: current.revision,
                    name: DESCRIPTOR_XATTR.to_vec(),
                    value,
                    mode: XattrSetMode::Create,
                })?,
                true,
                true,
            )?;
            Ok(())
        })();
        let released = self.release(ctx, created.handle);
        if let Err(error) = prepared.and(released) {
            self.unlink(ctx, parent, &temporary)?;
            return Err(error);
        }
        match self.publish_prepared(ctx, parent, &temporary, name, created.entry.inode)? {
            super::super::PreparedPublication::Published => {}
            super::super::PreparedPublication::Occupied => {
                self.unlink(ctx, parent, &temporary)?;
                return self.match_lazy_file(self.lookup(ctx, parent, name)?, source);
            }
        }
        self.lookup(ctx, parent, name)
    }

    fn match_lazy_file(&self, entry: Entry, source: &LazyFileSpec) -> Result<Entry> {
        let inode = self.meta.get_inode(&self.inode_id(entry.inode)?)?;
        if descriptor(&inode)?.as_ref() != Some(source) || entry.attributes.size != source.length {
            return Err(Error::coded(
                afs_error::IO_ALREADY_EXISTS,
                "different lazy file already exists",
            ));
        }
        Ok(entry)
    }

    pub(super) fn lazy_read(
        &self,
        snapshot: &DfsFileHandleSnapshot,
        offset: u64,
        size: usize,
        deadline: Instant,
    ) -> Result<Option<prost::bytes::Bytes>> {
        if Instant::now() >= deadline {
            return Err(unavailable("lazy read deadline exceeded"));
        }
        let Some(source) = descriptor(&snapshot.opened_inode)? else {
            return Ok(None);
        };
        if self.read_engine.has_external_backend() {
            return Err(invalid("lazy fill requires native DFS"));
        }
        self.read_engine.validate_native_read_size(size)?;
        let authority = self.capture_native_read_view()?;
        if offset >= source.length || size == 0 {
            self.check_lazy_delivery(snapshot, &authority)?;
            return Ok(Some(prost::bytes::Bytes::new()));
        }
        let count = (source.length - offset).min(size as u64) as usize;
        let mut inode = self.validate_inode(self.meta.get_inode(&snapshot.inode_id)?)?;
        if descriptor(&inode)?.as_ref() != Some(&source) {
            return Err(stale("lazy-file source changed"));
        }
        let version = self.readonly_version_for_inode(&inode)?;
        if version.layout.file_length != source.length {
            return Err(invalid(
                "lazy-file length differs from its immutable source",
            ));
        }
        if !covered(&version.layout, offset, count as u64) {
            let (owner_inode, lease) = self.meta.resolve_write_authority(&inode.inode_id)?;
            inode = self.validate_inode(owner_inode)?;
            if lease.owner_node_id != self.node_id || lease.owner_session_id != self.session_id {
                // Reuse the existing owner open/read/release lifecycle. The remote
                // handle is read-only; it cannot be used to modify sealed bytes.
                let previous = self.current_remote_provider(&inode.inode_id)?;
                let session = self.open_remote_write_session(
                    &inode,
                    &lease,
                    libc::O_RDONLY,
                    OpenOptions::default(),
                )?;
                let result = (|| {
                    let remote = session
                        .remote
                        .as_ref()
                        .ok_or_else(|| invalid("missing remote owner"))?;
                    let mut reply = None;
                    self.read_engine.run_sync_boundary(Box::pin(async {
                        let scope = crate::node::read_pipeline::ReadScope {
                            deadline,
                            lifetime: snapshot.handle.clone(),
                        };
                        reply = Some(
                            crate::node::read_pipeline::in_read_scope(scope, async {
                                tokio::time::timeout_at(
                                    tokio::time::Instant::from_std(deadline),
                                    remote.owner.read_async(
                                        afs_protocol::node_data::DfsOwnerReadRequest {
                                            handle: Some(Self::data_owner_handle(&remote.handle)),
                                            offset,
                                            length: count as u64,
                                        },
                                    ),
                                )
                                .await
                                .map_err(|_| unavailable("lazy owner read expired"))?
                            })
                            .await?,
                        );
                        Ok(())
                    }))?;
                    let reply = reply.ok_or_else(|| unavailable("missing lazy owner reply"))?;
                    if reply.data.len() != count {
                        return Err(unavailable("short lazy owner read"));
                    }
                    Ok(prost::bytes::Bytes::from(reply.data))
                })();
                let closed =
                    self.close_temporary_remote_write_session(&inode.inode_id, &session, previous);
                let data = result?;
                closed?;
                self.check_lazy_delivery(snapshot, &authority)?;
                if Instant::now() >= deadline {
                    return Err(unavailable("lazy owner read expired before delivery"));
                }
                return Ok(Some(data));
            }
            self.fill_lazy_owned(&inode, lease, &source, offset, count, deadline)?;
            inode = self.validate_inode(self.meta.get_inode(&inode.inode_id)?)?;
        }
        let committed = self.readonly_version_for_inode(&inode)?;
        if !covered(&committed.layout, offset, count as u64) {
            return Err(unavailable("lazy range is not committed"));
        }
        // Never read dirty overlays or synthesize holes for a sealed file.
        let mut out = vec![0; count];
        if self.read_committed_inode(&inode, offset, &mut out)? != count {
            return Err(unavailable("short committed lazy read"));
        }
        self.check_lazy_delivery(snapshot, &authority)?;
        if Instant::now() >= deadline {
            return Err(unavailable("lazy read expired before delivery"));
        }
        Ok(Some(out.into()))
    }

    fn check_lazy_delivery(
        &self,
        snapshot: &DfsFileHandleSnapshot,
        authority: &NativeReadView,
    ) -> Result<()> {
        self.check_native_read_view(authority)?;
        if snapshot.handle.closed.load(Ordering::Acquire) {
            return Err(stale("lazy read handle closed"));
        }
        if snapshot
            .handle
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state poisoned"))?
            .native_denial_serial
            != snapshot.native_denial_serial
        {
            return Err(unavailable(
                "lazy read authorization changed before delivery",
            ));
        }
        Ok(())
    }

    fn fill_lazy_owned(
        &self,
        inode: &InodeRecord,
        lease: WriteLease,
        source: &LazyFileSpec,
        offset: u64,
        count: usize,
        deadline: Instant,
    ) -> Result<()> {
        // Lifetime admission counts waiters too, independently of the scarce
        // origin slots. Closing it makes shutdown race-free before any handle opens.
        let _activity = self
            .lazy_fill_lifetime
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable("lazy admission closed"))?;
        let handler = self.lazy_fill_registry.resolve(&source.binding.kind)?;
        self.install_write_state(inode.clone(), lease)?;
        let session = self.open_write_session(inode, libc::O_RDWR)?;
        let handle = self.allocate_handle(DfsFileHandle {
            origin_fill: true,
            gc_pin: None,
            inode_id: inode.inode_id.clone(),
            opened_inode: inode.clone(),
            opened_page_identity: None,
            native_read_view: None,
            native_read_generation: 0,
            native_denial_serial: 0,
            write_session: Some(Self::make_local_write_session(session.clone())),
            flags: libc::O_RDWR,
            owner_scope: None,
        });
        let handle = match handle {
            Ok(handle) => handle,
            Err(error) => {
                self.release_writer(&session)?;
                return Err(error);
            }
        };
        let result = (|| {
            let cell = self
                .write_state(&inode.inode_id)?
                .ok_or_else(|| stale("missing lazy owner state"))?;
            // Runtime permits coordinate work; committed extents remain the only
            // durable availability state. No DFS state mutex is held across I/O.
            let mut permits = None;
            self.read_engine.run_sync_boundary(Box::pin(async {
                let file = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    cell.lazy_fill_gate.clone().acquire_owned(),
                )
                .await
                .map_err(|_| unavailable("lazy file wait expired"))?
                .map_err(|_| unavailable("lazy file gate closed"))?;
                permits = Some(file);
                Ok(())
            }))?;
            let _permits = permits.ok_or_else(|| unavailable("missing lazy fill permits"))?;
            // Same-file waiters coalesce before occupying a global origin slot.
            let mut node_permit = None;
            self.read_engine.run_sync_boundary(Box::pin(async {
                node_permit = Some(
                    tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        self.lazy_fill_budget.clone().acquire_owned(),
                    )
                    .await
                    .map_err(|_| unavailable("lazy admission expired"))?
                    .map_err(|_| unavailable("lazy admission closed"))?,
                );
                Ok(())
            }))?;
            let _node_permit = node_permit.ok_or_else(|| unavailable("missing lazy admission"))?;
            let end = offset
                .checked_add(count as u64)
                .ok_or_else(|| invalid("lazy range overflow"))?;
            loop {
                if Instant::now() >= deadline {
                    return Err(unavailable("lazy fill deadline exceeded"));
                }
                let fresh = self.validate_inode(self.meta.get_inode(&inode.inode_id)?)?;
                if descriptor(&fresh)?.as_ref() != Some(source) {
                    return Err(stale("lazy source changed"));
                }
                let version = self.readonly_version_for_inode(&fresh)?;
                let Some((start, length)) = missing_range(&version.layout, offset, end) else {
                    break;
                };
                let request = LazyFillRequest {
                    binding: source.binding.clone(),
                    offset: start,
                    length,
                    deadline,
                };
                let mut body = None;
                self.read_engine.run_sync_boundary(Box::pin(async {
                    body = Some(
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline),
                            handler.fill(request),
                        )
                        .await
                        .map_err(|_| unavailable("lazy origin deadline exceeded"))??,
                    );
                    Ok(())
                }))?;
                let body = body.ok_or_else(|| unavailable("lazy handler returned no body"))?;
                if body.len() != length {
                    return Err(invalid("lazy handler returned an incomplete range"));
                }
                if Instant::now() >= deadline {
                    return Err(unavailable("lazy fill expired before write"));
                }
                // Never admit a late callback under a newer owner/epoch. The
                // ordinary write/commit fencing still arbitrates the final race.
                let (_, live_lease) = self.meta.resolve_write_authority(&inode.inode_id)?;
                if live_lease.owner_node_id != self.node_id
                    || live_lease.owner_session_id != self.session_id
                    || live_lease.lease_epoch != session.lease_epoch
                {
                    return Err(stale("lazy owner changed during origin read"));
                }
                let written = self.write_with_options(
                    &owner_request_context(),
                    handle,
                    start,
                    &body,
                    WriteOptions::default(),
                )?;
                if written != length {
                    return Err(unavailable("short lazy fill write"));
                }
                self.fsync(&owner_request_context(), handle, SyncMode::Full)?;
            }
            Ok(())
        })();
        let released = self.release(&owner_request_context(), handle);
        result.and(released)
    }
}
