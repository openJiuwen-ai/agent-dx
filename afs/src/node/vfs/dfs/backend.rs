use super::*;

impl Backend for DistributedFs {
    /// A read-only handle opened with no local write activity pins its version
    /// until close (decision 4a); FUSE must not let it share page-cache bytes
    /// with a handle pinned to another version.
    fn fixed_read_view(&self, handle: FileHandle) -> bool {
        self.handles
            .get(handle.0)
            .ok()
            .flatten()
            .is_some_and(|opened| {
                opened.flags & libc::O_ACCMODE == libc::O_RDONLY
                    && opened.owner_scope.is_none()
                    && !opened.dynamic_read_route.load(Ordering::Acquire)
                    && opened
                        .state
                        .lock()
                        .is_ok_and(|state| state.opened_page_identity.is_some())
            })
    }

    fn page_cache_identity(&self, handle: FileHandle) -> Option<PageCacheIdentity> {
        if self.read_engine.has_external_backend() {
            let opened = self.handles.get(handle.0).ok()??;
            if opened.flags & libc::O_ACCMODE != libc::O_RDONLY
                || opened.owner_scope.is_some()
                || opened.closed.load(Ordering::Acquire)
                || opened.dynamic_read_route.load(Ordering::Acquire)
            {
                return None;
            }
            let state = opened.state.lock().ok()?;
            if state.write_session.is_some() {
                return None;
            }
            return state.opened_page_identity.clone();
        }
        let opened = self.native_read_capability(handle)?;
        let state = opened.state.lock().ok()?;
        self.check_native_read_view(state.native_read_view.as_ref()?)
            .ok()?;
        state.opened_page_identity.clone()
    }

    fn supports_parallel_reads(&self, handle: FileHandle) -> bool {
        self.native_read_capability(handle).is_some()
    }

    fn root_inode(&self) -> BackendInode {
        BackendInode { value: ROOT_INODE }
    }

    fn supports_killpriv_v2(&self) -> bool {
        true
    }

    fn supports_advisory_locks(&self) -> bool {
        true
    }

    fn statfs(&self, _: &RequestContext, inode: BackendInode) -> Result<FilesystemCapacity> {
        let inode_id = self.inode_id(inode)?;
        let record = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        if record.inode_id != inode_id {
            return Err(invalid("Meta returned a different DFS inode for statfs"));
        }
        self.chunk_store.capacity()
    }

    fn getlk(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
    ) -> Result<Option<FileLockConflict>> {
        self.lock_entry_getlk(ctx, inode, handle, request)
    }

    fn setlk(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
        waiter: Option<LockWaiterId>,
    ) -> Result<()> {
        self.lock_entry_setlk(ctx, inode, handle, request, waiter)
    }

    fn cancel_lock_wait(&self, waiter: LockWaiterId) -> Result<()> {
        self.lock_entry_cancel_lock_wait(waiter)
    }

    fn release_locks(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        owner: FileLockOwner,
        kind: ReleaseKind,
    ) -> Result<()> {
        self.lock_entry_release_locks(ctx, inode, handle, owner, kind)
    }

    fn release_lock_session(&self, ingress_session_id: &str) -> Result<()> {
        self.lock_entry_release_lock_session(ingress_session_id)
    }

    fn lookup(&self, _: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<Entry> {
        let inode = self.validate_inode(
            self.meta
                .lookup(&self.inode_id(parent)?, name.as_encoded_bytes())?
                .ok_or_else(|| {
                    Error::coded(afs_error::NODE_VFS_NOT_FOUND, "DFS dentry not found")
                })?,
        )?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: self.visible_attributes(&inode)?,
        })
    }

    fn getattr(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
    ) -> Result<FileAttributes> {
        if let Some(handle) = handle {
            let snapshot = self.handle_snapshot(handle)?;
            // A pinned reader reports the version it reads (decision 4a).
            if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY
                && snapshot.write_session.is_none()
                && snapshot.opened_inode.kind == InodeKind::Regular
                && self
                    .remote_provider_visible_for_reads(&snapshot.inode_id)?
                    .is_none()
                && !self.local_write_activity(&snapshot.inode_id)?
            {
                let opened = snapshot.opened_inode.as_ref();
                let (size, blocks) = self.inode_size_and_blocks(opened)?;
                return Ok(attributes(opened, size, blocks));
            }
            if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
                return self
                    .write_coordinator
                    .remote_getattr(&remote)
                    .inspect_err(|error| {
                        self.invalidate_native_inode_views_on_error(&snapshot.inode_id, error);
                    });
            }
            let observed = self
                .meta
                .get_inode(&snapshot.inode_id)
                .inspect_err(|error| {
                    self.invalidate_native_inode_views_on_error(&snapshot.inode_id, error);
                })?;
            if self.read_engine.has_external_backend() {
                let record = self.refresh_inode_record(observed, true, true)?;
                return self.visible_attributes(&record);
            }
            let record = self.observe_inode_record(observed)?;
            return self.visible_attributes_with_observed_inode(&snapshot.opened_inode, &record);
        }
        let inode_id = self.inode_id(inode)?;
        if let Some(remote) = self.remote_provider_visible_for_reads(&inode_id)? {
            return self
                .write_coordinator
                .remote_getattr(&remote)
                .inspect_err(|error| {
                    self.invalidate_native_inode_views_on_error(&inode_id, error);
                });
        }
        let observed = self.meta.get_inode(&inode_id).inspect_err(|error| {
            self.invalidate_native_inode_views_on_error(&inode_id, error);
        })?;
        let record = if self.read_engine.has_external_backend() {
            self.refresh_inode_record(observed, true, true)?
        } else {
            self.observe_inode_record(observed)?
        };
        self.visible_attributes_with_observed_inode(&record, &record)
    }

    fn setattr(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
        change: &AttributeChange,
    ) -> Result<FileAttributes> {
        self.setattr_with_options(ctx, inode, handle, change, SetAttrOptions::default())
    }

    fn setattr_with_options(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
        change: &AttributeChange,
        options: SetAttrOptions,
    ) -> Result<FileAttributes> {
        let inode_id = self.inode_id(inode)?;
        let mut change = change.clone();
        let mut killpriv_setattr = false;
        let _native_ticket = if let Some(cell) = self.write_state(&inode_id)? {
            let ticket = self.mutation_ticket(&cell)?;
            if self.write_resources.pipeline.is_some() {
                drop(self.begin_inode_operation(&inode_id, cell)?);
            }
            ticket
        } else {
            None
        };
        if options.kill_suidgid {
            let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
            if current.kind == InodeKind::Regular {
                if let Some(state) = self.local_write_state_for_killpriv(&current)? {
                    killpriv_setattr = true;
                    let mut state = self.lock_for_mutation(&inode_id, &state)?;
                    if change.mode.is_some()
                        && !super::super::types::is_legacy_privilege_clear(
                            &attributes(&state.inode, state.logical_length, 0),
                            &change,
                        )
                    {
                        return Err(Error::from(std::io::Error::from(
                            std::io::ErrorKind::PermissionDenied,
                        )));
                    }
                    Self::apply_kernel_killpriv_to_state(&mut state);
                    change.mode = None;
                } else if change.size.is_some() {
                    // The owner is another node: its temporary write session
                    // opens with `kill_suidgid` and commits the clear with the
                    // resize. Without a resize the legacy clear stays the plain
                    // mode change below.
                    change.mode = None;
                }
            }
        } else if change.mode.is_some() && self.has_pending_killpriv(&inode_id)? {
            self.commit_inode(
                &inode_id,
                CommitReason::DATA_SYNC,
                CommitWait::AfterOperations,
            )?;
        }
        if change.mode.is_some()
            || change.uid.is_some()
            || change.gid.is_some()
            || change.atime.is_some()
            || change.mtime.is_some()
            || options.timestamps_now
        {
            let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
            let updated = self.refresh_inode_record(
                self.meta.set_inode_attributes(SetInodeAttrRequest {
                    caller_id: self.identity.node_id.clone(),
                    operation_id: self.identity.operation_id("setattr"),
                    caller: caller_context(ctx),
                    inode_id: inode_id.clone(),
                    expected_inode_revision: current.revision,
                    update: InodeAttrUpdate {
                        mode: change.mode,
                        uid: change.uid,
                        gid: change.gid,
                        atime_unix_ms: change.atime.map(system_time_ms),
                        mtime_unix_ms: change.mtime.map(system_time_ms),
                        ctime_unix_ms: Some(now_unix_ms()),
                        timestamps_now: options.timestamps_now,
                    },
                })?,
                change.atime.is_none() && !options.timestamps_now,
                change.mtime.is_none() && !options.timestamps_now,
            )?;
            if change.size.is_none() {
                if killpriv_setattr {
                    self.commit_inode(
                        &inode_id,
                        CommitReason::DATA_SYNC,
                        CommitWait::AfterOperations,
                    )?;
                    let updated =
                        self.refresh_inode_record(self.meta.get_inode(&inode_id)?, true, true)?;
                    return self.visible_attributes(&updated);
                }
                return self.visible_attributes(&updated);
            }
        }

        let Some(length) = change.size else {
            if killpriv_setattr {
                self.commit_inode(
                    &inode_id,
                    CommitReason::DATA_SYNC,
                    CommitWait::AfterOperations,
                )?;
            }
            return self.getattr(ctx, inode, handle);
        };
        let (record, accepted_seq) = if let Some(handle) = handle {
            self.observe_handle_error(handle)?;
            let snapshot = self.handle_snapshot(handle)?;
            if snapshot.inode_id != inode_id {
                return Err(stale("DFS file handle does not name the requested inode"));
            }
            if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY {
                return Err(bad_file_descriptor("DFS handle was not opened for writing"));
            }
            let Some(session) = snapshot.write_session.as_ref() else {
                return Err(invalid("writable DFS handle has no write session"));
            };
            if snapshot.opened_inode.kind != InodeKind::Regular {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::EISDIR)));
            }
            let accepted_seq = if session.remote.is_some() {
                self.write_coordinator
                    .remote_resize(session, length, options)?
            } else {
                self.resize_dirty_inode(&snapshot.inode_id, length, options.kill_suidgid)?
            };
            (
                snapshot.opened_inode.as_ref().clone(),
                Some((handle, accepted_seq)),
            )
        } else {
            let (record, write_lease) = self.meta.open_write(&inode_id)?;
            let mut record = self.validate_inode(record)?;
            if record.kind != InodeKind::Regular {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::EISDIR)));
            }
            if write_lease.owner_node_id == self.identity.node_id
                && write_lease.owner_session_id == self.identity.session_id
            {
                self.install_write_state(record.clone(), write_lease)?;
                self.resize_dirty_inode(&record.inode_id, length, options.kill_suidgid)?;
            } else {
                let previous_provider = self
                    .write_coordinator
                    .current_remote_provider(&record.inode_id)?;
                let mut session = self.open_remote_write_session(
                    &record,
                    &write_lease,
                    libc::O_WRONLY,
                    OpenOptions {
                        kill_suidgid: options.kill_suidgid,
                    },
                )?;
                match self
                    .write_coordinator
                    .remote_resize(&session, length, options)
                {
                    Ok(accepted_seq) => {
                        session.local.last_accepted_seq = accepted_seq;
                    }
                    Err(error) => {
                        let _ = self.close_temporary_remote_write_session(
                            &record.inode_id,
                            &session,
                            previous_provider.clone(),
                        );
                        return Err(error);
                    }
                }
                if let Err(error) = self.write_coordinator.remote_sync(&session, SyncMode::Full) {
                    let _ = self.close_temporary_remote_write_session(
                        &record.inode_id,
                        &session,
                        previous_provider.clone(),
                    );
                    return Err(error);
                }
                self.close_temporary_remote_write_session(
                    &record.inode_id,
                    &session,
                    previous_provider,
                )?;
                record = self.observe_inode_record(self.meta.get_inode(&inode_id)?)?;
            }
            (record, None)
        };
        if let Some((handle, accepted_seq)) = accepted_seq {
            self.update_handle_write_progress(handle, accepted_seq)?;
        }
        self.visible_attributes(&record)
    }

    fn create(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<CreatedFile> {
        self.create_with_options(ctx, parent, name, mode, flags, OpenOptions::default())
    }

    fn create_with_options(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
        flags: i32,
        options: OpenOptions,
    ) -> Result<CreatedFile> {
        let now = now_unix_ms();
        let mode = mode & !ctx.umask;
        let mode = if options.kill_suidgid {
            Self::clear_kernel_write_privileges(mode)
        } else {
            mode
        };
        let operation_id = self.identity.operation_id("create");
        // Dropped on any failure below, which queues its release: the reply may
        // be lost after Meta already admitted the reference.
        let gc_pin = self
            .gc
            .references
            .as_ref()
            .map(|references| references.created_handle_pin(&operation_id));
        let (inode, write_lease) = self.meta.create(
            &operation_id,
            &self.inode_id(parent)?,
            name.as_encoded_bytes(),
            InodeAttributes {
                mode,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
        )?;
        let inode = self.validate_inode(inode)?;
        self.install_write_state(inode.clone(), write_lease)?;
        let session = self.open_write_session(&inode, flags)?;
        let handle = self.allocate_handle(DfsFileHandle {
            gc_pin,
            inode_id: inode.inode_id.clone(),
            opened_inode: inode.clone(),
            opened_page_identity: None,
            native_read_view: None,
            native_read_generation: 0,
            native_denial_serial: 0,
            write_session: Some(Self::make_local_write_session(session)),
            flags,
            owner_scope: None,
        })?;
        Ok(CreatedFile {
            entry: Entry {
                inode: self.backend_inode(&inode.inode_id)?,
                attributes: attributes(&inode, 0, 0),
            },
            handle,
        })
    }

    fn mknod(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        kind: SpecialFileKind,
        mode: u32,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.mknod(MknodRequest {
            caller_id: self.identity.node_id.clone(),
            operation_id: self.identity.operation_id("mknod"),
            namespace_id: self.identity.namespace_id.clone(),
            parent_inode_id: self.inode_id(parent)?,
            name: name.as_encoded_bytes().to_vec(),
            kind: dfs_special_kind(kind),
            attributes: InodeAttributes {
                mode: mode & !ctx.umask,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller: caller_context(ctx),
        })?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn open(&self, ctx: &RequestContext, inode: BackendInode, flags: i32) -> Result<FileHandle> {
        self.open_with_options(ctx, inode, flags, OpenOptions::default())
    }

    fn open_with_options(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        flags: i32,
        options: OpenOptions,
    ) -> Result<FileHandle> {
        let inode_id = self.inode_id(inode)?;
        let native_read_view = if !self.read_engine.has_external_backend()
            && flags & libc::O_ACCMODE == libc::O_RDONLY
        {
            Some(self.capture_native_read_view()?)
        } else {
            None
        };
        let preflight =
            self.validate_inode(self.meta.get_inode(&inode_id).inspect_err(|error| {
                self.invalidate_native_inode_views_on_error(&inode_id, error);
            })?)?;
        if matches!(preflight.kind, InodeKind::Special(_)) {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "DFS special inode data path is handled by kernel/local device semantics",
            ));
        }
        let writable = flags & libc::O_ACCMODE != libc::O_RDONLY;
        let (opened_inode, write_session) = if writable {
            let (inode, write_lease) = self.meta.resolve_write_authority(&inode_id)?;
            let inode = self.validate_inode(inode)?;
            let session = if write_lease.owner_node_id == self.identity.node_id
                && write_lease.owner_session_id == self.identity.session_id
            {
                self.install_write_state(inode.clone(), write_lease.clone())?;
                let session = self.open_write_session(&inode, flags)?;
                let session = Self::make_local_write_session(session);
                if options.kill_suidgid {
                    let state = self
                        .write_state(&inode.inode_id)?
                        .ok_or_else(|| stale("DFS write state is no longer open"))?;
                    let mut state = self.lock_for_mutation(&inode.inode_id, &state)?;
                    Self::apply_kernel_killpriv_to_state(&mut state);
                }
                if flags & libc::O_TRUNC != 0
                    && let Err(error) =
                        self.resize_dirty_inode(&inode.inode_id, 0, options.kill_suidgid)
                {
                    self.abandon_writer(&session.local)?;
                    return Err(error);
                }
                session
            } else {
                let previous_provider = self
                    .write_coordinator
                    .current_remote_provider(&inode.inode_id)?;
                let session =
                    self.open_remote_write_session(&inode, &write_lease, flags, options)?;
                if flags & libc::O_TRUNC != 0
                    && let Err(error) = self.write_coordinator.remote_resize(
                        &session,
                        0,
                        SetAttrOptions {
                            kill_suidgid: options.kill_suidgid,
                            timestamps_now: false,
                        },
                    )
                {
                    let _ = self.close_temporary_remote_write_session(
                        &inode.inode_id,
                        &session,
                        previous_provider,
                    );
                    return Err(error);
                }
                session
            };
            (inode, Some(session))
        } else {
            let inode = if self.read_engine.has_external_backend() {
                self.refresh_inode_record(preflight, true, true)?
            } else {
                self.observe_inode_record(preflight)?
            };
            (inode, None)
        };
        // Pinned at open: a read-only open with no local write activity and no
        // proxy provider reads this committed version until close (4a).
        let committed_read_only = !writable
            && opened_inode.kind == InodeKind::Regular
            && !self.local_write_activity(&inode_id)?
            && self
                .write_coordinator
                .current_remote_provider(&inode_id)?
                .is_none();
        let opened_page_identity = if committed_read_only {
            // The first version/layout cache miss costs one Meta RPC, outside
            // all handle locks. Authority still starts before the inode RPC.
            Some(self.opened_page_identity(&opened_inode)?)
        } else {
            None
        };
        if let Some(view) = native_read_view.as_ref() {
            self.check_native_read_view(view)?;
        }
        self.allocate_handle(DfsFileHandle {
            gc_pin: None,
            inode_id: opened_inode.inode_id.clone(),
            opened_inode,
            opened_page_identity,
            native_read_view,
            native_read_generation: 0,
            native_denial_serial: 0,
            write_session,
            flags,
            owner_scope: None,
        })
    }

    fn read(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        if !self.read_engine.has_external_backend() {
            let result = self.read_native_owned(handle, &snapshot, offset, out.len())?;
            out[..result.len()].copy_from_slice(&result);
            return Ok(result.len());
        }
        if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
            return self.write_coordinator.remote_read(&remote, offset, out);
        }
        self.read_visible_from_handle_inode(
            &snapshot.inode_id,
            Some(&snapshot.opened_inode),
            offset,
            out,
        )
    }

    fn supports_async_reads(&self) -> bool {
        self.read_engine.async_read_enabled()
    }

    fn read_reply_async(
        self: Arc<Self>,
        _ctx: RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
        deadline: Instant,
        lifetime: crate::node::read::admission::ReadLifetime,
    ) -> Result<Option<crate::node::read::admission::ReadReplyFuture>> {
        self.prepare_native_async_read(handle, offset, size, deadline, lifetime)
    }

    fn read_reply(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<prost::bytes::Bytes> {
        if self.read_engine.has_external_backend() {
            return self.read_owned(ctx, handle, offset, size).map(Into::into);
        }
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        self.read_native_owned(handle, &snapshot, offset, size)
    }

    fn read_owned(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        if !self.read_engine.has_external_backend() {
            return self
                .read_native_owned(handle, &snapshot, offset, size)
                .map(|bytes| bytes.to_vec());
        }
        let fallback = || {
            let mut out = vec![0; size];
            let n = <Self as Backend>::read(self, ctx, handle, offset, &mut out)?;
            out.truncate(n);
            Ok(out)
        };
        if self
            .remote_provider_visible_for_reads(&snapshot.inode_id)?
            .is_some()
        {
            return fallback();
        }
        // Local writes not yet committed are served from this Node's write
        // state (dirty overlay); reading never commits them.
        if self.local_write_activity(&snapshot.inode_id)? {
            return fallback();
        }
        if let Some(data) =
            self.try_read_external_version_owned(&snapshot.opened_inode, offset, size)?
        {
            return Ok(data);
        }
        fallback()
    }

    fn write(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        self.write_with_options(ctx, handle, offset, data, WriteOptions::default())
    }

    fn accepts_queued_writes(&self, handle: FileHandle) -> bool {
        self.write_resources.pipeline.is_some()
            && self.handle_snapshot(handle).ok().is_some_and(|snapshot| {
                snapshot.flags & libc::O_ACCMODE != libc::O_RDONLY
                    && snapshot.flags & libc::O_APPEND == 0
                    && snapshot.flags & libc::O_SYNC == 0
                    && !has_o_dsync(snapshot.flags)
                    && snapshot
                        .write_session
                        .as_ref()
                        .is_some_and(|session| session.remote.is_none())
            })
    }

    fn write_admission_mode(&self, handle: FileHandle) -> WriteAdmissionMode {
        // Native admission may wait for mutation/publication or shared body
        // capacity. Keep those waits on a worker so the FUSE receiver can
        // admit unrelated reads and writeback needed to make progress.
        if self.write_resources.pipeline.is_some() {
            return WriteAdmissionMode::Queued;
        }
        if self
            .handle_snapshot(handle)
            .ok()
            .and_then(|snapshot| snapshot.write_session)
            .is_some_and(|session| session.remote.is_none())
        {
            WriteAdmissionMode::Borrowed
        } else {
            WriteAdmissionMode::Queued
        }
    }

    fn write_with_options(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<usize> {
        self.observe_handle_error(handle)?;
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for writing"));
        }
        let Some(write_session) = snapshot.write_session.as_ref() else {
            return Err(invalid("writable DFS handle has no write session"));
        };
        if write_session.remote.is_some() {
            let written = self.write_coordinator.remote_write(
                write_session,
                snapshot.flags,
                offset,
                data,
                options,
            )?;
            if snapshot.flags & libc::O_SYNC == libc::O_SYNC {
                self.write_coordinator
                    .remote_sync(write_session, SyncMode::Full)?;
            } else if has_o_dsync(snapshot.flags) {
                self.write_coordinator
                    .remote_sync(write_session, SyncMode::DataOnly)?;
            }
            self.update_handle_write_progress(handle, written.1)?;
            return Ok(written.0);
        }
        let state = self
            .write_state(&snapshot.inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?;
        if data.is_empty() {
            return Ok(0);
        }
        if self.write_resources.pipeline.is_none() {
            return Err(unavailable(
                "DFS store has no native write pipeline configured",
            ));
        }
        self.pipeline_ordinary_write(handle, &snapshot, state, offset, data, options)
    }

    fn copy_file_range(
        &self,
        _: &RequestContext,
        range: super::super::types::CopyFileRange,
    ) -> Result<u64> {
        let source = self.handle_snapshot(range.source)?;
        let destination = self.handle_snapshot(range.destination)?;
        if source.flags & libc::O_ACCMODE == libc::O_WRONLY
            || destination.flags & libc::O_ACCMODE == libc::O_RDONLY
        {
            return Err(bad_file_descriptor(
                "COW copy requires readable source and writable destination",
            ));
        }
        if source.inode_id == destination.inode_id {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "same-inode COW range is unsupported",
            ));
        }
        let session = destination
            .write_session
            .as_ref()
            .ok_or_else(|| bad_file_descriptor("missing destination write session"))?;
        if session.remote.is_some()
            || destination.flags & libc::O_APPEND != 0
            || self
                .remote_provider_visible_for_reads(&source.inode_id)?
                .is_some()
        {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "COW requires local destination authority and committed source",
            ));
        }
        // Snapshot the source before acquiring destination admission, avoiding
        // cross-copy deadlock. Concurrent later writes create another version.
        self.commit_inode(
            &source.inode_id,
            CommitReason::FULL_SYNC,
            CommitWait::AfterOperations,
        )?;
        let inode = self.validate_inode(self.meta.get_inode(&source.inode_id)?)?;
        let Some(version_id) = inode.head_version else {
            return Ok(0);
        };
        let (source_version, source_layout) = self.meta.get_file_version(&version_id)?;
        if range.source_offset >= source_version.length || range.length == 0 {
            return Ok(0);
        }
        let length = range
            .length
            .min(source_version.length - range.source_offset)
            .min(u64::from(u32::MAX));
        let cell = self
            .write_state(&destination.inode_id)?
            .ok_or_else(|| stale("destination write state is missing"))?;
        let _operation = self.begin_inode_operation(&destination.inode_id, cell.clone())?;
        self.commit_inode(
            &destination.inode_id,
            CommitReason::FULL_SYNC,
            CommitWait::InsideOperation,
        )?;
        let pending = {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            // J4: a fresh lease is part of this call's contract (see fault::lease_keeper).
            ensure_fresh(&mut state.write_lease, |lease| {
                self.meta.renew_write_lease(lease)
            })?;
            let clone_source = crate::dfs::CloneFileRange {
                version: version_id,
                source_offset: range.source_offset,
                destination_offset: range.destination_offset,
                length,
            };
            let generation = self.identity.next_operation.fetch_add(1, Ordering::Relaxed);
            let layout = crate::dfs::clone_file_layout(
                &state.base_layout,
                &source_layout,
                &clone_source,
                LayoutRootId::new(format!(
                    "{}-cow-layout-{generation}",
                    self.identity.session_id
                )),
            )?;
            let now = now_unix_ms();
            state.next_write_seq = state.next_write_seq.saturating_add(1);
            state.visible_write_seq = state.next_write_seq;
            state.logical_length = layout.file_length;
            state.metadata_dirty = true;
            state.inode.attributes.mtime_unix_ms = now;
            state.inode.attributes.ctime_unix_ms = now;
            Self::apply_kernel_killpriv_to_state(&mut state);
            let frozen = FrozenCommit {
                through_seq: state.visible_write_seq,
                logical_length: layout.file_length,
                inode: state.inode.clone(),
                write_lease: state.write_lease.clone(),
                base_version: state.base_version.clone(),
                base_layout: state.base_layout.clone(),
                dirty_extents: DirtyExtentMap::default(),
                kill_suidgid_dirty: state.kill_suidgid_dirty,
            };
            let commit = CommitFileVersion {
                gc_fence: None,
                operation_id: self.identity.operation_id("cow-copy"),
                inode_id: destination.inode_id.clone(),
                write_lease: state.write_lease.clone(),
                expected_inode_revision: state.inode.revision,
                expected_head_version: state.inode.head_version.clone(),
                file_version: FileVersion {
                    id: FileVersionId::new(format!(
                        "{}-cow-version-{generation}",
                        self.identity.session_id
                    )),
                    inode_id: destination.inode_id.clone(),
                    parent_version: state.inode.head_version.clone(),
                    length: layout.file_length,
                    layout_root: layout.id.clone(),
                    created_at_unix_ms: now,
                },
                layout_root: layout,
                chunk_receipts: Vec::new(),
                clone_source: Some(clone_source),
                metadata_delta: CommitReason::FULL_SYNC.metadata_delta(
                    now,
                    now,
                    state.kill_suidgid_dirty,
                ),
            };
            let pending = PendingFileCommit {
                frozen,
                batch: CommitBatch {
                    _gc_pin: None,
                    through_seq: state.visible_write_seq,
                    commit: Arc::new(commit),
                    _pipeline_accounting: None,
                    _pipeline_uploads: Vec::new(),
                },
            };
            state.in_flight = Some(InFlightCommit::File(Box::new(pending.clone())));
            state.commit_busy = true;
            InFlightCommit::File(Box::new(pending))
        };
        if let Some(seq) = self.finish_commit(cell, pending, "cow-copy")? {
            self.update_handles_after_commit(&destination.inode_id, seq)?;
            self.update_handle_write_progress(range.destination, seq)?;
        }
        afs_logging::info!("dfs.cow_range_committed";
            "source_inode" => source.inode_id.0,
            "source_version" => source_version.id.0,
            "destination_inode" => destination.inode_id.0,
            "source_offset" => range.source_offset,
            "destination_offset" => range.destination_offset,
            "length" => length);
        Ok(length)
    }

    fn flush(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref() {
            if session.remote.is_some() {
                self.write_coordinator
                    .remote_sync(session, SyncMode::DataOnly)
                    .map(|_| ())
            } else {
                self.sync_handle(handle, CommitReason::CLOSE, CommitWait::AfterOperations)
            }
        } else {
            self.observe_handle_error(handle)
        }
    }

    fn fsync(&self, _: &RequestContext, handle: FileHandle, mode: SyncMode) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref()
            && session.remote.is_some()
        {
            self.write_coordinator
                .remote_sync(session, mode)
                .map(|_| ())
        } else {
            self.sync_handle(
                handle,
                match mode {
                    SyncMode::DataOnly => CommitReason::DATA_SYNC,
                    SyncMode::Full => CommitReason::FULL_SYNC,
                },
                CommitWait::AfterOperations,
            )
        }
    }

    fn fsync_receipt(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        mode: SyncMode,
    ) -> Result<SyncReceipt> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref()
            && session.remote.is_some()
        {
            let reply = self.write_coordinator.remote_sync(session, mode)?;
            return Ok(SyncReceipt {
                file_version_id: reply.file_version_id,
                durable_write_seq: reply.durable_write_seq,
            });
        }
        Backend::fsync(self, ctx, handle, mode)?;
        self.local_sync_receipt(handle)
    }

    fn release(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        self.release_file_handle(handle)?.map_or(Ok(()), Err)
    }

    fn opendir(&self, _: &RequestContext, inode: BackendInode) -> Result<DirectoryHandle> {
        let inode_id = self.inode_id(inode)?;
        let record = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        if record.kind != InodeKind::Directory {
            return Err(Error::from(std::io::Error::from(
                std::io::ErrorKind::NotADirectory,
            )));
        }
        let entries = self
            .meta
            .read_dir(&inode_id)?
            .into_iter()
            .map(|entry| self.directory_entry(entry))
            .collect::<Result<Vec<_>>>()?;
        let id = self.handles.next_id();
        self.handles
            .directories
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?
            .insert(
                id,
                DfsDirectoryHandle {
                    entries,
                    _gc_pin: self
                        .gc
                        .references
                        .as_ref()
                        .map(|references| references.pin(&inode_id, None))
                        .transpose()?,
                },
            );
        Ok(DirectoryHandle(id))
    }

    fn readdir(
        &self,
        _: &RequestContext,
        handle: DirectoryHandle,
        cookie: u64,
        max_entries: usize,
    ) -> Result<Vec<DirectoryEntry>> {
        let handles = self
            .handles
            .directories
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?;
        let directory = handles
            .get(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        slice_directory_entries(directory.entries.clone(), cookie, max_entries)
    }

    fn fsyncdir(&self, _: &RequestContext, handle: DirectoryHandle, _: SyncMode) -> Result<()> {
        let handles = self
            .handles
            .directories
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?;
        handles
            .get(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        Ok(())
    }

    fn releasedir(&self, _: &RequestContext, handle: DirectoryHandle) -> Result<()> {
        self.handles
            .directories
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?
            .remove(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        Ok(())
    }

    fn mkdir(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.mkdir(
            &self.identity.operation_id("mkdir"),
            &self.inode_id(parent)?,
            name.as_encoded_bytes(),
            InodeAttributes {
                mode: mode & !ctx.umask,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 2,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller_context(ctx),
        )?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn unlink(&self, ctx: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.refresh_inode_record(
            self.meta.unlink(
                &self.identity.operation_id("unlink"),
                &self.inode_id(parent)?,
                name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(())
    }

    fn rmdir(&self, ctx: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.refresh_inode_record(
            self.meta.rmdir(
                &self.identity.operation_id("rmdir"),
                &self.inode_id(parent)?,
                name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(())
    }

    fn rename(
        &self,
        ctx: &RequestContext,
        from_parent: BackendInode,
        from_name: &OsStr,
        to_parent: BackendInode,
        to_name: &OsStr,
        flags: RenameFlags,
    ) -> Result<()> {
        let mode = rename_mode(flags)?;
        let outcome = self.meta.rename(
            &self.identity.operation_id("rename"),
            &self.inode_id(from_parent)?,
            from_name.as_encoded_bytes(),
            &self.inode_id(to_parent)?,
            to_name.as_encoded_bytes(),
            mode,
            caller_context(ctx),
        )?;
        self.refresh_inode_record(outcome.inode, true, true)?;
        if let Some(replaced) = outcome.replaced_inode {
            self.refresh_inode_record(replaced, true, true)?;
        }
        Ok(())
    }

    fn symlink(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        target: &OsStr,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.symlink(SymlinkRequest {
            caller_id: self.identity.node_id.clone(),
            operation_id: self.identity.operation_id("symlink"),
            namespace_id: self.identity.namespace_id.clone(),
            parent_inode_id: self.inode_id(parent)?,
            name: name.as_encoded_bytes().to_vec(),
            target: target.as_encoded_bytes().to_vec(),
            attributes: InodeAttributes {
                mode: libc::S_IFLNK | 0o777,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller: caller_context(ctx),
        })?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn readlink(&self, _: &RequestContext, inode: BackendInode) -> Result<OsString> {
        let target = self.meta.read_link(ReadLinkRequest {
            namespace_id: self.identity.namespace_id.clone(),
            inode_id: self.inode_id(inode)?,
        })?;
        Ok(OsString::from_vec(target))
    }

    fn link(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        new_parent: BackendInode,
        new_name: &OsStr,
    ) -> Result<Entry> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        let linked = self.refresh_inode_record(
            self.meta.link(
                &self.identity.operation_id("link"),
                &inode_id,
                current.revision,
                &self.inode_id(new_parent)?,
                new_name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(Entry {
            inode: self.backend_inode(&linked.inode_id)?,
            attributes: self.visible_attributes(&linked)?,
        })
    }

    fn getxattr_is_fixed_local(&self, name: &OsStr) -> bool {
        name.as_encoded_bytes() == b"security.capability"
    }

    fn getxattr(&self, ctx: &RequestContext, inode: BackendInode, name: &OsStr) -> Result<Vec<u8>> {
        let inode_id = self.inode_id(inode)?;
        if name.as_encoded_bytes() == b"security.capability" {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "DFS currently supports only user.* xattrs",
            ));
        }
        self.meta.get_xattr(GetXattrRequest {
            caller: caller_context(ctx),
            inode_id,
            name: name.as_encoded_bytes().to_vec(),
        })
    }

    fn listxattr(&self, ctx: &RequestContext, inode: BackendInode) -> Result<Vec<u8>> {
        let names = self.meta.list_xattr(ListXattrRequest {
            caller: caller_context(ctx),
            inode_id: self.inode_id(inode)?,
        })?;
        let total = names.iter().map(|name| name.len() + 1).sum();
        let mut encoded = Vec::with_capacity(total);
        for name in names {
            encoded.extend_from_slice(&name);
            encoded.push(0);
        }
        Ok(encoded)
    }

    fn setxattr(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> Result<()> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        self.refresh_inode_record(
            self.meta.set_xattr(SetXattrRequest {
                caller_id: self.identity.node_id.clone(),
                operation_id: self.identity.operation_id("setxattr"),
                caller: caller_context(ctx),
                inode_id,
                expected_inode_revision: current.revision,
                name: name.as_encoded_bytes().to_vec(),
                value: value.to_vec(),
                mode: xattr_set_mode(flags)?,
            })?,
            true,
            true,
        )?;
        Ok(())
    }

    fn removexattr(&self, ctx: &RequestContext, inode: BackendInode, name: &OsStr) -> Result<()> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        self.refresh_inode_record(
            self.meta.remove_xattr(RemoveXattrRequest {
                caller_id: self.identity.node_id.clone(),
                operation_id: self.identity.operation_id("removexattr"),
                caller: caller_context(ctx),
                inode_id,
                expected_inode_revision: current.revision,
                name: name.as_encoded_bytes().to_vec(),
            })?,
            true,
            true,
        )?;
        Ok(())
    }
}
