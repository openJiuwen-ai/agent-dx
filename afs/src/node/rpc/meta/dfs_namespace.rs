//! The DfsMeta namespace implementation over GrpcDfsMeta.

use super::*;

#[cfg(feature = "dfs")]
impl crate::node::vfs::dfs::DfsMeta for GrpcDfsMeta {
    fn restore_file_chunk(
        &self,
        request: crate::dfs::RestoreFileChunk,
        timeout: Duration,
    ) -> afs_error::Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        crate::node::call_deadline::within(deadline, || {
            self.run(
                self.client()
                    .restore_file_chunk(afs_tracing::request_with_current_context(
                        afs_protocol::meta::RestoreDfsFileChunkRequest {
                            caller_id: request.caller_id,
                            operation_id: request.operation_id.0,
                            file_version_id: request.file_version_id.0,
                            layout_root_id: request.layout_root_id.0,
                            write_lease: Some(wire_dfs_write_lease(request.write_lease)),
                            receipt: Some(wire_chunk_receipt(request.receipt)),
                        },
                    )),
            )
        })?;
        Ok(())
    }

    fn lookup(
        &self,
        parent: &crate::dfs::InodeId,
        name: &[u8],
    ) -> afs_error::Result<Option<crate::dfs::InodeRecord>> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaLookup,
            || {
                let reply = self
                    .run(self.client().lookup(afs_protocol::meta::DfsLookupRequest {
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                        name: name.to_vec(),
                    }))?
                    .into_inner();
                if !reply.found {
                    return Ok(None);
                }
                required(reply.inode, "DfsLookup.inode")
                    .and_then(domain_dfs_inode)
                    .map(Some)
            },
        )
    }

    fn create(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        attributes: crate::dfs::InodeAttributes,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaCreate,
            || {
                let reply = self
                    .run(self.client().create(afs_protocol::meta::DfsCreateRequest {
                        caller_id: self.node_id.clone(),
                        operation_id: operation_id.0.clone(),
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                        name: name.to_vec(),
                        attributes: Some(wire_dfs_attributes(attributes)),
                        owner_session_id: self.session_id.clone(),
                        lease_seconds: crate::dfs::DFS_WRITE_LEASE_SECONDS,
                    }))?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "DfsCreate.inode")?)?,
                    domain_dfs_write_lease(required(reply.write_lease, "DfsCreate.write_lease")?),
                ))
            },
        )
    }

    fn mkdir(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        attributes: crate::dfs::InodeAttributes,
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().mkdir(afs_protocol::meta::DfsMkdirRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                attributes: Some(wire_dfs_attributes(attributes)),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsMkdir.inode").and_then(domain_dfs_inode)
    }

    fn read_dir_page(
        &self,
        parent: &crate::dfs::InodeId,
        after: &[u8],
        limit: u32,
    ) -> afs_error::Result<crate::dfs::DirPage> {
        let reply = self
            .run(
                self.client()
                    .read_dir(afs_protocol::meta::DfsReadDirRequest {
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                        after_name: after.to_vec(),
                        limit,
                    }),
            )?
            .into_inner();
        Ok(crate::dfs::DirPage {
            entries: reply
                .entries
                .into_iter()
                .map(domain_dfs_dir_entry)
                .collect::<afs_error::Result<_>>()?,
            eof: reply.eof,
        })
    }

    fn unlink(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().unlink(afs_protocol::meta::DfsUnlinkRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsUnlink.inode").and_then(domain_dfs_inode)
    }

    fn rmdir(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().rmdir(afs_protocol::meta::DfsRmdirRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsRmdir.inode").and_then(domain_dfs_inode)
    }

    fn rename(
        &self,
        operation_id: &crate::dfs::OperationId,
        old_parent: &crate::dfs::InodeId,
        old_name: &[u8],
        new_parent: &crate::dfs::InodeId,
        new_name: &[u8],
        mode: crate::dfs::RenameMode,
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::RenameOutcome> {
        let mode = match mode {
            crate::dfs::RenameMode::NoReplace => afs_protocol::meta::DfsRenameMode::NoReplace,
            crate::dfs::RenameMode::Replace => afs_protocol::meta::DfsRenameMode::Replace,
        };
        let reply = self
            .run(self.client().rename(afs_protocol::meta::DfsRenameRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                old_parent_inode_id: old_parent.0.clone(),
                old_name: old_name.to_vec(),
                new_parent_inode_id: new_parent.0.clone(),
                new_name: new_name.to_vec(),
                mode: mode as i32,
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        Ok(crate::dfs::RenameOutcome {
            inode: domain_dfs_inode(required(reply.inode, "DfsRename.inode")?)?,
            replaced_inode: reply.replaced_inode.map(domain_dfs_inode).transpose()?,
        })
    }

    fn link(
        &self,
        operation_id: &crate::dfs::OperationId,
        inode_id: &crate::dfs::InodeId,
        expected_inode_revision: u64,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().link(afs_protocol::meta::DfsLinkRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                existing_inode_id: inode_id.0.clone(),
                expected_inode_revision,
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsLink.inode").and_then(domain_dfs_inode)
    }

    fn symlink(
        &self,
        request: crate::dfs::SymlinkRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(
                self.client()
                    .symlink(afs_protocol::meta::DfsSymlinkRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        namespace_id: request.namespace_id.0,
                        parent_inode_id: request.parent_inode_id.0,
                        name: request.name,
                        target: request.target,
                        attributes: Some(wire_dfs_attributes(request.attributes)),
                        caller: Some(wire_dfs_caller_context(request.caller)),
                    }),
            )?
            .into_inner();
        required(reply.inode, "DfsSymlink.inode").and_then(domain_dfs_inode)
    }

    fn mknod(
        &self,
        request: crate::dfs::MknodRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().mknod(afs_protocol::meta::DfsMknodRequest {
                caller_id: request.caller_id,
                operation_id: request.operation_id.0,
                namespace_id: request.namespace_id.0,
                parent_inode_id: request.parent_inode_id.0,
                name: request.name,
                special_node: Some(wire_dfs_special_node(request.kind)),
                attributes: Some(wire_dfs_attributes(request.attributes)),
                caller: Some(wire_dfs_caller_context(request.caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsMknod.inode").and_then(domain_dfs_inode)
    }

    fn read_link(&self, request: crate::dfs::ReadLinkRequest) -> afs_error::Result<Vec<u8>> {
        let reply = self
            .run(
                self.client()
                    .read_link(afs_protocol::meta::DfsReadLinkRequest {
                        namespace_id: request.namespace_id.0,
                        inode_id: request.inode_id.0,
                    }),
            )?
            .into_inner();
        Ok(reply.target)
    }

    fn open_write(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaOpenWrite,
            || {
                let reply = self
                    .run(
                        self.client()
                            .open_write(afs_protocol::meta::OpenDfsWriteRequest {
                                caller_id: self.node_id.clone(),
                                owner_session_id: self.session_id.clone(),
                                operation_id: self.request_id(),
                                inode_id: inode_id.0.clone(),
                                lease_seconds: crate::dfs::DFS_WRITE_LEASE_SECONDS,
                            }),
                    )?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "OpenWrite.inode")?)?,
                    domain_dfs_write_lease(required(reply.write_lease, "OpenWrite.write_lease")?),
                ))
            },
        )
    }

    fn resolve_lock_authority(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        let reply = self
            .run(self.client().resolve_lock_authority(
                afs_protocol::meta::ResolveDfsLockAuthorityRequest {
                    caller_id: self.node_id.clone(),
                    owner_session_id: self.session_id.clone(),
                    operation_id: self.request_id(),
                    inode_id: inode_id.0.clone(),
                    lease_seconds: crate::dfs::DFS_WRITE_LEASE_SECONDS,
                },
            ))?
            .into_inner();
        Ok((
            domain_dfs_inode(required(reply.inode, "ResolveLockAuthority.inode")?)?,
            domain_dfs_write_lease(required(
                reply.write_lease,
                "ResolveLockAuthority.write_lease",
            )?),
        ))
    }

    fn resolve_write_authority(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        crate::node::bridge::block_on(
            &self.runtime,
            self.resolve_write_authority_async(inode_id.clone()),
        )
    }

    fn resolve_write_authority_async(
        &self,
        inode_id: crate::dfs::InodeId,
    ) -> crate::node::read::admission::ReadFuture<
        '_,
        (crate::dfs::InodeRecord, crate::dfs::WriteLease),
    > {
        self.resolve_dfs_write_authority(inode_id, crate::dfs::DFS_WRITE_LEASE_SECONDS)
    }

    fn observe_write_authority(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        crate::node::bridge::block_on(
            &self.runtime,
            self.resolve_dfs_write_authority(inode_id.clone(), 0),
        )
    }

    fn renew_write_lease(
        &self,
        lease: crate::dfs::WriteLease,
    ) -> afs_error::Result<crate::dfs::WriteLease> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaRenewWriteLease,
            || {
                let reply = self
                    .run(self.client().renew_write_lease(
                        afs_protocol::meta::RenewDfsWriteLeaseRequest {
                            caller_id: self.node_id.clone(),
                            owner_session_id: self.session_id.clone(),
                            operation_id: self.request_id(),
                            current_lease: Some(wire_dfs_write_lease(lease)),
                            lease_seconds: crate::dfs::DFS_WRITE_LEASE_SECONDS,
                        },
                    ))?
                    .into_inner();
                Ok(domain_dfs_write_lease(required(
                    reply.write_lease,
                    "RenewWriteLease.write_lease",
                )?))
            },
        )
    }

    fn get_inode(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaGetInode,
            || {
                self.record_read_rpc("get_inode");
                let reply = self
                    .run(
                        self.client()
                            .get_inode(afs_tracing::request_with_current_context(
                                afs_protocol::meta::GetDfsInodeRequest {
                                    inode_id: inode_id.0.clone(),
                                },
                            )),
                    )?
                    .into_inner();
                required(reply.inode, "GetDfsInode.inode").and_then(domain_dfs_inode)
            },
        )
    }

    fn get_file_version_async(
        &self,
        version_id: crate::dfs::FileVersionId,
    ) -> crate::node::read::admission::ReadFuture<
        '_,
        (crate::dfs::FileVersion, crate::dfs::LayoutRoot),
    > {
        Box::pin(async move {
            self.record_read_rpc("get_file_version");
            let mut client = self.client();
            let reply = tokio::time::timeout(
                self.timeout,
                client.get_file_version(afs_tracing::request_with_current_context(
                    afs_protocol::meta::GetFileVersionRequest {
                        version_id: version_id.0,
                    },
                )),
            )
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "async layout lookup deadline exceeded",
                )
            })?
            .map_err(afs_transport::grpc::error_status::status_to_error)?
            .into_inner();
            Ok((
                domain_dfs_version(required(reply.version, "GetFileVersion.version")?),
                domain_dfs_layout(required(reply.layout, "GetFileVersion.layout")?),
            ))
        })
    }

    fn get_file_version(
        &self,
        version_id: &crate::dfs::FileVersionId,
    ) -> afs_error::Result<(crate::dfs::FileVersion, crate::dfs::LayoutRoot)> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaGetFileVersion,
            || {
                self.record_read_rpc("get_file_version");
                let reply = self
                    .run(self.client().get_file_version(
                        afs_tracing::request_with_current_context(
                            afs_protocol::meta::GetFileVersionRequest {
                                version_id: version_id.0.clone(),
                            },
                        ),
                    ))?
                    .into_inner();
                Ok((
                    domain_dfs_version(required(reply.version, "GetFileVersion.version")?),
                    domain_dfs_layout(required(reply.layout, "GetFileVersion.layout")?),
                ))
            },
        )
    }

    fn sync_inode_metadata(
        &self,
        sync: crate::dfs::SyncInodeMetadata,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaSyncInodeMetadata,
            || {
                let reply = self
                    .run(
                        self.client().sync_inode_metadata(
                            afs_protocol::meta::SyncDfsInodeMetadataRequest {
                                caller_id: self.node_id.clone(),
                                operation_id: sync.operation_id.0,
                                inode_id: sync.inode_id.0,
                                write_lease: Some(wire_dfs_write_lease(sync.write_lease)),
                                expected_inode_revision: sync.expected_inode_revision,
                                expected_head_version_id: sync
                                    .expected_head_version
                                    .map_or_else(String::new, |id| id.0),
                                metadata_delta: Some(wire_dfs_metadata_delta(sync.metadata_delta)),
                            },
                        ),
                    )?
                    .into_inner();
                required(reply.inode, "SyncInodeMetadata.inode").and_then(domain_dfs_inode)
            },
        )
    }

    fn set_inode_attributes(
        &self,
        request: crate::dfs::SetInodeAttrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().set_inode_attributes(
                afs_protocol::meta::DfsSetInodeAttributesRequest {
                    caller_id: request.caller_id,
                    operation_id: request.operation_id.0,
                    caller: Some(wire_dfs_caller_context(request.caller)),
                    inode_id: request.inode_id.0,
                    expected_inode_revision: request.expected_inode_revision,
                    update: Some(wire_dfs_attr_update(request.update)),
                },
            ))?
            .into_inner();
        required(reply.inode, "SetInodeAttributes.inode").and_then(domain_dfs_inode)
    }

    fn get_xattr(&self, request: crate::dfs::GetXattrRequest) -> afs_error::Result<Vec<u8>> {
        let reply = self
            .run(
                self.client()
                    .get_xattr(afs_protocol::meta::DfsGetXattrRequest {
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        name: request.name,
                    }),
            )?
            .into_inner();
        Ok(reply.value)
    }

    fn list_xattr(&self, request: crate::dfs::ListXattrRequest) -> afs_error::Result<Vec<Vec<u8>>> {
        let reply = self
            .run(
                self.client()
                    .list_xattr(afs_protocol::meta::DfsListXattrRequest {
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                    }),
            )?
            .into_inner();
        Ok(reply.names)
    }

    fn bind_lazy_source(
        &self,
        request: crate::dfs::BindLazySourceRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply =
            self.run(self.client().bind_lazy_source(
                afs_protocol::meta::DfsBindLazySourceRequest {
                    caller_id: request.caller_id,
                    caller_session_id: request.caller_session_id,
                    operation_id: request.operation_id.0,
                    inode_id: request.inode_id.0,
                    expected_inode_revision: request.expected_inode_revision,
                    length: request.source.length,
                    kind: request.source.binding.kind,
                    binding: request.source.binding.payload,
                },
            ))?
            .into_inner();
        required(reply.inode, "BindLazySource.inode").and_then(domain_dfs_inode)
    }

    fn set_xattr(
        &self,
        request: crate::dfs::SetXattrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let mode = match request.mode {
            crate::dfs::XattrSetMode::Upsert => afs_protocol::meta::DfsXattrSetMode::Upsert,
            crate::dfs::XattrSetMode::Create => afs_protocol::meta::DfsXattrSetMode::Create,
            crate::dfs::XattrSetMode::Replace => afs_protocol::meta::DfsXattrSetMode::Replace,
        };
        let reply = self
            .run(
                self.client()
                    .set_xattr(afs_protocol::meta::DfsSetXattrRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        expected_inode_revision: request.expected_inode_revision,
                        name: request.name,
                        value: request.value,
                        mode: mode as i32,
                    }),
            )?
            .into_inner();
        required(reply.inode, "SetXattr.inode").and_then(domain_dfs_inode)
    }

    fn remove_xattr(
        &self,
        request: crate::dfs::RemoveXattrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(
                self.client()
                    .remove_xattr(afs_protocol::meta::DfsRemoveXattrRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        expected_inode_revision: request.expected_inode_revision,
                        name: request.name,
                    }),
            )?
            .into_inner();
        required(reply.inode, "RemoveXattr.inode").and_then(domain_dfs_inode)
    }

    fn lookup_node_location(
        &self,
        node_id: &str,
    ) -> afs_error::Result<Option<crate::node::vfs::dfs::DfsNodeLocation>> {
        self.read_diagnostic(
            crate::node::metrics::read_timing::ReadDiagnosticStage::MetaLookupNodeLocation,
            || {
                let reply =
                    self.run(self.root_client().lookup_node(
                        afs_protocol::meta::LookupNodeRequest {
                            request_id: self.request_id(),
                            node_id: node_id.to_owned(),
                        },
                    ))?
                    .into_inner();
                if !reply.found {
                    return Ok(None);
                }
                let node = required(reply.node, "LookupNode.node")?;
                let endpoint = required(node.endpoint, "LookupNode.node.endpoint")?;
                Ok(Some(crate::node::vfs::dfs::DfsNodeLocation {
                    node_id: node.node_id,
                    node_epoch: reply.lease_epoch,
                    data_endpoint: endpoint.data_addr,
                }))
            },
        )
    }

    fn current_node_session(&self, node_id: &str) -> afs_error::Result<Option<String>> {
        crate::node::bridge::block_on(
            &self.runtime,
            self.current_node_session_async(node_id.to_owned()),
        )
    }

    fn current_node_session_async(
        &self,
        node_id: String,
    ) -> crate::node::read::admission::ReadFuture<'_, Option<String>> {
        Box::pin(async move {
            let reply =
                self.run_async(self.root_client().lookup_node(
                    afs_protocol::meta::LookupNodeRequest {
                        request_id: self.request_id(),
                        node_id,
                    },
                ))
                .await?
                .into_inner();
            if !reply.found {
                return Ok(None);
            }
            let node = required(reply.node, "LookupNode.node")?;
            if node.session_id.is_empty() {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "LookupNode returned an empty process session",
                ));
            }
            Ok(Some(node.session_id))
        })
    }

    fn commit_file_version(
        &self,
        commit: crate::dfs::CommitFileVersion,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(
                self.client()
                    .commit_file_version(afs_tracing::request_with_current_context(
                        afs_protocol::meta::CommitFileVersionRequest {
                            gc_fence: commit.gc_fence.map(crate::dfs::GcFence::wire),
                            caller_id: self.node_id.clone(),
                            operation_id: commit.operation_id.0,
                            inode_id: commit.inode_id.0,
                            write_lease: Some(wire_dfs_write_lease(commit.write_lease)),
                            expected_inode_revision: commit.expected_inode_revision,
                            expected_head_version_id: commit
                                .expected_head_version
                                .map_or_else(String::new, |id| id.0),
                            version: Some(wire_dfs_version(commit.file_version)),
                            layout: Some(wire_dfs_layout(commit.layout_root)),
                            chunk_receipts: commit
                                .chunk_receipts
                                .into_iter()
                                .map(wire_chunk_receipt)
                                .collect(),
                            metadata_delta: Some(wire_dfs_metadata_delta(commit.metadata_delta)),
                            clone_source: commit.clone_source.map(|source| {
                                afs_protocol::meta::DfsCloneFileRange {
                                    version_id: source.version.0,
                                    source_offset: source.source_offset,
                                    destination_offset: source.destination_offset,
                                    length: source.length,
                                }
                            }),
                        },
                    )),
            )?
            .into_inner();
        required(reply.inode, "CommitFileVersion.inode").and_then(domain_dfs_inode)
    }
}

#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    fn resolve_dfs_write_authority(
        &self,
        inode_id: crate::dfs::InodeId,
        lease_seconds: u64,
    ) -> crate::node::read::admission::ReadFuture<
        '_,
        (crate::dfs::InodeRecord, crate::dfs::WriteLease),
    > {
        Box::pin(async move {
            let (diagnostic, _coarse) = self.begin_read_diagnostic(
                crate::node::metrics::read_timing::ReadDiagnosticStage::MetaResolveWriteAuthority,
            );
            let result = async {
                let reply = self
                    .run_async(self.client().resolve_write_authority(
                        afs_protocol::meta::ResolveDfsWriteAuthorityRequest {
                            caller_id: self.node_id.clone(),
                            owner_session_id: self.session_id.clone(),
                            operation_id: self.request_id(),
                            inode_id: inode_id.0,
                            lease_seconds,
                        },
                    ))
                    .await?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "ResolveWriteAuthority.inode")?)?,
                    domain_dfs_write_lease(required(
                        reply.write_lease,
                        "ResolveWriteAuthority.write_lease",
                    )?),
                ))
            }
            .await;
            if let Some(diagnostic) = diagnostic {
                diagnostic.finish(result.is_ok());
            }
            result
        })
    }
}
