//! The DfsMetaService handlers.

use super::*;

#[tonic::async_trait]
impl DfsMetaService for DfsMetaRpc {
    async fn update_gc_reference(
        &self,
        request: Request<afs_protocol::meta::UpdateDfsGcReferenceRequest>,
    ) -> Result<Response<afs_protocol::meta::UpdateDfsGcReferenceReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.node_id)?;
        if !request.chunks.is_empty() {
            // Per-chunk publication references were removed (R33); a write
            // generation reference protects its chunks instead.
            return Err(invalid("GC references no longer carry chunks"));
        }
        if request.capture_gc_fence && (!request.publication || request.inode_id.is_empty()) {
            return Err(Status::invalid_argument(
                "GC fence requires initial publication retain",
            ));
        }
        if request.capture_gc_fence && !request.reference_id.starts_with("write:") {
            return Err(invalid("GC fence capture requires a write: reference"));
        }
        let observation = if request.capture_gc_fence {
            "dfs_gc_capture"
        } else {
            "dfs_gc_retain"
        };
        let capture = request.capture_gc_fence;
        dfs_service(&self.0)?
            .update_gc_reference(crate::dfs::UpdateGcReference {
                node_id: request.node_id,
                session_id: request.session_id,
                reference_id: request.reference_id,
                inode_id: optional_id(request.inode_id).map(crate::dfs::InodeId::new),
                version_id: optional_id(request.version_id).map(crate::dfs::FileVersionId::new),
                publication: request.publication,
                admission_deadline_unix_ms: request.admission_deadline_unix_ms,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        // No payload is admitted by the Node until this response is received.
        let gc_fence = capture.then(|| crate::dfs::GcFence::WriteGeneration.wire());
        self.0.observability.record("meta", observation, true);
        Ok(Response::new(
            afs_protocol::meta::UpdateDfsGcReferenceReply { gc_fence },
        ))
    }
    async fn release_gc_references(
        &self,
        request: Request<afs_protocol::meta::ReleaseDfsGcReferencesRequest>,
    ) -> Result<Response<afs_protocol::meta::ReleaseDfsGcReferencesReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.node_id)?;
        dfs_service(&self.0)?
            .release_gc_references(request.node_id, request.session_id, request.reference_ids)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        self.0.observability.record("meta", "dfs_gc_release", true);
        Ok(Response::new(
            afs_protocol::meta::ReleaseDfsGcReferencesReply {},
        ))
    }
    async fn collect_garbage(
        &self,
        request: Request<afs_protocol::meta::CollectDfsGarbageRequest>,
    ) -> Result<Response<afs_protocol::meta::CollectDfsGarbageReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.node_id)?;
        let node = request.node_id.clone();
        let list_external_inventory = dfs_service(&self.0)?
            .lists_external_inventory(&node)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let state = dfs_service(&self.0)?
            .collect_garbage(
                request.node_id,
                request.session_id,
                request.completed_generation,
                request
                    .inventory
                    .into_iter()
                    .map(domain_chunk_object)
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        self.0.observability.record("meta", "dfs_gc_collect", true);
        Ok(Response::new(afs_protocol::meta::CollectDfsGarbageReply {
            generation: state.generation,
            pending_for_node: state.waiting_nodes.contains(&node),
            chunk_ids: state.chunks.into_iter().map(|id| id.0).collect(),
            list_external_inventory,
        }))
    }

    async fn lookup(
        &self,
        request: Request<DfsLookupRequest>,
    ) -> Result<Response<DfsLookupReply>, Status> {
        // Read RPCs require a trusted-node certificate, not a per-caller binding.
        authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let inode = dfs_service(&self.0)?
            .lookup(
                crate::dfs::NamespaceId::new(request.namespace_id),
                crate::dfs::InodeId::new(request.parent_inode_id),
                request.name,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsLookupReply {
            found: inode.is_some(),
            inode: inode.map(wire_dfs_inode),
        }))
    }

    async fn create(
        &self,
        request: Request<DfsCreateRequest>,
    ) -> Result<Response<DfsCreateReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS create requires attributes"))?;
        let (inode, lease) = dfs_service(&self.0)?
            .create(CreateFileRequest {
                caller_id: request.caller_id,
                owner_session_id: request.owner_session_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                attributes: domain_dfs_attributes(attributes),
                lease_seconds: request.lease_seconds,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsCreateReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn mkdir(
        &self,
        request: Request<DfsMkdirRequest>,
    ) -> Result<Response<DfsMkdirReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS mkdir requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS mkdir requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .mkdir(crate::dfs::MkdirRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                attributes: domain_dfs_attributes(attributes),
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsMkdirReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn read_dir(
        &self,
        request: Request<DfsReadDirRequest>,
    ) -> Result<Response<DfsReadDirReply>, Status> {
        authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let page = dfs_service(&self.0)?
            .read_dir_page(
                crate::dfs::NamespaceId::new(request.namespace_id),
                crate::dfs::InodeId::new(request.parent_inode_id),
                request.after_name,
                usize::try_from(request.limit).unwrap_or(usize::MAX),
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsReadDirReply {
            eof: page.eof,
            entries: page.entries.into_iter().map(wire_dfs_dir_entry).collect(),
        }))
    }

    async fn unlink(
        &self,
        request: Request<DfsUnlinkRequest>,
    ) -> Result<Response<DfsUnlinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS unlink requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .unlink(crate::dfs::UnlinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsUnlinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn rmdir(
        &self,
        request: Request<DfsRmdirRequest>,
    ) -> Result<Response<DfsRmdirReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS rmdir requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .rmdir(crate::dfs::RmdirRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRmdirReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn rename(
        &self,
        request: Request<DfsRenameRequest>,
    ) -> Result<Response<DfsRenameReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.old_parent_inode_id, "old_parent_inode_id")?;
        require_text(&request.new_parent_inode_id, "new_parent_inode_id")?;
        let mode = match PbDfsRenameMode::try_from(request.mode) {
            Ok(PbDfsRenameMode::NoReplace) => crate::dfs::RenameMode::NoReplace,
            Ok(PbDfsRenameMode::Replace) => crate::dfs::RenameMode::Replace,
            Ok(PbDfsRenameMode::Unspecified) | Err(_) => {
                return Err(invalid("DFS rename mode is required"));
            }
        };
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS rename requires caller context"))?;
        let outcome = dfs_service(&self.0)?
            .rename(crate::dfs::RenameRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                old_parent_inode_id: crate::dfs::InodeId::new(request.old_parent_inode_id),
                old_name: request.old_name,
                new_parent_inode_id: crate::dfs::InodeId::new(request.new_parent_inode_id),
                new_name: request.new_name,
                mode,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRenameReply {
            inode: Some(wire_dfs_inode(outcome.inode)),
            replaced: outcome.replaced_inode.is_some(),
            replaced_inode: outcome.replaced_inode.map(wire_dfs_inode),
        }))
    }

    async fn link(
        &self,
        request: Request<DfsLinkRequest>,
    ) -> Result<Response<DfsLinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.existing_inode_id, "existing_inode_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS link requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .link(crate::dfs::LinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                existing_inode_id: crate::dfs::InodeId::new(request.existing_inode_id),
                expected_inode_revision: request.expected_inode_revision,
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsLinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn symlink(
        &self,
        request: Request<DfsSymlinkRequest>,
    ) -> Result<Response<DfsSymlinkReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS symlink requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS symlink requires caller context"))?;
        let inode = dfs_service(&self.0)?
            .symlink(crate::dfs::SymlinkRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
                name: request.name,
                target: request.target,
                attributes: domain_dfs_attributes(attributes),
                caller: domain_caller_context(caller),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSymlinkReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn mknod(
        &self,
        request: Request<DfsMknodRequest>,
    ) -> Result<Response<DfsMknodReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.parent_inode_id, "parent_inode_id")?;
        let attributes = request
            .attributes
            .ok_or_else(|| invalid("DFS mknod requires attributes"))?;
        let caller = request
            .caller
            .ok_or_else(|| invalid("DFS mknod requires caller context"))?;
        let special_node = request
            .special_node
            .ok_or_else(|| invalid("DFS mknod requires special_node"))?;
        let request = crate::dfs::MknodRequest {
            caller_id: request.caller_id,
            operation_id: crate::dfs::OperationId::new(request.operation_id),
            namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
            parent_inode_id: crate::dfs::InodeId::new(request.parent_inode_id),
            name: request.name,
            kind: domain_dfs_special_node(special_node)?,
            attributes: domain_dfs_attributes(attributes),
            caller: domain_caller_context(caller),
        };
        let inode = dfs_service(&self.0)?
            .mknod(request)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsMknodReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn read_link(
        &self,
        request: Request<DfsReadLinkRequest>,
    ) -> Result<Response<DfsReadLinkReply>, Status> {
        authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let target = dfs_service(&self.0)?
            .read_link(crate::dfs::ReadLinkRequest {
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsReadLinkReply { target }))
    }

    async fn set_inode_attributes(
        &self,
        request: Request<DfsSetInodeAttributesRequest>,
    ) -> Result<Response<DfsSetInodeAttributesReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .set_inode_attributes(crate::dfs::SetInodeAttrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS setattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                update: domain_inode_attr_update(
                    request
                        .update
                        .ok_or_else(|| invalid("DFS setattr requires update"))?,
                ),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSetInodeAttributesReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn get_xattr(
        &self,
        request: Request<DfsGetXattrRequest>,
    ) -> Result<Response<DfsGetXattrReply>, Status> {
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.inode_id, "inode_id")?;
        let result = dfs_service(&self.0)?
            .get_xattr(crate::dfs::GetXattrRequest {
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS getxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                name: request.name,
            })
            .await;
        self.0
            .observability
            .record("meta", "dfs_get_xattr", result.is_ok());
        let value = result.map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsGetXattrReply { value }))
    }

    async fn list_xattr(
        &self,
        request: Request<DfsListXattrRequest>,
    ) -> Result<Response<DfsListXattrReply>, Status> {
        let _authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        require_text(&request.inode_id, "inode_id")?;
        let names = dfs_service(&self.0)?
            .list_xattr(crate::dfs::ListXattrRequest {
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS listxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsListXattrReply { names }))
    }

    async fn set_xattr(
        &self,
        request: Request<DfsSetXattrRequest>,
    ) -> Result<Response<DfsSetXattrReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let mode = match PbDfsXattrSetMode::try_from(request.mode) {
            Ok(PbDfsXattrSetMode::Upsert) => crate::dfs::XattrSetMode::Upsert,
            Ok(PbDfsXattrSetMode::Create) => crate::dfs::XattrSetMode::Create,
            Ok(PbDfsXattrSetMode::Replace) => crate::dfs::XattrSetMode::Replace,
            Ok(PbDfsXattrSetMode::Unspecified) | Err(_) => {
                return Err(invalid("DFS setxattr mode is required"));
            }
        };
        let inode = dfs_service(&self.0)?
            .set_xattr(crate::dfs::SetXattrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS setxattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                name: request.name,
                value: request.value,
                mode,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsSetXattrReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn remove_xattr(
        &self,
        request: Request<DfsRemoveXattrRequest>,
    ) -> Result<Response<DfsRemoveXattrReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .remove_xattr(crate::dfs::RemoveXattrRequest {
                caller_id: request.caller_id,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                caller: domain_caller_context(
                    request
                        .caller
                        .ok_or_else(|| invalid("DFS removexattr requires caller context"))?,
                ),
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                expected_inode_revision: request.expected_inode_revision,
                name: request.name,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsRemoveXattrReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn open_write(
        &self,
        request: Request<OpenDfsWriteRequest>,
    ) -> Result<Response<OpenDfsWriteReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .open_write(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(OpenDfsWriteReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn resolve_lock_authority(
        &self,
        request: Request<ResolveDfsLockAuthorityRequest>,
    ) -> Result<Response<ResolveDfsLockAuthorityReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .resolve_lock_authority(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ResolveDfsLockAuthorityReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn resolve_write_authority(
        &self,
        request: Request<ResolveDfsWriteAuthorityRequest>,
    ) -> Result<Response<ResolveDfsWriteAuthorityReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let (inode, lease) = dfs_service(&self.0)?
            .resolve_write_authority(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                crate::dfs::InodeId::new(request.inode_id),
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ResolveDfsWriteAuthorityReply {
            inode: Some(wire_dfs_inode(inode)),
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn renew_write_lease(
        &self,
        request: Request<RenewDfsWriteLeaseRequest>,
    ) -> Result<Response<DfsWriteLeaseReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.owner_session_id, "owner_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let current = domain_dfs_write_lease(
            request
                .current_lease
                .ok_or_else(|| invalid("DFS renew requires current lease"))?,
        );
        let lease = dfs_service(&self.0)?
            .renew_write_lease(
                request.caller_id,
                request.owner_session_id,
                crate::dfs::OperationId::new(request.operation_id),
                current,
                request.lease_seconds,
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(DfsWriteLeaseReply {
            write_lease: Some(wire_dfs_write_lease(lease)),
        }))
    }

    async fn get_inode(
        &self,
        request: Request<GetDfsInodeRequest>,
    ) -> Result<Response<GetDfsInodeReply>, Status> {
        authenticated_node_id(&self.0, &request)?;
        require_text(&request.get_ref().inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .get_inode(crate::dfs::InodeId::new(request.into_inner().inode_id))
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(GetDfsInodeReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn get_file_version(
        &self,
        request: Request<GetFileVersionRequest>,
    ) -> Result<Response<GetFileVersionReply>, Status> {
        authenticated_node_id(&self.0, &request)?;
        require_text(&request.get_ref().version_id, "version_id")?;
        let (version, layout) = dfs_service(&self.0)?
            .get_shared_file_version(crate::dfs::FileVersionId::new(
                request.into_inner().version_id,
            ))
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(GetFileVersionReply {
            version: Some(wire_dfs_version(version)),
            layout: Some(wire_dfs_layout(&layout)),
        }))
    }

    async fn get_placement_snapshot(
        &self,
        request: Request<GetDfsPlacementSnapshotRequest>,
    ) -> Result<Response<GetDfsPlacementSnapshotReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        let snapshot = dfs_service(&self.0)?
            .placement_snapshot(request.caller_id)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        if snapshot.revision < request.minimum_revision {
            return Err(invalid("placement snapshot is older than minimum_revision"));
        }
        Ok(Response::new(GetDfsPlacementSnapshotReply {
            snapshot: Some(wire_placement_snapshot(snapshot)),
        }))
    }

    async fn validate_replica_write(
        &self,
        request: Request<ValidateDfsReplicaWriteRequest>,
    ) -> Result<Response<ValidateDfsReplicaWriteReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.requester_node_id)?;
        let grant = dfs_service(&self.0)?
            .validate_replica_write(domain_validate_replica_write(request)?)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(ValidateDfsReplicaWriteReply {
            grant: Some(wire_replica_write_grant(grant)),
        }))
    }

    async fn claim_replication_task(
        &self,
        request: Request<afs_protocol::meta::ClaimDfsReplicationTaskRequest>,
    ) -> Result<Response<afs_protocol::meta::ClaimDfsReplicationTaskReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let claim = dfs_service(&self.0)?
            .claim_replication_task(crate::dfs::ClaimReplicationTask {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                lease_seconds: request.lease_seconds,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ClaimDfsReplicationTaskReply {
                claimed: claim.is_some(),
                claim: claim.map(wire_replication_claim),
            },
        ))
    }

    async fn report_replication_task(
        &self,
        request: Request<afs_protocol::meta::ReportDfsReplicationTaskRequest>,
    ) -> Result<Response<afs_protocol::meta::ReportDfsReplicationTaskReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        let durable_acks = request
            .durable_acks
            .into_iter()
            .map(domain_replica_ack)
            .collect::<Result<Vec<_>, Status>>()?;
        let task = dfs_service(&self.0)?
            .report_replication_task(crate::dfs::ReportReplicationTask {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                claim: domain_replication_claim(
                    request
                        .claim
                        .ok_or_else(|| invalid("DFS replication report requires claim"))?,
                )?,
                durable_acks,
                error: request.error,
                source_invalid: request.source_invalid,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ReportDfsReplicationTaskReply {
                task: Some(wire_replication_task(task)),
            },
        ))
    }

    async fn report_chunk_corruption(
        &self,
        request: Request<afs_protocol::meta::ReportDfsChunkCorruptionRequest>,
    ) -> Result<Response<afs_protocol::meta::ReportDfsChunkCorruptionReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.caller_session_id, "caller_session_id")?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.chunk_id, "chunk_id")?;
        require_text(&request.device_id, "device_id")?;
        dfs_service(&self.0)?
            .report_chunk_corruption(crate::dfs::ReportChunkCorruption {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: crate::dfs::OperationId::new(request.operation_id),
                chunk_id: crate::dfs::ChunkId::new(request.chunk_id),
                device_id: request.device_id,
                device_epoch: request.device_epoch,
                catalog_revision: request.catalog_revision,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ReportDfsChunkCorruptionReply {},
        ))
    }

    async fn validate_read_grants(
        &self,
        request: Request<afs_protocol::meta::ValidateDfsReadGrantsRequest>,
    ) -> Result<Response<afs_protocol::meta::ValidateDfsReadGrantsReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.receiver_node_id)?;
        let validations = request
            .validations
            .into_iter()
            .map(|validation| {
                let grant = validation
                    .grant
                    .ok_or_else(|| invalid("read validation grant is missing"))?;
                Ok(crate::dfs::DfsReadValidation {
                    grant: crate::dfs::DfsReadGrant {
                        namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id),
                        file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id),
                        layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id),
                        caller_node_id: grant.caller_node_id,
                        caller_node_epoch: grant.caller_node_epoch,
                        expires_at_unix_ms: grant.expires_at_unix_ms,
                        fence: grant.fence,
                        token: grant.token,
                    },
                    chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                    copy_id: crate::dfs::CopyId::new(validation.copy_id),
                    chunk_offset: validation.chunk_offset,
                    length: validation.length,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let authorized = dfs_service(&self.0)?
            .validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                receiver_node_id: request.receiver_node_id,
                receiver_node_epoch: request.receiver_node_epoch,
                peer_node_id: request.peer_node_id,
                validations,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ValidateDfsReadGrantsReply {
                authorizations: authorized
                    .into_iter()
                    .map(|entry| {
                        let validation = entry.validation;
                        afs_protocol::meta::DfsAuthorizedRead {
                            validation: Some(afs_protocol::meta::DfsReadValidation {
                                grant: Some(wire_read_grant(validation.grant)),
                                chunk_id: validation.chunk_id.0,
                                copy_id: validation.copy_id.0,
                                chunk_offset: validation.chunk_offset,
                                length: validation.length,
                            }),
                            allowed_ranges: entry
                                .allowed_ranges
                                .into_iter()
                                .map(|(offset, length)| {
                                    afs_protocol::meta::dfs_authorized_read::Range {
                                        offset,
                                        length,
                                    }
                                })
                                .collect(),
                            expires_at_unix_ms: entry.expires_at_unix_ms,
                        }
                    })
                    .collect(),
            },
        ))
    }

    async fn validate_external_read_grants(
        &self,
        request: Request<afs_protocol::meta::ValidateExternalReadGrantsRequest>,
    ) -> Result<Response<afs_protocol::meta::ValidateExternalReadGrantsReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        let validations = request
            .validations
            .into_iter()
            .map(domain_read_validation)
            .collect::<Result<Vec<_>, Status>>()?;
        let authorizations = dfs_service(&self.0)?
            .validate_external_read_grants(request.caller_id, validations)
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::ValidateExternalReadGrantsReply {
                authorizations: authorizations
                    .into_iter()
                    .map(|entry| afs_protocol::meta::DfsExternalAuthorizedRead {
                        authorized: Some(wire_authorized_read(entry.authorized)),
                        chunk: Some(wire_chunk_object(entry.chunk)),
                        location: Some(wire_copy_location(entry.location)),
                    })
                    .collect(),
            },
        ))
    }

    async fn authorize_current_external_read(
        &self,
        request: Request<afs_protocol::meta::AuthorizeCurrentExternalReadRequest>,
    ) -> Result<Response<afs_protocol::meta::AuthorizeCurrentExternalReadReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        for (value, field) in [
            (&request.caller_id, "caller_id"),
            (&request.inode_id, "inode_id"),
            (&request.namespace_id, "namespace_id"),
            (
                &request.expected_head_version_id,
                "expected_head_version_id",
            ),
            (&request.layout_root_id, "layout_root_id"),
        ] {
            require_text(value, field)?;
        }
        let validations = request
            .validations
            .into_iter()
            .map(domain_read_validation)
            .collect::<Result<Vec<_>, Status>>()?;
        let missing_sources = request
            .missing_sources
            .into_iter()
            .map(domain_missing_read_source)
            .collect::<Result<Vec<_>, Status>>()?;
        let reply = dfs_service(&self.0)?
            .authorize_current_external_read(crate::dfs::AuthorizeCurrentExternalRead {
                caller_id: request.caller_id,
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                expected_head_version: crate::dfs::FileVersionId::new(
                    request.expected_head_version_id,
                ),
                layout_root_id: crate::dfs::LayoutRootId::new(request.layout_root_id),
                validations,
                missing_sources,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        let (inode, head_changed, authorizations) = match reply {
            crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                inode,
                authorizations,
            } => (inode, false, authorizations),
            crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode) => {
                (inode, true, Vec::new())
            }
        };
        Ok(Response::new(
            afs_protocol::meta::AuthorizeCurrentExternalReadReply {
                inode: Some(wire_dfs_inode(inode)),
                head_changed,
                authorizations: authorizations
                    .into_iter()
                    .map(|entry| afs_protocol::meta::DfsExternalAuthorizedRead {
                        authorized: Some(wire_authorized_read(entry.authorized)),
                        chunk: Some(wire_chunk_object(entry.chunk)),
                        location: Some(wire_copy_location(entry.location)),
                    })
                    .collect(),
            },
        ))
    }

    async fn authorize_external_version_read(
        &self,
        request: Request<afs_protocol::meta::AuthorizeExternalVersionReadRequest>,
    ) -> Result<Response<afs_protocol::meta::AuthorizeExternalVersionReadReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        for (value, field) in [
            (&request.caller_id, "caller_id"),
            (&request.inode_id, "inode_id"),
            (&request.namespace_id, "namespace_id"),
            (&request.file_version_id, "file_version_id"),
            (&request.layout_root_id, "layout_root_id"),
        ] {
            require_text(value, field)?;
        }
        let requests = request
            .requests
            .into_iter()
            .map(domain_missing_read_source)
            .collect::<Result<Vec<_>, Status>>()?;
        let reply = dfs_service(&self.0)?
            .authorize_external_version_read(crate::dfs::AuthorizeExternalVersionRead {
                caller_id: request.caller_id,
                inode_id: crate::dfs::InodeId::new(request.inode_id),
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                file_version_id: crate::dfs::FileVersionId::new(request.file_version_id),
                layout_root_id: crate::dfs::LayoutRootId::new(request.layout_root_id),
                requests,
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::meta::AuthorizeExternalVersionReadReply {
                authorizations: reply
                    .authorizations
                    .into_iter()
                    .map(|entry| afs_protocol::meta::DfsExternalAuthorizedRead {
                        authorized: Some(wire_authorized_read(entry.authorized)),
                        chunk: Some(wire_chunk_object(entry.chunk)),
                        location: Some(wire_copy_location(entry.location)),
                    })
                    .collect(),
                remaining_validity_ms: reply.remaining_validity_ms,
            },
        ))
    }

    async fn get_chunk_sources(
        &self,
        request: Request<GetDfsChunkSourcesRequest>,
    ) -> Result<Response<GetDfsChunkSourcesReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.namespace_id, "namespace_id")?;
        require_text(&request.file_version_id, "file_version_id")?;
        require_text(&request.layout_root_id, "layout_root_id")?;
        let reply = dfs_service(&self.0)?
            .chunk_sources(crate::dfs::DfsChunkSourcesRequest {
                caller_id: request.caller_id,
                namespace_id: crate::dfs::NamespaceId::new(request.namespace_id),
                file_version_id: crate::dfs::FileVersionId::new(request.file_version_id),
                layout_root_id: crate::dfs::LayoutRootId::new(request.layout_root_id),
                chunk_ids: request
                    .chunk_ids
                    .into_iter()
                    .map(crate::dfs::ChunkId::new)
                    .collect(),
            })
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(wire_chunk_sources_reply(reply)))
    }

    async fn sync_inode_metadata(
        &self,
        request: Request<SyncDfsInodeMetadataRequest>,
    ) -> Result<Response<SyncDfsInodeMetadataReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let inode = dfs_service(&self.0)?
            .sync_inode_metadata(
                request.caller_id,
                crate::dfs::SyncInodeMetadata {
                    operation_id: crate::dfs::OperationId::new(request.operation_id),
                    inode_id: crate::dfs::InodeId::new(request.inode_id),
                    write_lease: domain_dfs_write_lease(
                        request
                            .write_lease
                            .ok_or_else(|| invalid("DFS metadata sync requires write lease"))?,
                    ),
                    expected_inode_revision: request.expected_inode_revision,
                    expected_head_version: optional_id(request.expected_head_version_id)
                        .map(crate::dfs::FileVersionId::new),
                    metadata_delta: domain_dfs_metadata_delta(
                        request
                            .metadata_delta
                            .ok_or_else(|| invalid("DFS metadata sync requires metadata delta"))?,
                    )?,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(SyncDfsInodeMetadataReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }

    async fn commit_file_version(
        &self,
        request: Request<CommitFileVersionRequest>,
    ) -> Result<Response<CommitFileVersionReply>, Status> {
        let authenticated = authenticated_node_id(&self.0, &request)?;
        let request = request.into_inner();
        validate_caller(authenticated.as_deref(), &request.caller_id)?;
        require_text(&request.operation_id, "operation_id")?;
        require_text(&request.inode_id, "inode_id")?;
        let version = domain_dfs_version(
            request
                .version
                .ok_or_else(|| invalid("DFS commit requires FileVersion"))?,
        );
        let layout = domain_dfs_layout(
            request
                .layout
                .ok_or_else(|| invalid("DFS commit requires LayoutRoot"))?,
        );
        let receipts = request
            .chunk_receipts
            .into_iter()
            .map(domain_chunk_receipt)
            .collect::<Result<Vec<_>, _>>()?;
        let write_lease = domain_dfs_write_lease(
            request
                .write_lease
                .ok_or_else(|| invalid("DFS commit requires write lease"))?,
        );
        let metadata_delta = domain_dfs_metadata_delta(
            request
                .metadata_delta
                .ok_or_else(|| invalid("DFS commit requires metadata delta"))?,
        )?;
        let inode = dfs_service(&self.0)?
            .commit_file_version(
                request.caller_id,
                crate::dfs::CommitFileVersion {
                    gc_fence: request
                        .gc_fence
                        .map(crate::dfs::GcFence::from_wire)
                        .transpose()
                        .map_err(afs_transport::grpc::error_status::error_to_status)?,
                    operation_id: crate::dfs::OperationId::new(request.operation_id),
                    inode_id: crate::dfs::InodeId::new(request.inode_id),
                    write_lease,
                    expected_inode_revision: request.expected_inode_revision,
                    expected_head_version: optional_id(request.expected_head_version_id)
                        .map(crate::dfs::FileVersionId::new),
                    file_version: version,
                    layout_root: layout,
                    chunk_receipts: receipts,
                    clone_source: request
                        .clone_source
                        .map(|source| crate::dfs::CloneFileRange {
                            version: crate::dfs::FileVersionId::new(source.version_id),
                            source_offset: source.source_offset,
                            destination_offset: source.destination_offset,
                            length: source.length,
                        }),
                    metadata_delta,
                },
            )
            .await
            .map_err(afs_transport::grpc::error_status::error_to_status)?;
        self.0
            .observability
            .record("meta", "dfs_commit_file_version", true);
        Ok(Response::new(CommitFileVersionReply {
            inode: Some(wire_dfs_inode(inode)),
        }))
    }
}

fn dfs_service(meta: &super::super::Meta) -> Result<&super::super::dfs::DfsService, Status> {
    meta.dfs.as_ref().ok_or_else(|| {
        afs_transport::grpc::error_status::error_to_status(
            super::super::store::unavailable_meta_store(),
        )
    })
}
