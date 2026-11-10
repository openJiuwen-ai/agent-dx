//! Read sources and read authorization, native and external.

use super::*;

struct MissingExternalReadContext<'a> {
    view: &'a MetaReadView,
    caller_id: &'a str,
    namespace_id: &'a NamespaceId,
    file_version_id: &'a FileVersionId,
    layout_root_id: &'a LayoutRootId,
    layout: &'a LayoutRoot,
    caller_session: &'a NodeSession,
}

impl DfsService {
    pub async fn chunk_sources(
        &self,
        request: DfsChunkSourcesRequest,
    ) -> Result<DfsChunkSourcesReply> {
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.file_version_id.0, "file_version_id")?;
        require_id(&request.layout_root_id.0, "layout_root_id")?;
        if request.chunk_ids.is_empty() {
            return Ok(DfsChunkSourcesReply {
                revision: 0,
                chunks: Vec::new(),
            });
        }
        let view = self.store.read_view().await?;
        let Some(MetaEntity::DfsFileVersion(version)) = view
            .read(MetaRead::DfsFileVersion(request.file_version_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source FileVersion does not exist"));
        };
        let Some(MetaEntity::DfsLayoutRoot(layout)) = view
            .read(MetaRead::DfsLayoutRoot(request.layout_root_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source LayoutRoot does not exist"));
        };
        let Some(MetaEntity::DfsInode(inode)) = view
            .read(MetaRead::DfsInode(version.inode_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source inode does not exist"));
        };
        if inode.namespace_id != request.namespace_id {
            return Err(invalid("DFS read source namespace does not match the file"));
        }
        if version.layout_root != request.layout_root_id || layout.id != request.layout_root_id {
            return Err(conflict(
                "DFS read source request references a stale LayoutRoot",
            ));
        }
        let requested = request.chunk_ids.iter().cloned().collect::<HashSet<_>>();
        for chunk_id in &requested {
            if !layout
                .inline_extents
                .iter()
                .any(|extent| extent.chunk_id == *chunk_id)
            {
                return Err(invalid(
                    "DFS read source request includes a Chunk outside the FileVersion layout",
                ));
            }
        }

        let caller_session = view
            .read(MetaRead::CurrentNodeSession {
                node_id: request.caller_id.clone(),
            })
            .await?;
        let caller_epoch = match caller_session.entity {
            Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now_unix_ms()) => {
                session.lease_epoch
            }
            _ => {
                return Err(conflict(
                    "DFS read source request requires a live caller session",
                ));
            }
        };

        self.current_durability_policy().await?;
        let mut revision = 0;
        let mut chunks = Vec::with_capacity(request.chunk_ids.len());
        for chunk_id in request.chunk_ids {
            let chunk = match view
                .read(MetaRead::DfsChunk(chunk_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsChunk(chunk)) => chunk,
                _ => return Err(invalid("DFS source chunk identity is missing")),
            };
            // Compute once from the same authority view used for every MAC.
            // Large layouts keep precise grants but omit the optional hint.
            let ranges = read_chunk_intervals(&layout, &chunk)?;
            let allowed_ranges = if ranges.len() <= 128 {
                ranges.clone()
            } else {
                Vec::new()
            };
            let placement_snapshot = view.read(MetaRead::DfsPlacement(chunk_id.clone())).await?;
            revision = revision.max(placement_snapshot.revision.0);
            let Some(MetaEntity::DfsPlacement(placement)) = placement_snapshot.entity else {
                chunks.push(ChunkSources {
                    chunk_id,
                    chunk: Some(chunk),
                    sources: Vec::new(),
                    allowed_ranges,
                });
                continue;
            };
            let mut sources = Vec::new();
            for copy_id in placement.copies {
                let copy_snapshot = view.read(MetaRead::DfsCopy(copy_id)).await?;
                revision = revision.max(copy_snapshot.revision.0);
                let Some(MetaEntity::DfsCopy(copy)) = copy_snapshot.entity else {
                    continue;
                };
                let (serving_copy, data_endpoint) = if self.is_external_backend() {
                    if !self.external_copy_matches(&copy, &chunk)? {
                        continue;
                    }
                    (copy.clone(), None)
                } else {
                    if copy.chunk_id != chunk_id || !copy.is_ready_durable() {
                        continue;
                    }
                    let CopyLocation::Node { node_id, .. } = &copy.location else {
                        continue;
                    };
                    let session_snapshot = view
                        .read(MetaRead::CurrentNodeSession {
                            node_id: node_id.clone(),
                        })
                        .await?;
                    revision = revision.max(session_snapshot.revision.0);
                    let Some(MetaEntity::NodeSession(session)) = session_snapshot.entity else {
                        continue;
                    };
                    let Some(serving_copy) = serving_read_copy(&copy, &session, now_unix_ms())
                    else {
                        continue;
                    };
                    (serving_copy, Some(session.data_addr))
                };
                let mut read_grant = DfsReadGrant {
                    namespace_id: request.namespace_id.clone(),
                    file_version_id: request.file_version_id.clone(),
                    layout_root_id: request.layout_root_id.clone(),
                    caller_node_id: request.caller_id.clone(),
                    caller_node_epoch: caller_epoch,
                    expires_at_unix_ms: now_unix_ms().saturating_add(30_000),
                    fence: revision,
                    token: String::new(),
                };
                read_grant.token =
                    self.read_grant_token(&read_grant, &serving_copy, &chunk, &ranges)?;
                sources.push(SourceCandidate {
                    copy_id: copy.id.clone(),
                    chunk_id: chunk_id.clone(),
                    role: copy.role,
                    state: copy.state,
                    location: serving_copy.location.clone(),
                    data_endpoint,
                    load_hint: 0,
                    read_grant,
                });
            }
            chunks.push(ChunkSources {
                chunk_id,
                chunk: Some(chunk),
                sources,
                allowed_ranges,
            });
        }
        Ok(DfsChunkSourcesReply { revision, chunks })
    }

    fn external_copy_matches(&self, copy: &CopyRecord, chunk: &ChunkObject) -> Result<bool> {
        external_copy_matches(&self.durability_policy, copy, chunk)
    }

    /// Authorizes backend reads from the committed catalog and fixed layout.
    /// The authenticated requesting Node is the reader; no writer/Peer session
    /// is consulted. Invalid, expired or out-of-layout grants are rejected.
    pub async fn validate_external_read_grants(
        &self,
        caller_id: String,
        validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        if !self.is_external_backend() || validations.is_empty() || validations.len() > 128 {
            return Err(invalid(
                "external read authorization requires the external policy and 1..128 entries",
            ));
        }
        self.current_durability_policy().await?;
        let now = now_unix_ms();
        let view = self.store.read_view().await?;
        self.validate_external_read_grants_in_view(&view, &caller_id, validations, now)
            .await
    }

    /// Checks freshness and grants against one pinned view. Cached layouts are
    /// only hints: no data authority is returned when their head has changed.
    pub async fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> Result<crate::dfs::AuthorizeCurrentExternalReadReply> {
        let view = self.store.read_view().await?;
        self.authorize_current_external_read_in_view(&view, request, now_unix_ms())
            .await
    }

    pub(super) async fn authorize_current_external_read_in_view(
        &self,
        view: &MetaReadView,
        request: crate::dfs::AuthorizeCurrentExternalRead,
        now: u64,
    ) -> Result<crate::dfs::AuthorizeCurrentExternalReadReply> {
        use crate::dfs::AuthorizeCurrentExternalReadReply;
        let requested_reads = request
            .validations
            .len()
            .saturating_add(request.missing_sources.len());
        if !self.is_external_backend() || requested_reads == 0 || requested_reads > 128 {
            return Err(invalid(
                "current external read requires the external policy and 1..128 entries",
            ));
        }
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.expected_head_version.0, "expected_head_version")?;
        require_id(&request.layout_root_id.0, "layout_root_id")?;
        match view.read(MetaRead::DfsDurabilityPolicy).await?.entity {
            Some(MetaEntity::DfsDurabilityPolicy(policy)) if policy == self.durability_policy => {}
            Some(MetaEntity::DfsDurabilityPolicy(_)) => {
                return Err(conflict(
                    "DFS durability policy differs from the initialized filesystem",
                ));
            }
            _ => return Err(invalid("DFS durability policy is not initialized")),
        }
        let caller_id = request.caller_id.clone();
        let inode_id = request.inode_id.clone();
        let namespace_id = request.namespace_id.clone();
        let expected_head_version = request.expected_head_version.clone();
        let layout_root_id = request.layout_root_id.clone();
        let inode = match view
            .read(MetaRead::DfsInode(inode_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsInode(inode))
                if inode.namespace_id == namespace_id && inode.kind == InodeKind::Regular =>
            {
                inode
            }
            _ => {
                return Err(permission_denied(
                    "current external read inode or namespace differs",
                ));
            }
        };
        match view
            .read(MetaRead::DfsFileVersion(expected_head_version.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsFileVersion(version))
                if version.inode_id == inode_id && version.layout_root == layout_root_id => {}
            _ => {
                return Err(permission_denied(
                    "current external read version does not belong to the inode and layout",
                ));
            }
        };
        let layout = match view
            .read(MetaRead::DfsLayoutRoot(layout_root_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsLayoutRoot(layout)) if layout.id == layout_root_id => layout,
            _ => return Err(permission_denied("current external read layout is missing")),
        };
        if request.validations.iter().any(|validation| {
            validation.grant.namespace_id != namespace_id
                || validation.grant.file_version_id != expected_head_version
                || validation.grant.layout_root_id != layout_root_id
        }) {
            return Err(permission_denied(
                "current external read grants differ from the requested head",
            ));
        }
        let validations = request.validations;
        let missing_sources = request.missing_sources;
        // A changed head must not turn invalid/revoked grants into a successful
        // fallback signal. All existing checks run before choosing the reply.
        let mut authorizations = self
            .validate_external_read_grants_in_view(view, &caller_id, validations, now)
            .await?;
        let caller_session = if missing_sources.is_empty() {
            None
        } else {
            match view
                .read(MetaRead::CurrentNodeSession {
                    node_id: caller_id.clone(),
                })
                .await?
                .entity
            {
                Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now) => {
                    Some(session)
                }
                _ => {
                    return Err(permission_denied(
                        "current external read missing source requires a live caller session",
                    ));
                }
            }
        };
        if inode.head_version.as_ref() != Some(&expected_head_version) {
            return Ok(AuthorizeCurrentExternalReadReply::HeadChanged(inode));
        }
        let mut resolved_missing = Vec::with_capacity(missing_sources.len());
        for missing in &missing_sources {
            let Some(caller_session) = caller_session.as_ref() else {
                return Err(permission_denied(
                    "current external read missing source requires a live caller session",
                ));
            };
            resolved_missing.push(
                self.resolve_missing_external_read_source_in_view(
                    &MissingExternalReadContext {
                        view,
                        caller_id: &caller_id,
                        namespace_id: &namespace_id,
                        file_version_id: &expected_head_version,
                        layout_root_id: &layout_root_id,
                        layout: &layout,
                        caller_session,
                    },
                    missing,
                    now,
                )
                .await?,
            );
        }
        authorizations.extend(resolved_missing);
        Ok(AuthorizeCurrentExternalReadReply::Authorized {
            inode,
            authorizations,
        })
    }

    /// Re-admits external object reads for one immutable version/layout. This
    /// path intentionally does not compare the requested version with the inode
    /// head and does not accept old read MACs as renewal credentials.
    pub async fn authorize_external_version_read(
        &self,
        request: crate::dfs::AuthorizeExternalVersionRead,
    ) -> Result<crate::dfs::AuthorizeExternalVersionReadReply> {
        let view = self.store.read_view().await?;
        self.authorize_external_version_read_in_view(&view, request, now_unix_ms())
            .await
    }

    pub(super) async fn authorize_external_version_read_in_view(
        &self,
        view: &MetaReadView,
        request: crate::dfs::AuthorizeExternalVersionRead,
        now: u64,
    ) -> Result<crate::dfs::AuthorizeExternalVersionReadReply> {
        if !self.is_external_backend()
            || request.requests.is_empty()
            || request.requests.len() > 128
        {
            return Err(invalid(
                "external version read requires the external policy and 1..128 entries",
            ));
        }
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.file_version_id.0, "file_version_id")?;
        require_id(&request.layout_root_id.0, "layout_root_id")?;
        match view.read(MetaRead::DfsDurabilityPolicy).await?.entity {
            Some(MetaEntity::DfsDurabilityPolicy(policy)) if policy == self.durability_policy => {}
            Some(MetaEntity::DfsDurabilityPolicy(_)) => {
                return Err(conflict(
                    "DFS durability policy differs from the initialized filesystem",
                ));
            }
            _ => return Err(invalid("DFS durability policy is not initialized")),
        }
        let inode = match view
            .read(MetaRead::DfsInode(request.inode_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsInode(inode))
                if inode.namespace_id == request.namespace_id
                    && inode.kind == InodeKind::Regular =>
            {
                inode
            }
            _ => {
                return Err(permission_denied(
                    "external version read inode or namespace differs",
                ));
            }
        };
        match view
            .read(MetaRead::DfsFileVersion(request.file_version_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsFileVersion(version))
                if version.inode_id == inode.inode_id
                    && version.layout_root == request.layout_root_id => {}
            _ => {
                return Err(permission_denied(
                    "external version read version does not belong to the inode and layout",
                ));
            }
        };
        let layout = match view
            .read(MetaRead::DfsLayoutRoot(request.layout_root_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsLayoutRoot(layout)) if layout.id == request.layout_root_id => {
                layout
            }
            _ => return Err(permission_denied("external version read layout is missing")),
        };
        let caller_session = match view
            .read(MetaRead::CurrentNodeSession {
                node_id: request.caller_id.clone(),
            })
            .await?
            .entity
        {
            Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now) => session,
            _ => {
                return Err(permission_denied(
                    "external version read requires a live caller session",
                ));
            }
        };
        let mut authorizations = Vec::with_capacity(request.requests.len());
        for missing in &request.requests {
            authorizations.push(
                self.resolve_missing_external_read_source_in_view(
                    &MissingExternalReadContext {
                        view,
                        caller_id: &request.caller_id,
                        namespace_id: &request.namespace_id,
                        file_version_id: &request.file_version_id,
                        layout_root_id: &request.layout_root_id,
                        layout: &layout,
                        caller_session: &caller_session,
                    },
                    missing,
                    now,
                )
                .await?,
            );
        }
        let remaining_validity_ms = authorizations
            .iter()
            .map(|entry| entry.authorized.expires_at_unix_ms.saturating_sub(now))
            .min()
            .unwrap_or(0)
            .min(5_000);
        Ok(crate::dfs::AuthorizeExternalVersionReadReply {
            authorizations,
            remaining_validity_ms,
        })
    }

    async fn resolve_missing_external_read_source_in_view(
        &self,
        context: &MissingExternalReadContext<'_>,
        missing: &crate::dfs::DfsMissingReadSource,
        now: u64,
    ) -> Result<crate::dfs::DfsExternalAuthorizedRead> {
        if missing.length == 0 {
            return Err(invalid(
                "current external missing source length must be non-zero",
            ));
        }
        let chunk = match context
            .view
            .read(MetaRead::DfsChunk(missing.chunk_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsChunk(chunk)) => chunk,
            _ => {
                return Err(permission_denied(
                    "current external missing source chunk is missing",
                ));
            }
        };
        if missing
            .chunk_offset
            .checked_add(missing.length)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(permission_denied(
                "current external missing source range exceeds the chunk",
            ));
        }
        let intervals = read_chunk_intervals(context.layout, &chunk)?;
        if !read_range_covered(&intervals, missing.chunk_offset, missing.length) {
            return Err(permission_denied(
                "current external missing source range is outside the committed layout",
            ));
        }
        let placement_snapshot = context
            .view
            .read(MetaRead::DfsPlacement(missing.chunk_id.clone()))
            .await?;
        let mut revision = placement_snapshot.revision.0;
        let Some(MetaEntity::DfsPlacement(placement)) = placement_snapshot.entity else {
            return Err(permission_denied(
                "current external missing source has no committed placement",
            ));
        };
        for copy_id in placement.copies {
            let copy_snapshot = context.view.read(MetaRead::DfsCopy(copy_id)).await?;
            revision = revision.max(copy_snapshot.revision.0);
            let Some(MetaEntity::DfsCopy(copy)) = copy_snapshot.entity else {
                continue;
            };
            if !self.external_copy_matches(&copy, &chunk)? {
                continue;
            }
            let mut grant = DfsReadGrant {
                namespace_id: context.namespace_id.clone(),
                file_version_id: context.file_version_id.clone(),
                layout_root_id: context.layout_root_id.clone(),
                caller_node_id: context.caller_id.to_owned(),
                caller_node_epoch: context.caller_session.lease_epoch,
                expires_at_unix_ms: now.saturating_add(30_000),
                fence: revision,
                token: String::new(),
            };
            grant.token = self.read_grant_token(&grant, &copy, &chunk, &intervals)?;
            let allowed_ranges = if intervals.len() <= 128 {
                intervals.clone()
            } else {
                vec![(missing.chunk_offset, missing.length)]
            };
            return Ok(crate::dfs::DfsExternalAuthorizedRead {
                authorized: crate::dfs::DfsAuthorizedRead {
                    validation: crate::dfs::DfsReadValidation {
                        grant,
                        chunk_id: missing.chunk_id.clone(),
                        copy_id: copy.id.clone(),
                        chunk_offset: missing.chunk_offset,
                        length: missing.length,
                    },
                    allowed_ranges,
                    expires_at_unix_ms: now
                        .saturating_add(5_000)
                        .min(context.caller_session.expires_at_unix_ms),
                },
                chunk,
                location: copy.location,
            });
        }
        Err(permission_denied(
            "current external missing source has no committed Ready copy",
        ))
    }

    async fn validate_external_read_grants_in_view(
        &self,
        view: &MetaReadView,
        caller_id: &str,
        validations: Vec<crate::dfs::DfsReadValidation>,
        now: u64,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        let mut result = Vec::with_capacity(validations.len());
        for validation in validations {
            let grant = &validation.grant;
            if grant.caller_node_id != caller_id
                || grant.expires_at_unix_ms <= now
                || grant.token.len() != 76
            {
                return Err(permission_denied(
                    "external read grant caller or expiry differs",
                ));
            }
            let caller = live_read_session(view, caller_id, grant.caller_node_epoch, now).await?;
            let version = match view
                .read(MetaRead::DfsFileVersion(grant.file_version_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsFileVersion(version))
                    if version.layout_root == grant.layout_root_id =>
                {
                    version
                }
                _ => {
                    return Err(permission_denied(
                        "external read grant version or layout differs",
                    ));
                }
            };
            match view
                .read(MetaRead::DfsInode(version.inode_id))
                .await?
                .entity
            {
                Some(MetaEntity::DfsInode(inode)) if inode.namespace_id == grant.namespace_id => {}
                _ => return Err(permission_denied("external read grant namespace differs")),
            }
            let layout = match view
                .read(MetaRead::DfsLayoutRoot(grant.layout_root_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsLayoutRoot(layout)) => layout,
                _ => return Err(permission_denied("external read grant layout is missing")),
            };
            let chunk = match view
                .read(MetaRead::DfsChunk(validation.chunk_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsChunk(chunk)) => chunk,
                _ => return Err(permission_denied("external read grant chunk is missing")),
            };
            let copy = match view
                .read(MetaRead::DfsCopy(validation.copy_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsCopy(copy)) if self.external_copy_matches(&copy, &chunk)? => {
                    copy
                }
                _ => {
                    return Err(permission_denied(
                        "external read grant copy is not committed and Ready",
                    ));
                }
            };
            let intervals = read_chunk_intervals(&layout, &chunk)?;
            let supplied = grant
                .token
                .strip_prefix("dfs-read-v1:")
                .and_then(|hex| blake3::Hash::from_hex(hex).ok())
                .ok_or_else(|| permission_denied("external read grant MAC format is invalid"))?;
            if supplied != self.read_grant_mac(grant, &copy, &chunk, &intervals)? {
                return Err(permission_denied(
                    "external read grant MAC authentication failed",
                ));
            }
            if !read_range_covered(&intervals, validation.chunk_offset, validation.length) {
                return Err(permission_denied(
                    "external read range is outside the committed layout",
                ));
            }
            let allowed_ranges = if intervals.len() <= 128 {
                intervals
            } else {
                vec![(validation.chunk_offset, validation.length)]
            };
            result.push(crate::dfs::DfsExternalAuthorizedRead {
                authorized: crate::dfs::DfsAuthorizedRead {
                    expires_at_unix_ms: now
                        .saturating_add(5_000)
                        .min(grant.expires_at_unix_ms)
                        .min(caller.expires_at_unix_ms),
                    validation,
                    allowed_ranges,
                },
                chunk,
                location: copy.location,
            });
        }
        Ok(result)
    }

    fn read_grant_mac(
        &self,
        grant: &DfsReadGrant,
        copy: &CopyRecord,
        chunk: &crate::dfs::ChunkObject,
        ranges: &[(u64, u64)],
    ) -> Result<blake3::Hash> {
        let key = self
            .read_grant_key
            .as_ref()
            .as_ref()
            .ok_or_else(|| permission_denied("Meta read grant entropy is unavailable"))?;
        let mut unsigned = grant.clone();
        unsigned.token.clear();
        let canonical = serde_json::to_vec(&("afs-dfs-read-v1", unsigned, copy, chunk, ranges))
            .map_err(|_| invalid("read grant canonical serialization failed"))?;
        Ok(blake3::keyed_hash(key, &canonical))
    }

    fn read_grant_token(
        &self,
        grant: &DfsReadGrant,
        copy: &CopyRecord,
        chunk: &crate::dfs::ChunkObject,
        ranges: &[(u64, u64)],
    ) -> Result<String> {
        Ok(format!(
            "dfs-read-v1:{}",
            self.read_grant_mac(grant, copy, chunk, ranges)?.to_hex()
        ))
    }

    pub async fn validate_read_grants(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        if request.validations.is_empty() || request.validations.len() > 128 {
            return Err(invalid(
                "read grant validation batch requires 1..128 entries",
            ));
        }
        let now = now_unix_ms();
        let view = self.store.read_view().await?;
        let receiver = live_read_session(
            &view,
            &request.receiver_node_id,
            request.receiver_node_epoch,
            now,
        )
        .await?;
        let mut result = Vec::with_capacity(request.validations.len());
        for validation in request.validations {
            let grant = &validation.grant;
            if grant.caller_node_id != request.peer_node_id
                || grant.expires_at_unix_ms <= now
                || grant.token.len() != 76
            {
                return Err(permission_denied("read grant peer or expiry differs"));
            }
            let caller =
                live_read_session(&view, &request.peer_node_id, grant.caller_node_epoch, now)
                    .await?;
            let version = match view
                .read(MetaRead::DfsFileVersion(grant.file_version_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsFileVersion(value))
                    if value.layout_root == grant.layout_root_id =>
                {
                    value
                }
                _ => return Err(permission_denied("read grant version or layout differs")),
            };
            match view
                .read(MetaRead::DfsInode(version.inode_id))
                .await?
                .entity
            {
                Some(MetaEntity::DfsInode(inode)) if inode.namespace_id == grant.namespace_id => {}
                _ => return Err(permission_denied("read grant namespace differs")),
            }
            let layout = match view
                .read(MetaRead::DfsLayoutRoot(grant.layout_root_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsLayoutRoot(value)) => value,
                _ => return Err(permission_denied("read grant layout is missing")),
            };
            let chunk = match view
                .read(MetaRead::DfsChunk(validation.chunk_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsChunk(value)) => value,
                _ => return Err(permission_denied("read grant chunk is missing")),
            };
            let copy = match view
                .read(MetaRead::DfsCopy(validation.copy_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsCopy(value))
                    if value.chunk_id == validation.chunk_id && value.is_ready_durable() =>
                {
                    value
                }
                _ => return Err(permission_denied("read grant copy is not Ready")),
            };
            let serving_copy = serving_read_copy(&copy, &receiver, now).ok_or_else(|| {
                permission_denied("read grant does not select a recovered receiver device")
            })?;
            let intervals = read_chunk_intervals(&layout, &chunk)?;
            let supplied = grant
                .token
                .strip_prefix("dfs-read-v1:")
                .and_then(|hex| blake3::Hash::from_hex(hex).ok())
                .ok_or_else(|| permission_denied("read grant MAC format is invalid"))?;
            if supplied != self.read_grant_mac(grant, &serving_copy, &chunk, &intervals)? {
                return Err(permission_denied("read grant MAC authentication failed"));
            }
            if !read_range_covered(&intervals, validation.chunk_offset, validation.length) {
                return Err(permission_denied(
                    "read range is outside the committed layout",
                ));
            }
            // Highly fragmented layouts retain functionality without unbounded
            // authority replies/cache entries: grant this already checked range.
            let allowed_ranges = if intervals.len() <= 128 {
                intervals
            } else {
                vec![(validation.chunk_offset, validation.length)]
            };
            let expires_at_unix_ms = now
                .saturating_add(5_000)
                .min(grant.expires_at_unix_ms)
                .min(caller.expires_at_unix_ms)
                .min(receiver.expires_at_unix_ms);
            result.push(crate::dfs::DfsAuthorizedRead {
                validation,
                allowed_ranges,
                expires_at_unix_ms,
            });
        }
        Ok(result)
    }
}

/// A committed copy records the process that produced its durable receipt;
/// reads use the live process serving that same persistent device. After a
/// restart, registration follows LocalChunkStore recovery and advertises the
/// recovered catalog revision. Its floor must cover the original receipt, so
/// an older catalog/device cannot acquire read authority for that copy.
/// Same-process writes may advance beyond the startup descriptor, so that
/// additional floor is required only across process epochs. This projection
/// is read-only: neither persisted copy evidence nor write/replica fencing is
/// changed. Signing the projected epoch rejects old grants on a new process.
pub(in crate::meta) fn serving_read_copy(
    copy: &CopyRecord,
    session: &NodeSession,
    now: u64,
) -> Option<CopyRecord> {
    let CopyLocation::Node {
        node_id,
        node_epoch,
        device_id,
        device_epoch,
        catalog_revision,
    } = &copy.location
    else {
        return None;
    };
    if !copy.is_ready_durable()
        || node_id != &session.node_id
        || *node_epoch == 0
        || session.lease_epoch < *node_epoch
        || !session.is_live_at_unix_ms(now)
        || !session.storage_devices.iter().any(|device| {
            device.device_id == *device_id
                && device.device_epoch == *device_epoch
                && (session.lease_epoch == *node_epoch
                    || device.catalog_revision >= *catalog_revision)
        })
    {
        return None;
    }
    let mut serving_copy = copy.clone();
    if let CopyLocation::Node { node_epoch, .. } = &mut serving_copy.location {
        *node_epoch = session.lease_epoch;
    }
    Some(serving_copy)
}

async fn live_read_session(
    view: &MetaReadView,
    node_id: &str,
    epoch: u64,
    now: u64,
) -> Result<NodeSession> {
    match view
        .read(MetaRead::CurrentNodeSession {
            node_id: node_id.into(),
        })
        .await?
        .entity
    {
        Some(MetaEntity::NodeSession(session))
            if epoch != 0 && session.lease_epoch == epoch && session.is_live_at_unix_ms(now) =>
        {
            Ok(session)
        }
        _ => Err(permission_denied("read grant Node session is not live")),
    }
}

pub(super) fn read_chunk_intervals(
    layout: &LayoutRoot,
    chunk: &crate::dfs::ChunkObject,
) -> Result<Vec<(u64, u64)>> {
    let mut intervals = Vec::new();
    for extent in layout
        .inline_extents
        .iter()
        .filter(|extent| extent.chunk_id == chunk.id)
    {
        let length = extent
            .length
            .min(layout.file_length.saturating_sub(extent.file_offset));
        let end = extent
            .chunk_offset
            .checked_add(length)
            .filter(|end| *end <= chunk.length)
            .ok_or_else(|| invalid("read layout extent exceeds immutable chunk"))?;
        if length != 0 {
            intervals.push((extent.chunk_offset, end));
        }
    }
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in intervals {
        if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(merged
        .into_iter()
        .map(|(start, end)| (start, end - start))
        .collect())
}

pub(super) fn read_range_covered(intervals: &[(u64, u64)], offset: u64, length: u64) -> bool {
    length != 0
        && offset.checked_add(length).is_some_and(|end| {
            intervals.iter().any(|(start, len)| {
                offset >= *start && start.checked_add(*len).is_some_and(|limit| end <= limit)
            })
        })
}
