//! File authorization uses an existing durable open pin, not a node-wide trust
//! shortcut. A bounded page is returned only over the Meta-to-serving-node RPC.
use super::*;

impl DfsService {
    pub(super) async fn validate_open_read_context(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        if request.validations.len() != 1 {
            return Err(invalid("open read context requires one requested range"));
        }
        let validation = request.validations[0].clone();
        let grant = &validation.grant;
        let reference_id = grant
            .token
            .strip_prefix("dfs-open-v1:")
            .filter(|id| id.starts_with("read-open:") && id.len() <= 512)
            .ok_or_else(|| permission_denied("invalid open read context id"))?;
        let now = now_unix_ms();
        let view = self.store.read_view().await?;
        let receiver = live_read_session(
            &view,
            &request.receiver_node_id,
            request.receiver_node_epoch,
            now,
        )
        .await?;
        let caller =
            live_read_session(&view, &request.peer_node_id, grant.caller_node_epoch, now).await?;
        if grant.caller_node_id != request.peer_node_id || grant.expires_at_unix_ms <= now {
            return Err(permission_denied(
                "open read context caller or expiry differs",
            ));
        }
        let reference = match view
            .lookup(&MetaKey::DfsReference {
                node_id: request.peer_node_id.clone(),
                session_id: caller.session_id.clone(),
                reference_id: reference_id.into(),
            })?
            .entity
        {
            Some(MetaEntity::DfsReference(r))
                if r.node_id == request.peer_node_id
                    && r.session_id == caller.session_id
                    && r.retired_at_unix_ms.is_none()
                    && !r.publication =>
            {
                r
            }
            _ => return Err(permission_denied("open read context has no live GC owner")),
        };
        let identity = reference
            .open_read
            .as_ref()
            .filter(|v| {
                !v.closed
                    && v.version_id == grant.file_version_id
                    && v.layout_root_id == grant.layout_root_id
            })
            .ok_or_else(|| {
                permission_denied("open read context is closed or has another version")
            })?;
        let version = match view
            .lookup(&MetaKey::DfsFileVersion(identity.version_id.clone()))?
            .entity
        {
            Some(MetaEntity::DfsFileVersion(v))
                if Some(&v.inode_id) == reference.inode_id.as_ref()
                    && v.layout_root == identity.layout_root_id =>
            {
                v
            }
            _ => {
                return Err(permission_denied(
                    "open read context version differs from pin",
                ));
            }
        };
        match view.lookup(&MetaKey::DfsInode(version.inode_id))?.entity {
            Some(MetaEntity::DfsInode(i)) if i.namespace_id == grant.namespace_id => {}
            _ => return Err(permission_denied("open read context namespace differs")),
        }
        let layout = match view
            .lookup(&MetaKey::DfsLayoutRoot(identity.layout_root_id.clone()))?
            .entity
        {
            Some(MetaEntity::DfsLayoutRoot(v)) => v,
            _ => return Err(permission_denied("open read context layout is absent")),
        };
        // Page limits bound retained metadata. Large files reuse the same open
        // identity while loading another page; expiry never switches its head.
        let ids = layout
            .inline_extents
            .iter()
            .map(|e| e.chunk_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let position = ids
            .iter()
            .position(|id| id == &validation.chunk_id)
            .ok_or_else(|| permission_denied("requested chunk is outside opened layout"))?;
        let first = position / 512 * 512;
        let mut sources = Vec::new();
        let mut requested_ranges = None;
        for id in ids.iter().skip(first).take(512) {
            let chunk = match view.lookup(&MetaKey::DfsChunk(id.clone()))?.entity {
                Some(MetaEntity::DfsChunk(v)) => v,
                _ => continue,
            };
            let ranges = read_chunk_intervals(&layout, &chunk)?;
            if ranges.is_empty() || ranges.len() > 128 {
                if id == &validation.chunk_id {
                    return Err(permission_denied("open read layout range budget exceeded"));
                }
                continue;
            }
            let placement = match view.lookup(&MetaKey::DfsPlacement(id.clone()))?.entity {
                Some(MetaEntity::DfsPlacement(v)) => v,
                _ => continue,
            };
            let mut copies = Vec::new();
            for copy_id in &placement.copies {
                if let Some(MetaEntity::DfsCopy(copy)) =
                    view.lookup(&MetaKey::DfsCopy(copy_id.clone()))?.entity
                    && copy.chunk_id == *id
                    && copy.is_ready_readable()
                    && serving_read_copy(&copy, &receiver, now).is_some()
                {
                    copies.push(copy.id);
                }
            }
            if copies.len() > 64 {
                return Err(invalid("open read source copy budget exceeded"));
            }
            if id == &validation.chunk_id {
                if !copies.contains(&validation.copy_id)
                    || !read_range_covered(&ranges, validation.chunk_offset, validation.length)
                {
                    return Err(permission_denied("open read copy or range is not admitted"));
                }
                requested_ranges = Some(ranges.clone());
            }
            if !copies.is_empty() {
                sources.push(crate::dfs::OpenReadSource {
                    chunk_id: id.clone(),
                    copy_ids: copies,
                    ranges,
                });
            }
        }
        let allowed_ranges = requested_ranges.ok_or_else(|| {
            permission_denied("opened file has no ready local source for requested range")
        })?;
        let open_sources =
            serde_json::to_vec(&sources).map_err(|_| invalid("open read page encoding failed"))?;
        if open_sources.len() > 4 * 1024 * 1024 {
            return Err(invalid("open read page exceeds metadata budget"));
        }
        let expires_at_unix_ms = now
            .saturating_add(5_000)
            .min(grant.expires_at_unix_ms)
            .min(caller.expires_at_unix_ms)
            .min(receiver.expires_at_unix_ms);
        Ok(vec![crate::dfs::DfsAuthorizedRead {
            validation,
            allowed_ranges,
            expires_at_unix_ms,
            open_sources,
        }])
    }
}
