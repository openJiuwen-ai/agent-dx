use super::*;

impl DistributedFs {
    pub(super) fn validate_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        if inode.namespace_id != self.namespace_id {
            return Err(invalid("Meta returned an inode from another DFS namespace"));
        }
        Ok(inode)
    }

    pub(super) fn inode_size_and_blocks(&self, inode: &InodeRecord) -> Result<(u64, u64)> {
        match inode.head_version.as_ref() {
            Some(_) => {
                let cached = self.readonly_version_for_inode(inode)?;
                let length = cached.layout.file_length;
                Ok((
                    length,
                    allocated_blocks(&cached.layout, length, None, &DirtyExtentMap::default())?,
                ))
            }
            None if inode.kind == InodeKind::Symlink => Ok((
                inode
                    .symlink_target
                    .as_ref()
                    .map_or(0, |target| target.len() as u64),
                0,
            )),
            None => Ok((0, 0)),
        }
    }

    pub(super) fn inode_id(&self, inode: BackendInode) -> Result<InodeId> {
        self.backend_to_inode
            .read(&inode.value)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(&inode.value)
            .cloned()
            .ok_or_else(|| stale("DFS mount inode is no longer known"))
    }

    pub(super) fn backend_inode(&self, inode: &InodeId) -> Result<BackendInode> {
        if let Some(value) = self
            .inode_to_backend
            .read(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
        {
            return Ok(BackendInode { value });
        }
        #[cfg(test)]
        {
            let barrier = self.backend_inode_after_missing.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        let _creation = self
            .inode_mapping_creation
            .lock()
            .map_err(|_| unavailable("DFS inode mapping creation lock is poisoned"))?;
        if let Some(value) = self
            .inode_to_backend
            .read(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
        {
            return Ok(BackendInode { value });
        }
        let value = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let mut forward = self
            .inode_to_backend
            .write(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?;
        let mut reverse = self
            .backend_to_inode
            .write(&value)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?;
        reverse.insert(value, inode.clone());
        forward.insert(inode.clone(), value);
        Ok(BackendInode { value })
    }

    pub(super) fn directory_entry(&self, entry: DentryRecord) -> Result<DirectoryEntry> {
        let inode = self.validate_inode(entry.inode)?;
        let kind = file_kind(inode.kind);
        Ok(DirectoryEntry {
            name: OsString::from_vec(entry.name),
            inode: self.backend_inode(&inode.inode_id)?,
            kind,
            next_cookie: 0,
        })
    }
}
