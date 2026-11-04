use super::*;

/// Mount-local inode numbering: backend inode <-> DFS `InodeId`.
///
/// `creation` serialises allocation of new numbers so two racing lookups of
/// the same `InodeId` cannot hand out two backend inodes.
pub(super) struct InodeMap {
    inode_to_backend: ShardedMap<InodeId, u64>,
    backend_to_inode: ShardedMap<u64, InodeId>,
    creation: Mutex<()>,
    next_inode: AtomicU64,
}

impl Default for InodeMap {
    fn default() -> Self {
        Self {
            inode_to_backend: ShardedMap::from_entries([(InodeId::new("1"), ROOT_INODE)]),
            backend_to_inode: ShardedMap::from_entries([(ROOT_INODE, InodeId::new("1"))]),
            creation: Mutex::new(()),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }
}

impl InodeMap {
    pub(super) fn inode_id(&self, inode: BackendInode) -> Result<InodeId> {
        self.backend_to_inode
            .read(&inode.value)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(&inode.value)
            .cloned()
            .ok_or_else(|| stale("DFS mount inode is no longer known"))
    }

    pub(super) fn lookup(&self, inode: &InodeId) -> Result<Option<BackendInode>> {
        Ok(self
            .inode_to_backend
            .read(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
            .map(|value| BackendInode { value }))
    }

    /// Returns the existing number or allocates one under the creation lock.
    pub(super) fn get_or_create(&self, inode: &InodeId) -> Result<BackendInode> {
        let _creation = self
            .creation
            .lock()
            .map_err(|_| unavailable("DFS inode mapping creation lock is poisoned"))?;
        if let Some(value) = self.lookup(inode)? {
            return Ok(value);
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
}

impl DistributedFs {
    pub(super) fn validate_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        self.identity.validate_inode(inode)
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
        self.inode_map.inode_id(inode)
    }

    pub(super) fn backend_inode(&self, inode: &InodeId) -> Result<BackendInode> {
        if let Some(value) = self.inode_map.lookup(inode)? {
            return Ok(value);
        }
        #[cfg(test)]
        {
            let barrier = self
                .hooks
                .backend_inode_after_missing
                .lock()
                .unwrap()
                .clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        self.inode_map.get_or_create(inode)
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
