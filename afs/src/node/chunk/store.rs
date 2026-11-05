//! LocalChunkStore construction, durable persist and verified local opens.

use super::*;

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct CatalogCommitTestControl {
    pub(super) ready: Mutex<Option<Arc<std::sync::Barrier>>>,
    pub(super) before_prepare: Mutex<Option<PeerFillTestBlock>>,
    pub(super) before_wal: Mutex<VecDeque<PeerFillTestBlock>>,
    pub(super) fail_wal_step: std::sync::atomic::AtomicUsize,
    pub(super) fail_chunks_dir: std::sync::atomic::AtomicBool,
    pub(super) fail_prepare_after: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl CatalogCommitTestControl {
    pub(super) fn before_group(&self) {
        let barrier = self.ready.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }

    fn pause_prepare(&self) -> Result<()> {
        let remaining = self.fail_prepare_after.load(Ordering::Acquire);
        if remaining != 0 && self.fail_prepare_after.fetch_sub(1, Ordering::AcqRel) == 1 {
            return Err(Error::coded(
                afs_error::IO_OTHER,
                "injected later body preparation failure",
            ));
        }
        let block = self.before_prepare.lock().unwrap().take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
        Ok(())
    }

    pub(super) fn pause_wal(&self) {
        let block = self.before_wal.lock().unwrap().pop_front();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
    }
}

impl LocalChunkStore {
    pub fn open(root: impl AsRef<Path>, node_id: impl Into<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let chunks = root.join("chunks");
        let staging = root.join("staging");
        let outboards = root.join("outboards");
        let mut new_ancestors = Vec::new();
        let mut ancestor = root.as_path();
        while !ancestor.as_os_str().is_empty() && !ancestor.exists() {
            if let Some(parent) = ancestor.parent() {
                new_ancestors.push(if parent.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    parent.to_path_buf()
                });
                ancestor = parent;
            } else {
                break;
            }
        }
        fs::create_dir_all(&outboards).map_err(Error::from)?;
        fs::create_dir_all(&chunks).map_err(Error::from)?;
        fs::create_dir_all(&staging).map_err(Error::from)?;
        // Also repair a previous process's unconfirmed rename before healthy
        // replay can ACK it. A same-OS reopen is not itself a directory barrier.
        sync_store_directory(&chunks)?;
        sync_store_directory(&root)?;
        for parent in new_ancestors {
            sync_store_directory(&parent)?;
        }
        let device_epoch = open_device_epoch(&root)?;
        let root_dir = File::open(&root).map_err(Error::from)?;
        let catalog_path = root.join("catalog.wal");
        let catalog = LocalCatalog::recover(&catalog_path)?;
        // A prior failed sync may have left a complete transaction in the OS
        // cache. Validate first, then persist both WAL and its name before a
        // reopened store may issue a healthy replay ACK.
        if catalog_path.exists() {
            sync_store_catalog(&catalog_path)?;
            sync_store_directory(&root)?;
        }
        let store = Self {
            node_id: node_id.into(),
            device_id: "local-0".into(),
            device_epoch,
            root,
            root_dir,
            chunks,
            staging,
            outboards,
            outboard_budget: OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES),
            outboard_repairs: Arc::new(std::array::from_fn(|_| Mutex::new(()))),
            catalog_path,
            catalog: Mutex::new(catalog),
            catalog_committer: CatalogCommitter::default(),
            next_staging: AtomicU64::new(1),
            gc_inventory_cursor: AtomicU64::new(0),
            peer_read_crc_mode: crate::config::DfsReadCrcMode::Both,
            read_work: None,
            sidecar_installer: SidecarInstaller::default(),
            #[cfg(test)]
            catalog_commit_test: CatalogCommitTestControl::default(),
            #[cfg(test)]
            sidecar_install_test: Arc::new(SidecarInstallTestControl::default()),
            #[cfg(test)]
            peer_fill_test_blocks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            peer_ready_test_block: Mutex::new(None),
            #[cfg(test)]
            peer_payload_test_block: Mutex::new(None),
            #[cfg(any(feature = "dfs", test))]
            peer_read_budget: Arc::new(PeerReadBudget::default()),
            #[cfg(any(feature = "dfs", test))]
            peer_buffer_pool: super::super::read_buffer::ReadBufferPool::new("serve"),
            #[cfg(any(feature = "dfs", test))]
            peer_read_cache: Mutex::new(PeerServeState::default()),
            #[cfg(any(feature = "dfs", test))]
            peer_serve_changed: Condvar::new(),
        };
        store.recover()?;
        Ok(store)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_outboard_budget(mut self, budget: Arc<OutboardBudget>) -> Self {
        self.outboard_budget = budget;
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_peer_read_crc_mode(mut self, mode: crate::config::DfsReadCrcMode) -> Self {
        self.peer_read_crc_mode = mode;
        self
    }

    pub(crate) fn peer_read_crc_mode(&self) -> crate::config::DfsReadCrcMode {
        self.peer_read_crc_mode
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn read_work_metrics(&self) -> Option<&super::super::read_work::ReadWorkMetrics> {
        self.read_work.as_ref()
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: super::super::read_work::ReadWorkMetrics,
    ) -> Self {
        self.catalog_committer.set_metrics(metrics.clone());
        self.read_work = Some(metrics);
        self.sidecar_installer.metrics = self.read_work.clone();
        if let Ok(state) = self.peer_read_cache.lock() {
            self.observe_peer_serve(&state);
        }
        {
            let state = self
                .sidecar_installer
                .queue
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.sidecar_installer.observe(&state);
        }
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_catalog_commit_queue(mut self, limit: usize) -> Result<Self> {
        self.catalog_committer.configure(limit)?;
        Ok(self)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn note_catalog_stop(&self) {
        self.catalog_committer
            .note_stop(Instant::now() + SIDECAR_STOP_WAIT);
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn shutdown_catalog_committer(&self) -> Result<()> {
        self.catalog_committer.shutdown()
    }

    #[cfg(test)]
    pub(crate) fn catalog_commit_stats(&self) -> catalog_commit::CatalogCommitStats {
        self.catalog_committer.stats()
    }

    pub(in crate::node) fn coarse_diagnostic(
        &self,
        phase: &'static str,
        bytes: u64,
    ) -> Option<super::super::read_work::ReadCoarseDiagnosticSpan> {
        self.read_work.as_ref()?.coarse_diagnostic(phase, bytes)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(in crate::node) fn read_diagnostic(
        &self,
        stage: super::super::read_work::ReadDiagnosticStage,
    ) -> Option<super::super::read_work::ReadDiagnosticSpan> {
        self.read_work.as_ref()?.diagnostic(stage)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_disk_cache_event(&self, result: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_disk_cache_event(result);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_cache_fill_stage(
        &self,
        stage: &'static str,
        wall_ns: u64,
        cpu_ns: Option<u64>,
    ) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_cache_fill_stage(stage, wall_ns, cpu_ns);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> Option<super::super::read_work::PeerCacheFillWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_disk_cache_fill_work(bytes, output))
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_source_cache_resources(
        &self,
        entries: usize,
        bytes: usize,
        peak: usize,
        limit: usize,
    ) {
        if let Some(metrics) = &self.read_work {
            metrics.source_cache_resources(entries, bytes, peak, limit);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_crc_read(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.crc_read(stage, bytes);
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn record_read_check(&self, stage: super::super::read_work::ReadCheck) {
        if let Some(metrics) = &self.read_work {
            metrics.check(stage);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_hash(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.hash(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_copy(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.copy(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_read_event(&self, event: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_read_event(event);
        }
    }

    pub fn capacity(&self) -> Result<FilesystemCapacity> {
        capacity_from_statvfs_fd(&self.root_dir).map_err(Error::from)
    }

    pub fn device_descriptor(&self) -> Result<StorageDeviceDescriptor> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        Ok(StorageDeviceDescriptor {
            device_id: self.device_id.clone(),
            device_epoch: self.device_epoch,
            catalog_revision: catalog.revision,
            failure_domain: self.node_id.clone(),
        })
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        for entry in fs::read_dir(&self.staging).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file() {
                fs::remove_file(entry.path()).map_err(Error::from)?;
                report.removed_staging_files = report.removed_staging_files.saturating_add(1);
            }
        }
        for entry in fs::read_dir(&self.outboards).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file()
                && entry.file_name().to_string_lossy().ends_with(".tmp")
            {
                fs::remove_file(entry.path()).map_err(Error::from)?;
            }
        }
        // Preserve the original startup metadata scan without cloning the whole
        // historical catalog. Hashing/quarantine/WAL work below runs after unlock.
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut damaged = Vec::new();
        for record in catalog.records.values() {
            // Version first: every persisted descriptor is identified before
            // its shape and digest; later reads use these checked records.
            if let Some(table) = &record.segment_crc {
                table.check_format(format_args!("of chunk {}", record.chunk.id.0))?;
            }
            if record.chunk.is_segment_crc32c()
                && (!record.chunk.valid_immutable_identity()
                    || !record.segment_crc.as_ref().is_some_and(|t| {
                        t.valid_shape(record.chunk.length)
                            && t.descriptor_digest() == record.chunk.content_digest
                    }))
            {
                return Err(corrupt(
                    &record.chunk.id,
                    "invalid persisted CRC32C descriptor",
                ));
            }
            if record.device_id != self.device_id
                || record.device_epoch != self.device_epoch
                || record.stored_length != record.chunk.length
                || record.stored_checksum != record.chunk.content_digest
            {
                return Err(invalid(
                    "durable local Chunk record is internally inconsistent",
                ));
            }
            if record.state != LocalChunkState::Durable {
                continue;
            }
            let path = self.path_for_record(record)?;
            match path.metadata() {
                Ok(metadata) if metadata.is_file() && metadata.len() == record.chunk.length => {
                    report.durable_chunks = report.durable_chunks.saturating_add(1);
                }
                Ok(_) => damaged.push(record.clone()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    damaged.push(record.clone());
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
        drop(catalog);
        for record in damaged {
            self.lifecycle().quarantine_checked(&record.chunk.id)?;
        }
        for entry in fs::read_dir(&self.chunks).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
            if !self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .records
                .contains_key(&id)
            {
                report.orphan_files = report.orphan_files.saturating_add(1);
            }
        }
        Ok(report)
    }

    pub(super) fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        self.chunks.join(&id.0)
    }

    pub(super) fn path_for_record(&self, record: &LocalChunkRecord) -> Result<PathBuf> {
        match &record.location {
            PhysicalLocation::PerChunkFile { relative_path } => {
                if relative_path != &format!("chunks/{}", record.chunk.id.0) {
                    return Err(invalid("local chunk record has a non-canonical path"));
                }
                let path = self.root.join(relative_path);
                if !path.starts_with(&self.chunks) {
                    return Err(invalid("local chunk record escapes the chunk directory"));
                }
                Ok(path)
            }
        }
    }

    pub fn persist_batch(
        &self,
        staged: &[StagedChunk],
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<Vec<ReplicaAck>> {
        let _diagnostic = self.coarse_diagnostic("persist_batch", 0);
        #[cfg(any(feature = "dfs", test))]
        let batch_diagnostic =
            self.read_diagnostic(super::super::read_work::ReadDiagnosticStage::PersistBatch);
        if target.node_id != self.node_id
            || target.device.device_id != self.device_id
            || target.device.device_epoch != self.device_epoch
        {
            return Err(invalid(
                "replica target does not identify this local chunk store",
            ));
        }
        let admission = self.admit_catalog(staged)?;
        let result = (|| -> Result<Vec<ReplicaAck>> {
            if self
                .catalog
                .lock()
                .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                .revision
                < target.device.catalog_revision
            {
                return Err(invalid(
                    "local catalog is older than the placement revision floor",
                ));
            }

            let mut records = Vec::new();
            let mut installed_chunk_ids = HashSet::new();
            for item in staged {
                let mut replace_existing = false;
                let existing = self
                    .catalog
                    .lock()
                    .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
                    .records
                    .get(&item.chunk.id)
                    .cloned();
                let set_capacity = installed_chunk_ids.capacity()
                    * (std::mem::size_of::<ChunkId>() + std::mem::size_of::<usize>() + 1)
                    * 2
                    + installed_chunk_ids
                        .iter()
                        .map(|id: &ChunkId| id.0.capacity())
                        .sum::<usize>();
                self.observe_catalog_preparing(
                    &admission,
                    catalog_commit::record_capacity(&records)
                        + set_capacity
                        + existing
                            .as_ref()
                            .map_or(0, catalog_commit::record_allocation),
                )?;
                #[cfg(test)]
                self.catalog_commit_test.pause_prepare()?;
                if let Some(existing) = existing.as_ref() {
                    if existing.chunk != item.chunk {
                        return Err(invalid(
                            "immutable Chunk ID collides with another local record",
                        ));
                    }
                    if existing.state == LocalChunkState::Quarantined {
                        replace_existing = true;
                    } else {
                        match if existing.segment_crc.is_some() {
                            self.verify_crc_file(
                                &self.path_for_record(existing)?,
                                &item.chunk,
                                existing.segment_crc.as_ref().ok_or_else(|| {
                                    corrupt(&item.chunk.id, "CRC descriptor missing")
                                })?,
                            )
                        } else {
                            verify_file(&self.path_for_record(existing)?, &item.chunk)
                        } {
                            Ok(()) => continue,
                            Err(error) if verified_copy_is_invalid(&error) => {
                                replace_existing = true
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                let temp_path = self.staging.join(format!(
                    "{}.{}.tmp",
                    safe_id(&item.operation_id.0),
                    self.next_staging.fetch_add(1, Ordering::Relaxed)
                ));
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temp_path)
                    .map_err(Error::from)?;
                {
                    let _diagnostic =
                        self.coarse_diagnostic("persist_body_write", item.chunk.length);
                    file.write_all(item.bytes()).map_err(Error::from)?;
                }
                {
                    let _diagnostic =
                        self.coarse_diagnostic("persist_body_sync", item.chunk.length);
                    file.sync_all().map_err(Error::from)?;
                }
                let final_path = self.chunk_path(&item.chunk.id);
                if replace_existing {
                    // Publish a new inode; never truncate a file held by readers.
                    // Its immutable content identity is unchanged. Directory and
                    // catalog barriers below still precede the durable ACK.
                    fs::rename(&temp_path, &final_path).map_err(Error::from)?;
                    admission.namespace_changed();
                } else {
                    match fs::hard_link(&temp_path, &final_path) {
                        Ok(()) => admission.namespace_changed(),
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if let Some(table) = &item.segment_crc {
                                self.verify_crc_file(&final_path, &item.chunk, table)?;
                                // Adopting a synced but unpublished orphan still
                                // requires the namespace barrier before its ACK.
                                admission.namespace_changed();
                            } else {
                                verify_file(&final_path, &item.chunk)?;
                            }
                        }
                        Err(error) => return Err(Error::from(error)),
                    }
                    fs::remove_file(&temp_path).map_err(Error::from)?;
                }
                if installed_chunk_ids.insert(item.chunk.id.clone()) {
                    records.push(LocalChunkRecord {
                        location: PhysicalLocation::PerChunkFile {
                            relative_path: format!("chunks/{}", item.chunk.id.0),
                        },
                        segment_crc: item.segment_crc.as_deref().cloned(),
                        stored_length: item.chunk.length,
                        stored_checksum: item.chunk.content_digest.clone(),
                        device_id: self.device_id.clone(),
                        device_epoch: self.device_epoch,
                        chunk: item.chunk.clone(),
                        encoding: PhysicalEncoding::Raw,
                        state: LocalChunkState::Durable,
                        catalog_revision: 0,
                    });
                }
                let set_capacity = installed_chunk_ids.capacity()
                    * (std::mem::size_of::<ChunkId>() + std::mem::size_of::<usize>() + 1)
                    * 2
                    + installed_chunk_ids
                        .iter()
                        .map(|id| id.0.capacity())
                        .sum::<usize>();
                self.observe_catalog_preparing(
                    &admission,
                    catalog_commit::record_capacity(&records)
                        + set_capacity
                        + temp_path.capacity()
                        + final_path.capacity()
                        + existing
                            .as_ref()
                            .map_or(0, catalog_commit::record_allocation),
                )?;
            }
            drop(installed_chunk_ids);
            let catalog_revision = self.commit_catalog(&admission, records)?;
            admission.confirm();
            // Optional proof installation remains after every required durability barrier.
            self.sidecar_installer.enqueue_batch(self, staged);
            let acks = staged
                .iter()
                .map(|item| ReplicaAck {
                    operation_id: item.operation_id.clone(),
                    chunk_id: item.chunk.id.clone(),
                    placement_revision,
                    placement_epoch,
                    node_id: target.node_id.clone(),
                    node_epoch: target.node_epoch,
                    device_id: target.device.device_id.clone(),
                    device_epoch: target.device.device_epoch,
                    catalog_revision,
                    persisted_bytes: item.chunk.length,
                    verified_digest: item.chunk.content_digest.clone(),
                })
                .collect();
            #[cfg(any(feature = "dfs", test))]
            if let Some(diagnostic) = batch_diagnostic {
                diagnostic.finish(true);
            }
            Ok(acks)
        })();
        if let Err(error) = &result {
            admission.fail_if_unconfirmed(error.clone());
        }
        result
    }

    pub fn persist(
        &self,
        staged: &StagedChunk,
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<ReplicaAck> {
        self.persist_batch(
            std::slice::from_ref(staged),
            target,
            placement_revision,
            placement_epoch,
        )?
        .pop()
        .ok_or_else(|| invalid("local batch finalize returned no ReplicaAck"))
    }

    pub fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        self.open_verified(chunk_id)?
            .read_at(offset, out)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.lifecycle()
                        .quarantine(chunk_id, QuarantineTrigger::LocalRead);
                }
            })
    }

    /// Fill a caller-owned, unpublished range with one complete integrity scan.
    /// The output may be dirtied on failure and must not be returned to a peer
    /// until this operation succeeds. No verified reader or cache is retained.
    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    pub(crate) fn read_range_at_uncommitted(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let reader = self.open_cataloged(chunk_id)?;
        let result = verified_ranges_into(
            &reader.file,
            &reader.chunk,
            &[VerifiedRangeCopy {
                chunk_offset: offset,
                length: out.len(),
                output_offset: 0,
            }],
            out,
            true,
            self.read_work.as_ref(),
        );
        result.map(|counts| counts[0]).inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.lifecycle()
                    .quarantine(chunk_id, QuarantineTrigger::UncommittedRangeRead);
            }
        })
    }

    pub fn open_verified(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        let reader = self.open_cataloged(chunk_id)?;
        reader.verify_full_and_build().inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.lifecycle()
                    .quarantine(chunk_id, QuarantineTrigger::VerifiedOpen);
            }
        })?;
        Ok(reader)
    }

    /// Pin the exact receipt object without an eager full scan. Returning this
    /// descriptor is not an integrity claim: read_at/read_ranges still validate
    /// the delivered coverage groups or use the existing full-scan fallback.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn open_receipt_reader(&self, expected: &ChunkObject) -> Result<PinnedChunkReader> {
        let reader = self.open_cataloged(&expected.id)?;
        if reader.chunk != *expected {
            return Err(corrupt(
                &expected.id,
                "catalog differs from native receipt object",
            ));
        }
        Ok(reader)
    }

    // Pin the cataloged inode without claiming its integrity. Callers must
    // verify requested coverage groups or the complete object before delivery.
    // Direct verifier also handles unregistered orphans and missing files. It must
    // not call open_cataloged, whose read-error quarantine hook would recurse.
    pub(super) fn verify_crc_file(
        &self,
        path: &Path,
        chunk: &ChunkObject,
        table: &super::super::chunk_crc32c::CrcTable,
    ) -> Result<()> {
        let file = File::open(path).map_err(Error::from)?;
        let identity = PeerFileIdentity::read(&file)?;
        let reader = PinnedChunkReader {
            verify_crc_body: true,
            segment_crc: Some(table.clone()),
            file,
            chunk: chunk.clone(),
            outboard_path: PathBuf::new(),
            outboard_budget: self.outboard_budget.clone(),
            outboard_repairs: self.outboard_repairs.clone(),
            outboard: Mutex::new(None),
            proof_identity: Mutex::new(identity),
            read_work: None,
        };
        reader.verify_full_and_build()
    }

    pub(super) fn open_cataloged(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        #[cfg(feature = "dfs")]
        self.record_read_check(super::super::read_work::ReadCheck::CatalogOpen);
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| invalid(format!("local Chunk '{}' is not cataloged", chunk_id.0)))?;
        if record.state != LocalChunkState::Durable {
            return Err(corrupt(chunk_id, "local copy is quarantined"));
        }
        let file = File::open(self.path_for_record(&record)?)
            .map_err(Error::from)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.lifecycle()
                        .quarantine(chunk_id, QuarantineTrigger::CatalogOpen);
                }
            })?;
        if let Some(expected) = ChunkObject::from_content_addressed_id(chunk_id)
            && expected != record.chunk
        {
            return Err(corrupt(
                chunk_id,
                "catalog differs from content-addressed identity",
            ));
        }
        if record.chunk.is_segment_crc32c() != record.segment_crc.is_some() {
            return Err(corrupt(
                chunk_id,
                "CRC32C format and checksum descriptor differ",
            ));
        }
        if record.chunk.is_segment_crc32c() {
            let table = record
                .segment_crc
                .as_ref()
                .ok_or_else(|| corrupt(chunk_id, "CRC descriptor missing"))?;
            if !table.valid_shape(record.chunk.length)
                || table.descriptor_digest() != record.chunk.content_digest
            {
                return Err(corrupt(chunk_id, "CRC descriptor identity differs"));
            }
        }
        let proof_identity = Mutex::new(PeerFileIdentity::read(&file)?);
        Ok(PinnedChunkReader {
            verify_crc_body: true,
            segment_crc: record.segment_crc.clone(),
            file,
            proof_identity,
            outboard_path: self
                .outboards
                .join(format!("{}.bao", safe_id(&record.chunk.id.0))),
            outboard_budget: self.outboard_budget.clone(),
            outboard_repairs: self.outboard_repairs.clone(),
            outboard: Mutex::new(None),
            chunk: record.chunk,
            read_work: self.read_work.clone(),
        })
    }
}

#[cfg(test)]
impl ChunkSink for LocalChunkStore {
    fn put_chunks(&self, staged: Vec<StagedChunk>, _intent: PutIntent) -> Result<Vec<StoredChunk>> {
        let descriptor = self.device_descriptor()?;
        let target = ReplicaTarget {
            node_id: self.node_id.clone(),
            node_epoch: 1,
            data_endpoint: String::new(),
            device: descriptor,
        };
        let acks = self.persist_batch(&staged, &target, 1, 1)?;
        Ok(staged
            .into_iter()
            .zip(acks)
            .map(|(item, ack)| {
                StoredChunk::unreleased(ChunkReceipt {
                    external_commit: None,
                    operation_id: item.operation_id,
                    chunk: item.chunk,
                    placement_revision: 1,
                    placement_epoch: 1,
                    replica_group_id: ReplicaGroupId::new(format!("local:{}", self.node_id)),
                    durable_acks: vec![ack],
                })
            })
            .collect())
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        LocalChunkStore::capacity(self)
    }
}
