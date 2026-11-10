//! Disposable read copies share the ordinary immutable files and catalog WAL.
use super::*;

/// Provenance for discovery, not a new file/object identity. The catalog retains
/// this desired copy until its absence has been acknowledged by Meta.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReadCacheIntent {
    pub(crate) namespace_id: crate::dfs::NamespaceId,
    pub(crate) file_version_id: crate::dfs::FileVersionId,
    pub(crate) layout_root_id: crate::dfs::LayoutRootId,
    pub(crate) copy: crate::dfs::CopyRecord,
}

pub(crate) struct ReadCacheWrite {
    pub(crate) receipt: Option<ReplicaAck>,
    pub(crate) retired: Vec<crate::dfs::CopyRecord>,
}

impl LocalChunkStore {
    pub(crate) fn persist_read_cache(
        &self,
        staged: &StagedChunk,
        node_epoch: u64,
        capacity: u64,
    ) -> Result<ReadCacheWrite> {
        let mut result = ReadCacheWrite {
            receipt: None,
            retired: Vec::new(),
        };
        let charge = staged
            .chunk
            .length
            .checked_add(4096)
            .ok_or_else(|| invalid("read cache capacity overflow"))?;
        if capacity == 0 || charge > capacity {
            return Ok(result);
        }
        if node_epoch == 0 {
            return Err(invalid("read cache requires live node epoch"));
        }
        let _cache_admission = self
            .read_cache_admission
            .lock()
            .map_err(|_| invalid("read cache admission poisoned"))?;
        // Reuse catalog exclusion: promotions and ordinary installs cannot race eviction.
        {
            let _admission = self
                .catalog_committer
                .admit(std::iter::empty(), usize::MAX)?;
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| invalid("chunk catalog poisoned"))?;
            if catalog
                .records
                .get(&staged.chunk.id)
                .is_some_and(|r| r.read_cache_epoch == 0)
            {
                return Ok(result); // Never downgrade an ordinary retained copy.
            }
            let mut total = catalog.cache_bytes;
            if !catalog.records.contains_key(&staged.chunk.id) {
                total = total.saturating_add(charge);
            }
            let mut victims = Vec::new();
            for (_, id) in catalog.cache_order.iter().take(256) {
                if total <= capacity {
                    break;
                }
                if id == &staged.chunk.id {
                    continue;
                }
                let record = catalog
                    .records
                    .get(id)
                    .ok_or_else(|| invalid("cache catalog index disagrees"))?;
                total = total.saturating_sub(record.stored_length.saturating_add(4096));
                victims.push(record.clone());
            }
            // Avoid deleting anything when this admission cannot fit within its work bound.
            if total > capacity {
                return Ok(result);
            }
            if !victims.is_empty() {
                let revision = catalog
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| invalid("catalog revision exhausted"))?;
                let removed = victims
                    .iter()
                    .map(|r| r.chunk.id.clone())
                    .collect::<Vec<_>>();
                let records: Vec<LocalChunkRecord> = Vec::new();
                let mut txn = CatalogTxn {
                    cache_updates: Vec::new(),
                    cache_acks: Vec::new(),
                    revision,
                    records,
                    removed,
                    checksum: String::new(),
                };
                txn.checksum = txn.expected_checksum()?;
                // Fence the catalog first. A crash can leave disposable orphan bytes, never
                // a surviving catalog entry falsely claiming a removed file is present.
                if let Err(error) = append_catalog_txn_observed(
                    &self.catalog_path,
                    &txn,
                    self.read_work.as_ref(),
                    |_| Ok(()),
                ) {
                    return Err(self.catalog_committer.fail(error).as_ref().clone());
                }
                for id in &txn.removed {
                    catalog.remove_record(id);
                }
                catalog.revision = revision;
                drop(catalog);
                #[cfg(any(feature = "dfs", test))]
                {
                    let mut state = self
                        .peer_read_cache
                        .lock()
                        .map_err(|_| invalid("peer read cache poisoned"))?;
                    for id in &txn.removed {
                        state.remove_resident(id);
                    }
                    self.observe_peer_serve(&state);
                    self.peer_serve_changed.notify_all();
                }
                for record in victims {
                    result.retired.push(crate::dfs::read_cache_copy(
                        &self.cache_receipt(&record, OperationId::new("cache-evicted")),
                    )?);
                    for path in [
                        self.path_for_record(&record)?,
                        self.outboards
                            .join(format!("{}.bao", safe_id(&record.chunk.id.0))),
                    ] {
                        match fs::remove_file(path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                File::open(&self.chunks)?.sync_all()?;
                File::open(&self.outboards)?.sync_all()?;
            }
        }
        let target = ReplicaTarget {
            node_id: self.node_id.clone(),
            node_epoch,
            data_endpoint: String::new(),
            device: self.device_descriptor()?,
        };
        self.persist_batch_kind(std::slice::from_ref(staged), &target, 0, 0, node_epoch)?;
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        if let Some(record) = catalog
            .records
            .get(&staged.chunk.id)
            .filter(|r| r.read_cache_epoch == node_epoch)
        {
            result.receipt = Some(self.cache_receipt(record, staged.operation_id.clone()));
        }
        Ok(result)
    }

    fn cache_receipt(&self, record: &LocalChunkRecord, operation_id: OperationId) -> ReplicaAck {
        ReplicaAck {
            operation_id,
            chunk_id: record.chunk.id.clone(),
            placement_revision: 0,
            placement_epoch: 0,
            node_id: self.node_id.clone(),
            node_epoch: record.read_cache_epoch,
            device_id: record.device_id.clone(),
            device_epoch: record.device_epoch,
            catalog_revision: record.catalog_revision,
            persisted_bytes: record.stored_length,
            verified_digest: record.stored_checksum.clone(),
        }
    }
}

impl LocalChunkStore {
    /// Persist before sending any publication. Removing the physical record
    /// leaves this intent in the same WAL until exact retirement is acknowledged.
    pub(crate) fn track_read_cache(&self, intent: ReadCacheIntent) -> Result<bool> {
        let _admission = self
            .catalog_committer
            .admit(std::iter::empty(), usize::MAX)?;
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        let Some(record) = catalog.records.get(&intent.copy.chunk_id) else {
            return Ok(false);
        };
        if record.read_cache_epoch == 0
            || record.state != LocalChunkState::Durable
            || crate::dfs::read_cache_copy(
                &self.cache_receipt(record, OperationId::new("observe")),
            )? != intent.copy
        {
            return Ok(false);
        }
        if catalog.cache_intents.get(&intent.copy.chunk_id) == Some(&intent) {
            return Ok(true);
        }
        if catalog.cache_intents.len() >= 1024
            && !catalog.cache_intents.contains_key(&intent.copy.chunk_id)
        {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "read cache reconciliation backlog is full",
            ));
        }
        self.commit_cache_sync(&mut catalog, Vec::new(), vec![intent], Vec::new())?;
        Ok(true)
    }

    pub(crate) fn read_cache_intents(&self) -> Result<Vec<ReadCacheIntent>> {
        Ok(self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?
            .cache_intents
            .values()
            .cloned()
            .collect())
    }

    /// Return the current desired copy. Full physical verification is required
    /// before first publication in this process; subsequent ticks only check
    /// catalog/namespace presence unless the incarnation changed.
    pub(crate) fn refresh_read_cache_intent(
        &self,
        expected: &ReadCacheIntent,
        epoch: u64,
        verified: bool,
    ) -> Result<Option<ReadCacheIntent>> {
        if epoch == 0 {
            return Err(invalid("cache reconciliation requires current node epoch"));
        }
        let record = {
            let catalog = self
                .catalog
                .lock()
                .map_err(|_| invalid("chunk catalog poisoned"))?;
            if catalog.cache_intents.get(&expected.copy.chunk_id) != Some(expected) {
                return Err(Error::coded(
                    afs_error::NODE_DFS_STALE_HANDLE,
                    "cache intent changed",
                ));
            }
            catalog.records.get(&expected.copy.chunk_id).cloned()
        };
        let Some(record) =
            record.filter(|r| r.read_cache_epoch != 0 && r.state == LocalChunkState::Durable)
        else {
            return Ok(None);
        };
        let path = self.path_for_record(&record)?;
        let physical = if verified
            && crate::dfs::read_cache_copy(
                &self.cache_receipt(&record, OperationId::new("observe")),
            )? == expected.copy
        {
            match fs::metadata(&path) {
                Ok(meta) if meta.is_file() && meta.len() == record.stored_length => Ok(()),
                Ok(_) => Err(corrupt(&record.chunk.id, "cache physical length changed")),
                Err(error) => Err(error.into()),
            }
        } else if let Some(table) = &record.segment_crc {
            self.verify_crc_file(&path, &record.chunk, table)
        } else {
            verify_file(&path, &record.chunk)
        };
        if let Err(error) = physical {
            if verified_copy_is_invalid(&error) {
                return Ok(None);
            }
            return Err(error);
        }
        let _admission = self
            .catalog_committer
            .admit(std::iter::empty(), usize::MAX)?;
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        if catalog.cache_intents.get(&expected.copy.chunk_id) != Some(expected) {
            return Err(Error::coded(
                afs_error::NODE_DFS_STALE_HANDLE,
                "cache intent changed",
            ));
        }
        if catalog.records.get(&record.chunk.id).is_none_or(|current| {
            current.catalog_revision != record.catalog_revision
                || current.state != record.state
                || current.read_cache_epoch != record.read_cache_epoch
        }) {
            return Ok(None);
        }
        let mut current = record;
        let mut records = Vec::new();
        if current.read_cache_epoch != epoch {
            current.read_cache_epoch = epoch;
            current.catalog_revision = catalog
                .revision
                .checked_add(1)
                .ok_or_else(|| invalid("catalog revision exhausted"))?;
            records.push(current.clone());
        }
        let mut updated = expected.clone();
        updated.copy = crate::dfs::read_cache_copy(
            &self.cache_receipt(&current, OperationId::new("observe")),
        )?;
        if updated != *expected || !records.is_empty() {
            self.commit_cache_sync(&mut catalog, records, vec![updated.clone()], Vec::new())?;
        }
        Ok(Some(updated))
    }

    pub(crate) fn acknowledge_cache_retirement(&self, expected: &ReadCacheIntent) -> Result<()> {
        let _admission = self
            .catalog_committer
            .admit(std::iter::empty(), usize::MAX)?;
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?;
        if catalog.cache_intents.get(&expected.copy.chunk_id) == Some(expected) {
            self.commit_cache_sync(&mut catalog, Vec::new(), Vec::new(), vec![expected.clone()])?;
        }
        Ok(())
    }

    fn commit_cache_sync(
        &self,
        catalog: &mut LocalCatalog,
        mut records: Vec<LocalChunkRecord>,
        updates: Vec<ReadCacheIntent>,
        acknowledgements: Vec<ReadCacheIntent>,
    ) -> Result<()> {
        let revision = catalog
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("catalog revision exhausted"))?;
        for record in &mut records {
            record.catalog_revision = revision;
        }
        let mut txn = CatalogTxn {
            revision,
            records,
            removed: Vec::new(),
            cache_updates: updates,
            cache_acks: acknowledgements,
            checksum: String::new(),
        };
        txn.checksum = txn.expected_checksum()?;
        if let Err(error) =
            append_catalog_txn_observed(&self.catalog_path, &txn, self.read_work.as_ref(), |_| {
                Ok(())
            })
        {
            return Err(self.catalog_committer.fail(error).as_ref().clone());
        }
        for record in txn.records {
            catalog.insert_record(record);
        }
        catalog.apply_cache_sync(txn.cache_updates, txn.cache_acks);
        catalog.revision = revision;
        Ok(())
    }
}

impl LocalChunkStore {
    pub(crate) fn has_durable_record(&self, id: &ChunkId) -> Result<bool> {
        Ok(self
            .catalog
            .lock()
            .map_err(|_| invalid("chunk catalog poisoned"))?
            .records
            .get(id)
            .is_some_and(|r| r.state == LocalChunkState::Durable && r.read_cache_epoch == 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_cache_intent_survives_catalog_replay() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("intent"), b"data".to_vec());
        let ack = store
            .persist_read_cache(&staged, 1, 4100)
            .unwrap()
            .receipt
            .unwrap();
        let intent = ReadCacheIntent {
            namespace_id: crate::dfs::NamespaceId::new("default"),
            file_version_id: crate::dfs::FileVersionId::new("version"),
            layout_root_id: crate::dfs::LayoutRootId::new("layout"),
            copy: crate::dfs::read_cache_copy(&ack).unwrap(),
        };
        assert!(store.track_read_cache(intent.clone()).unwrap());
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(
            store
                .catalog
                .lock()
                .unwrap()
                .cache_intents
                .get(&staged.chunk.id),
            Some(&intent)
        );
        store
            .collect_generation(1, std::slice::from_ref(&staged.chunk.id))
            .unwrap();
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(store.read_cache_intents().unwrap(), vec![intent.clone()]);
        assert!(
            store
                .refresh_read_cache_intent(&intent, 2, false)
                .unwrap()
                .is_none()
        );
        store.acknowledge_cache_retirement(&intent).unwrap();
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(store.read_cache_intents().unwrap().is_empty());
    }

    #[test]
    fn distribution_cache_store_reuses_catalog_and_evicts_only_disposable_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let durable = StagedChunk::new(OperationId::new("durable"), b"keep".to_vec());
        store.put(durable.clone()).unwrap();
        let first = StagedChunk::new(OperationId::new("cache-1"), b"one!".to_vec());
        let second = StagedChunk::new(OperationId::new("cache-2"), b"two!".to_vec());
        let capacity = 4096 + 4;
        let first_receipt = store
            .persist_read_cache(&first, 1, capacity)
            .unwrap()
            .receipt
            .expect("complete read cache must persist");
        assert_eq!(first_receipt.verified_digest, first.chunk.content_digest);
        let pin = store.open_verified(&first.chunk.id).unwrap();
        let result = store.persist_read_cache(&second, 1, capacity).unwrap();
        assert!(result.receipt.is_some());
        assert_eq!(result.retired.len(), 1);
        assert_eq!(result.retired[0].chunk_id, first.chunk.id);
        assert!(store.open_verified(&first.chunk.id).is_err());
        assert!(store.open_verified(&durable.chunk.id).is_ok());
        let mut bytes = [0; 4];
        assert_eq!(pin.read_at(0, &mut bytes).unwrap(), 4);
        assert_eq!(
            &bytes, b"one!",
            "admitted reader retains old immutable file"
        );
        drop(pin);
        drop(store);
        let reopened = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(reopened.open_verified(&durable.chunk.id).is_ok());
        assert!(reopened.open_verified(&second.chunk.id).is_ok());
        assert!(reopened.open_verified(&first.chunk.id).is_err());
    }

    #[test]
    fn distribution_cache_store_promotion_survives_cache_pressure_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let first = StagedChunk::new(OperationId::new("cache-promote"), b"keep".to_vec());
        assert!(
            store
                .persist_read_cache(&first, 1, 4100)
                .unwrap()
                .receipt
                .is_some()
        );
        store.put(first.clone()).unwrap();
        drop(store);
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let second = StagedChunk::new(OperationId::new("cache-new"), b"next".to_vec());
        let result = store.persist_read_cache(&second, 2, 4100).unwrap();
        assert!(result.receipt.is_some());
        assert!(result.retired.is_empty());
        assert!(store.open_verified(&first.chunk.id).is_ok());
    }
}
