//! Disposable read copies share the ordinary immutable files and catalog WAL.
use super::*;

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
                let bytes = serde_json::to_vec(&(revision, &records, &removed))
                    .map_err(|e| invalid(e.to_string()))?;
                let txn = CatalogTxn {
                    revision,
                    records,
                    removed,
                    checksum: blake3::hash(&bytes).to_hex().to_string(),
                };
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
                    self.retire_fixed_peer_readers(&txn.removed)?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
