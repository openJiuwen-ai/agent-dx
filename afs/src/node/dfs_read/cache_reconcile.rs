//! Catalog-backed desired state. Runtime state is only an optimization; after a
//! crash every pending publication/retirement is reconstructed from the same WAL.
use super::super::chunk::ReadCacheIntent;
use super::*;

#[derive(Default)]
pub(super) struct CacheSyncState {
    confirmed: BTreeMap<ChunkId, ReadCacheIntent>,
    cursor: usize,
}

impl DfsReadEngine {
    pub(crate) async fn reconcile_read_caches(&self, deadline: Instant) -> Result<usize> {
        let Some(session) = &self.cache_session else {
            return Ok(0);
        };
        let authority = self.capture_native_read_authority()?;
        if authority.epoch == 0 {
            return Ok(0);
        }
        let mut state = tokio::time::timeout_at(deadline.into(), self.cache_sync.lock())
            .await
            .map_err(|_| unavailable("cache reconciliation wait expired"))?;
        let intents = self.local.read_cache_intents()?;
        state
            .confirmed
            .retain(|id, _| intents.iter().any(|i| &i.copy.chunk_id == id));
        if intents.is_empty() {
            return Ok(0);
        }
        let start = state.cursor % intents.len();
        let mut completed = 0;
        let mut failure = None;
        for intent in intents
            .iter()
            .cycle()
            .skip(start)
            .take(intents.len().min(16))
        {
            if Instant::now() >= deadline {
                break;
            }
            state.cursor = state.cursor.wrapping_add(1);
            let verified = state.confirmed.get(&intent.copy.chunk_id) == Some(intent);
            let store = self.local.clone();
            let expected = intent.clone();
            let refreshed = tokio::task::spawn_blocking(move || {
                store.refresh_read_cache_intent(&expected, authority.epoch, verified)
            })
            .await
            .map_err(|e| unavailable(format!("cache reconciliation worker failed: {e}")))?;
            let current = match refreshed {
                Ok(value) => value,
                Err(error) => {
                    failure = Some(error);
                    continue;
                }
            };
            self.check_native_read_authority(&authority)?;
            let result = if let Some(current) = current {
                if state.confirmed.get(&current.copy.chunk_id) == Some(&current) {
                    continue;
                }
                let operation_id = cache_sync_operation("publish", session, &current)?;
                let CopyLocation::Node {
                    node_id,
                    node_epoch,
                    device_id,
                    device_epoch,
                    catalog_revision,
                } = &current.copy.location
                else {
                    return Err(invalid("cache intent must identify a node copy"));
                };
                let receipt = crate::dfs::ReplicaAck {
                    operation_id: operation_id.clone(),
                    chunk_id: current.copy.chunk_id.clone(),
                    placement_revision: 0,
                    placement_epoch: 0,
                    node_id: node_id.clone(),
                    node_epoch: *node_epoch,
                    device_id: device_id.clone(),
                    device_epoch: *device_epoch,
                    catalog_revision: *catalog_revision,
                    persisted_bytes: current.copy.persisted_bytes,
                    verified_digest: current.copy.verified_digest.clone(),
                };
                self.source_provider
                    .publish_read_cache(
                        crate::dfs::PublishReadCache {
                            caller_id: self.local_node_id.clone(),
                            caller_session_id: session.clone(),
                            caller_node_epoch: authority.epoch,
                            operation_id,
                            namespace_id: current.namespace_id.clone(),
                            file_version_id: current.file_version_id.clone(),
                            layout_root_id: current.layout_root_id.clone(),
                            receipt,
                        },
                        deadline,
                    )
                    .await
                    .map(|_| {
                        state
                            .confirmed
                            .insert(current.copy.chunk_id.clone(), current);
                    })
            } else {
                let operation_id = cache_sync_operation("retire", session, intent)?;
                match self
                    .source_provider
                    .retire_read_cache(
                        crate::dfs::RetireReadCache {
                            caller_id: self.local_node_id.clone(),
                            caller_session_id: session.clone(),
                            caller_node_epoch: authority.epoch,
                            operation_id,
                            copy: intent.copy.clone(),
                        },
                        deadline,
                    )
                    .await
                {
                    Ok(()) => {
                        let store = self.local.clone();
                        let expected = intent.clone();
                        tokio::task::spawn_blocking(move || {
                            store.acknowledge_cache_retirement(&expected)
                        })
                        .await
                        .map_err(|e| unavailable(format!("cache acknowledgement failed: {e}")))??;
                        state.confirmed.remove(&intent.copy.chunk_id);
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            };
            match result {
                Ok(()) => completed += 1,
                Err(error) => failure = Some(error),
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(completed)
    }
}

fn cache_sync_operation(
    kind: &str,
    session: &str,
    intent: &ReadCacheIntent,
) -> Result<crate::dfs::OperationId> {
    let identity = serde_json::to_vec(&(session, intent)).map_err(|e| invalid(e.to_string()))?;
    Ok(crate::dfs::OperationId::new(format!(
        "cache-{kind}:{}",
        blake3::hash(&identity).to_hex()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::chunk::{ChunkStore, StagedChunk};
    use std::sync::atomic::AtomicBool;

    #[derive(Default)]
    struct Meta {
        fail_publish: AtomicBool,
        fail_retire: AtomicBool,
        copies: Mutex<BTreeMap<crate::dfs::CopyId, crate::dfs::CopyRecord>>,
        publications: Mutex<Vec<crate::dfs::PublishReadCache>>,
        retirements: Mutex<Vec<crate::dfs::RetireReadCache>>,
    }
    impl ReadSourceProvider for Meta {
        fn sources_for(&self, _: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            Ok(Default::default())
        }
        fn publish_read_cache(
            &self,
            request: crate::dfs::PublishReadCache,
            _: Instant,
        ) -> super::super::super::read_pipeline::ReadFuture<'_, crate::dfs::CopyRecord> {
            Box::pin(async move {
                let copy = crate::dfs::read_cache_copy(&request.receipt)?;
                self.copies
                    .lock()
                    .unwrap()
                    .insert(copy.id.clone(), copy.clone());
                self.publications.lock().unwrap().push(request);
                if self.fail_publish.load(Ordering::SeqCst) {
                    return Err(unavailable("lost publication response"));
                }
                Ok(copy)
            })
        }
        fn retire_read_cache(
            &self,
            request: crate::dfs::RetireReadCache,
            _: Instant,
        ) -> super::super::super::read_pipeline::ReadFuture<'_, ()> {
            Box::pin(async move {
                let mut copies = self.copies.lock().unwrap();
                if copies.get(&request.copy.id) == Some(&request.copy) {
                    copies.remove(&request.copy.id);
                }
                self.retirements.lock().unwrap().push(request);
                if self.fail_retire.load(Ordering::SeqCst) {
                    return Err(unavailable("lost retirement response"));
                }
                Ok(())
            })
        }
    }
    fn engine(path: &std::path::Path, meta: Arc<Meta>, epoch: u64) -> DfsReadEngine {
        DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "n".into(),
            Arc::new(LocalChunkStore::open(path, "n").unwrap()),
            meta,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_cache_session(format!("session-{epoch}"))
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            epoch,
            Instant::now() + Duration::from_secs(60),
        )))
    }
    fn install(engine: &DfsReadEngine, staged: &StagedChunk, epoch: u64) -> ReadCacheIntent {
        let ack = engine
            .local
            .persist_read_cache(staged, epoch, 4100)
            .unwrap()
            .receipt
            .unwrap();
        let intent = ReadCacheIntent {
            namespace_id: crate::dfs::NamespaceId::new("default"),
            file_version_id: crate::dfs::FileVersionId::new("version"),
            layout_root_id: crate::dfs::LayoutRootId::new("layout"),
            copy: crate::dfs::read_cache_copy(&ack).unwrap(),
        };
        assert!(engine.local.track_read_cache(intent.clone()).unwrap());
        intent
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[tokio::test]
    async fn recovery_cache_retry_restart_and_checkpoint_retirement_converge() {
        let temp = tempfile::tempdir().unwrap();
        let meta = Arc::new(Meta::default());
        let first = engine(temp.path(), meta.clone(), 1);
        let chunk = StagedChunk::new(crate::dfs::OperationId::new("cache"), b"data".to_vec());
        install(&first, &chunk, 1);
        meta.fail_publish.store(true, Ordering::SeqCst);
        assert!(first.reconcile_read_caches(deadline()).await.is_err());
        assert_eq!(first.local.read_cache_intents().unwrap().len(), 1);
        assert!(first.reconcile_read_caches(deadline()).await.is_err());
        let requests = meta.publications.lock().unwrap().clone();
        assert_eq!(
            requests[0], requests[1],
            "unknown outcome retries exact request"
        );
        drop(first);
        meta.fail_publish.store(false, Ordering::SeqCst);
        let second = engine(temp.path(), meta.clone(), 2);
        assert_eq!(second.reconcile_read_caches(deadline()).await.unwrap(), 1);
        assert_eq!(
            meta.publications
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .receipt
                .node_epoch,
            2
        );
        // GC rotates the existing catalog. Its snapshot must keep the retirement intent.
        second
            .local
            .collect_generation(1, std::slice::from_ref(&chunk.chunk.id))
            .unwrap();
        meta.fail_retire.store(true, Ordering::SeqCst);
        assert!(second.reconcile_read_caches(deadline()).await.is_err());
        assert_eq!(second.local.read_cache_intents().unwrap().len(), 1);
        drop(second);
        meta.fail_retire.store(false, Ordering::SeqCst);
        let third = engine(temp.path(), meta.clone(), 3);
        assert_eq!(third.reconcile_read_caches(deadline()).await.unwrap(), 1);
        assert!(third.local.read_cache_intents().unwrap().is_empty());
        assert!(meta.copies.lock().unwrap().is_empty());
        drop(third);
        assert!(
            engine(temp.path(), meta, 4)
                .local
                .read_cache_intents()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn recovery_cache_reinstall_rejects_stale_ack_and_promotion_keeps_body() {
        let temp = tempfile::tempdir().unwrap();
        let meta = Arc::new(Meta::default());
        let e = engine(temp.path(), meta.clone(), 1);
        let a = StagedChunk::new(crate::dfs::OperationId::new("a"), b"aaaa".to_vec());
        let b = StagedChunk::new(crate::dfs::OperationId::new("b"), b"bbbb".to_vec());
        let old = install(&e, &a, 1);
        e.reconcile_read_caches(deadline()).await.unwrap();
        install(&e, &b, 1);
        let new = install(&e, &a, 1);
        assert_ne!(old.copy.location, new.copy.location);
        e.local.acknowledge_cache_retirement(&old).unwrap();
        assert!(e.local.read_cache_intents().unwrap().contains(&new));
        assert!(e.local.refresh_read_cache_intent(&old, 1, false).is_err());
        e.reconcile_read_caches(deadline()).await.unwrap();
        assert_eq!(
            meta.copies.lock().unwrap().get(&new.copy.id),
            Some(&new.copy)
        );
        e.local.put(a.clone()).unwrap();
        e.reconcile_read_caches(deadline()).await.unwrap();
        assert!(e.local.open_verified(&a.chunk.id).is_ok());
        assert!(meta.copies.lock().unwrap().is_empty());
        assert!(e.local.read_cache_intents().unwrap().is_empty());
    }
}
