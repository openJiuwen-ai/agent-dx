//! Catalog-backed desired state. Runtime state is only an optimization; after a
//! crash every pending publication/retirement is reconstructed from the same WAL.
use super::*;
use crate::node::chunk::ReadCacheIntent;
use std::{collections::VecDeque, ops::Range};

#[derive(Default)]
struct CacheSyncState {
    confirmed: BTreeMap<ChunkId, ReadCacheIntent>,
    cursor: usize,
}

impl DfsReadEngine {
    async fn reconcile_read_caches(
        &self,
        state: &mut CacheSyncState,
        deadline: Instant,
    ) -> Result<usize> {
        let Some(cache) = &self.distributed_cache else {
            return Ok(0);
        };
        let session = &cache.session;
        let authority = self.capture_native_read_authority()?;
        if authority.epoch == 0 {
            return Ok(0);
        }
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
            let refreshed = crate::node::bridge::spawn_blocking(move || {
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
                match self
                    .source_provider
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
                {
                    Ok(_) => {
                        state
                            .confirmed
                            .insert(current.copy.chunk_id.clone(), current);
                        Ok(())
                    }
                    Err(error) if error.code() == afs_error::IO_NOT_FOUND => {
                        self.retire_cache_intent(session, authority.epoch, &current, deadline)
                            .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                self.retire_cache_intent(session, authority.epoch, intent, deadline)
                    .await
            };
            match result {
                Ok(()) => {
                    // A retired entry may remain in the sweep snapshot until
                    // the next pass, but never consumes publication authority.
                    completed += 1;
                }
                Err(error) => failure = Some(error),
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(completed)
    }

    async fn retire_cache_intent(
        &self,
        session: &str,
        epoch: u64,
        intent: &ReadCacheIntent,
        deadline: Instant,
    ) -> Result<()> {
        let operation_id = cache_sync_operation("retire", session, intent)?;
        self.source_provider
            .retire_read_cache(
                crate::dfs::RetireReadCache {
                    caller_id: self.local_node_id.clone(),
                    caller_session_id: session.to_owned(),
                    caller_node_epoch: epoch,
                    operation_id,
                    copy: intent.copy.clone(),
                },
                deadline,
            )
            .await?;
        let store = self.local.clone();
        let expected = intent.clone();
        crate::node::bridge::spawn_blocking(move || store.acknowledge_cache_retirement(&expected))
            .await
            .map_err(|error| unavailable(format!("cache acknowledgement failed: {error}")))?
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

/// One bounded queue and one supervised worker per node. Cache persistence
/// and metadata publication never gate foreground delivery.
pub(super) struct ReadCacheRuntime {
    session: String,
    capacity: u64,
    send: tokio::sync::mpsc::Sender<CacheJob>,
    receive: Mutex<Option<tokio::sync::mpsc::Receiver<CacheJob>>>,
    partial: Mutex<VecDeque<PartialCopy>>,
}

struct CacheJob {
    chunk: ChunkObject,
    body: Vec<u8>,
    version: FileVersionId,
    layout: LayoutRootId,
    epoch: u64,
}

/// Cache copies can arrive as separate verified FUSE pages. The two-entry
/// cap bounds unqueued body storage at 8 MiB and a fragmented read cannot
/// retain unbounded coverage metadata. Missing bytes are never synthesized.
const MAX_PARTIAL_COPIES: usize = 2;
const MAX_PARTIAL_RANGES: usize = 256;

struct PartialCopy {
    job: CacheJob,
    coverage: Vec<Range<usize>>,
}

impl PartialCopy {
    fn copy_range(&mut self, offset: usize, data: &[u8]) -> bool {
        let mut end = offset + data.len();
        let mut start = offset;
        self.job.body[start..end].copy_from_slice(data);
        let left = self.coverage.partition_point(|range| range.end < start);
        let mut right = left;
        while let Some(range) = self.coverage.get(right).filter(|range| range.start <= end) {
            start = start.min(range.start);
            end = end.max(range.end);
            right += 1;
        }
        self.coverage
            .splice(left..right, std::iter::once(start..end));
        matches!(self.coverage.as_slice(), [range] if range.start == 0 && range.end == self.job.body.len())
    }
}

impl ReadCacheRuntime {
    fn stage(
        &self,
        batch: &ReadBatch,
        version: &FileVersionId,
        chunk: &ChunkObject,
        epoch: u64,
        offset: usize,
        data: &[u8],
    ) {
        let make_job = |body| CacheJob {
            chunk: chunk.clone(),
            body,
            version: version.clone(),
            layout: batch.layout_root_id.clone(),
            epoch,
        };
        if offset == 0 && data.len() as u64 == chunk.length {
            if let Ok(permit) = self.send.try_reserve() {
                permit.send(make_job(data.to_vec()));
            }
            return;
        }
        let Ok(mut partial) = self.partial.try_lock() else {
            return;
        };
        let position = partial.iter().position(|copy| {
            copy.job.chunk == *chunk
                && copy.job.version == *version
                && copy.job.layout == batch.layout_root_id
                && copy.job.epoch == epoch
        });
        let mut copy = match position.and_then(|position| partial.remove(position)) {
            Some(copy) => copy,
            None => {
                if partial.len() == MAX_PARTIAL_COPIES {
                    partial.pop_front();
                }
                PartialCopy {
                    job: make_job(vec![0; chunk.length as usize]),
                    coverage: Vec::new(),
                }
            }
        };
        if copy.copy_range(offset, data)
            && let Ok(permit) = self.send.try_reserve()
        {
            permit.send(copy.job);
        } else if copy.coverage.len() <= MAX_PARTIAL_RANGES {
            partial.push_back(copy);
        }
    }
}

impl DfsReadEngine {
    pub(crate) fn with_distributed_cache(mut self, session: String, capacity: u64) -> Self {
        let (send, receive) = tokio::sync::mpsc::channel(2);
        self.distributed_cache = Some(Arc::new(ReadCacheRuntime {
            session,
            capacity,
            send,
            receive: Mutex::new(Some(receive)),
            partial: Mutex::new(VecDeque::new()),
        }));
        self
    }

    pub(super) fn cache_completed_ranges(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &[u8],
    ) {
        let (Some(cache), Some(version)) = (&self.distributed_cache, &batch.file_version_id) else {
            return;
        };
        let Ok(authority) = self.capture_native_read_authority() else {
            return;
        };
        if authority.epoch == 0 {
            return;
        }
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                continue;
            };
            let Some(chunk) = set.chunk.as_ref() else {
                continue;
            };
            if op.length == 0
                || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
                || chunk.length.saturating_add(4096) > cache.capacity
                || !range_covered(&set.allowed_ranges, 0, chunk.length)
                || op
                    .chunk_offset
                    .checked_add(op.length)
                    .is_none_or(|end| end > chunk.length)
            {
                continue;
            }
            let Some(body) =
                out.get(op.output_offset..op.output_offset.saturating_add(op.length as usize))
            else {
                continue;
            };
            cache.stage(
                batch,
                version,
                chunk,
                authority.epoch,
                op.chunk_offset as usize,
                body,
            );
        }
    }

    pub(crate) fn start_distributed_cache(
        self: &Arc<Self>,
        services: &mut crate::runtime::Services,
    ) {
        let Some(cache) = &self.distributed_cache else {
            return;
        };
        let Some(mut receive) = cache
            .receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        else {
            return;
        };
        let engine = self.clone();
        let capacity = cache.capacity;
        let stop = crate::runtime::cancelled(services.stop.subscribe());
        services.spawn(async move {
            tokio::pin!(stop);
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut state = CacheSyncState::default();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop => return Ok(()),
                    _ = interval.tick() => {
                        let deadline = Instant::now() + engine.read_pipeline_timeout();
                        if let Err(error) = engine.reconcile_read_caches(&mut state, deadline).await {
                            afs_logging::warn!("dfs.read_cache_reconcile_retry"; "error" => error.to_string());
                        }
                    }
                    job = receive.recv() => {
                        let Some(job) = job else { return Ok(()) };
                        let local = engine.local.clone();
                        let namespace = engine.namespace_id.clone();
                        let result = crate::node::bridge::spawn_blocking(move || {
                            let item = crate::node::chunk::StagedChunk::from_replica(
                                crate::dfs::OperationId::new("read-cache"),
                                &job.chunk,
                                job.body,
                            )?;
                            if let Some(receipt) = local.persist_read_cache(&item, job.epoch, capacity)?.receipt {
                                local.track_read_cache(ReadCacheIntent {
                                    namespace_id: namespace,
                                    file_version_id: job.version,
                                    layout_root_id: job.layout,
                                    copy: crate::dfs::read_cache_copy(&receipt)?,
                                })?;
                            }
                            Ok::<(), Error>(())
                        }).await;
                        match result {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => afs_logging::warn!("dfs.read_cache_install_failed"; "error" => error.to_string()),
                            Err(error) => afs_logging::warn!("dfs.read_cache_worker_failed"; "error" => error.to_string()),
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_cache_accumulates_out_of_order_verified_ranges_without_holes() {
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("paged"),
            b"0123456789".to_vec(),
        );
        let mut partial = PartialCopy {
            job: CacheJob {
                chunk: staged.chunk.clone(),
                body: vec![0; 10],
                version: FileVersionId::new("version"),
                layout: LayoutRootId::new("layout"),
                epoch: 1,
            },
            coverage: Vec::new(),
        };
        assert!(!partial.copy_range(7, b"789"));
        assert!(!partial.copy_range(0, b"0123"));
        assert!(!partial.copy_range(2, b"234"));
        assert!(partial.copy_range(5, b"56"));
        assert_eq!(partial.job.body, b"0123456789");
    }
}
