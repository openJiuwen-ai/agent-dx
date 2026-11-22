//! Install committed DFS Chunks in the native read cache and await the result.
use super::{cache_reconcile::CacheJob, *};
use crate::node::read::admission::{ReadLifetime, ReadScope, in_read_scope};

pub(in crate::node) struct PrefetchChunk {
    pub(in crate::node) batch: ReadBatch,
    pub(in crate::node) pin: ReadLifetime,
}

impl DfsReadEngine {
    pub(in crate::node) async fn prefetch_committed(
        &self,
        chunks: BTreeMap<ChunkId, PrefetchChunk>,
        deadline: Instant,
    ) -> Result<()> {
        if self.has_external_backend() {
            return Err(invalid("range prefetch requires native DFS"));
        }
        let authority = self.capture_native_read_authority()?;
        let mut charge = 0;
        let mut pending = Vec::new();
        for (id, request) in &chunks {
            if Instant::now() >= deadline {
                return Err(unavailable("DFS range prefetch expired"));
            }
            let local = crate::node::bridge::local_io(|| self.local.open_for_range_reads(id)).ok();
            if local.is_some() && self.local.has_durable_record(id)? {
                continue;
            }
            let cache = self
                .distributed_cache
                .as_ref()
                .ok_or_else(|| unavailable("native DFS read cache is disabled"))?;
            let object = match local
                .as_ref()
                .map(|reader| reader.chunk().clone())
                .or_else(|| ChunkObject::from_content_addressed_id(id))
            {
                Some(object) => object,
                None => self
                    .replicated_sources()
                    .checked_sources(&request.batch, deadline, request.pin.clone())
                    .await?
                    .into_iter()
                    .find(|sources| sources.chunk_id == *id)
                    .and_then(|sources| sources.chunk)
                    .ok_or_else(|| invalid("prefetch Chunk has no committed identity"))?,
            };
            charge = charge_prefetch(charge, object.length, cache.capacity())?;
            if local.is_none() {
                pending.push((object, request));
            }
        }
        for (object, request) in pending {
            self.prefetch_chunk(object, request, deadline).await?;
        }
        self.check_native_read_authority(&authority)?;
        let no_longer_resident = || {
            Error::coded(
                afs_error::IO_CAPACITY,
                "requested Chunks did not remain in the DFS read cache",
            )
        };
        crate::node::bridge::local_io(|| {
            let mut checked = Vec::with_capacity(chunks.len());
            for id in chunks.keys() {
                let revision = self
                    .local
                    .check_cache_resident(id)
                    .map_err(|_| no_longer_resident())?;
                checked.push((id, revision));
                #[cfg(test)]
                {
                    let hook = self
                        .test_hooks
                        .after_prefetch_resident_check
                        .lock()
                        .unwrap()
                        .clone();
                    if let Some(hook) = hook {
                        hook(id);
                    }
                }
            }
            if self.local.confirm_cache_resident(&checked)? {
                Ok(())
            } else {
                Err(no_longer_resident())
            }
        })?;
        if Instant::now() >= deadline {
            return Err(unavailable("DFS range prefetch expired"));
        }
        Ok(())
    }

    async fn prefetch_chunk(
        &self,
        object: ChunkObject,
        request: &PrefetchChunk,
        deadline: Instant,
    ) -> Result<()> {
        let cache = self
            .distributed_cache
            .as_ref()
            .ok_or_else(|| unavailable("native DFS read cache is disabled"))?;
        let (mut result, leader) = cache.prefetch_flight(&object.id)?;
        if let Some(leader) = leader {
            let engine = self.task_reader();
            let batch = request.batch.clone();
            let lifetime = request.pin.clone();
            tokio::spawn(async move {
                let scope = ReadScope {
                    deadline,
                    lifetime: lifetime.clone(),
                };
                let outcome = in_read_scope(
                    scope,
                    engine.fetch_and_install(object, batch, deadline, lifetime),
                )
                .await;
                leader.send_replace(Some(outcome));
            });
        }
        let installed = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            result.wait_for(Option::is_some),
        )
        .await
        .map_err(|_| unavailable("DFS prefetch installation wait expired"))?
        .map_err(|_| unavailable("DFS prefetch task stopped"))?;
        installed
            .clone()
            .ok_or_else(|| unavailable("DFS prefetch installer returned no result"))?
    }

    async fn fetch_and_install(
        &self,
        object: ChunkObject,
        mut batch: ReadBatch,
        deadline: Instant,
        lifetime: ReadLifetime,
    ) -> Result<()> {
        let cache = self
            .distributed_cache
            .as_ref()
            .ok_or_else(|| unavailable("native DFS read cache is disabled"))?;
        let length = usize::try_from(object.length)
            .map_err(|_| invalid("prefetch Chunk length overflow"))?;
        if length == 0 || length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES {
            return Err(invalid("prefetch Chunk length outside supported bounds"));
        }
        let charge = cache.enqueue_prefetch(object.length, deadline).await?;
        if crate::node::bridge::local_io(|| self.local.open_for_range_reads(&object.id)).is_ok() {
            return Ok(());
        }
        let authority = self.capture_native_read_authority()?;
        batch.ops = vec![ChunkReadOp {
            chunk_id: object.id.clone(),
            chunk_offset: 0,
            length: object.length,
            output_offset: 0,
        }];
        let route = self.replicated_sources();
        let sources = route.checked_sources(&batch, deadline, lifetime).await?;
        if !sources.iter().any(|set| {
            set.chunk.as_ref() == Some(&object)
                && range_covered(&set.allowed_ranges, 0, object.length)
        }) {
            return Err(external_denied(
                "file version does not authorize the full prefetch Chunk",
            ));
        }
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS prefetch requires a committed file version"))?;
        let mut body = vec![0; length];
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            route.read_remote(&batch, &sources, &mut body, true, Some(deadline)),
        )
        .await
        .map_err(|_| unavailable("DFS prefetch Chunk fetch expired"))??;
        self.check_native_read_authority(&authority)?;
        let (job, completed) =
            CacheJob::new(object, body, version, batch.layout_root_id, authority.epoch)
                .explicit(deadline);
        cache.submit(job, charge)?;
        // The shared flight ends with the actual install, even if its waiters time out.
        completed
            .await
            .map_err(|_| unavailable("DFS read cache installer stopped"))?
    }
}

fn charge_prefetch(current: u64, length: u64, capacity: u64) -> Result<u64> {
    current
        .checked_add(LocalChunkStore::read_cache_charge(length)?)
        .filter(|total| *total <= capacity)
        .ok_or_else(|| {
            Error::coded(
                afs_error::IO_CAPACITY,
                "requested Chunks exceed DFS read cache capacity",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_prefetch_capacity_counts_chunk_overhead_and_entire_plan() {
        let capacity = 8200;
        let first = charge_prefetch(0, 4, capacity).unwrap();
        assert_eq!(charge_prefetch(first, 4, capacity).unwrap(), capacity);
        assert_eq!(
            charge_prefetch(first, 5, capacity).unwrap_err().code(),
            afs_error::IO_CAPACITY
        );
    }
}
