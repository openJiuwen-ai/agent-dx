//! `ChunkSource`: where a read batch is served from.
//!
//! There are exactly two routes. An engine with an external backend is S3
//! mode and reads every chunk through `ExternalSource`; otherwise the batch
//! takes the Native route: local copies first, then the remote copies Meta
//! listed, tried as fan-out, verified peer cache, then peer ranges. Durable
//! and verified-cache copies share one candidate list in Meta order.
//!
//! Every source is a borrowed view of the engine with inherent methods:
//! static dispatch, no boxing, no extra threads, queues or copies.

use super::*;

/// The operations of a batch that the local source left for remote copies.
pub(super) struct RemoteMask(Vec<bool>);

impl RemoteMask {
    pub(super) fn new(ops: usize) -> Self {
        Self(vec![false; ops])
    }

    pub(super) fn mark(&mut self, index: usize) {
        self.0[index] = true;
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.0.contains(&true)
    }

    /// Bytes of the operations the local source served itself.
    pub(super) fn local_bytes(&self, batch: &ReadBatch) -> u64 {
        batch
            .ops
            .iter()
            .zip(&self.0)
            .filter(|(_, remote)| !**remote)
            .map(|(op, _)| op.length)
            .sum()
    }

    /// The marked operations, in batch order, as their own remote batch.
    fn remote_batch(&self, batch: &ReadBatch) -> ReadBatch {
        let ops = batch
            .ops
            .iter()
            .enumerate()
            .filter(|(index, _op)| self.0[*index])
            .map(|(_, op)| op.clone())
            .collect::<Vec<_>>();
        ReadBatch {
            lookahead: batch.lookahead.clone(),
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops,
        }
    }
}

/// What one source did with its share of a batch.
#[must_use]
pub(super) enum SourceOutcome {
    /// Every requested byte is in the (unpublished) output.
    Done,
    /// Some operations still need a remote copy (local source only).
    Partial(RemoteMask),
    /// This source does not apply; the next source in order is tried.
    NotApplicable,
}

impl SourceOutcome {
    pub(super) fn served(done: bool) -> Self {
        if done {
            Self::Done
        } else {
            Self::NotApplicable
        }
    }

    pub(super) fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}

/// The read route chosen once per batch by the engine's configuration.
pub(super) enum ChunkSource<'a> {
    Native(ReplicatedSources<'a>),
    External(ExternalSource<'a>),
}

#[derive(Clone, Copy)]
pub(super) struct ReplicatedSources<'a> {
    engine: &'a DfsReadEngine,
}

#[derive(Clone, Copy)]
pub(super) struct ExternalSource<'a> {
    engine: &'a DfsReadEngine,
    backend: &'a ExternalReadBackend,
}

#[derive(Clone, Copy)]
struct LocalSource<'a>(&'a DfsReadEngine);

#[derive(Clone, Copy)]
struct FanoutSource<'a>(&'a DfsReadEngine);

#[derive(Clone, Copy)]
pub(super) struct PeerCacheSource<'a>(pub(super) &'a DfsReadEngine);

#[derive(Clone, Copy)]
struct PeerRangeSource<'a>(&'a DfsReadEngine);

impl DfsReadEngine {
    pub(super) fn chunk_source(&self) -> ChunkSource<'_> {
        match &self.external_backend {
            Some(backend) => ChunkSource::External(ExternalSource {
                engine: self,
                backend,
            }),
            None => ChunkSource::Native(ReplicatedSources { engine: self }),
        }
    }

    /// The Native route for callers that already rejected an external backend.
    pub(super) fn replicated_sources(&self) -> ReplicatedSources<'_> {
        ReplicatedSources { engine: self }
    }

    /// A remote copy that may serve a native read: readable, durable or
    /// verified cache, on another Node. The one candidate rule of every
    /// remote source; the peer cache adds its own identity requirements.
    pub(super) fn is_remote_candidate(&self, source: &SourceCandidate) -> bool {
        source.state.is_readable()
            && matches!(
                source.role,
                CopyRole::DurableReplica | CopyRole::VerifiedCache
            )
            && matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id != &self.local_node_id)
    }
}

impl ExternalSource<'_> {
    pub(super) fn read(self, batch: &ReadBatch, out: &mut [u8]) -> Result<Option<u64>> {
        self.engine.read_external_batch(self.backend, batch, out)
    }
}

impl ReplicatedSources<'_> {
    pub(super) fn read(
        self,
        batch: &ReadBatch,
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        self.engine.transfer.block_on_read(Box::pin(self.read_async(
            batch,
            out,
            final_owned,
            deadline,
            Arc::new(()),
        )))
    }

    pub(super) async fn read_async(
        self,
        batch: &ReadBatch,
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
        lifetime: super::super::admission::ReadLifetime,
    ) -> Result<()> {
        let remote = match LocalSource(self.engine).read(batch, out)? {
            SourceOutcome::Partial(remote) => remote,
            SourceOutcome::Done => {
                let bytes = batch.ops.iter().map(|op| op.length).sum();
                self.engine.local.record_read_source_bytes("local", bytes);
                return Ok(());
            }
            SourceOutcome::NotApplicable => return Ok(()),
        };
        self.engine
            .local
            .record_read_source_bytes("local", remote.local_bytes(batch));
        let remote_batch = remote.remote_batch(batch);
        let deadline =
            deadline.unwrap_or_else(|| Instant::now() + self.engine.read_pipeline_timeout());
        let looked_up = Instant::now();
        let sources = self
            .checked_sources(&remote_batch, deadline, lifetime)
            .await?;
        self.engine.time_route("sources", looked_up);
        // H2 step 2 (review/18 a1): a Chunk counts as in flight here only
        // while it is fetched from a copy that is not a holder; the marks the
        // attempts leave in this scope last until the Chunks are staged for
        // installation (queued installs keep their own mark).
        let one_chunk = remote_batch
            .ops
            .windows(2)
            .all(|pair| pair[0].chunk_id == pair[1].chunk_id);
        super::remote::keeping_fetched(one_chunk, async {
            let reading = Instant::now();
            self.read_remote(&remote_batch, &sources, out, final_owned, Some(deadline))
                .await?;
            self.engine.time_route("remote", reading);
            let staging = Instant::now();
            self.engine
                .cache_completed_ranges(&remote_batch, &sources, out);
            self.engine.time_route("staging", staging);
            Ok(())
        })
        .await
    }

    pub(super) async fn checked_sources(
        self,
        batch: &ReadBatch,
        deadline: Instant,
        lifetime: super::super::admission::ReadLifetime,
    ) -> Result<Vec<ChunkSources>> {
        let sources = self
            .engine
            .sources_for_attempt_async(batch, true, deadline, lifetime)
            .await?;
        let authority = self.engine.capture_native_read_authority()?;
        if authority.epoch != 0
            && sources
                .iter()
                .flat_map(|set| &set.sources)
                .any(|source| source.read_grant.caller_node_epoch != authority.epoch)
        {
            self.engine.invalidate_native_batch_authority(batch);
            return Err(external_denied(
                "DFS source caller epoch differs from the registered session",
            ));
        }
        Ok(sources)
    }

    /// Remote sources in order; the first that applies serves the batch.
    pub(super) async fn read_remote(
        self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let object_bytes = || batch.ops.iter().map(|op| op.length).sum::<u64>();
        // H2 step 2: other readers sharing this version join the candidates.
        let with_holders;
        let sources = if self.engine.holder_lists.is_some() {
            let listing = Instant::now();
            with_holders = self
                .engine
                .with_version_holders(
                    batch,
                    sources,
                    deadline
                        .unwrap_or_else(|| Instant::now() + self.engine.read_pipeline_timeout()),
                )
                .await;
            self.engine.time_route("holder_list", listing);
            &with_holders[..]
        } else {
            sources
        };
        if final_owned
            && FanoutSource(self.engine)
                .read(batch, sources, out, deadline)
                .await?
                .is_done()
        {
            self.engine
                .local
                .record_read_source_bytes("object", object_bytes());
            return Ok(());
        }
        if PeerCacheSource(self.engine)
            .read(batch, sources, out, final_owned, false)
            .await?
            .is_done()
        {
            self.engine
                .local
                .record_read_source_bytes("object", object_bytes());
            return Ok(());
        }
        if final_owned
            && self
                .engine
                .read_distributed_ranges(batch, sources, out, deadline)
                .await?
        {
            return Ok(());
        }
        // D3-5: the candidate order applies at any parallelism, so one
        // reader with parallelism 1 still prefers cache holders and spreads
        // its Chunks over replicas instead of always asking Meta's first copy.
        let ordered = self.engine.distributed_sources(batch, sources);
        PeerRangeSource(self.engine)
            .read(batch, &ordered, out, final_owned, deadline)
            .await
    }
}

impl LocalSource<'_> {
    fn read(self, batch: &ReadBatch, out: &mut [u8]) -> Result<SourceOutcome> {
        self.0.read_local_copies(batch, out)
    }
}

impl FanoutSource<'_> {
    async fn read(
        self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<SourceOutcome> {
        self.0
            .fanout_private_objects_async(batch, sources, out, deadline)
            .await
    }
}

impl PeerCacheSource<'_> {
    pub(super) async fn read(
        self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        stream_only: bool,
    ) -> Result<SourceOutcome> {
        self.0
            .read_replicated_peer_cache_mode_async(batch, sources, out, final_owned, stream_only)
            .await
    }
}

impl PeerRangeSource<'_> {
    async fn read(
        self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        self.0
            .read_remote_ranges_async(batch, sources, out, final_owned, deadline)
            .await
    }
}
