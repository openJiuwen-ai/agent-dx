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
    Native(NativeSources<'a>),
    External(ExternalSource<'a>),
}

#[derive(Clone, Copy)]
pub(super) struct NativeSources<'a> {
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
            None => ChunkSource::Native(NativeSources { engine: self }),
        }
    }

    /// The Native route for callers that already rejected an external backend.
    pub(super) fn native_sources(&self) -> NativeSources<'_> {
        NativeSources { engine: self }
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

impl NativeSources<'_> {
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
        lifetime: super::super::read_pipeline::ReadLifetime,
    ) -> Result<()> {
        let remote = match LocalSource(self.engine).read(batch, out)? {
            SourceOutcome::Partial(remote) => remote,
            SourceOutcome::Done | SourceOutcome::NotApplicable => return Ok(()),
        };
        let remote_batch = remote.remote_batch(batch);
        let deadline =
            deadline.unwrap_or_else(|| Instant::now() + self.engine.read_pipeline_timeout());
        let sources = self
            .engine
            .sources_for_attempt_async(&remote_batch, true, deadline, lifetime)
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
        self.read_remote(&remote_batch, &sources, out, final_owned, Some(deadline))
            .await
    }

    /// Remote sources in order; the first that applies serves the batch.
    async fn read_remote(
        self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        if final_owned
            && FanoutSource(self.engine)
                .read(batch, sources, out, deadline)
                .await?
                .is_done()
        {
            return Ok(());
        }
        if PeerCacheSource(self.engine)
            .read(batch, sources, out, final_owned, false)
            .await?
            .is_done()
        {
            return Ok(());
        }
        PeerRangeSource(self.engine)
            .read(batch, sources, out, final_owned)
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
            .read_native_peer_cache_mode_async(batch, sources, out, final_owned, stream_only)
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
    ) -> Result<()> {
        self.0
            .read_remote_ranges_async(batch, sources, out, final_owned)
            .await
    }
}
