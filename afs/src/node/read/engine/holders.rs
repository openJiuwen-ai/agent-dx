//! H2 step 2: reading from version holders (plan/15 v3 §1.2, §1.3).
//!
//! A reader with sharing on asks Meta which other readers share the version
//! it reads, at most once per [`HolderLists::ttl`] per version, and adds them
//! as candidates of every Chunk: after the published read-cache copies and
//! before the durable copies (the writer stays the fallback). A holder that
//! answers `NOT_READY` is skipped without being demoted; one that refuses
//! the grant drops the cached list so the next read asks Meta again.
use super::*;

/// The longest a read spends asking Meta for holders; a slower Meta only
/// means no holders for this read.
const HOLDER_LOOKUP_WAIT: Duration = Duration::from_millis(250);
/// A holder lookup takes at most this share (1/n) of the read's time left,
/// so the copies the read already has keep the rest (!318 P1).
const HOLDER_LOOKUP_SHARE: u32 = 4;
/// The wait a holder read asks for: `budget` (`dfs_holder_wait_ms`, default
/// 30 ms, review/19) only when the holder ranks above this reader for the
/// Chunk (rendezvous over the version, the Chunk and the Node, re-review P2)
/// and the read covers one Chunk only (!318 P2-4,
/// [`super::remote::keeping_fetched`]), otherwise none. Two readers fetching
/// the same Chunk at once are each other's holders; with this order only one
/// of them waits for the other, so the waits for one Chunk can never form a
/// cycle and the highest-ranked reader never waits at all.
pub(crate) fn holder_wait_ms(version: &FileVersionId, item: &ResolvedReadOp, budget: u32) -> u32 {
    let Some(grant) = &item.source.holder_grant else {
        return 0;
    };
    if budget != 0
        && super::remote::holder_wait_allowed()
        && holder_ranks_above(
            version,
            &item.op.chunk_id,
            &grant.holder_node_id,
            &grant.caller_node_id,
        )
    {
        budget
    } else {
        0
    }
}

/// Whether `holder` ranks above `caller` for `chunk` of `version`.
pub(crate) fn holder_ranks_above(
    version: &FileVersionId,
    chunk: &ChunkId,
    holder: &str,
    caller: &str,
) -> bool {
    let weight = |node: &str| super::distribution::rendezvous_weight(&version.0, chunk, node);
    weight(holder) > weight(caller)
}
const MAX_HOLDER_LISTS: usize = 256;

type ListKey = (FileVersionId, LayoutRootId);
type HolderList = Arc<Vec<crate::dfs::VersionHolder>>;

#[derive(Default)]
struct ListState {
    /// The list of each version and when it stops being current.
    lists: HashMap<ListKey, (Instant, HolderList)>,
    /// Versions whose list one read is asking Meta for right now.
    looking_up: std::collections::HashSet<ListKey>,
}

/// Cached holder lists of the versions this Node reads.
///
/// One lookup per version at a time (review/18 a2): while it runs, other
/// reads use the previous list (kept one more TTL) or go without holders;
/// none of them waits for Meta.
pub(crate) struct HolderLists {
    ttl: Duration,
    state: Mutex<ListState>,
}

/// What a read gets from the cache for one version.
enum Cached {
    Current(HolderList),
    /// Not current: this read asks Meta and must report back (`Lookup`).
    Lookup(Lookup),
    /// Another read is asking Meta: use this list for now, if any.
    Stale(Option<HolderList>),
}

/// The one lookup of a version in progress; ends it when dropped, also when
/// the read is cancelled.
struct Lookup {
    lists: Arc<HolderLists>,
    key: ListKey,
}

impl Drop for Lookup {
    fn drop(&mut self) {
        if let Ok(mut state) = self.lists.state.lock() {
            state.looking_up.remove(&self.key);
        }
    }
}

impl HolderLists {
    pub(crate) fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            ttl,
            state: Mutex::default(),
        })
    }

    fn cached(self: &Arc<Self>, key: &ListKey) -> Cached {
        let Ok(mut state) = self.state.lock() else {
            return Cached::Stale(None);
        };
        let now = Instant::now();
        let list = state.lists.get(key).cloned();
        if let Some((until, list)) = &list
            && *until > now
        {
            return Cached::Current(list.clone());
        }
        if !state.looking_up.insert(key.clone()) {
            // A previous list stays usable for one more TTL.
            return Cached::Stale(
                list.filter(|(until, _)| *until + self.ttl > now)
                    .map(|(_, list)| list),
            );
        }
        Cached::Lookup(Lookup {
            lists: self.clone(),
            key: key.clone(),
        })
    }

    fn store(&self, key: ListKey, list: HolderList) {
        if let Ok(mut state) = self.state.lock() {
            let now = Instant::now();
            let ttl = self.ttl;
            state.lists.retain(|_, (until, _)| *until + ttl > now);
            if state.lists.len() < MAX_HOLDER_LISTS || state.lists.contains_key(&key) {
                state.lists.insert(key, (now + self.ttl, list));
            }
        }
    }

    pub(super) fn forget(&self, version: &FileVersionId, layout: &LayoutRootId) {
        if let Ok(mut state) = self.state.lock() {
            state.lists.remove(&(version.clone(), layout.clone()));
        }
    }
}

/// The candidate a holder is for one Chunk.
pub(super) fn holder_candidate(
    holder: &crate::dfs::VersionHolder,
    chunk: &ChunkId,
) -> SourceCandidate {
    let grant = &holder.grant;
    SourceCandidate {
        copy_id: crate::dfs::CopyId::new(format!(
            "holder:{}:{}",
            holder.node_id, holder.node_epoch
        )),
        chunk_id: chunk.clone(),
        role: CopyRole::VerifiedCache,
        state: crate::dfs::CopyState::Ready,
        location: CopyLocation::Node {
            node_id: holder.node_id.clone(),
            node_epoch: holder.node_epoch,
            device_id: String::new(),
            device_epoch: 0,
            catalog_revision: 0,
        },
        data_endpoint: Some(holder.data_endpoint.clone()),
        load_hint: 0,
        // The per-copy grant fields mirror the holder grant so the usual
        // freshness filter (`source_matches`) applies, except the token: it
        // stays empty, so a send path that forgot the holder form sends an
        // empty per-copy token, which every per-copy entry of the holder
        // refuses, instead of the holder token. The transfers send
        // `holder_grant`.
        read_grant: crate::dfs::DfsReadGrant {
            namespace_id: grant.namespace_id.clone(),
            file_version_id: grant.file_version_id.clone(),
            layout_root_id: grant.layout_root_id.clone(),
            caller_node_id: grant.caller_node_id.clone(),
            caller_node_epoch: grant.caller_node_epoch,
            expires_at_unix_ms: grant.expires_at_unix_ms,
            fence: 0,
            token: String::new(),
        },
        holder_grant: Some(grant.clone()),
    }
}

impl DfsReadEngine {
    /// H2 step 2: `sources` with this version's holders added to every Chunk
    /// (a holder already offered for a Chunk is not added twice). Without
    /// sharing, or when Meta offers no holder in time, `sources` unchanged.
    pub(super) async fn with_version_holders(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        deadline: Instant,
    ) -> Vec<ChunkSources> {
        let mut result = sources.to_vec();
        let (Some(lists), Some(version)) = (&self.holder_lists, batch.file_version_id.as_ref())
        else {
            return result;
        };
        let key = (version.clone(), batch.layout_root_id.clone());
        let holders = match lists.cached(&key) {
            Cached::Current(holders) | Cached::Stale(Some(holders)) => holders,
            Cached::Stale(None) => return result,
            Cached::Lookup(_lookup) => {
                let wait = (deadline.saturating_duration_since(Instant::now())
                    / HOLDER_LOOKUP_SHARE)
                    .min(HOLDER_LOOKUP_WAIT);
                let request = crate::dfs::DfsVersionHoldersRequest {
                    caller_id: self.local_node_id.clone(),
                    namespace_id: self.namespace_id.clone(),
                    file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                };
                let holders = match tokio::time::timeout(
                    wait,
                    self.source_provider.version_holders_async(request),
                )
                .await
                {
                    Ok(Ok(holders)) => holders,
                    // No holders for this read; the next one asks again.
                    _ => return result,
                };
                let holders = Arc::new(
                    holders
                        .into_iter()
                        .filter(|holder| holder.node_id != self.local_node_id)
                        .collect::<Vec<_>>(),
                );
                lists.store(key, holders.clone());
                holders
            }
        };
        if holders.is_empty() {
            return result;
        }
        // A demoted holder is left out until its demotion ends (re-review
        // P3): a holder is optional, and ranked after the durable copies it
        // could turn a busy writer into a failed read.
        let demoted = self.demoted_now();
        for set in &mut result {
            // A holder read is a range read without a BLAKE3 outboard, so the
            // reader checks its bytes only against a CRC-segment identity.
            // Only a Chunk Meta describes as such gets a holder (!318 P3-1):
            // not one identified by its whole-body BLAKE3, nor one whose
            // object the reply leaves out. Native writes produce CRC-segment
            // Chunks only, and Meta's replies describe them.
            if !set
                .chunk
                .as_ref()
                .is_some_and(ChunkObject::is_segment_crc32c)
            {
                continue;
            }
            // review/18 a3: only the holder ranked highest for this Chunk
            // (rendezvous over the version, the Chunk and the Node); its
            // NOT_READY sends the read straight to the copies.
            let best = holders
                .iter()
                .map(|holder| holder_candidate(holder, &set.chunk_id))
                .filter(|candidate| !demoted.contains(&candidate.copy_id))
                .max_by_key(|candidate| {
                    let node = match &candidate.location {
                        CopyLocation::Node { node_id, .. } => node_id.as_str(),
                        CopyLocation::External { .. } => "",
                    };
                    super::distribution::rendezvous_weight(&version.0, &set.chunk_id, node)
                });
            let Some(best) = best else { continue };
            let offered = set
                .sources
                .iter()
                .any(|source| source.holder_grant.is_some() && source.location == best.location);
            if !offered {
                set.sources.push(best);
            }
        }
        result
    }
}
