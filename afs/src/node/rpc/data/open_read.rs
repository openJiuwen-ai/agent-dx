use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
type Key = (String, String);

/// A weak identity supplied by the accepted mTLS connection. No address reuse
/// can inherit it, and retaining an open context cannot keep the socket alive.
#[derive(Clone)]
pub struct DfsOpenReadConnection(std::sync::Weak<dyn std::any::Any + Send + Sync>);
impl DfsOpenReadConnection {
    pub(super) fn from_request<T>(request: &tonic::Request<T>) -> Option<Self> {
        afs_transport::grpc::peer_certificates(request).map(|certificates| {
            let owner: Arc<dyn std::any::Any + Send + Sync> = certificates;
            Self(Arc::downgrade(&owner))
        })
    }
    pub(super) fn identity(&self) -> usize {
        self.0.as_ptr() as *const () as usize
    }
    fn same_connection(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &other.0)
    }
}

// Tonic 0.14 supplies one certificate Arc per accepted TLS connection. The
// connection service and in-flight requests own it; we retain only Weak. This
// maintenance task never validates a range, frame, TTL or Meta grant.
pub(super) fn start_connection_retirement(state: &Arc<std::sync::Mutex<OpenAuthorityState>>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    {
        let Ok(mut state) = state.lock() else {
            return;
        };
        if state.retirement_started {
            return;
        }
        state.retirement_started = true;
    }
    let state = Arc::downgrade(state);
    runtime.spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let Some(state) = state.upgrade() else {
                break;
            };
            if let Ok(mut state) = state.lock() {
                state.entries.retain(|_, entry| {
                    let disconnected = entry
                        .fast
                        .as_ref()
                        .is_some_and(|context| context.connection.0.strong_count() == 0);
                    if disconnected {
                        entry.retire();
                    }
                    !disconnected
                });
            }
        }
    });
}

/// The durable B-side open pin, rather than the source-page TTL, owns this view.
/// Close prevents new admission; requests already holding an Arc may drain.
pub struct DfsOpenReadContext {
    version: String,
    layout: String,
    sources: HashMap<String, crate::dfs::OpenReadSource>,
    active: Arc<AtomicBool>,
    connection: DfsOpenReadConnection,
    readers: crate::node::chunk::FixedPeerReaders,
    readers_registered: AtomicBool,
}
impl DfsOpenReadContext {
    fn admits(&self, request: &DfsReadRangesRequest) -> afs_error::Result<bool> {
        if !self.active.load(Ordering::Acquire)
            || request.file_version_id != self.version
            || request.layout_root_id != self.layout
            || request.operations.is_empty()
            || request.operations.len() > super::MAX_DFS_READ_OPS
            || request.read_id.is_empty()
            || request.read_id.len() > 512
            || request.attempt_id.is_empty()
            || request.attempt_id.len() > 512
        {
            return Err(replica_denied(
                "fixed open context identity differs or is closed",
            ));
        }
        let first_token = request.operations[0].grant.as_ref().map(|g| &g.token);
        if first_token.is_none() {
            return Err(replica_denied("fixed open context token missing"));
        }
        let mut destination = 0_u64;
        let mut all_present = true;
        for (index, op) in request.operations.iter().enumerate() {
            if op.operation_index as usize != index
                || op.length == 0
                || op.destination_offset != destination
                || op.grant.as_ref().map(|g| &g.token) != first_token
            {
                return Err(replica_denied("fixed open range shape differs"));
            }
            destination = destination
                .checked_add(op.length)
                .filter(|n| *n <= super::MAX_DFS_READ_BYTES)
                .ok_or_else(|| replica_denied("fixed open range budget exceeded"))?;
            match self.sources.get(&op.chunk_id) {
                Some(source) if covers(source, op) => (),
                Some(_) => {
                    return Err(replica_denied(
                        "fixed open range exceeds its admitted object",
                    ));
                }
                None => all_present = false,
            }
        }
        Ok(all_present)
    }

    fn fixed_reader(
        &self,
        local: &LocalChunkStore,
        op: &afs_protocol::node_data::DfsChunkReadOp,
    ) -> afs_error::Result<Arc<crate::node::chunk::PinnedChunkReader>> {
        let id = crate::dfs::ChunkId::new(op.chunk_id.clone());
        let reader = {
            let mut readers = self
                .readers
                .lock()
                .map_err(|_| replica_denied("fixed open reader cache poisoned"))?;
            // Register before publishing any reader. Another first request must
            // not bypass registration while quarantine snapshots the registry.
            if !self.readers_registered.load(Ordering::Acquire) {
                local.register_fixed_peer_readers(&self.readers)?;
                self.readers_registered.store(true, Ordering::Release);
            }
            if let Some(reader) = readers.get(&id) {
                reader.clone()
            } else {
                let reader = match local.open_fixed_peer_reader(&id) {
                    Ok(reader) => Arc::new(reader),
                    Err(error) => {
                        // Quarantine can invalidate this cache. Keep successful
                        // open/publication under its lock, but never reenter
                        // that lock through the missing-file recovery hook.
                        drop(readers);
                        local.try_quarantine(&id);
                        return Err(error);
                    }
                };
                // 64 bounded descriptors per context, independently of payload budgets.
                if readers.len() >= 64
                    && let Some(old) = readers.keys().next().cloned()
                {
                    readers.remove(&old);
                }
                readers.insert(id.clone(), reader.clone());
                reader
            }
        };
        Ok(reader)
    }

    fn forget_failed_reader(&self, id: &crate::dfs::ChunkId) {
        if let Ok(mut readers) = self.readers.lock() {
            readers.remove(id);
        }
    }

    #[cfg(test)]
    pub(super) fn payload(
        &self,
        local: &LocalChunkStore,
        op: &afs_protocol::node_data::DfsChunkReadOp,
        deadline: std::time::Instant,
    ) -> afs_error::Result<crate::node::chunk::PeerReadPayload> {
        let id = crate::dfs::ChunkId::new(op.chunk_id.clone());
        let reader = self.fixed_reader(local, op)?;
        local
            .read_fixed_peer_range(
                &reader,
                op.chunk_offset,
                usize::try_from(op.length)
                    .map_err(|_| replica_denied("fixed open length exceeds address space"))?,
                deadline,
            )
            .inspect_err(|_| self.forget_failed_reader(&id))
    }

    #[cfg(test)]
    pub(super) async fn payload_async(
        self: Arc<Self>,
        local: Arc<LocalChunkStore>,
        op: afs_protocol::node_data::DfsChunkReadOp,
        deadline: std::time::Instant,
        disk_slots: Arc<tokio::sync::Semaphore>,
    ) -> afs_error::Result<crate::node::chunk::PeerReadPayload> {
        let length = usize::try_from(op.length)
            .map_err(|_| replica_denied("fixed open length exceeds address space"))?;
        let reservation = local.reserve_fixed_read_until(length, deadline).await?;
        self.payload_reserved_async(local, op, deadline, disk_slots, reservation, Arc::new(()))
            .await
    }

    pub(super) async fn payload_reserved_async(
        self: Arc<Self>,
        local: Arc<LocalChunkStore>,
        op: afs_protocol::node_data::DfsChunkReadOp,
        deadline: std::time::Instant,
        disk_slots: Arc<tokio::sync::Semaphore>,
        reservation: crate::node::chunk::FixedReadReservation,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> afs_error::Result<crate::node::chunk::PeerReadPayload> {
        let length = usize::try_from(op.length)
            .map_err(|_| replica_denied("fixed open length exceeds address space"))?;
        let disk_wait = local.read_work_metrics().map(|m| {
            m.activities
                .enter(crate::node::read_activity::Activity::DiskWait, 0)
        });
        let disk = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            disk_slots.acquire_owned(),
        )
        .await
        .map_err(|_| {
            afs_error::Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "disk admission deadline exceeded",
            )
        })?
        .map_err(|_| afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "disk admission closed"))?;
        drop(disk_wait);
        crate::node::read_pipeline::blocking_read(disk, move || {
            let _lifetime = lifetime;
            let id = crate::dfs::ChunkId::new(op.chunk_id.clone());
            let reader = self.fixed_reader(&local, &op)?;
            local
                .read_fixed_peer_range_reserved(&reader, op.chunk_offset, length, reservation)
                .inspect_err(|_| self.forget_failed_reader(&id))
        })
        .await
    }
}

#[derive(Default)]
pub(super) struct OpenAuthorityState {
    retirement_started: bool,
    entries: HashMap<Key, OpenEntry>,
    pending: HashMap<Key, (Arc<AtomicBool>, usize)>,
}
struct OpenEntry {
    grant: crate::dfs::DfsReadGrant,
    sources: HashMap<String, crate::dfs::OpenReadSource>,
    deadline: std::time::Instant,
    expires: u64,
    active: Arc<AtomicBool>,
    bytes: usize,
    fast: Option<Arc<DfsOpenReadContext>>,
}
impl OpenEntry {
    fn retire(&self) {
        self.active.store(false, Ordering::Release);
        if let Some(context) = &self.fast
            && let Ok(mut readers) = context.readers.lock()
        {
            readers.clear();
        }
    }
}
struct Pending<'a> {
    state: &'a std::sync::Mutex<OpenAuthorityState>,
    key: Key,
    active: Arc<AtomicBool>,
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Ok(mut s) = self.state.lock()
            && let Some((active, n)) = s.pending.get_mut(&self.key)
            && Arc::ptr_eq(active, &self.active)
        {
            *n -= 1;
            if *n == 0 {
                s.pending.remove(&self.key);
            }
        }
    }
}
fn same_open(a: &crate::dfs::DfsReadGrant, b: &afs_protocol::node_data::DfsReadGrant) -> bool {
    a.namespace_id.0 == b.namespace_id
        && a.file_version_id.0 == b.file_version_id
        && a.layout_root_id.0 == b.layout_root_id
        && a.caller_node_id == b.caller_node_id
        && a.caller_node_epoch == b.caller_node_epoch
        && a.token == b.token
}
fn covers(
    source: &crate::dfs::OpenReadSource,
    op: &afs_protocol::node_data::DfsChunkReadOp,
) -> bool {
    source.copy_ids.iter().any(|v| v.0 == op.source_copy_id)
        && op.length != 0
        && op.chunk_offset.checked_add(op.length).is_some_and(|end| {
            source.ranges.iter().any(|(start, n)| {
                op.chunk_offset >= *start && start.checked_add(*n).is_some_and(|limit| end <= limit)
            })
        })
}
impl CachedDfsReadAuthorizer {
    pub(super) fn cached_open_context(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
        connection: Option<&DfsOpenReadConnection>,
    ) -> afs_error::Result<Option<Arc<DfsOpenReadContext>>> {
        let metrics = self.validator.authorization_metrics();
        let _scope = metrics
            .as_ref()
            .map(|m| m.authorization_scope("context_lookup"));
        let Some(connection) = connection else {
            return Ok(None);
        };
        let Some(grant) = request.operations.first().and_then(|op| op.grant.as_ref()) else {
            return Err(replica_denied("fixed open context is absent"));
        };
        if !grant.token.starts_with("dfs-open-v1:read-open:") || grant.token.len() > 524 {
            return Ok(None);
        }
        let context = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?
            .entries
            .get(&(peer.to_owned(), grant.token.clone()))
            .and_then(|entry| entry.fast.clone());
        if let Some(context) = context
            && context.connection.same_connection(connection)
            && context.admits(request)?
        {
            if let Some(metrics) = &metrics {
                metrics.check(crate::node::read_work::ReadCheck::ContextReuse);
            }
            return Ok(Some(context));
        }
        Ok(None)
    }

    pub(super) fn fast_open_context(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
        connection: Option<DfsOpenReadConnection>,
    ) -> afs_error::Result<Option<Arc<DfsOpenReadContext>>> {
        // Without transport lifecycle identity use the ordinary lane; an absent
        // connection must never become a reusable None == None identity.
        let Some(connection) = connection else {
            return Ok(None);
        };
        let Some(grant) = request.operations.first().and_then(|op| op.grant.as_ref()) else {
            return Err(replica_denied("fixed open context is absent"));
        };
        if !grant.token.starts_with("dfs-open-v1:read-open:") || grant.token.len() > 524 {
            return Ok(None);
        }
        let key = (peer.to_owned(), grant.token.clone());
        let existing = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?
            .entries
            .get(&key)
            .and_then(|entry| entry.fast.clone());
        if let Some(context) = existing {
            if context.connection.same_connection(&connection) && context.admits(request)? {
                if let Some(metrics) = self.validator.authorization_metrics() {
                    metrics.check(crate::node::read_work::ReadCheck::ContextReuse);
                }
                return Ok(Some(context));
            }
            if !context.connection.same_connection(&connection) && !grant.caller_node_id.is_empty()
            {
                // Reconnect is recovery: re-establish Meta authority instead of
                // letting a new connection inherit a dead caller's context.
                if let Some(entry) = self
                    .open_cache
                    .lock()
                    .map_err(|_| replica_denied("open read cache poisoned"))?
                    .entries
                    .remove(&key)
                {
                    entry.retire();
                }
            }
        }
        if grant.caller_node_id.is_empty() {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "fixed open setup required",
            ));
        }
        // Also covers authorizers constructed before entering the server runtime.
        start_connection_retirement(&self.open_cache);
        // First access (or an evicted/missing source page) uses the full Meta-
        // anchored validator. Its TTL never becomes a per-request refresh timer.
        self.authorize(peer, request)?;
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        let retained: usize = state.entries.values().map(|entry| entry.bytes).sum();
        let needs_context = state
            .entries
            .get(&key)
            .is_some_and(|entry| entry.fast.is_none());
        if needs_context && retained.saturating_add(512 * 1024) > 32 * 1024 * 1024 {
            return Err(replica_denied("fixed open metadata budget exhausted"));
        }
        if connection.0.strong_count() == 0 {
            if let Some(entry) = state.entries.remove(&key) {
                entry.retire();
            }
            return Err(replica_denied(
                "fixed open connection ended during admission",
            ));
        }
        let entry = state
            .entries
            .get_mut(&key)
            .ok_or_else(|| replica_denied("open read context disappeared during admission"))?;
        if entry
            .fast
            .as_ref()
            .is_some_and(|context| !context.connection.same_connection(&connection))
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "fixed open setup raced a different connection",
            ));
        }
        if entry.fast.is_none() {
            // Covers the second bounded source index and up to64 pinned
            // native descriptors, in addition to the original page charge.
            entry.bytes = entry.bytes.saturating_add(512 * 1024);
            entry.fast = Some(Arc::new(DfsOpenReadContext {
                version: entry.grant.file_version_id.0.clone(),
                layout: entry.grant.layout_root_id.0.clone(),
                sources: entry.sources.clone(),
                active: entry.active.clone(),
                connection,
                readers: Arc::new(std::sync::Mutex::new(HashMap::new())),
                readers_registered: AtomicBool::new(false),
            }));
        }
        let context = entry
            .fast
            .as_ref()
            .ok_or_else(|| replica_denied("fixed context was not published"))?
            .clone();
        if !context.admits(request)? {
            return Err(replica_denied("source page did not cover fixed open range"));
        }
        Ok(Some(context))
    }
    pub(super) fn close_open_context(&self, peer: &str, context: &str) -> afs_error::Result<()> {
        if !context.starts_with("dfs-open-v1:read-open:") || context.len() > 524 {
            return Err(replica_denied("invalid closed read context id"));
        }
        let key = (peer.into(), context.into());
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        if let Some(entry) = state.entries.remove(&key) {
            entry.retire();
        }
        if let Some((active, _)) = state.pending.get(&key) {
            active.store(false, Ordering::Release);
        }
        Ok(())
    }
    pub(super) fn authorize_open_context(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
        metrics: Option<&super::super::super::read_work::ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        for op in &request.operations {
            self.authorize_open_range(peer, op, metrics)?;
        }
        Ok(())
    }
    fn authorize_open_range(
        &self,
        peer: &str,
        op: &afs_protocol::node_data::DfsChunkReadOp,
        metrics: Option<&super::super::super::read_work::ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        let grant = op
            .grant
            .as_ref()
            .ok_or_else(|| replica_denied("open read grant missing"))?;
        let now = crate::meta::store::now_unix_ms();
        if grant.caller_node_id != peer
            || !grant.token.starts_with("dfs-open-v1:read-open:")
            || grant.token.len() > 524
            || grant.expires_at_unix_ms <= now
        {
            return Err(replica_denied("open read peer, token or expiry differs"));
        }
        let key = (peer.to_owned(), grant.token.clone());
        let pending = {
            let mut state = self
                .open_cache
                .lock()
                .map_err(|_| replica_denied("open read cache poisoned"))?;
            if let Some(entry) = state.entries.get(&key) {
                if !same_open(&entry.grant, grant) {
                    return Err(replica_denied("open read immutable identity changed"));
                }
                if !entry.active.load(Ordering::Acquire) {
                    return Err(replica_denied("open read context is closed"));
                }
                if entry.deadline > std::time::Instant::now()
                    && entry.expires > now
                    && entry
                        .sources
                        .get(&op.chunk_id)
                        .is_some_and(|s| covers(s, op))
                {
                    return Ok(());
                }
            }
            if !state.pending.contains_key(&key) && state.pending.len() >= 128 {
                return Err(replica_denied("open read pending context budget exhausted"));
            }
            let (active, n) = state
                .pending
                .entry(key.clone())
                .or_insert_with(|| (Arc::new(AtomicBool::new(true)), 0));
            if !active.load(Ordering::Acquire) {
                return Err(replica_denied("open read context closed during admission"));
            }
            *n += 1;
            Pending {
                state: &self.open_cache,
                key: key.clone(),
                active: active.clone(),
            }
        };
        let check = crate::dfs::DfsReadValidation {
            grant: crate::dfs::DfsReadGrant {
                namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id.clone()),
                file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id.clone()),
                layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id.clone()),
                caller_node_id: grant.caller_node_id.clone(),
                caller_node_epoch: grant.caller_node_epoch,
                expires_at_unix_ms: grant.expires_at_unix_ms,
                fence: grant.fence,
                token: grant.token.clone(),
            },
            chunk_id: crate::dfs::ChunkId::new(op.chunk_id.clone()),
            copy_id: crate::dfs::CopyId::new(op.source_copy_id.clone()),
            chunk_offset: op.chunk_offset,
            length: op.length,
        };
        let timer = metrics.map(|m| m.authorization_scope("meta_rpc"));
        let mut reply = self.validator.validate(crate::dfs::ValidateDfsReadGrants {
            receiver_node_id: self.node_id.clone(),
            receiver_node_epoch: self.node_epoch,
            peer_node_id: peer.into(),
            validations: vec![check.clone()],
        })?;
        drop(timer);
        if reply.len() != 1 {
            return Err(replica_denied("open read authority reply count differs"));
        }
        let reply = reply.remove(0);
        let completed = crate::meta::store::now_unix_ms();
        if reply.validation != check
            || reply.expires_at_unix_ms <= completed
            || reply.expires_at_unix_ms > grant.expires_at_unix_ms
            || reply.open_sources.len() > 4 * 1024 * 1024
        {
            return Err(replica_denied(
                "open read reply identity, lease or budget differs",
            ));
        }
        let page: Vec<crate::dfs::OpenReadSource> = serde_json::from_slice(&reply.open_sources)
            .map_err(|_| replica_denied("open read source page invalid"))?;
        if page.is_empty() || page.len() > 512 {
            return Err(replica_denied("open read source page count exceeds budget"));
        }
        let mut sources = HashMap::new();
        for s in page {
            if s.chunk_id.0.is_empty()
                || s.chunk_id.0.len() > 512
                || s.copy_ids.is_empty()
                || s.copy_ids.len() > 64
                || s.copy_ids.iter().any(|v| v.0.is_empty() || v.0.len() > 512)
                || s.ranges.is_empty()
                || s.ranges.len() > 128
                || s.ranges
                    .iter()
                    .any(|(a, n)| *n == 0 || a.checked_add(*n).is_none())
            {
                return Err(replica_denied("open read source page shape differs"));
            }
            if sources.insert(s.chunk_id.0.clone(), s).is_some() {
                return Err(replica_denied("duplicate open read source entry"));
            }
        }
        if !sources.get(&op.chunk_id).is_some_and(|s| covers(s, op)) {
            return Err(replica_denied(
                "open read page does not cover requested source",
            ));
        }
        let bytes = reply
            .open_sources
            .len()
            .saturating_mul(3)
            .saturating_add(4096);
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        if !pending.active.load(Ordering::Acquire) {
            return Err(replica_denied(
                "open read closed before authorization publication",
            ));
        }
        // Ready pages may be evicted without changing the pinned open identity.
        state.entries.remove(&key);
        while state.entries.len() >= 8
            || state
                .entries
                .values()
                .map(|v| v.bytes)
                .sum::<usize>()
                .saturating_add(bytes)
                > 32 * 1024 * 1024
        {
            let old = state
                .entries
                .keys()
                .next()
                .cloned()
                .ok_or_else(|| replica_denied("open read cache byte budget exceeded"))?;
            state.entries.remove(&old);
        }
        let remaining = reply
            .expires_at_unix_ms
            .saturating_sub(completed)
            .min(5_000);
        state.entries.insert(
            key,
            OpenEntry {
                grant: check.grant,
                sources,
                deadline: std::time::Instant::now() + std::time::Duration::from_millis(remaining),
                expires: reply.expires_at_unix_ms,
                active: pending.active.clone(),
                bytes,
                fast: None,
            },
        );
        drop(state);
        Ok(())
    }
}

#[cfg(test)]
mod test_tls;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, atomic::AtomicUsize};
    struct PageValidator {
        calls: AtomicUsize,
        gate: Option<Arc<Barrier>>,
    }
    impl DfsReadGrantValidator for PageValidator {
        fn validate(
            &self,
            request: crate::dfs::ValidateDfsReadGrants,
        ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.wait();
                gate.wait();
            }
            let validation = request.validations[0].clone();
            let sources = ["object-1", "object-2"].map(|id| crate::dfs::OpenReadSource {
                chunk_id: crate::dfs::ChunkId::new(id),
                copy_ids: vec![crate::dfs::CopyId::new("copy")],
                ranges: vec![(0, 4096)],
            });
            Ok(vec![crate::dfs::DfsAuthorizedRead {
                expires_at_unix_ms: crate::meta::store::now_unix_ms() + 4_000,
                validation,
                allowed_ranges: vec![(0, 4096)],
                open_sources: serde_json::to_vec(&sources).unwrap(),
            }])
        }
    }
    fn connection() -> (
        Arc<dyn std::any::Any + Send + Sync>,
        Option<DfsOpenReadConnection>,
    ) {
        let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(());
        let identity = Some(DfsOpenReadConnection(Arc::downgrade(&owner)));
        (owner, identity)
    }
    fn request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "v".into(),
            layout_root_id: "l".into(),
            operations: vec![afs_protocol::node_data::DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "object-1".into(),
                source_copy_id: "copy".into(),
                chunk_offset: 0,
                length: 1024,
                destination_offset: 0,
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "n".into(),
                    file_version_id: "v".into(),
                    layout_root_id: "l".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: crate::meta::store::now_unix_ms() + 30_000,
                    fence: 1,
                    token: "dfs-open-v1:read-open:test".into(),
                }),
            }],
        }
    }
    struct PipelineTlsFixture {
        client:
            afs_protocol::node_data::dfs_chunks_client::DfsChunksClient<tonic::transport::Channel>,
        server: tokio::task::JoinHandle<()>,
    }
    async fn pipeline_tls_fixture(
        local: Arc<LocalChunkStore>,
        validator: Arc<dyn DfsReadGrantValidator>,
        limits: crate::node::read_pipeline::ReadPipelineLimits,
    ) -> PipelineTlsFixture {
        pipeline_tls_modes(local, validator, limits, true, true).await
    }
    async fn pipeline_tls_modes(
        local: Arc<LocalChunkStore>,
        validator: Arc<dyn DfsReadGrantValidator>,
        limits: crate::node::read_pipeline::ReadPipelineLimits,
        server_encrypted: bool,
        client_encrypted: bool,
    ) -> PipelineTlsFixture {
        use super::test_tls::*;
        use tonic::transport::Endpoint;
        struct Peer;
        impl PeerAuthenticator for Peer {
            fn authenticate(
                &self,
                _: &tonic::metadata::MetadataMap,
                _: Option<SocketAddr>,
                cert: Option<&[u8]>,
            ) -> afs_error::Result<String> {
                assert!(
                    cert.is_some(),
                    "test must use an authenticated TLS connection"
                );
                Ok("reader".into())
            }
        }
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let service = make_dfs_chunks_server_with_transport(
            Some(local),
            Arc::new(Peer),
            authorizer.clone(),
            Arc::new(DenyDfsReplicaAuthorizer),
            None,
            std::time::Duration::from_secs(3),
            DfsChunkTransportResources {
                read_pipeline: limits,
                rdma_sessions: super::super::super::control::RdmaSessionRegistry::new(None),
                payload_metrics: None,
                hixl: None,
            },
        );
        let credentials = tempfile::tempdir().unwrap();
        let make_policy = |encrypted, cert: &str, key: &str, label| {
            let ca = credentials.path().join("ca.pem");
            std::fs::write(&ca, CA_PEM).unwrap();
            let c = credentials.path().join(format!("{label}.pem"));
            std::fs::write(&c, cert).unwrap();
            let k = credentials.path().join(format!("{label}.key"));
            std::fs::write(&k, key).unwrap();
            afs_transport::SecurityManager::new(afs_transport::TlsConfig::MutualTls {
                encryption_enabled: encrypted,
                ca_certificate: ca,
                identity_certificate: c,
                identity_private_key: k,
                server_name: "localhost".into(),
            })
            .unwrap()
        };
        let server_security =
            make_policy(server_encrypted, SERVER_CERT_PEM, SERVER_KEY_PEM, "server");
        let client_security =
            make_policy(client_encrypted, CLIENT_CERT_PEM, CLIENT_KEY_PEM, "client");
        let incoming = server_security
            .incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let channel = client_security
            .connect_lazy(
                Endpoint::from_shared(client_security.endpoint_uri(&endpoint))
                    .unwrap()
                    .initial_stream_window_size(65_535)
                    .initial_connection_window_size(65_535),
            )
            .unwrap();
        let client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::new(channel);
        PipelineTlsFixture { client, server }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_tls_modes_keep_open_authorization_fast_lane() {
        for (bulk, client_bulk) in [(true, true), (false, false)] {
            let temp = tempfile::tempdir().unwrap();
            let local = Arc::new(
                LocalChunkStore::open(temp.path(), "node-a")
                    .unwrap()
                    .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
            );
            let validator = Arc::new(PageValidator {
                calls: AtomicUsize::new(0),
                gate: None,
            });
            let PipelineTlsFixture { mut client, server } = pipeline_tls_modes(
                local,
                validator.clone(),
                crate::node::read_pipeline::ReadPipelineLimits {
                    ..Default::default()
                },
                bulk,
                client_bulk,
            )
            .await;
            let mut warm = tonic::Request::new(request());
            warm.metadata_mut()
                .insert("x-afs-open-fast", "1".parse().unwrap());
            warm.metadata_mut()
                .insert("x-afs-read-crc-mode", "off".parse().unwrap());
            let mut body = client.read_ranges(warm).await.unwrap().into_inner();
            let _ = body.message().await;
            drop(body);
            let mut group = request();
            let token = group.operations[0].grant.as_ref().unwrap().token.clone();
            group.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
                token,
                ..Default::default()
            });
            let mut second = group.operations[0].clone();
            second.operation_index = 1;
            second.chunk_offset = 1024;
            second.destination_offset = 1024;
            group.operations.push(second);
            let mut group = tonic::Request::new(group);
            group
                .metadata_mut()
                .insert("x-afs-open-fast", "1".parse().unwrap());
            group
                .metadata_mut()
                .insert("x-afs-read-crc-mode", "off".parse().unwrap());
            let response = client.read_ranges(group).await;
            server.abort();
            assert!(
                response.is_ok(),
                "enabled authenticated grouped request must reach fixed-open admission: {:?}",
                response.as_ref().err()
            );
            let response = response.unwrap();
            assert_eq!(response.metadata().get("x-afs-open-fast").unwrap(), "1");
            assert!(response.metadata().get("x-afs-read-batch-v1").is_some());
            assert_eq!(
                validator.calls.load(Ordering::SeqCst),
                1,
                "same one open authorization in both modes"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unified_tls_mode_mismatch_fails_before_authorization() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let PipelineTlsFixture { mut client, server } =
            pipeline_tls_modes(local, validator.clone(), Default::default(), true, false).await;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            client.read_ranges(request()),
        )
        .await;
        server.abort();
        assert!(response.is_err() || response.unwrap().is_err());
        assert_eq!(validator.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_concurrency_real_mtls_service_routes_group_to_fast_lane() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let PipelineTlsFixture { mut client, server } = pipeline_tls_fixture(
            local,
            validator,
            crate::node::read_pipeline::ReadPipelineLimits {
                ..Default::default()
            },
        )
        .await;
        let mut warm = tonic::Request::new(request());
        warm.metadata_mut()
            .insert("x-afs-open-fast", "1".parse().unwrap());
        warm.metadata_mut()
            .insert("x-afs-read-crc-mode", "off".parse().unwrap());
        let mut body = client.read_ranges(warm).await.unwrap().into_inner();
        let _ = body.message().await;
        drop(body);
        let mut group = request();
        let token = group.operations[0].grant.as_ref().unwrap().token.clone();
        group.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
            token,
            ..Default::default()
        });
        let mut second = group.operations[0].clone();
        second.operation_index = 1;
        second.chunk_offset = 1024;
        second.destination_offset = 1024;
        group.operations.push(second);
        let mut group = tonic::Request::new(group);
        group
            .metadata_mut()
            .insert("x-afs-open-fast", "1".parse().unwrap());
        group
            .metadata_mut()
            .insert("x-afs-read-crc-mode", "off".parse().unwrap());
        let response = client.read_ranges(group).await;
        server.abort();
        assert!(
            response.is_ok(),
            "enabled authenticated grouped request must reach fixed-open admission: {:?}",
            response.as_ref().err()
        );
        let response = response.unwrap();
        assert_eq!(response.metadata().get("x-afs-open-fast").unwrap(), "1");
        assert!(response.metadata().get("x-afs-read-batch-v1").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_concurrency_stream_lifetime_is_not_the_resource_admission_timeout() {
        use crate::node::chunk::{ChunkStore, StagedChunk};
        struct SourceValidator(crate::dfs::OpenReadSource);
        impl DfsReadGrantValidator for SourceValidator {
            fn validate(
                &self,
                request: crate::dfs::ValidateDfsReadGrants,
            ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
                let validation = request.validations[0].clone();
                Ok(vec![crate::dfs::DfsAuthorizedRead {
                    expires_at_unix_ms: crate::meta::store::now_unix_ms() + 10000,
                    validation,
                    allowed_ranges: self.0.ranges.clone(),
                    open_sources: serde_json::to_vec(std::slice::from_ref(&self.0)).unwrap(),
                }])
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let expected = vec![37; 4 * 1024 * 1024];
        let staged = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("stream-lifetime"),
            expected.clone(),
        )
        .unwrap();
        local.put(staged.clone()).unwrap();
        let validator = Arc::new(SourceValidator(crate::dfs::OpenReadSource {
            chunk_id: staged.chunk.id.clone(),
            copy_ids: vec![crate::dfs::CopyId::new("copy")],
            ranges: vec![(0, expected.len() as u64)],
        }));
        let PipelineTlsFixture { mut client, server } = pipeline_tls_fixture(
            local,
            validator,
            crate::node::read_pipeline::ReadPipelineLimits {
                task_timeout_ms: 5000,
                ..Default::default()
            },
        )
        .await;
        let mut r = request();
        r.operations[0].chunk_id = staged.chunk.id.0.clone();
        r.operations[0].length = expected.len() as u64;
        let mut r = tonic::Request::new(r);
        r.set_timeout(std::time::Duration::from_secs(5));
        r.metadata_mut()
            .insert("x-afs-open-fast", "1".parse().unwrap());
        r.metadata_mut()
            .insert("x-afs-read-crc-mode", "off".parse().unwrap());
        // This checks a 20ms admission bound, not a 1s throughput guarantee.
        let started = std::time::Instant::now();
        let mut stream = client.read_ranges(r).await.unwrap().into_inner();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut received = Vec::new();
        let mut error = None;
        loop {
            match stream.message().await {
                Ok(Some(frame)) => {
                    if let Some(afs_protocol::node_data::dfs_read_ranges_frame::Body::Data(bytes)) =
                        frame.body
                    {
                        received.extend_from_slice(&bytes);
                    }
                }
                Ok(None) => break,
                Err(failure) => {
                    error = Some(failure);
                    break;
                }
            }
        }
        server.abort();
        eprintln!(
            "slow consumer: elapsed={:?}, received={}, error={error:?}",
            started.elapsed(),
            received.len()
        );
        assert!(
            error.is_none(),
            "stream must outlive 20ms resource admission window within request deadline: {error:?}"
        );
        assert_eq!(received, expected);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retirement_mtls_disconnect_retires_context() {
        use super::test_tls::*;
        use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};
        struct Peer;
        impl PeerAuthenticator for Peer {
            fn authenticate(
                &self,
                _: &tonic::metadata::MetadataMap,
                _: Option<SocketAddr>,
                cert: Option<&[u8]>,
            ) -> afs_error::Result<String> {
                assert!(
                    cert.is_some(),
                    "test must use an authenticated TLS connection"
                );
                Ok("reader".into())
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let service = make_dfs_chunks_server(Some(local), Arc::new(Peer), authorizer.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .client_ca_root(Certificate::from_pem(CA_PEM))
                        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM)),
                )
                .unwrap()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(endpoint)
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(CA_PEM))
                    .identity(Identity::from_pem(CLIENT_CERT_PEM, CLIENT_KEY_PEM))
                    .domain_name("localhost"),
            )
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::new(channel);
        let mut r = tonic::Request::new(request());
        r.metadata_mut()
            .insert("x-afs-open-fast", "1".parse().unwrap());
        r.metadata_mut()
            .insert("x-afs-read-crc-mode", "off".parse().unwrap());
        // The object is intentionally absent: context admission still precedes IO.
        let mut body = client.read_ranges(r).await.unwrap().into_inner();
        let _ = body.message().await;
        assert_eq!(authorizer.open_cache.lock().unwrap().entries.len(), 1);
        let context = authorizer
            .open_cache
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .fast
            .clone()
            .unwrap();
        drop(body);
        drop(client);
        let retired = tokio::time::timeout(std::time::Duration::from_secs(4), async {
            loop {
                if authorizer.open_cache.lock().unwrap().entries.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        server.abort();
        let _ = server.await;
        assert!(
            retired.is_ok(),
            "disconnected context must retire without a later read"
        );
        assert!(!context.active.load(Ordering::Acquire));
        assert!(context.readers.lock().unwrap().is_empty());
    }
    #[test]
    fn retirement_close_drops_cached_descriptors_but_admitted_io_drains() {
        use crate::node::chunk::{ChunkStore, StagedChunk};
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off);
        let staged =
            StagedChunk::new_crc32c(crate::dfs::OperationId::new("close-drain"), vec![29; 4096])
                .unwrap();
        local.put(staged.clone()).unwrap();
        let reader = Arc::new(local.open_fixed_peer_reader(&staged.chunk.id).unwrap());
        let weak = Arc::downgrade(&reader);
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator, "receiver".into(), 1);
        let (_owner, connection) = connection();
        let r = request();
        let context = authorizer
            .authorize_fast_open("reader", &r, connection)
            .unwrap()
            .unwrap();
        context
            .readers
            .lock()
            .unwrap()
            .insert(staged.chunk.id.clone(), reader.clone());
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(!context.active.load(Ordering::Acquire));
        assert!(
            context.readers.lock().unwrap().is_empty(),
            "close must release descriptor cache even with an admitted context Arc"
        );
        let bytes = local
            .read_fixed_peer_range(
                &reader,
                0,
                4096,
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .unwrap();
        assert_eq!(bytes.as_slice(), &[29; 4096]);
        drop(bytes);
        drop(reader);
        assert!(weak.upgrade().is_none());
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap(); // idempotent
    }

    #[test]
    fn retirement_disconnect_during_meta_setup_cannot_publish_context() {
        let gate = Arc::new(Barrier::new(2));
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: Some(gate.clone()),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        ));
        let (owner, connection) = connection();
        let pending = authorizer.clone();
        let worker = std::thread::spawn(move || {
            pending.authorize_fast_open("reader", &request(), connection)
        });
        gate.wait();
        drop(owner);
        gate.wait();
        assert!(
            worker.join().unwrap().is_err(),
            "disconnected setup cannot publish a reusable context"
        );
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn read_concurrency_cached_authority_lookup_has_no_meta_and_keeps_identity_checks() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let (_owner, connection) = connection();
        let mut r = request();
        assert!(
            authorizer
                .cached_fast_open_context("reader", &r, connection.as_ref())
                .unwrap()
                .is_none()
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 0);
        authorizer
            .authorize_fast_open("reader", &r, connection.clone())
            .unwrap()
            .unwrap();
        for _ in 0..8 {
            assert!(
                authorizer
                    .cached_fast_open_context("reader", &r, connection.as_ref())
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        r.file_version_id = "other-version".into();
        assert!(
            authorizer
                .cached_fast_open_context("reader", &r, connection.as_ref())
                .is_err()
        );
        r = request();
        let (_other_owner, other) = self::connection();
        assert!(
            authorizer
                .cached_fast_open_context("reader", &r, other.as_ref())
                .unwrap()
                .is_none()
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(
            authorizer
                .cached_fast_open_context("reader", &r, connection.as_ref())
                .unwrap()
                .is_none()
        );
    }

    fn async_fixed_fixture() -> (
        tempfile::TempDir,
        Arc<LocalChunkStore>,
        Arc<DfsOpenReadContext>,
        DfsReadRangesRequest,
        Vec<u8>,
    ) {
        use crate::node::chunk::{ChunkStore, StagedChunk};
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        let staged = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("async-fixed-read"),
            data.clone(),
        )
        .unwrap();
        local.put(staged.clone()).unwrap();
        let (_owner, connection) = connection();
        let context = Arc::new(DfsOpenReadContext {
            version: "v".into(),
            layout: "l".into(),
            sources: HashMap::from([(
                staged.chunk.id.0.clone(),
                crate::dfs::OpenReadSource {
                    chunk_id: staged.chunk.id.clone(),
                    copy_ids: vec![crate::dfs::CopyId::new("copy")],
                    ranges: vec![(0, data.len() as u64)],
                },
            )]),
            active: Arc::new(AtomicBool::new(true)),
            connection: connection.unwrap(),
            readers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            readers_registered: AtomicBool::new(false),
        });
        let mut r = request();
        r.operations[0].chunk_id = staged.chunk.id.0.clone();
        r.operations[0].length = data.len() as u64;
        (temp, local, context, r, data)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_concurrency_async_payload_preserves_bytes_and_last_reference_budget() {
        use std::time::{Duration, Instant};
        let (_temp, local, context, r, data) = async_fixed_fixture();
        let payload = context
            .payload_async(
                local.clone(),
                r.operations[0].clone(),
                Instant::now() + Duration::from_secs(3),
                Arc::new(tokio::sync::Semaphore::new(2)),
            )
            .await
            .unwrap();
        assert_eq!(payload.as_slice(), data);
        let shared = payload.into_shared_bytes();
        let last = shared.slice(0..1024);
        drop(shared);
        let full = crate::node::chunk::PEER_READ_PAYLOAD_LIMIT - crate::node::chunk_crc32c::SEGMENT;
        let blocked = tokio::time::timeout(
            Duration::from_millis(20),
            local.reserve_fixed_read_until(full, Instant::now() + Duration::from_secs(3)),
        )
        .await;
        assert!(
            !matches!(blocked, Ok(Ok(_))),
            "live final frame must retain its body charge"
        );
        drop(last);
        let reservation = tokio::time::timeout(
            Duration::from_secs(1),
            local.reserve_fixed_read_until(full, Instant::now() + Duration::from_secs(3)),
        )
        .await
        .unwrap()
        .unwrap();
        drop(reservation);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_concurrency_group_send_backpressure_releases_disk_and_drains_children() {
        use std::time::{Duration, Instant};
        let (_temp, local, context, mut request, data) = async_fixed_fixture();
        request.operations[0].length = (data.len() / 2) as u64;
        let mut second = request.operations[0].clone();
        second.operation_index = 1;
        second.chunk_offset = second.length;
        second.destination_offset = second.length;
        request.operations.push(second);
        assert!(context.admits(&request).unwrap());
        let mut invalid = request.clone();
        invalid.operations[1].destination_offset = 0;
        assert!(context.admits(&invalid).is_err());
        let disk = Arc::new(tokio::sync::Semaphore::new(2));
        let service = Arc::new(tokio::sync::Semaphore::new(1));
        let lease = Arc::new(service.clone().acquire_owned().await.unwrap());
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let observe = sender.clone();
        let producer = super::super::read_pipeline::AsyncReadProducer {
            local: local.clone(),
            context,
            request,
            sender,
            metrics: None,
            deadline: Instant::now() + Duration::from_secs(3),
            disk: disk.clone(),
            window: 2,
            service: lease,
        };
        let task = tokio::spawn(async move { producer.produce().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while observe.capacity() != 0 || disk.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        assert_eq!(service.available_permits(), 0);
        drop(receiver);
        assert!(task.await.unwrap().is_err());
        assert_eq!(disk.available_permits(), 2);
        assert_eq!(service.available_permits(), 1);
        let full = crate::node::chunk::PEER_READ_PAYLOAD_LIMIT - crate::node::chunk_crc32c::SEGMENT;
        drop(
            local
                .reserve_fixed_read_until(full, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
        );
    }

    #[test]
    fn retirement_missing_cold_file_quarantines_without_cache_lock_recursion() {
        use crate::node::chunk::{ChunkStore, StagedChunk};
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let staged = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("missing-cold-file"),
            vec![29; 4096],
        )
        .unwrap();
        local.put(staged.clone()).unwrap();
        std::fs::remove_file(temp.path().join("chunks").join(&staged.chunk.id.0)).unwrap();
        let (_owner, connection) = connection();
        let context = DfsOpenReadContext {
            version: "v".into(),
            layout: "l".into(),
            sources: HashMap::from([(
                staged.chunk.id.0.clone(),
                crate::dfs::OpenReadSource {
                    chunk_id: staged.chunk.id.clone(),
                    copy_ids: vec![crate::dfs::CopyId::new("copy")],
                    ranges: vec![(0, 4096)],
                },
            )]),
            active: Arc::new(AtomicBool::new(true)),
            connection: connection.unwrap(),
            readers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            readers_registered: AtomicBool::new(false),
        };
        let mut r = request();
        r.operations[0].chunk_id = staged.chunk.id.0.clone();
        assert!(context.admits(&r).unwrap());
        let (sender, receiver) = std::sync::mpsc::channel();
        let store = local.clone();
        let worker = std::thread::spawn(move || {
            let result = context.payload(
                &store,
                &r.operations[0],
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            );
            sender.send(result.is_err()).unwrap();
        });
        assert_eq!(
            receiver.recv_timeout(std::time::Duration::from_secs(2)),
            Ok(true),
            "missing cold file must fail without reader-cache lock recursion"
        );
        worker.join().unwrap();
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn retirement_no_connection_cannot_publish_reusable_authority() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        assert!(
            authorizer
                .authorize_fast_open("reader", &request(), None)
                .unwrap()
                .is_none()
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 0);
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn fast_open_context_survives_source_ttl_and_close_retires_it() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let (_owner, connection) = connection();
        let r = request();
        let first = authorizer
            .authorize_fast_open("reader", &r, connection.clone())
            .unwrap();
        assert!(
            first.is_some(),
            "fast open must publish a reusable fixed context"
        );
        for entry in authorizer.open_cache.lock().unwrap().entries.values_mut() {
            entry.deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        }
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            validator.calls.load(Ordering::SeqCst),
            1,
            "source cache TTL is not the open lifetime"
        );
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }
    #[test]
    fn fast_open_compact_range_cannot_change_version_object_or_peer() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let (_owner, connection) = connection();
        let mut r = request();
        let context = authorizer
            .authorize_fast_open("reader", &r, connection.clone())
            .unwrap()
            .unwrap();
        let token = r.operations[0].grant.as_ref().unwrap().token.clone();
        r.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
            token,
            ..Default::default()
        });
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .unwrap()
                .is_some()
        );
        assert!(
            authorizer
                .authorize_fast_open("other-peer", &r, connection.clone())
                .is_err()
        );
        r.file_version_id = "other-version".into();
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .is_err()
        );
        r.file_version_id = "v".into();
        r.operations[0].chunk_offset = u64::MAX;
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .is_err()
        );
        r.operations[0].chunk_offset = 0;
        r.operations[0].chunk_id = "object-2".into();
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .unwrap()
                .is_some()
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(!context.active.load(Ordering::Acquire));
        assert!(
            context.admits(&r).is_err(),
            "close stops new admission while existing Arc may drain"
        );
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, connection.clone())
                .is_err(),
            "compact cache miss requires explicit same-version setup"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_open_grpc_recovers_lost_context_and_closed_owner_is_denied() {
        use super::test_tls::*;
        use crate::dfs::*;
        use crate::node::{
            chunk::{ChunkStore, LocalChunkStore, StagedChunk},
            dfs_read::{ChunkReadOp, ChunkTransfer, PeerReadBatch, ResolvedReadOp},
            rpc::peer::{GrpcChunkTransfer, PeerConnectionPool},
        };
        use tonic::transport::{Certificate, Identity, ServerTlsConfig};
        struct Peer;
        impl PeerAuthenticator for Peer {
            fn authenticate(
                &self,
                _: &tonic::metadata::MetadataMap,
                _: Option<std::net::SocketAddr>,
                _: Option<&[u8]>,
            ) -> afs_error::Result<String> {
                Ok("reader".into())
            }
        }
        struct Validator {
            calls: AtomicUsize,
            live: AtomicBool,
        }
        impl DfsReadGrantValidator for Validator {
            fn validate(
                &self,
                r: ValidateDfsReadGrants,
            ) -> afs_error::Result<Vec<DfsAuthorizedRead>> {
                if !self.live.load(Ordering::Acquire) {
                    return Err(replica_denied("open owner is closed"));
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                let validation = r.validations[0].clone();
                let sources = vec![OpenReadSource {
                    chunk_id: validation.chunk_id.clone(),
                    copy_ids: vec![validation.copy_id.clone()],
                    ranges: vec![(0, 65536)],
                }];
                Ok(vec![DfsAuthorizedRead {
                    expires_at_unix_ms: validation
                        .grant
                        .expires_at_unix_ms
                        .min(crate::meta::store::now_unix_ms() + 4000),
                    validation,
                    allowed_ranges: vec![(0, 65536)],
                    open_sources: serde_json::to_vec(&sources).unwrap(),
                }])
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let staged =
            StagedChunk::new_crc32c(OperationId::new("grpc-fixed"), vec![27; 65536]).unwrap();
        local.put(staged.clone()).unwrap();
        let validator = Arc::new(Validator {
            calls: AtomicUsize::new(0),
            live: AtomicBool::new(true),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator.clone(),
            "receiver".into(),
            1,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let service = make_dfs_chunks_server(Some(local), Arc::new(Peer), authorizer.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .client_ca_root(Certificate::from_pem(CA_PEM))
                        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM)),
                )
                .unwrap()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let ca = temp.path().join("ca.pem");
        let cert = temp.path().join("client.pem");
        let key = temp.path().join("client-key.pem");
        std::fs::write(&ca, CA_PEM).unwrap();
        std::fs::write(&cert, CLIENT_CERT_PEM).unwrap();
        std::fs::write(&key, CLIENT_KEY_PEM).unwrap();
        let peers = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::MutualTls {
                    encryption_enabled: true,
                    ca_certificate: ca,
                    identity_certificate: cert,
                    identity_private_key: key,
                    server_name: "localhost".into(),
                },
                8,
            )
            .unwrap()
            .with_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let transfer =
            Arc::new(GrpcChunkTransfer::new(peers, std::time::Duration::from_secs(5)).unwrap());
        let batch = PeerReadBatch {
            file_version_id: FileVersionId::new("v"),
            layout_root_id: LayoutRootId::new("l"),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: staged.chunk.id.clone(),
                    chunk_offset: 0,
                    length: 65536,
                    output_offset: 0,
                },
                source: SourceCandidate {
                    copy_id: CopyId::new("copy"),
                    chunk_id: staged.chunk.id.clone(),
                    role: CopyRole::DurableReplica,
                    state: CopyState::Ready,
                    location: CopyLocation::Node {
                        node_id: "node-a".into(),
                        node_epoch: 1,
                        device_id: "d".into(),
                        device_epoch: 1,
                        catalog_revision: 1,
                    },
                    data_endpoint: Some(endpoint),
                    load_hint: 0,
                    read_grant: DfsReadGrant {
                        namespace_id: NamespaceId::new("n"),
                        file_version_id: FileVersionId::new("v"),
                        layout_root_id: LayoutRootId::new("l"),
                        caller_node_id: "reader".into(),
                        caller_node_epoch: 1,
                        expires_at_unix_ms: crate::meta::store::now_unix_ms() + 30000,
                        fence: 1,
                        token: "dfs-open-v1:read-open:test".into(),
                    },
                },
                chunk: None,
            }],
        };
        async fn read(
            transfer: Arc<GrpcChunkTransfer>,
            batch: PeerReadBatch,
            chunk: ChunkObject,
        ) -> afs_error::Result<Vec<u8>> {
            tokio::task::spawn_blocking(move || {
                let mut out = vec![0; 65536];
                transfer.read_crc32c_range(&batch, &chunk, &mut out)?;
                Ok(out)
            })
            .await
            .unwrap()
        }
        assert_eq!(
            read(transfer.clone(), batch.clone(), staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536]
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        let mut expired = batch.clone();
        expired.operations[0].source.read_grant.expires_at_unix_ms = 1;
        assert_eq!(
            read(transfer.clone(), expired, staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536],
            "fixed context outlives source grant TTL"
        );
        authorizer.open_cache.lock().unwrap().entries.clear(); // node cache loss, durable owner remains live.
        assert_eq!(
            read(transfer.clone(), batch.clone(), staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536]
        );
        assert_eq!(
            validator.calls.load(Ordering::SeqCst),
            2,
            "one full setup retry rebuilds the same version context"
        );
        validator.live.store(false, Ordering::Release);
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(read(transfer.clone(), batch, staged.chunk).await.is_err());
        server.abort();
    }
    #[test]
    fn fast_open_new_connection_must_reestablish_file_authority() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let r = request();
        let (_owner_a, a) = connection();
        let (_owner_b, b) = connection(); // Even if the same socket address is reused.
        authorizer
            .authorize_fast_open("reader", &r, a.clone())
            .unwrap();
        let mut compact = r.clone();
        compact.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
            token: r.operations[0].grant.as_ref().unwrap().token.clone(),
            ..Default::default()
        });
        assert!(
            authorizer
                .authorize_fast_open("reader", &compact, a.clone())
                .unwrap()
                .is_some()
        );
        assert!(
            authorizer
                .authorize_fast_open("reader", &compact, b.clone())
                .is_err()
        );
        authorizer.authorize_fast_open("reader", &r, b).unwrap();
        assert_eq!(validator.calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn open_authority_one_file_page_reuses_across_objects() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let mut r = request();
        authorizer.authorize("reader", &r).unwrap();
        r.operations[0].chunk_offset = 1024;
        authorizer.authorize("reader", &r).unwrap();
        r.operations[0].chunk_id = "object-2".into();
        authorizer.authorize("reader", &r).unwrap();
        let mut second = r.operations[0].clone();
        second.operation_index = 1;
        second.destination_offset = 1024;
        second.chunk_offset = 2048;
        r.operations.push(second);
        authorizer.authorize("reader", &r).unwrap();
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn open_authority_close_cancels_pending_meta_reply() {
        let gate = Arc::new(Barrier::new(2));
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: Some(gate.clone()),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        ));
        let a = authorizer.clone();
        let pending = std::thread::spawn(move || a.authorize("reader", &request()));
        gate.wait();
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        gate.wait();
        assert!(pending.join().unwrap().is_err());
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }
}
