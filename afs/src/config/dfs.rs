//! DFS settings: read CRC mode, replication, read budgets, version cache,
//! write/queue/GC/Peer-serve tuning, and the storage checks (S3 external
//! backend, write buffers, Peer cache).

use super::{
    Cli, Config, DataMode, DfsLocalCopy, DfsReadCrcMode, FileConfig,
    MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES, Role, invalid,
};
use afs_error::Result;

pub(super) fn resolve_read_crc_mode(
    cli: &Cli,
    file: &FileConfig,
    dfs: bool,
    data_mode: DataMode,
) -> DfsReadCrcMode {
    // This policy is implemented only for native CRC-format gRPC reads.
    // Keep unrelated backends and transports on their existing path.
    let default_read_crc_mode =
        if dfs && file.dfs_external_backend.is_none() && data_mode == DataMode::Grpc {
            DfsReadCrcMode::default()
        } else {
            DfsReadCrcMode::Both
        };
    cli.dfs_read_crc_mode
        .or(file.dfs_read_crc_mode)
        .unwrap_or(default_read_crc_mode)
}

/// Replication policy (Meta uses it; Node resolves the same keys).
pub(super) struct DfsReplication {
    pub(super) dfs_desired_copies: u16,
    pub(super) dfs_sync_required_copies: u16,
    pub(super) dfs_min_distinct_nodes: u16,
    pub(super) dfs_min_distinct_failure_domains: u16,
    pub(super) dfs_local_copy: DfsLocalCopy,
}

pub(super) fn resolve_replication(cli: &mut Cli, file: &mut FileConfig) -> Result<DfsReplication> {
    let dfs_desired_copies = cli
        .dfs_desired_copies
        .or(file.dfs_desired_copies)
        .unwrap_or(1);
    let dfs_sync_required_copies = cli
        .dfs_sync_required_copies
        .or(file.dfs_sync_required_copies)
        .unwrap_or(1);
    let dfs_min_distinct_nodes = cli
        .dfs_min_distinct_nodes
        .or(file.dfs_min_distinct_nodes)
        .unwrap_or(1);
    let dfs_min_distinct_failure_domains = cli
        .dfs_min_distinct_failure_domains
        .or(file.dfs_min_distinct_failure_domains)
        .unwrap_or(1);
    let dfs_local_copy = cli
        .dfs_local_copy
        .take()
        .or(file.dfs_local_copy.take())
        .unwrap_or(DfsLocalCopy::Required);
    if dfs_sync_required_copies == 0
        || dfs_sync_required_copies > dfs_desired_copies
        || dfs_min_distinct_nodes == 0
        || dfs_min_distinct_nodes > dfs_desired_copies
        || dfs_min_distinct_failure_domains == 0
        || dfs_min_distinct_failure_domains > dfs_desired_copies
    {
        return Err(invalid(
            "DFS replication counts must be non-zero and no greater than dfs_desired_copies",
        ));
    }
    Ok(DfsReplication {
        dfs_desired_copies,
        dfs_sync_required_copies,
        dfs_min_distinct_nodes,
        dfs_min_distinct_failure_domains,
        dfs_local_copy,
    })
}

/// Read batching and source-metadata cache.
pub(super) struct DfsReadBudget {
    pub(super) dfs_read_max_ops_per_batch: usize,
    pub(super) dfs_read_max_inflight_bytes: u64,
    pub(super) dfs_source_cache_max_ms: u64,
    pub(super) dfs_source_lookahead_chunks: usize,
    pub(super) dfs_source_cache_bytes: usize,
}

pub(super) fn resolve_read_budget(cli: &Cli, file: &FileConfig) -> Result<DfsReadBudget> {
    let dfs_read_max_ops_per_batch = cli
        .dfs_read_max_ops_per_batch
        .or(file.dfs_read_max_ops_per_batch)
        .unwrap_or(128);
    let dfs_read_max_inflight_bytes = cli
        .dfs_read_max_inflight_bytes
        .or(file.dfs_read_max_inflight_bytes)
        .unwrap_or(8 * 1024 * 1024);
    let dfs_source_cache_max_ms = cli
        .dfs_source_cache_max_ms
        .or(file.dfs_source_cache_max_ms)
        .unwrap_or(5000);
    let dfs_source_lookahead_chunks = cli
        .dfs_source_lookahead_chunks
        .or(file.dfs_source_lookahead_chunks)
        .unwrap_or(64);
    let dfs_source_cache_bytes = cli
        .dfs_source_cache_bytes
        .or(file.dfs_source_cache_bytes)
        .unwrap_or(4 * 1024 * 1024);
    if dfs_source_lookahead_chunks > 128 || dfs_source_cache_bytes > 4 * 1024 * 1024 {
        return Err(invalid(
            "DFS source metadata requires lookahead <= 128 chunks and cache <= 4194304 bytes",
        ));
    }
    if dfs_read_max_ops_per_batch == 0
        || dfs_read_max_inflight_bytes == 0
        || dfs_source_cache_max_ms > 60_000
    {
        return Err(invalid(
            "DFS read budgets must be non-zero and source cache TTL must be <= 60000 ms",
        ));
    }
    Ok(DfsReadBudget {
        dfs_read_max_ops_per_batch,
        dfs_read_max_inflight_bytes,
        dfs_source_cache_max_ms,
        dfs_source_lookahead_chunks,
        dfs_source_cache_bytes,
    })
}

/// `(dfs_version_cache_entries, dfs_version_cache_bytes)`.
pub(super) fn resolve_version_cache(
    role: Role,
    cli: &Cli,
    file: &FileConfig,
) -> Result<(usize, u64)> {
    let dfs_version_cache_entries = cli
        .dfs_version_cache_entries
        .or(file.dfs_version_cache_entries)
        .unwrap_or(4096);
    if !(1..=65536).contains(&dfs_version_cache_entries) {
        return Err(invalid("DFS version cache entries must be 1..=65536"));
    }
    let dfs_version_cache_bytes = cli
        .dfs_version_cache_bytes
        .or(file.dfs_version_cache_bytes)
        .unwrap_or(32 * 1024 * 1024);
    if !(1..=1024 * 1024 * 1024).contains(&dfs_version_cache_bytes) {
        return Err(invalid("DFS version cache bytes must be 1..=1073741824"));
    }
    #[cfg(feature = "dfs")]
    if role == Role::Node {
        let minimum = crate::node::vfs::dfs::version_cache_minimum_bytes(dfs_version_cache_entries)
            .ok_or_else(|| invalid("DFS version cache minimum byte estimate overflows"))?;
        if usize::try_from(dfs_version_cache_bytes)
            .ok()
            .is_none_or(|bytes| bytes < minimum)
        {
            return Err(invalid(format!(
                "DFS version cache bytes {dfs_version_cache_bytes} is below minimum {minimum} for {dfs_version_cache_entries} entries",
            )));
        }
    }
    #[cfg(not(feature = "dfs"))]
    let _ = role;
    Ok((dfs_version_cache_entries, dfs_version_cache_bytes))
}

/// Write, queue, GC and Peer-serve tuning, each a value plus its range check.
pub(super) struct DfsTuning {
    pub(super) dfs_outboard_work_wait_ms: u64,
    pub(super) dfs_catalog_commit_queue: usize,
    pub(super) dfs_write_queue_bytes: u64,
    pub(super) dfs_dirty_budget_bytes: u64,
    pub(super) dfs_gc_grace_ms: u64,
    pub(super) dfs_unpublished_descriptors: usize,
    pub(super) dfs_upload_inflight_chunks: usize,
    pub(super) dfs_background_commit_ms: u64,
    pub(super) dfs_sidecar_install_queue: usize,
    pub(super) dfs_external_gc_list_pages_per_pass: usize,
    pub(super) dfs_fetch_max_inflight_objects: usize,
    pub(super) dfs_readahead_max_bytes: u64,
    pub(super) dfs_peer_serve_resident_objects: usize,
    pub(super) dfs_peer_serve_payload_bytes: u64,
    pub(super) dfs_peer_serve_wait_ms: u64,
}

pub(super) fn resolve_tuning(cli: &Cli, file: &FileConfig) -> Result<DfsTuning> {
    let dfs_outboard_work_wait_ms = cli
        .dfs_outboard_work_wait_ms
        .or(file.dfs_outboard_work_wait_ms)
        .unwrap_or(2);
    if dfs_outboard_work_wait_ms > 50 {
        return Err(invalid("DFS outboard work wait must be 0..=50 ms"));
    }
    let dfs_catalog_commit_queue = cli
        .dfs_catalog_commit_queue
        .or(file.dfs_catalog_commit_queue)
        .unwrap_or(256);
    if !(1..=256).contains(&dfs_catalog_commit_queue) {
        return Err(invalid("DFS catalog commit queue must be 1..=256"));
    }
    let dfs_write_queue_bytes = cli
        .dfs_write_queue_bytes
        .or(file.dfs_write_queue_bytes)
        .unwrap_or(2 * 1024 * 1024 * 1024);
    if !(8 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&dfs_write_queue_bytes)
        || usize::try_from(dfs_write_queue_bytes).is_err()
    {
        return Err(invalid(
            "DFS write queue bytes must be 8MiB..=1TiB and fit usize",
        ));
    }
    let dfs_dirty_budget_bytes = cli
        .dfs_dirty_budget_bytes
        .or(file.dfs_dirty_budget_bytes)
        .unwrap_or(64 * 1024 * 1024);
    if !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&dfs_dirty_budget_bytes) {
        return Err(invalid("DFS native dirty budget bytes must be 4..=64MiB"));
    }
    let dfs_gc_grace_ms = cli
        .dfs_gc_grace_ms
        .or(file.dfs_gc_grace_ms)
        .unwrap_or(1_000);
    if dfs_gc_grace_ms > 600_000 {
        return Err(invalid("DFS GC grace must be 0..=600000ms"));
    }
    let dfs_unpublished_descriptors = cli
        .dfs_unpublished_descriptors
        .or(file.dfs_unpublished_descriptors)
        .unwrap_or(1024);
    if !(1..=1024).contains(&dfs_unpublished_descriptors) {
        return Err(invalid("DFS unpublished descriptors must be 1..=1024"));
    }
    let dfs_upload_inflight_chunks = cli
        .dfs_upload_inflight_chunks
        .or(file.dfs_upload_inflight_chunks)
        .unwrap_or(8);
    if !(1..=8).contains(&dfs_upload_inflight_chunks) {
        return Err(invalid("DFS native upload inflight chunks must be 1..=8"));
    }
    let dfs_background_commit_ms = cli
        .dfs_background_commit_ms
        .or(file.dfs_background_commit_ms)
        .unwrap_or(5000);
    if !(1..=60000).contains(&dfs_background_commit_ms) {
        return Err(invalid(
            "DFS native background commit period must be 1..=60000ms",
        ));
    }
    let dfs_sidecar_install_queue = cli
        .dfs_sidecar_install_queue
        .or(file.dfs_sidecar_install_queue)
        .unwrap_or(64);
    if !(1..=64).contains(&dfs_sidecar_install_queue) {
        return Err(invalid("DFS sidecar install queue must be 1..=64"));
    }
    let dfs_external_gc_list_pages_per_pass = cli
        .dfs_external_gc_list_pages_per_pass
        .or(file.dfs_external_gc_list_pages_per_pass)
        .unwrap_or(1);
    if !(1..=100).contains(&dfs_external_gc_list_pages_per_pass) {
        return Err(invalid(
            "DFS external GC list pages per pass must be 1..=100",
        ));
    }
    let dfs_fetch_max_inflight_objects = cli
        .dfs_fetch_max_inflight_objects
        .or(file.dfs_fetch_max_inflight_objects)
        .unwrap_or(8);
    if !(1..=8).contains(&dfs_fetch_max_inflight_objects) {
        return Err(invalid("DFS fetch inflight objects must be 1..=8"));
    }
    let dfs_readahead_max_bytes = cli
        .dfs_readahead_max_bytes
        .or(file.dfs_readahead_max_bytes)
        .unwrap_or(0);
    if dfs_readahead_max_bytes > 32 * 1024 * 1024 {
        return Err(invalid("DFS readahead distance must be 0..=32MiB"));
    }
    let dfs_peer_serve_resident_objects = cli
        .dfs_peer_serve_resident_objects
        .or(file.dfs_peer_serve_resident_objects)
        .unwrap_or(2);
    if !(1..=16).contains(&dfs_peer_serve_resident_objects) {
        return Err(invalid("DFS Peer serve resident objects must be 1..=16"));
    }
    let dfs_peer_serve_payload_bytes = cli
        .dfs_peer_serve_payload_bytes
        .or(file.dfs_peer_serve_payload_bytes)
        .unwrap_or(16 * 1024 * 1024);
    if !(4 * 1024 * 1024..=64 * 1024 * 1024).contains(&dfs_peer_serve_payload_bytes) {
        return Err(invalid("DFS Peer serve payload bytes must be 4..=64 MiB"));
    }
    let dfs_peer_serve_wait_ms = cli
        .dfs_peer_serve_wait_ms
        .or(file.dfs_peer_serve_wait_ms)
        .unwrap_or(20);
    if dfs_peer_serve_wait_ms > 50 {
        return Err(invalid("DFS Peer serve wait must be 0..=50 ms"));
    }
    Ok(DfsTuning {
        dfs_outboard_work_wait_ms,
        dfs_catalog_commit_queue,
        dfs_write_queue_bytes,
        dfs_dirty_budget_bytes,
        dfs_gc_grace_ms,
        dfs_unpublished_descriptors,
        dfs_upload_inflight_chunks,
        dfs_background_commit_ms,
        dfs_sidecar_install_queue,
        dfs_external_gc_list_pages_per_pass,
        dfs_fetch_max_inflight_objects,
        dfs_readahead_max_bytes,
        dfs_peer_serve_resident_objects,
        dfs_peer_serve_payload_bytes,
        dfs_peer_serve_wait_ms,
    })
}

/// S3 external backend, write-buffer idle capacity and Peer cache.
pub(super) fn validate_storage(role: Role, cfg: &Config) -> Result<()> {
    if let Some(external) = &cfg.dfs_external_backend {
        external.validate()?;
        if !cfg.dfs
            || cfg.dfs_desired_copies != 1
            || cfg.dfs_sync_required_copies != 1
            || cfg.dfs_min_distinct_nodes != 1
            || cfg.dfs_min_distinct_failure_domains != 1
            || cfg.dfs_local_copy != DfsLocalCopy::Required
        {
            return Err(invalid(
                "external backend requires DFS with fixed single-copy defaults; Node replication combinations are unsupported",
            ));
        }
    }
    if cfg.dfs_write_buffer_idle_bytes > (1_u64 << 40)
        || usize::try_from(cfg.dfs_write_buffer_idle_bytes).is_err()
    {
        return Err(invalid(
            "dfs_write_buffer_idle_bytes must be in 0..=1 TiB and fit this platform; it limits idle capacity only",
        ));
    }
    if let Some(cache) = &cfg.dfs_peer_cache {
        let overlaps = |other: &std::path::Path| {
            cache.directory.starts_with(other) || other.starts_with(&cache.directory)
        };
        if role != Role::Node
            || !cfg.dfs
            || cfg.dfs_external_backend.is_some()
            || !cache.directory.is_absolute()
            || cache.directory.parent().is_none()
            || cache.directory.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || cache.capacity_bytes == 0
            || cache.capacity_bytes > MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES
            || overlaps(&cfg.data_dir)
            || [&cfg.dfs_mount, &cfg.ownerfs_mount]
                .into_iter()
                .flatten()
                .any(|mount| overlaps(mount))
        {
            return Err(invalid(
                "dfs_peer_cache requires a native DFS Node, a separate absolute directory without traversal, and capacity in 1..=1 TiB; external-backend Peer caching is not enabled",
            ));
        }
    }
    Ok(())
}
