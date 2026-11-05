//! FUSE threads and tasks, and the DFS read pipeline limits resolved with them.

use super::{Cli, FileConfig, invalid};
use afs_error::Result;

/// FUSE execution limits.
pub(super) struct FuseSettings {
    pub(super) fuse_threads: u16,
    pub(super) fuse_workers: u16,
    pub(super) fuse_max_tasks: usize,
    pub(super) dfs_read_pipeline: crate::node::read::admission::ReadPipelineLimits,
    pub(super) fuse_max_readahead_bytes: u32,
}

pub(super) fn resolve(cli: &Cli, file: &mut FileConfig) -> Result<FuseSettings> {
    let fuse_threads = cli.fuse_threads.or(file.fuse_threads).unwrap_or(4);
    let fuse_workers = cli.fuse_workers.or(file.fuse_workers).unwrap_or(32);
    let fuse_max_tasks = cli
        .fuse_max_tasks
        .or(file.fuse_max_tasks)
        .unwrap_or(usize::from(fuse_workers) * 8);
    let dfs_read_pipeline = file.dfs_read_pipeline.take().unwrap_or_default();
    let dfs_read_pipeline = dfs_read_pipeline.validate()?;
    let fuse_max_readahead_bytes = cli
        .fuse_max_readahead_bytes
        .or(file.fuse_max_readahead_bytes)
        .unwrap_or(16 * 1024 * 1024);
    if !(1..=64).contains(&fuse_threads)
        || !(1..=1024).contains(&fuse_workers)
        || !(1..=65_536).contains(&fuse_max_tasks)
        || fuse_max_readahead_bytes > 16 * 1024 * 1024
    {
        return Err(invalid(
            "FUSE threads must be 1..=64, workers 1..=1024, accepted tasks 1..=65536 and read-ahead <= 16 MiB",
        ));
    }
    Ok(FuseSettings {
        fuse_threads,
        fuse_workers,
        fuse_max_tasks,
        dfs_read_pipeline,
        fuse_max_readahead_bytes,
    })
}
