//! Disposable native Peer object cache, never replicas or read authority.
//!
//! Two streaming fills share Node transport work; a legacy body is exclusive.
//! Metadata locks never cover runtime disk I/O, verification or Peer waits.
//! Ready bodies live only on disk. Hits verify complete requested 32 KiB coverage
//! groups against the trusted standard BLAKE3 root. Changed identity requires a root scan.
//! Callers must discard private output on a miss or error.

use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::{
    dfs::{ChunkEncoding, ChunkObject, DigestAlgorithm, NamespaceId},
    node::chunk::{
        VerifiedRangeCopy,
        outboard::{
            ChunkOutboard, OutboardBudget, OutboardBuilder, OutboardLease, OutboardReadWork,
            SharedOutboard, build_borrowed,
        },
    },
};

mod fill;
mod operation;
mod sink;
mod state;
mod store;
use fill::*;
use operation::*;
pub(super) use sink::PeerStreamBinding;
pub use sink::{CompletedPeerDiskRead, PeerStreamSink, VerifiedPeerChunk};
use state::*;
pub use state::{
    DiskPeerCache, PeerCacheHashWork, PeerCacheOperation, PeerCacheReadStages, PeerCacheStats,
};
pub(crate) use state::{FillLeader, FillOutcome, FillRole, OwnedFillLeader};
use store::*;

pub const MAX_PEER_CACHE_ENTRIES: usize = 4096;
const MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 4096;
const SCRATCH_BYTES: usize = 64 * 1024;
#[cfg(test)]
const PEER_CACHE_SEGMENT_BYTES: usize = 1024 * 1024;
pub const MAX_PEER_CACHE_PROOF_BYTES: u64 = 1024 * 1024;
#[cfg(test)]
const COVERAGE_GROUP_BYTES: usize = 32 * 1024;
const MAX_OUTBOARD_BYTES: usize = 8192;
const MARKER: &str = ".afs-peer-cache-v1";
const PENDING: &str = ".afs-peer-cache.pending";
const MAGIC: &[u8; 8] = b"AFSPC002";
const OLD_MAGIC: &[u8; 8] = b"AFSPC001";

#[derive(Clone, Debug)]
pub struct PeerCacheOptions {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub namespace_id: NamespaceId,
}

fn read_header(file: &mut File) -> Result<(Header, u64)> {
    let mut prefix = [0_u8; 12];
    file.read_exact(&mut prefix)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    if &prefix[..8] != MAGIC && &prefix[..8] != OLD_MAGIC {
        return Err(corrupt("Peer cache header magic differs"));
    }
    let count = u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
    if count == 0 || count > MAX_HEADER_BYTES {
        return Err(corrupt("Peer cache header exceeds bound"));
    }
    let mut header = vec![0; count];
    file.read_exact(&mut header)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    let header: Header =
        serde_json::from_slice(&header).map_err(|_| corrupt("Peer cache header is invalid"))?;
    if &prefix[..8] == MAGIC {
        if header.version != 2 || count > MAX_HEADER_BYTES - 12 {
            return Err(corrupt("Peer cache v2 header shape differs"));
        }
        let mut padding = [0_u8; MAX_HEADER_BYTES];
        let padding = &mut padding[..MAX_HEADER_BYTES - 12 - count];
        file.read_exact(padding)
            .map_err(|_| corrupt("Peer cache header padding is truncated"))?;
        if padding.iter().any(|byte| *byte != 0) {
            return Err(corrupt("Peer cache header padding differs"));
        }
        Ok((header, MAX_HEADER_BYTES as u64))
    } else if header.version == 1 {
        Ok((header, 12 + count as u64))
    } else {
        Err(corrupt("Peer cache legacy header shape differs"))
    }
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if chunk.id.0.is_empty()
        || chunk.id.0.len() > 512
        || chunk.length == 0
        || chunk.encoding != ChunkEncoding::Raw
        || chunk.content_digest.algorithm != DigestAlgorithm::Blake3
    {
        return Err(invalid(
            "Peer cache requires a bounded immutable BLAKE3 object",
        ));
    }
    Ok(())
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err(invalid("Peer cache directory is not normalized"));
        }
        prefix.push(component.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid("Peer cache path contains a symlink"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
    }
    Ok(())
}

fn open_private(path: &Path) -> Result<(File, fs::Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    validate_private_file(&metadata)?;
    Ok((file, metadata))
}

fn validate_private_file(metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != effective_uid()
        || metadata.nlink() != 1
    {
        return Err(invalid(
            "Peer cache file must be private, regular and not linked elsewhere",
        ));
    }
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments and does not access borrowed memory.
    unsafe { libc::geteuid() }
}

fn fd_directory(root: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", root.as_raw_fd()))
}

fn allocated(metadata: &fs::Metadata) -> Result<u64> {
    metadata
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| invalid("Peer cache allocated size overflow"))
}

fn file_identity(metadata: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn valid_pending_name(name: &str) -> bool {
    name.strip_prefix(&format!("{PENDING}-"))
        .is_some_and(|suffix| {
            suffix.len() == 16
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn valid_ready_name(name: &str) -> bool {
    name.strip_suffix(".ready").is_some_and(|key| {
        key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod format_golden_tests;
