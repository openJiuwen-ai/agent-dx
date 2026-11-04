//! Bounded synchronous access to immutable S3 chunks. External receipts never
//! manufacture a Node replica acknowledgement or a placement request.

use std::{
    collections::{HashMap, VecDeque},
    fs::{self, DirBuilder, File, OpenOptions},
    future::Future,
    future::poll_fn,
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::fs::FileExt,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
    thread,
    time::Duration,
};

use afs_error::{Error, Result};
use afs_tracing::{Instrument, tracing};
use axum::body::Bytes;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig, aws::AmazonS3Builder,
    path::Path as ObjectPath,
};
use serde::Deserialize;

use crate::{
    config::MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES,
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, CopyLocation, CopyRole,
        CopyState, DigestAlgorithm, ExternalCommitReceipt, OperationId, ReplicaGroupId,
        SourceCandidate, external_object_key,
    },
    node::{
        chunk::{ChunkStore, MAX_STAGED_CHUNK_BYTES, PutIntent, StagedChunk, StoredChunk},
        dfs_read::{ExternalChunkReader, VerifiedExternalChunk},
    },
};

mod cache;
mod metrics;
mod store;
mod verifier;
mod worker;
use cache::*;
pub use metrics::S3ChunkStoreMetrics;
use metrics::*;
pub use store::S3ChunkStore;
use verifier::*;
use worker::*;

const MAX_BATCH: usize = 128;
/// Concurrent object PUTs; the Node's native upload depth is capped to this.
pub(crate) const MAX_UPLOADS: usize = 4;
const MAX_ACTIVE_REQUESTS: usize = 32;
const MAX_CREDENTIAL_BYTES: u64 = 16 * 1024;
const MAX_CACHE_JOBS: usize = 128;
const MAX_CACHE_ENTRIES: usize = 4096;
const MAX_CACHE_EVICTION_DIAGNOSTICS: u64 = 1024;
const S3_HASH_WORKERS: usize = 2;
#[cfg(test)]
const MAX_ACTIVE_HASHES: usize = 1;

#[derive(Clone, Debug)]
pub enum S3CredentialsSource {
    Environment,
    /// JSON containing access_key_id, secret_access_key and optional session_token.
    /// The file must be owned by this process's effective UID, with mode 0600 or stricter.
    File(PathBuf),
}

#[derive(Clone, Debug)]
pub struct S3ChunkStoreOptions {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub store_id: String,
    pub generation: String,
    pub allow_http: bool,
    pub unsigned_payload: bool,
    pub credentials: S3CredentialsSource,
    /// Deadline for a whole batch, including retry and response-body collection.
    pub request_timeout: Duration,
    pub max_retries: usize,
    pub cache: Option<S3ChunkCacheOptions>,
    pub metrics: Option<S3ChunkStoreMetrics>,
}

#[derive(Clone, Debug)]
pub struct S3ChunkCacheOptions {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub pending_bytes: u64,
    /// Matches the JuiceFS default boundary: large write fill is opt-in; reads
    /// always fill after full-object verification.
    pub cache_large_write: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

enum Request {
    Put(Vec<StagedChunk>),
    Read(ChunkObject),
}

enum Reply {
    Put(Vec<ChunkReceipt>),
    Read(VerifiedExternalChunk),
}

struct Command {
    request: Request,
    reply: mpsc::SyncSender<Result<Reply>>,
    span: tracing::Span,
    enqueued_at: std::time::Instant,
}

fn request_summary(request: &Request) -> (&'static str, usize, u64) {
    match request {
        Request::Put(staged) => (
            "put",
            staged.len(),
            staged
                .iter()
                .fold(0_u64, |sum, value| sum.saturating_add(value.chunk.length)),
        ),
        Request::Read(chunk) => ("read", 1, chunk.length),
    }
}

fn validate_options(options: &S3ChunkStoreOptions) -> Result<()> {
    let probe = StagedChunk::new(OperationId::new("identity-probe"), Vec::new());
    external_object_key(&options.store_id, &options.generation, &probe.chunk.id)?;
    if options.bucket.is_empty()
        || options.region.is_empty()
        || options.request_timeout.is_zero()
        || options.request_timeout > Duration::from_secs(300)
        || options.max_retries > 10
        || !(options.endpoint.starts_with("https://") || options.endpoint.starts_with("http://"))
        || options.endpoint.contains(['@', '?', '#'])
    {
        return Err(invalid(
            "invalid S3 endpoint, bucket, region or bounded request settings",
        ));
    }
    if let Some(cache) = &options.cache {
        validate_cache_options(cache)?;
    }
    Ok(())
}

fn validate_cache_options(options: &S3ChunkCacheOptions) -> Result<()> {
    if !options.directory.is_absolute()
        || options.capacity_bytes == 0
        || options.pending_bytes == 0
        || options.pending_bytes > options.capacity_bytes
        || options.capacity_bytes > MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES
    {
        return Err(invalid(
            "invalid S3 cache directory or bounded byte budgets",
        ));
    }
    Ok(())
}

fn load_credentials(source: &S3CredentialsSource) -> Result<Credentials> {
    let credentials = match source {
        S3CredentialsSource::Environment => Credentials {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
                .map_err(|_| invalid("AWS_ACCESS_KEY_ID is required"))?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .map_err(|_| invalid("AWS_SECRET_ACCESS_KEY is required"))?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        },
        S3CredentialsSource::File(path) => {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .map_err(|_| invalid("cannot open S3 credentials file"))?;
            let metadata = file
                .metadata()
                .map_err(|_| invalid("cannot inspect S3 credentials file"))?;
            // SAFETY: geteuid has no arguments or memory preconditions.
            let uid = unsafe { libc::geteuid() };
            if !metadata.is_file() || metadata.mode() & 0o077 != 0 || metadata.uid() != uid {
                return Err(invalid(
                    "S3 credentials require a private file owned by the current UID",
                ));
            }
            let mut bytes = Vec::new();
            file.take(MAX_CREDENTIAL_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid("cannot read S3 credentials file"))?;
            if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
                return Err(invalid("S3 credentials file is too large"));
            }
            serde_json::from_slice::<Credentials>(&bytes)
                .map_err(|_| invalid("invalid S3 credentials file JSON"))?
        }
    };
    if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
        return Err(invalid(
            "S3 credentials must contain nonempty access and secret keys",
        ));
    }
    Ok(credentials)
}

fn chunk_from_id(id: &ChunkId) -> Result<ChunkObject> {
    let remainder =
        id.0.strip_prefix("blake3-")
            .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let (hex, length) = remainder
        .split_once('-')
        .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let length = length
        .parse::<u64>()
        .map_err(|_| invalid("invalid immutable Chunk length"))?;
    if hex.len() != 64
        || length > MAX_STAGED_CHUNK_BYTES as u64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || id.0 != format!("blake3-{hex}-{length}")
    {
        return Err(invalid(
            "noncanonical immutable Chunk ID or oversized Chunk",
        ));
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid immutable Chunk digest"))?;
    }
    Ok(ChunkObject {
        id: id.clone(),
        length,
        encoding: ChunkEncoding::Raw,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes,
        },
    })
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if &chunk_from_id(&chunk.id)? != chunk {
        return Err(invalid(
            "Chunk metadata does not match its immutable identity",
        ));
    }
    Ok(())
}

fn verify_bytes(chunk: &ChunkObject, bytes: &[u8]) -> Result<()> {
    tracing::info_span!(
        "node.dfs.s3.verify",
        chunk_id = %chunk.id.0,
        chunk_bytes = chunk.length,
        hash_bytes = bytes.len() as u64,
    )
    .in_scope(|| {
        if bytes.len() as u64 != chunk.length
            || blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes
        {
            return Err(corrupt("immutable Chunk length or BLAKE3 mismatch"));
        }
        Ok(())
    })
}

fn read_file_segment(
    file: &File,
    chunk: &ChunkObject,
    offset: u64,
    buf: &mut [u8],
    tail: bool,
) -> Result<()> {
    let span = tracing::info_span!(
        "node.dfs.s3.cache.load.segment_read",
        chunk_id = %chunk.id.0,
        segment_offset = offset,
        segment_bytes = buf.len() as u64,
        tail,
    );
    span.in_scope(|| read_exact_at(file, offset, buf))
}

fn read_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> Result<()> {
    while !buf.is_empty() {
        match file.read_at(buf, offset) {
            Ok(0) => return Err(corrupt("S3 cache entry ended before expected length")),
            Ok(read) => {
                offset = offset.saturating_add(read as u64);
                let (_, remaining) = std::mem::take(&mut buf).split_at_mut(read);
                buf = remaining;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err(unavailable("cannot read S3 cache entry")),
        }
    }
    Ok(())
}

fn probe_ready_eof(file: &File, chunk: &ChunkObject) -> Result<()> {
    let mut byte = [0_u8; 1];
    loop {
        match file.read_at(&mut byte, chunk.length) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(corrupt("S3 cache entry exceeds expected length")),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => return Err(unavailable("cannot read S3 cache entry")),
        }
    }
}

fn storage_error(error: object_store::Error) -> Error {
    let code = match error {
        object_store::Error::NotFound { .. } => afs_error::IO_NOT_FOUND,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => afs_error::IO_PERMISSION_DENIED,
        _ => afs_error::IO_UNAVAILABLE,
    };
    // SDK error text can contain request URLs or credential provider details.
    Error::coded(code, "external S3 operation failed")
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn capacity(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_CAPACITY, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

fn drain_spawned_handles(
    handles: &Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
) -> Vec<thread::JoinHandle<()>> {
    match handles.lock() {
        Ok(mut handles) => std::mem::take(&mut *handles),
        Err(_) => Vec::new(),
    }
}

fn join_verifier_handles(handles: Vec<thread::JoinHandle<()>>) -> (usize, Option<&'static str>) {
    let mut joined = 0;
    let mut failure = None;
    for handle in handles {
        joined += 1;
        if handle.join().is_err() && failure.is_none() {
            failure = Some("S3 verifier worker panicked during shutdown");
        }
    }
    (joined, failure)
}

fn panic_error(message: &'static str, payload: Box<dyn std::any::Any + Send + 'static>) -> Error {
    drop(payload);
    unavailable(message)
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    if path.exists() {
        validate_private_dir(path)?;
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("S3 cache directory requires a parent"))?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|_| invalid("cannot inspect S3 cache parent directory"))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(invalid("S3 cache parent must be a real directory"));
    }
    DirBuilder::new()
        .mode(0o700)
        .create(path)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|_| invalid("cannot create S3 cache directory"))?;
    validate_private_dir(path)
}

fn validate_private_dir(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid("cannot inspect S3 cache directory"))?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid(
            "S3 cache directory must be private and owned by this UID",
        ));
    }
    Ok(())
}

fn validate_private_file(file: &File, expected_len: u64) -> Result<()> {
    let metadata = file
        .metadata()
        .map_err(|_| unavailable("cannot inspect S3 cache file"))?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || (expected_len != 0 && metadata.len() != expected_len)
    {
        return Err(corrupt("S3 cache file identity does not match"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
