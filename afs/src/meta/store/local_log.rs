//! Local canonical operation log. The legacy JSON backend is not read here.
//! Two checked tail slots share the WAL's single group durability barrier.
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use afs_error::{Error, Result};

use super::{
    BackendPersistence, MetaFuture, StoreRevision,
    journal::{
        CheckpointLimits, LOG_HEADER_BYTES, LogLimits, LogTip, MetaLog, RecoveredCheckpoint,
        RecoveredLog, StoreIdentity, decode_frame, encode_batch, frame_length, full, invalid_log,
        unavailable,
    },
    log::Record,
};

const WAL: &str = "wal-00000000000000000001.log";
const IDENTITY: &str = "IDENTITY";
const STATIC_BYTES: usize = 256;
const SLOT_BYTES: usize = 64;
const EVIDENCE_LIMIT: usize = 2;

#[derive(Clone)]
pub struct LocalLog {
    inner: Arc<Inner>,
}
struct Inner {
    owner_pid: u32,
    dir: PathBuf,
    identity: StoreIdentity,
    limits: LogLimits,
    checkpoint_limits: CheckpointLimits,
    maintenance: super::journal::MaintenancePolicy,
    checkpoint_install: Mutex<()>,
    _lock: File,
    state: Mutex<LocalState>,
}
impl Inner {
    fn ensure_owner(&self) -> Result<()> {
        if self.owner_pid != std::process::id() {
            return Err(unavailable(
                "inherited local log handle is not a writer authority",
            ));
        }
        Ok(())
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        // Only the final owner in the creating process may release this lock.
        // A fork/dup can keep its open file description alive beyond close;
        // a child must neither use the inherited writer nor unlock its parent.
        if self.owner_pid == std::process::id()
            && let Err(error) = self._lock.unlock()
        {
            afs_tracing::tracing::warn!(error = %error, "Meta log owner unlock failed");
        }
    }
}
struct LocalState {
    file: File,
    tip: LogTip,
    end: u64,
    generation: u64,
    poisoned: bool,
    recovered: Option<RecoveredLog>,
    manifest: Current,
    revision: StoreRevision,
    wal_bytes: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct Tail {
    generation: u64,
    tip: LogTip,
    end: u64,
}

fn open_file(path: &Path, create_new: bool) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(create_new)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
fn create_dir(path: &Path) -> Result<()> {
    if !path.exists() {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        create_dir(parent)?;
        fs::create_dir(path)?;
        sync_dir(parent)?;
    }
    Ok(())
}
fn identity_bytes(identity: StoreIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(90);
    bytes.extend_from_slice(b"AFSI");
    bytes.extend_from_slice(&1u16.to_be_bytes());
    bytes.extend_from_slice(&identity.store_id);
    bytes.extend_from_slice(&identity.cluster_id);
    bytes.extend_from_slice(&identity.incarnation);
    let hash = *blake3::hash(&bytes).as_bytes();
    bytes.extend_from_slice(&hash);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}
fn read_identity(dir: &Path) -> Result<StoreIdentity> {
    read_identity_file(&dir.join(IDENTITY))
}
fn read_identity_file(path: &Path) -> Result<StoreIdentity> {
    let mut file = open_file(path, false)?;
    if file.metadata()?.len() != 90 {
        return Err(invalid_log("IDENTITY length"));
    }
    let mut bytes = [0; 90];
    file.read_exact(&mut bytes)?;
    if &bytes[..6] != b"AFSI\0\x01"
        || bytes[86..] != crc32c::crc32c(&bytes[..86]).to_be_bytes()
        || bytes[54..86] != *blake3::hash(&bytes[..54]).as_bytes()
    {
        return Err(invalid_log("IDENTITY format/hash/CRC"));
    }
    let mut identity = StoreIdentity {
        store_id: [0; 16],
        cluster_id: [0; 16],
        incarnation: [0; 16],
    };
    identity.store_id.copy_from_slice(&bytes[6..22]);
    identity.cluster_id.copy_from_slice(&bytes[22..38]);
    identity.incarnation.copy_from_slice(&bytes[38..54]);
    Ok(identity)
}
fn static_header(identity: StoreIdentity, base: LogTip) -> [u8; STATIC_BYTES] {
    let mut bytes = [0; STATIC_BYTES];
    bytes[..4].copy_from_slice(b"AFSW");
    bytes[4..6].copy_from_slice(&1u16.to_be_bytes());
    bytes[6..22].copy_from_slice(&identity.store_id);
    bytes[22..38].copy_from_slice(&identity.cluster_id);
    bytes[38..54].copy_from_slice(&identity.incarnation);
    bytes[54..62].copy_from_slice(&base.sequence.to_be_bytes());
    bytes[62..94].copy_from_slice(&base.record_hash);
    let hash = *blake3::hash(&bytes[..94]).as_bytes();
    bytes[94..126].copy_from_slice(&hash);
    let crc = crc32c::crc32c(&bytes[..126]);
    bytes[126..130].copy_from_slice(&crc.to_be_bytes());
    bytes
}
fn tail_bytes(tail: Tail) -> [u8; SLOT_BYTES] {
    let mut bytes = [0; SLOT_BYTES];
    bytes[..8].copy_from_slice(&tail.generation.to_be_bytes());
    bytes[8..16].copy_from_slice(&tail.tip.sequence.to_be_bytes());
    bytes[16..24].copy_from_slice(&tail.end.to_be_bytes());
    bytes[24..56].copy_from_slice(&tail.tip.record_hash);
    let crc = crc32c::crc32c(&bytes[..60]);
    bytes[60..].copy_from_slice(&crc.to_be_bytes());
    bytes
}
fn parse_tail(bytes: &[u8]) -> Result<Option<Tail>> {
    if bytes.iter().all(|b| *b == 0) {
        return Ok(None);
    }
    if bytes.len() != SLOT_BYTES
        || bytes[56..60] != [0; 4]
        || bytes[60..] != crc32c::crc32c(&bytes[..60]).to_be_bytes()
    {
        return Err(invalid_log(
            "WAL declared tail CRC/format; ambiguous tail is not discarded",
        ));
    }
    let number = |range: std::ops::Range<usize>| -> Result<u64> {
        Ok(u64::from_be_bytes(
            bytes[range]
                .try_into()
                .map_err(|_| invalid_log("tail integer width"))?,
        ))
    };
    let mut hash = [0; 32];
    hash.copy_from_slice(&bytes[24..56]);
    let tail = Tail {
        generation: number(0..8)?,
        tip: LogTip {
            sequence: number(8..16)?,
            record_hash: hash,
        },
        end: number(16..24)?,
    };
    if tail.end < LOG_HEADER_BYTES
        || (tail.tip.sequence == 0
            && (tail.end != LOG_HEADER_BYTES || tail.tip.record_hash != [0; 32]))
    {
        return Err(invalid_log("WAL declared tail position"));
    }
    Ok(Some(tail))
}
fn write_tail(file: &mut File, tail: Tail) -> Result<()> {
    file.seek(SeekFrom::Start(
        STATIC_BYTES as u64 + (tail.generation % 2) * SLOT_BYTES as u64,
    ))?;
    file.write_all(&tail_bytes(tail))?;
    Ok(())
}
fn header(file: &mut File, identity: StoreIdentity, base: LogTip) -> Result<Tail> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; LOG_HEADER_BYTES as usize];
    file.read_exact(&mut bytes)?;
    if bytes[..STATIC_BYTES] != static_header(identity, base)
        || bytes[STATIC_BYTES + 2 * SLOT_BYTES..]
            .iter()
            .any(|b| *b != 0)
    {
        return Err(invalid_log(format!(
            "{} offset 0 at base sequence {} header format/identity/base/hash/CRC; expected identity {identity:?}, stored identity bytes {:?}",
            wal_name(base)?,
            base.sequence,
            &bytes[6..54]
        )));
    }
    let a = parse_tail(&bytes[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES])?;
    let b = parse_tail(&bytes[STATIC_BYTES + SLOT_BYTES..STATIC_BYTES + 2 * SLOT_BYTES])?;
    let tail = match (a, b) {
        (Some(a), Some(b)) if a.generation == b.generation && a != b => {
            Err(invalid_log("WAL conflicting equal tail generations"))
        }
        (Some(a), Some(b)) => Ok(if a.generation >= b.generation { a } else { b }),
        (Some(tail), None) | (None, Some(tail)) => Ok(tail),
        (None, None) => Err(invalid_log("WAL missing declared tail")),
    }?;
    if tail.tip.sequence < base.sequence
        || (tail.tip.sequence == base.sequence
            && (tail.tip != base || tail.end != LOG_HEADER_BYTES))
    {
        return Err(invalid_log(
            "WAL declared tail precedes/mismatches segment base",
        ));
    }
    Ok(tail)
}
fn preserve_tail(dir: &Path, file: &mut File, start: u64, end: u64) -> Result<()> {
    let length = end
        .checked_sub(start)
        .ok_or_else(|| invalid_log("tail evidence position"))?;
    if length > 128 * 1024 * 1024 {
        return Err(full(
            "incomplete tail exceeds evidence budget; original WAL retained",
        ));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(length).map_err(|_| full("tail evidence length"))?)
        .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "tail evidence allocation"))?;
    file.seek(SeekFrom::Start(start))?;
    file.take(length).read_to_end(&mut bytes)?;
    let name = format!("incomplete-tail-{}", blake3::hash(&bytes).to_hex());
    let path = dir.join(name);
    if path.exists() {
        if fs::read(&path)? != bytes {
            return Err(invalid_log("tail evidence identity mismatch"));
        }
        return Ok(());
    }
    let mut count = 0;
    for entry in fs::read_dir(dir)? {
        if entry?
            .file_name()
            .to_string_lossy()
            .starts_with("incomplete-tail-")
        {
            count += 1;
        }
    }
    if count >= EVIDENCE_LIMIT {
        return Err(full("tail evidence slots exhausted; original WAL retained"));
    }
    let mut evidence = open_file(&path, true)?;
    evidence.write_all(&bytes)?;
    evidence.sync_all()?;
    sync_dir(dir)
}

const IMAGE_HEADER: usize = 146;
const CURRENT: &str = "CURRENT";
const MANIFEST_MAX: usize = 64 * 1024;
#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
struct CheckpointRef {
    covered: LogTip,
    revision: StoreRevision,
    bytes: u64,
    digest: [u8; 32],
}
/// A sealed segment is immutable. The last (active) segment has no seal;
/// its checked durable tail remains authoritative for appends after CURRENT.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct SegmentRef {
    base: LogTip,
    base_revision: StoreRevision,
    sealed: Option<(Tail, StoreRevision)>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct Current {
    identity: StoreIdentity,
    current: Option<CheckpointRef>,
    previous: Option<CheckpointRef>,
    #[serde(deserialize_with = "segment_refs")]
    segments: Vec<SegmentRef>,
    #[serde(deserialize_with = "segment_refs")]
    retired_segments: Vec<SegmentRef>,
    #[serde(deserialize_with = "retired_images")]
    retired_images: Vec<CheckpointRef>,
}
fn finite_items<'de, D, T, const N: usize>(d: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    struct Items<T, const N: usize>(std::marker::PhantomData<T>);
    impl<'de, T: serde::Deserialize<'de>, const N: usize> serde::de::Visitor<'de> for Items<T, N> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "at most {N} manifest entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Vec<T>, A::Error> {
            let mut result = Vec::new();
            while let Some(item) = seq.next_element()? {
                if result.len() == N {
                    return Err(serde::de::Error::custom("manifest entry count exceeded"));
                }
                result.try_reserve(1).map_err(serde::de::Error::custom)?;
                result.push(item);
            }
            Ok(result)
        }
    }
    d.deserialize_seq(Items::<T, N>(std::marker::PhantomData))
}
fn segment_refs<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<SegmentRef>, D::Error> {
    finite_items::<D, SegmentRef, 256>(d)
}
fn retired_images<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<CheckpointRef>, D::Error> {
    finite_items::<D, CheckpointRef, 8>(d)
}
impl Current {
    fn initial(identity: StoreIdentity) -> Self {
        Self {
            identity,
            current: None,
            previous: None,
            segments: vec![SegmentRef {
                base: LogTip::EMPTY,
                base_revision: StoreRevision::ZERO,
                sealed: None,
            }],
            retired_segments: Vec::new(),
            retired_images: Vec::new(),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.segments.is_empty()
            || self.segments.len() > 256
            || self.retired_segments.len() > 256
            || self.retired_images.len() > 8
            || self.previous.is_some_and(|old| {
                self.current
                    .is_none_or(|new| old.covered.sequence >= new.covered.sequence)
            })
        {
            return Err(invalid_log("CURRENT count/checkpoint ordering"));
        }
        let first = self.segments[0];
        let oldest = self.previous.or(self.current);
        if oldest.is_none()
            && (first.base != LogTip::EMPTY || first.base_revision != StoreRevision::ZERO)
            || oldest.is_some_and(|image| image.covered.sequence < first.base.sequence)
        {
            return Err(invalid_log(
                "CURRENT omits a retained recovery point's required prefix",
            ));
        }
        for (index, segment) in self.segments.iter().enumerate() {
            wal_name(segment.base)?;
            if index + 1 == self.segments.len() {
                if segment.sealed.is_some() {
                    return Err(invalid_log("CURRENT active segment must not be sealed"));
                }
            } else {
                let (tail, revision) = segment
                    .sealed
                    .ok_or_else(|| invalid_log("CURRENT unsealed middle segment"))?;
                let next = self.segments[index + 1];
                if tail.tip.sequence <= segment.base.sequence
                    || tail.end <= LOG_HEADER_BYTES
                    || next.base != tail.tip
                    || next.base_revision != revision
                {
                    return Err(invalid_log("CURRENT segment chain discontinuity"));
                }
            }
        }
        let mut retired_bases = std::collections::BTreeSet::new();
        for segment in &self.retired_segments {
            let (tail, _) = segment
                .sealed
                .ok_or_else(|| invalid_log("CURRENT cannot retire an active segment"))?;
            if tail.tip.sequence > first.base.sequence
                || tail.tip.sequence <= segment.base.sequence
                || !retired_bases.insert(segment.base.sequence)
            {
                return Err(invalid_log(
                    "CURRENT retired segment still required or duplicated",
                ));
            }
        }
        for image in &self.retired_images {
            if self.current == Some(*image) || self.previous == Some(*image) {
                return Err(invalid_log("CURRENT retires a selected image"));
            }
        }
        Ok(())
    }
}
fn wal_name(base: LogTip) -> Result<String> {
    Ok(format!(
        "wal-{:020}.log",
        base.sequence
            .checked_add(1)
            .ok_or_else(|| invalid_log("segment sequence exhausted"))?
    ))
}
fn wal_budget(dir: &Path, limits: LogLimits, extra_bytes: u64, extra_files: usize) -> Result<u64> {
    let mut bytes = extra_bytes;
    let mut files = extra_files;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-") && name.ends_with(".log") {
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                return Err(invalid_log("WAL entry is not a regular file"));
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| full("WAL byte budget overflow"))?;
            files = files
                .checked_add(1)
                .ok_or_else(|| full("WAL segment count overflow"))?;
        }
    }
    if bytes > limits.wal_max_bytes || files > limits.segments_max {
        return Err(full(
            "WAL byte budget or segment count exhausted; checkpoint required",
        ));
    }
    Ok(bytes)
}
fn image_name(image: CheckpointRef) -> String {
    let hex: String = image
        .digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("checkpoint-{:020}-{hex}.bin", image.covered.sequence)
}
fn image_header(
    identity: StoreIdentity,
    covered: LogTip,
    revision: StoreRevision,
    bytes: &[u8],
) -> ([u8; IMAGE_HEADER], CheckpointRef) {
    let mut header = [0u8; IMAGE_HEADER];
    header[..6].copy_from_slice(b"AFSC\0\x01");
    header[6..22].copy_from_slice(&identity.store_id);
    header[22..38].copy_from_slice(&identity.cluster_id);
    header[38..54].copy_from_slice(&identity.incarnation);
    header[54..62].copy_from_slice(&covered.sequence.to_be_bytes());
    header[62..94].copy_from_slice(&covered.record_hash);
    header[94..102].copy_from_slice(&revision.0.to_be_bytes());
    header[102..110].copy_from_slice(&(bytes.len() as u64).to_be_bytes());
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.checkpoint.v1");
    hash.update(&header[..110]);
    hash.update(bytes);
    let digest = *hash.finalize().as_bytes();
    header[110..142].copy_from_slice(&digest);
    let crc = crc32c::crc32c_append(crc32c::crc32c(&header[..142]), bytes);
    header[142..].copy_from_slice(&crc.to_be_bytes());
    (
        header,
        CheckpointRef {
            covered,
            revision,
            bytes: bytes.len() as u64,
            digest,
        },
    )
}
fn read_image(
    dir: &Path,
    identity: StoreIdentity,
    reference: CheckpointRef,
    limits: CheckpointLimits,
) -> Result<RecoveredCheckpoint> {
    let name = image_name(reference);
    let read = || -> Result<RecoveredCheckpoint> {
        let mut file = open_file(&dir.join(&name), false)?;
        let length = usize::try_from(reference.bytes)
            .map_err(|_| invalid_log("checkpoint length overflow"))?;
        if length > limits.image_max_bytes
            || !file.metadata()?.is_file()
            || file.metadata()?.len()
                != reference
                    .bytes
                    .checked_add(IMAGE_HEADER as u64)
                    .ok_or_else(|| invalid_log("checkpoint length overflow"))?
        {
            return Err(invalid_log("checkpoint length outside declared bound"));
        }
        let mut actual = [0u8; IMAGE_HEADER];
        file.read_exact(&mut actual)?;
        if actual[..6] != *b"AFSC\0\x01"
            || actual[6..22] != identity.store_id
            || actual[22..38] != identity.cluster_id
            || actual[38..54] != identity.incarnation
        {
            return Err(invalid_log(
                "checkpoint format/store/cluster/incarnation mismatch",
            ));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            Error::coded(afs_error::IO_OUT_OF_MEMORY, "checkpoint payload allocation")
        })?;
        bytes.resize(length, 0);
        file.read_exact(&mut bytes)?;
        let (expected, found) =
            image_header(identity, reference.covered, reference.revision, &bytes);
        if actual != expected || found != reference {
            return Err(invalid_log(
                "checkpoint position/revision/length/hash/CRC mismatch",
            ));
        }
        if super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?
            != reference.revision
        {
            return Err(invalid_log("checkpoint header/business revision mismatch"));
        }
        Ok(RecoveredCheckpoint {
            covered: reference.covered,
            bytes,
        })
    };
    read().map_err(|error| {
        invalid_log(format!(
            "{name} sequence {}: {error}",
            reference.covered.sequence
        ))
    })
}
fn encode_current(current: &Current) -> Result<Vec<u8>> {
    let payload = super::codec::encode_bounded(current, MANIFEST_MAX - 42)?;
    let mut bytes = Vec::with_capacity(payload.len() + 42);
    bytes.extend_from_slice(b"AFSM\0\x02");
    bytes.extend_from_slice(&payload);
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.current.v1");
    hash.update(&bytes);
    bytes.extend_from_slice(hash.finalize().as_bytes());
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    Ok(bytes)
}
fn read_current(dir: &Path, identity: StoreIdentity) -> Result<Option<Current>> {
    let path = dir.join(CURRENT);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid_log("CURRENT is not a regular file"));
        }
        Ok(_) => {}
    }
    let mut file = open_file(&path, false)?;
    let length =
        usize::try_from(file.metadata()?.len()).map_err(|_| invalid_log("CURRENT length"))?;
    if !(42..=MANIFEST_MAX).contains(&length) || !file.metadata()?.is_file() {
        return Err(invalid_log("CURRENT length/type"));
    }
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes)?;
    let hash_start = length - 36;
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.current.v1");
    hash.update(&bytes[..hash_start]);
    if bytes[..6] != *b"AFSM\0\x02"
        || bytes[hash_start..length - 4] != *hash.finalize().as_bytes()
        || bytes[length - 4..] != crc32c::crc32c(&bytes[..length - 4]).to_be_bytes()
    {
        return Err(invalid_log("CURRENT format/hash/CRC"));
    }
    let (current, tail): (Current, _) = postcard::take_from_bytes(&bytes[6..hash_start])
        .map_err(|error| invalid_log(format!("CURRENT decode: {error}")))?;
    if !tail.is_empty() || current.identity != identity {
        return Err(invalid_log("CURRENT identity/trailing bytes"));
    }
    current.validate()?;
    Ok(Some(current))
}
fn checkpoint_owned(name: &str) -> bool {
    name.starts_with("checkpoint-")
        || name == "checkpoint.tmp"
        || name.starts_with("orphan-checkpoint")
        || name.starts_with("orphan-CURRENT")
        || name == CURRENT
        || name == "CURRENT.tmp"
}
fn checkpoint_budget(
    dir: &Path,
    extra_bytes: u64,
    extra_files: usize,
    limits: CheckpointLimits,
) -> Result<()> {
    let mut bytes = extra_bytes;
    let mut files = extra_files;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if checkpoint_owned(&entry.file_name().to_string_lossy()) {
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                return Err(invalid_log("checkpoint entry is not regular file"));
            }
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| full("checkpoint disk charge overflow"))?;
            files = files
                .checked_add(1)
                .ok_or_else(|| full("checkpoint file count overflow"))?;
        }
    }
    if bytes > limits.disk_max_bytes || files > limits.files_max {
        return Err(full(
            "checkpoint disk/file reservation exceeded; old recovery chain retained",
        ));
    }
    Ok(())
}
fn reserve_file(file: &File, length: u64) -> Result<()> {
    let length = i64::try_from(length).map_err(|_| full("checkpoint file reservation overflow"))?;
    // SAFETY: the live regular-file FD remains owned by the caller; offsets
    // are checked and nonnegative. The kernel retains no Rust pointers.
    let code = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    if code != 0 {
        return Err(std::io::Error::from_raw_os_error(code).into());
    }
    Ok(())
}
fn quarantine_checkpoint_tmp(dir: &Path, name: &str, limits: CheckpointLimits) -> Result<()> {
    let path = dir.join(name);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid_log("checkpoint temporary is not a regular file"));
        }
        Ok(_) => {}
    }
    let mut file = open_file(&path, false)?;
    let max_bytes = if name == "CURRENT.tmp" {
        MANIFEST_MAX as u64
    } else {
        limits.image_max_bytes as u64 + IMAGE_HEADER as u64
    };
    if file.metadata()?.len() > max_bytes || !file.metadata()?.is_file() {
        return Err(invalid_log(
            "checkpoint temporary file outside evidence bound",
        ));
    }
    let mut hash = blake3::Hasher::new();
    let mut block = [0u8; 8192];
    loop {
        let read = file.read(&mut block)?;
        if read == 0 {
            break;
        }
        hash.update(&block[..read]);
    }
    let orphan = dir.join(format!("orphan-{name}-{}", hash.finalize().to_hex()));
    let mut count = 0usize;
    for entry in fs::read_dir(dir)? {
        if entry?.file_name().to_string_lossy().starts_with("orphan-") {
            count = count.saturating_add(1);
        }
    }
    if orphan.try_exists()? || count >= EVIDENCE_LIMIT {
        return Err(full(
            "checkpoint temporary evidence slots exhausted; originals retained",
        ));
    }
    fs::rename(path, orphan)?;
    sync_dir(dir)
}
fn verify_anchor(
    dir: &Path,
    identity: StoreIdentity,
    limits: LogLimits,
    segments: &[SegmentRef],
    covered: LogTip,
    revision: StoreRevision,
) -> Result<()> {
    let first = segments
        .first()
        .ok_or_else(|| invalid_log("missing anchor segments"))?;
    let mut tip = first.base;
    let mut business = first.base_revision;
    if covered.sequence < tip.sequence {
        return Err(invalid_log("checkpoint anchor before retained chain"));
    }
    for segment in segments {
        if tip.sequence >= covered.sequence {
            break;
        }
        if segment.base != tip || segment.base_revision != business {
            return Err(invalid_log("checkpoint anchor segment discontinuity"));
        }
        let mut file = open_file(&dir.join(wal_name(segment.base)?), false)?;
        let actual = header(&mut file, identity, segment.base)?;
        let end = segment.sealed.map_or(actual.end, |(tail, _)| tail.end);
        if file.metadata()?.len() < end {
            return Err(invalid_log("checkpoint anchor segment truncated"));
        }
        let mut offset = LOG_HEADER_BYTES;
        while tip.sequence < covered.sequence && offset < end {
            if end - offset < 8 {
                return Err(invalid_log("checkpoint anchor beyond WAL prefix"));
            }
            file.seek(SeekFrom::Start(offset))?;
            let mut prefix = [0u8; 8];
            file.read_exact(&mut prefix)?;
            let length = frame_length(&prefix, limits.record_max_bytes)?;
            if length as u64 > end - offset {
                return Err(invalid_log("checkpoint anchor inside incomplete frame"));
            }
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(length).map_err(|_| {
                Error::coded(
                    afs_error::IO_OUT_OF_MEMORY,
                    "checkpoint anchor frame allocation",
                )
            })?;
            bytes.resize(length, 0);
            bytes[..8].copy_from_slice(&prefix);
            file.read_exact(&mut bytes[8..])?;
            let (record, next) = decode_frame(identity, tip, &bytes, limits.record_max_bytes)?;
            if record.revision_before() != business {
                return Err(invalid_log("checkpoint anchor business revision gap"));
            }
            tip = next;
            business = record.revision_after();
            offset += length as u64;
        }
    }
    if tip != covered || business != revision {
        return Err(invalid_log(
            "checkpoint covered position/hash/revision does not match WAL",
        ));
    }
    Ok(())
}
struct Replay {
    recovered: RecoveredLog,
    file: File,
    end: u64,
    generation: u64,
    revision: StoreRevision,
    manifest: Current,
}
/// Reject hidden suffixes before replay can truncate or advance any active WAL.
/// Rotation may leave one exact, empty, identity-checked next segment. Its base
/// must equal the recovered active tip, checked before any replay mutation.
fn unselected_prepared_segment(dir: &Path, manifest: &Current) -> Result<Option<LogTip>> {
    let selected = manifest
        .segments
        .iter()
        .chain(&manifest.retired_segments)
        .map(|segment| wal_name(segment.base))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let mut prepared = None;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("wal-") || !name.ends_with(".log") || selected.contains(name.as_ref())
        {
            continue;
        }
        if prepared.is_some() || !fs::symlink_metadata(entry.path())?.is_file() {
            return Err(invalid_log(
                "unlisted WAL entries do not form one empty prepared segment; originals retained",
            ));
        }
        let mut file = open_file(&entry.path(), false)?;
        if file.metadata()?.len() != LOG_HEADER_BYTES {
            return Err(invalid_log(format!(
                "{name}: unlisted nonempty/incomplete WAL; CURRENT cannot omit a durable suffix"
            )));
        }
        let mut bytes = [0u8; 94];
        file.read_exact(&mut bytes)?;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[62..94]);
        let base = LogTip {
            sequence: u64::from_be_bytes(
                bytes[54..62]
                    .try_into()
                    .map_err(|_| invalid_log("unlisted WAL base width"))?,
            ),
            record_hash: hash,
        };
        if wal_name(base)? != name.as_ref()
            || header(&mut file, manifest.identity, base)?
                != (Tail {
                    generation: 0,
                    tip: base,
                    end: LOG_HEADER_BYTES,
                })
        {
            return Err(invalid_log(format!(
                "{name}: unlisted WAL is not an exact empty prepared segment"
            )));
        }
        prepared = Some(base);
    }
    Ok(prepared)
}

fn replay(
    dir: &Path,
    identity: StoreIdentity,
    limits: LogLimits,
    checkpoint_limits: CheckpointLimits,
) -> Result<Replay> {
    wal_budget(dir, limits, 0, 0)?;
    let manifest = read_current(dir, identity)?.ok_or_else(|| {
        invalid_log("CURRENT missing from initialized Meta log; originals retained")
    })?;
    manifest.validate()?;
    let prepared_segment = unselected_prepared_segment(dir, &manifest)?;
    let references: Vec<_> = manifest
        .current
        .into_iter()
        .chain(manifest.previous)
        .collect();
    let mut checkpoint = None;
    let mut errors = Vec::new();
    for reference in &references {
        match read_image(dir, identity, *reference, checkpoint_limits) {
            Ok(image) => {
                checkpoint = Some(image);
                break;
            }
            Err(error) => errors.push(error.to_string()),
        }
    }
    if !references.is_empty() && checkpoint.is_none() {
        return Err(invalid_log(format!(
            "CURRENT has no valid checkpoint: {}",
            errors.join("; ")
        )));
    }
    let mut tip = manifest.segments[0].base;
    let mut revision = manifest.segments[0].base_revision;
    let mut anchors: Vec<_> = references
        .iter()
        .map(|reference| reference.covered == tip && reference.revision == revision)
        .collect();
    let covered = checkpoint
        .as_ref()
        .map_or(0, |image| image.covered.sequence);
    let mut records = Vec::new();
    let mut active = None;
    for segment in &manifest.segments {
        if segment.base != tip || segment.base_revision != revision {
            return Err(invalid_log("WAL segment base/revision gap"));
        }
        let name = wal_name(segment.base)?;
        let mut file = open_file(&dir.join(&name), false).map_err(|error| {
            invalid_log(format!(
                "{name} offset 0 at base sequence {} required segment: {error}",
                segment.base.sequence
            ))
        })?;
        let declared = header(&mut file, identity, segment.base)?;
        let length = file.metadata()?.len();
        if declared.end > length
            || segment
                .sealed
                .is_some_and(|(tail, _)| declared != tail || length != tail.end)
        {
            return Err(invalid_log(format!(
                "{name} sealed/declared durable content missing or changed"
            )));
        }
        let mut offset = LOG_HEADER_BYTES;
        let mut matched = declared.end == offset && declared.tip == tip;
        while offset < length {
            if length - offset < 8 {
                break;
            }
            file.seek(SeekFrom::Start(offset))?;
            let mut prefix = [0; 8];
            file.read_exact(&mut prefix)?;
            let size = frame_length(&prefix, limits.record_max_bytes)
                .map_err(|error| invalid_log(format!("{name} offset {offset}: {error}")))?;
            if size as u64 > length - offset {
                break;
            }
            let mut frame = Vec::new();
            frame.try_reserve_exact(size).map_err(|_| {
                Error::coded(afs_error::IO_OUT_OF_MEMORY, "replay frame allocation")
            })?;
            frame.resize(size, 0);
            frame[..8].copy_from_slice(&prefix);
            file.read_exact(&mut frame[8..])?;
            let (record, next) = decode_frame(identity, tip, &frame, limits.record_max_bytes)
                .map_err(|error| {
                    invalid_log(format!(
                        "{name} offset {offset} after sequence {}: {error}",
                        tip.sequence
                    ))
                })?;
            if record.revision_before() != revision {
                return Err(invalid_log(format!(
                    "{name} offset {offset} after sequence {}: business revision gap; expected {revision}, found before {} after {}",
                    tip.sequence,
                    record.revision_before(),
                    record.revision_after()
                )));
            }
            tip = next;
            revision = record.revision_after();
            offset += size as u64;
            for (reference, matched) in references.iter().zip(&mut anchors) {
                if reference.covered.sequence == tip.sequence {
                    *matched = reference.covered == tip && reference.revision == revision;
                }
            }
            if tip.sequence > covered {
                records.push(record);
            }
            if offset == declared.end {
                if tip != declared.tip {
                    return Err(invalid_log(format!(
                        "{name} declared tail hash/sequence mismatch"
                    )));
                }
                matched = true;
            }
            if offset > declared.end && !matched {
                return Err(invalid_log(format!("{name} declared tail inside record")));
            }
        }
        if !matched || offset < declared.end {
            return Err(invalid_log(format!(
                "{name} declared durable content incomplete"
            )));
        }
        if let Some((sealed, business)) = segment.sealed {
            if tip != sealed.tip || offset != sealed.end || revision != business {
                return Err(invalid_log(format!("{name} seal/revision mismatch")));
            }
        } else {
            if anchors.iter().any(|matched| !matched) {
                return Err(invalid_log(
                    "CURRENT checkpoint chain anchor missing or mismatched; no fallback can discard acknowledged history",
                ));
            }
            if prepared_segment.is_some_and(|base| base != tip) {
                return Err(invalid_log(
                    "unlisted empty WAL is not the recovered next segment",
                ));
            }
            let changed = offset != length || tip != declared.tip;
            if offset != length {
                preserve_tail(dir, &mut file, offset, length)?;
                file.set_len(offset)?;
            }
            let generation = if changed {
                let generation = declared
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_log("tail generation exhausted"))?;
                write_tail(
                    &mut file,
                    Tail {
                        generation,
                        tip,
                        end: offset,
                    },
                )?;
                generation
            } else {
                declared.generation
            };
            // Confirm complete but previously unacknowledged same-kernel pages.
            file.sync_all()?;
            active = Some((file, offset, generation));
        }
    }
    let (file, end, generation) =
        active.ok_or_else(|| invalid_log("missing active WAL segment"))?;
    // Before any later GC, confirm the root selector itself, not just data pages.
    if dir.join(CURRENT).try_exists()? {
        open_file(&dir.join(CURRENT), false)?.sync_all()?;
    }
    sync_dir(dir)?;
    Ok(Replay {
        recovered: RecoveredLog {
            checkpoint,
            tip,
            records,
        },
        file,
        end,
        generation,
        revision,
        manifest,
    })
}
fn write_current(dir: &Path, manifest: &Current, limits: CheckpointLimits) -> Result<()> {
    manifest.validate()?;
    let bytes = encode_current(manifest)?;
    quarantine_checkpoint_tmp(dir, "CURRENT.tmp", limits)?;
    checkpoint_budget(dir, bytes.len() as u64, 1, limits)?;
    let tmp = dir.join("CURRENT.tmp");
    let mut file = open_file(&tmp, true)?;
    reserve_file(&file, bytes.len() as u64)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(tmp, dir.join(CURRENT))?;
    sync_dir(dir)
}
/// Only objects explicitly retired by a durable CURRENT are cleanup candidates.
/// Corrupt/foreign data is retained and continues consuming its finite budget.
/// A crash after image rename but before CURRENT selection leaves a complete
/// owned image outside both selected chains. Validate its complete envelope before deletion; foreign,
/// corrupt and unrecognized files remain counted against disk limits.
fn cleanup_unreferenced_images(
    dir: &Path,
    manifest: &Current,
    limits: CheckpointLimits,
    held_bytes: usize,
) -> Result<()> {
    let selected = manifest
        .current
        .into_iter()
        .chain(manifest.previous)
        .chain(manifest.retired_images.iter().copied())
        .map(image_name)
        .collect::<std::collections::BTreeSet<_>>();
    let mut removed = false;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("checkpoint-")
            || !name.ends_with(".bin")
            || selected.contains(name.as_ref())
            || !fs::symlink_metadata(entry.path())?.is_file()
        {
            continue;
        }
        let candidate = || -> Result<CheckpointRef> {
            let mut file = open_file(&entry.path(), false)?;
            let mut bytes = [0u8; IMAGE_HEADER];
            file.read_exact(&mut bytes)?;
            let number = |range: std::ops::Range<usize>| -> Result<u64> {
                Ok(u64::from_be_bytes(
                    bytes[range]
                        .try_into()
                        .map_err(|_| invalid_log("orphan image header width"))?,
                ))
            };
            let mut record_hash = [0; 32];
            record_hash.copy_from_slice(&bytes[62..94]);
            let mut digest = [0; 32];
            digest.copy_from_slice(&bytes[110..142]);
            let reference = CheckpointRef {
                covered: LogTip {
                    sequence: number(54..62)?,
                    record_hash,
                },
                revision: StoreRevision(number(94..102)?),
                bytes: number(102..110)?,
                digest,
            };
            if image_name(reference) != name.as_ref()
                || held_bytes
                    .checked_add(8192 + IMAGE_HEADER)
                    .is_none_or(|n| n > limits.capture_charge_max_bytes)
            {
                return Err(invalid_log(
                    "unreferenced image cannot be validated within the existing capture budget",
                ));
            }
            // Cleanup needs proven ownership/integrity, never adoption. Stream
            // the complete envelope without allocating/decoding another image.
            if bytes[..6] != *b"AFSC\0\x01"
                || bytes[6..22] != manifest.identity.store_id
                || bytes[22..38] != manifest.identity.cluster_id
                || bytes[38..54] != manifest.identity.incarnation
                || reference.bytes > limits.image_max_bytes as u64
                || file.metadata()?.len()
                    != reference
                        .bytes
                        .checked_add(IMAGE_HEADER as u64)
                        .ok_or_else(|| invalid_log("orphan length overflow"))?
            {
                return Err(invalid_log(
                    "unreferenced image identity or length mismatch",
                ));
            }
            let mut hash = blake3::Hasher::new();
            hash.update(b"afs.meta.checkpoint.v1");
            hash.update(&bytes[..110]);
            let mut crc = crc32c::crc32c(&bytes[..142]);
            let mut remaining = reference.bytes;
            let mut buffer = [0u8; 8192];
            while remaining > 0 {
                let count = remaining.min(buffer.len() as u64) as usize;
                file.read_exact(&mut buffer[..count])?;
                hash.update(&buffer[..count]);
                crc = crc32c::crc32c_append(crc, &buffer[..count]);
                remaining -= count as u64;
            }
            if hash.finalize().as_bytes() != &reference.digest
                || crc.to_be_bytes() != bytes[142..146]
            {
                return Err(invalid_log("unreferenced image full checksum mismatch"));
            }
            Ok(reference)
        };
        if candidate().is_ok() {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

fn cleanup_retired(
    dir: &Path,
    manifest: &Current,
    limits: LogLimits,
    checkpoint_limits: CheckpointLimits,
) -> Result<(Vec<SegmentRef>, Vec<CheckpointRef>)> {
    let mut segments = Vec::new();
    let mut images = Vec::new();
    for segment in &manifest.retired_segments {
        let path = dir.join(wal_name(segment.base)?);
        if !path.try_exists()? {
            continue;
        }
        let valid = (|| -> Result<()> {
            let (seal, revision) = segment
                .sealed
                .ok_or_else(|| invalid_log("retired active segment"))?;
            let mut file = open_file(&path, false)?;
            if header(&mut file, manifest.identity, segment.base)? != seal
                || file.metadata()?.len() != seal.end
            {
                return Err(invalid_log("retired seal changed"));
            }
            verify_anchor(
                dir,
                manifest.identity,
                limits,
                &[*segment],
                seal.tip,
                revision,
            )
        })();
        if valid.is_ok() {
            fs::remove_file(path)?;
            #[cfg(test)]
            super::crash_tests::pause("checkpoint_during_delete", "one retired segment removed");
        } else {
            segments.push(*segment);
        }
    }
    for image in &manifest.retired_images {
        let path = dir.join(image_name(*image));
        if !path.try_exists()? {
            continue;
        }
        if read_image(dir, manifest.identity, *image, checkpoint_limits).is_ok() {
            fs::remove_file(path)?;
        } else {
            images.push(*image);
        }
    }
    sync_dir(dir)?;
    Ok((segments, images))
}
/// The durable empty marker precedes every initialization file. No business
/// append or listener is admitted until it is removed and the directory synced.
fn initialize_marked(dir: &Path, limits: CheckpointLimits) -> Result<StoreIdentity> {
    let marker = open_file(&dir.join("INITIALIZING"), false)?;
    if marker.metadata()?.len() != 0 {
        return Err(invalid_log("initialization marker is not empty"));
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if !matches!(
            name.to_str(),
            Some(
                "LOCK"
                    | "INITIALIZING"
                    | "IDENTITY"
                    | "IDENTITY.tmp"
                    | "CURRENT"
                    | "CURRENT.tmp"
                    | "wal-00000000000000000001.log"
            )
        ) || !fs::symlink_metadata(entry.path())?.is_file()
        {
            return Err(invalid_log(
                "unknown data in unfinished initialization; originals retained",
            ));
        }
    }
    let identity = if dir.join(IDENTITY).try_exists()? {
        if dir.join("IDENTITY.tmp").try_exists()? {
            return Err(invalid_log("ambiguous initialization identity files"));
        }
        read_identity(dir)?
    } else {
        // Identity is installed before any WAL or selector can be created.
        if dir.join(WAL).try_exists()?
            || dir.join(CURRENT).try_exists()?
            || dir.join("CURRENT.tmp").try_exists()?
        {
            return Err(invalid_log("initialization data precedes durable identity"));
        }
        let tmp = dir.join("IDENTITY.tmp");
        let identity = if tmp.try_exists()? {
            let mut file = open_file(&tmp, false)?;
            let length = file.metadata()?.len();
            if length == 90 {
                read_identity_file(&tmp)?
            } else {
                if length > 90 {
                    return Err(invalid_log("initial identity temporary length"));
                }
                let mut bytes = vec![0; length as usize];
                file.read_exact(&mut bytes)?;
                let n = bytes.len().min(6);
                if bytes[..n] != b"AFSI\0\x01"[..n] {
                    return Err(invalid_log("foreign partial identity temporary retained"));
                }
                // The marker and absence of all data prove this is only an
                // interrupted identity write, never an acknowledged authority.
                drop(file);
                fs::remove_file(&tmp)?;
                StoreIdentity::new()?
            }
        } else {
            StoreIdentity::new()?
        };
        if !tmp.try_exists()? {
            let mut file = open_file(&tmp, true)?;
            file.write_all(&identity_bytes(identity))?;
            file.sync_all()?;
        } else {
            open_file(&tmp, false)?.sync_all()?;
        }
        fs::rename(&tmp, dir.join(IDENTITY))?;
        sync_dir(dir)?;
        identity
    };
    let mut expected = [0u8; LOG_HEADER_BYTES as usize];
    expected[..STATIC_BYTES].copy_from_slice(&static_header(identity, LogTip::EMPTY));
    expected[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
        generation: 0,
        tip: LogTip::EMPTY,
        end: LOG_HEADER_BYTES,
    }));
    let mut wal = if dir.join(WAL).try_exists()? {
        let mut file = open_file(&dir.join(WAL), false)?;
        let length = file.metadata()?.len();
        if length > LOG_HEADER_BYTES {
            return Err(invalid_log(
                "nonempty WAL in unfinished initialization retained",
            ));
        }
        let mut prefix = vec![0; length as usize];
        file.read_exact(&mut prefix)?;
        if prefix != expected[..prefix.len()] {
            return Err(invalid_log(
                "unfinished initial WAL identity/header mismatch",
            ));
        }
        file.seek(SeekFrom::Start(0))?;
        file
    } else {
        open_file(&dir.join(WAL), true)?
    };
    let initial = Current::initial(identity);
    let current = dir.join(CURRENT);
    if current.try_exists()? && read_current(dir, identity)? != Some(initial.clone()) {
        return Err(invalid_log(
            "noninitial CURRENT under initialization marker retained",
        ));
    }
    let temporary = dir.join("CURRENT.tmp");
    if temporary.try_exists()? {
        let expected = encode_current(&initial)?;
        let mut file = open_file(&temporary, false)?;
        let length = file.metadata()?.len();
        if length > expected.len() as u64 {
            return Err(invalid_log("initial CURRENT temporary length"));
        }
        let mut prefix = vec![0; length as usize];
        file.read_exact(&mut prefix)?;
        if prefix != expected[..prefix.len()] {
            return Err(invalid_log("foreign initial CURRENT temporary retained"));
        }
        drop(file);
        fs::remove_file(&temporary)?;
    }
    wal.write_all(&expected)?;
    wal.sync_all()?;
    sync_dir(dir)?;
    #[cfg(test)]
    super::crash_tests::pause(
        "initialization_wal_installed",
        "identity and empty WAL durable; initialization incomplete",
    );
    if !current.try_exists()? {
        write_current(dir, &initial, limits)?;
    }
    open_file(&current, false)?.sync_all()?;
    sync_dir(dir)?;
    fs::remove_file(dir.join("INITIALIZING"))?;
    sync_dir(dir)?;
    Ok(identity)
}

impl LocalLog {
    /// New directories only; old-format or foreign data is never adopted.
    pub fn open(path: impl AsRef<Path>, limits: LogLimits) -> Result<Self> {
        Self::open_with_checkpoint_limits(path, limits, CheckpointLimits::default())
    }
    pub fn open_with_checkpoint_limits(
        path: impl AsRef<Path>,
        limits: LogLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<Self> {
        Self::open_with_policies(
            path,
            limits,
            checkpoint_limits,
            super::journal::MaintenancePolicy::default(),
        )
    }
    /// Configure scheduling without changing the durable identity or format.
    pub fn open_with_policies(
        path: impl AsRef<Path>,
        limits: LogLimits,
        checkpoint_limits: CheckpointLimits,
        maintenance: super::journal::MaintenancePolicy,
    ) -> Result<Self> {
        let limits = limits.validate()?;
        let checkpoint_limits = checkpoint_limits.validate()?;
        let maintenance = maintenance.validate()?;
        let dir = path.as_ref().to_path_buf();
        create_dir(&dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(dir.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(unavailable("Meta log already locked")),
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        let identity = if dir.join("INITIALIZING").try_exists()? {
            initialize_marked(&dir, checkpoint_limits)?
        } else if dir.join(IDENTITY).try_exists()? {
            read_identity(&dir)?
        } else {
            for entry in fs::read_dir(&dir)? {
                if entry?.file_name() != "LOCK" {
                    return Err(invalid_log(
                        "nonempty directory without IDENTITY; legacy/recovery data retained; use an empty directory for the new format and keep the original binary and directory for old data",
                    ));
                }
            }
            let marker = open_file(&dir.join("INITIALIZING"), true)?;
            marker.sync_all()?;
            sync_dir(&dir)?;
            initialize_marked(&dir, checkpoint_limits)?
        };
        let replay = replay(&dir, identity, limits, checkpoint_limits)?;
        let mut manifest = replay.manifest;
        cleanup_unreferenced_images(
            &dir,
            &manifest,
            checkpoint_limits,
            replay
                .recovered
                .checkpoint
                .as_ref()
                .map_or(0, |image| image.bytes.capacity()),
        )?;
        let (retired_segments, retired_images) =
            cleanup_retired(&dir, &manifest, limits, checkpoint_limits)?;
        if manifest.retired_segments != retired_segments
            || manifest.retired_images != retired_images
        {
            manifest.retired_segments = retired_segments;
            manifest.retired_images = retired_images;
            write_current(&dir, &manifest, checkpoint_limits)?;
        }
        let wal_bytes = wal_budget(&dir, limits, 0, 0)?;
        Ok(Self {
            inner: Arc::new(Inner {
                owner_pid: std::process::id(),
                dir,
                identity,
                limits,
                checkpoint_limits,
                maintenance,
                checkpoint_install: Mutex::new(()),
                _lock: lock,
                state: Mutex::new(LocalState {
                    file: replay.file,
                    tip: replay.recovered.tip,
                    end: replay.end,
                    generation: replay.generation,
                    poisoned: false,
                    recovered: Some(replay.recovered),
                    manifest,
                    revision: replay.revision,
                    wal_bytes,
                }),
            }),
        })
    }
    pub fn identity(&self) -> StoreIdentity {
        self.inner.identity
    }
    fn check_files(inner: &Inner, state: &mut LocalState) -> Result<()> {
        if state.poisoned {
            return Err(unavailable("Meta WAL write failed; reopen required"));
        }
        if read_identity(&inner.dir)? != inner.identity {
            return Err(invalid_log("live IDENTITY changed"));
        }
        let active = state
            .manifest
            .segments
            .last()
            .ok_or_else(|| invalid_log("missing active segment"))?;
        let selected = read_current(&inner.dir, inner.identity)?
            .ok_or_else(|| invalid_log("live CURRENT missing"))?;
        if selected != state.manifest {
            return Err(invalid_log("live CURRENT changed"));
        }
        let actual = fs::symlink_metadata(inner.dir.join(wal_name(active.base)?))?;
        let opened = state.file.metadata()?;
        if !actual.is_file()
            || actual.dev() != opened.dev()
            || actual.ino() != opened.ino()
            || actual.len() != state.end
        {
            return Err(invalid_log("live WAL identity/length changed"));
        }
        let declared = header(&mut state.file, inner.identity, active.base)?;
        if declared.tip != state.tip
            || declared.end != state.end
            || declared.generation != state.generation
        {
            return Err(invalid_log("live WAL declared tail changed"));
        }
        Ok(())
    }
    fn rotate(inner: &Inner, state: &mut LocalState) -> Result<()> {
        let mut manifest = state.manifest.clone();
        let last = manifest
            .segments
            .last_mut()
            .ok_or_else(|| invalid_log("missing active segment"))?;
        if state.tip == last.base {
            return Err(invalid_log("cannot seal an empty active segment"));
        }
        last.sealed = Some((
            Tail {
                generation: state.generation,
                tip: state.tip,
                end: state.end,
            },
            state.revision,
        ));
        let next = SegmentRef {
            base: state.tip,
            base_revision: state.revision,
            sealed: None,
        };
        let target = inner.dir.join(wal_name(next.base)?);
        let exists = target.try_exists()?;
        let bytes = wal_budget(
            &inner.dir,
            inner.limits,
            if exists { 0 } else { LOG_HEADER_BYTES },
            usize::from(!exists),
        )?;
        let mut file = if exists {
            let mut file = open_file(&target, false)?;
            let tail = header(&mut file, inner.identity, next.base)?;
            if tail
                != (Tail {
                    generation: 0,
                    tip: next.base,
                    end: LOG_HEADER_BYTES,
                })
                || file.metadata()?.len() != LOG_HEADER_BYTES
            {
                return Err(invalid_log(
                    "unselected next segment is not an exact empty prepared segment; retained",
                ));
            }
            file
        } else {
            let mut file = open_file(&target, true)?;
            reserve_file(&file, LOG_HEADER_BYTES)?;
            let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
            bytes[..STATIC_BYTES].copy_from_slice(&static_header(inner.identity, next.base));
            bytes[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
                generation: 0,
                tip: next.base,
                end: LOG_HEADER_BYTES,
            }));
            file.write_all(&bytes)?;
            file
        };
        file.sync_all()?;
        sync_dir(&inner.dir)?;
        #[cfg(test)]
        super::crash_tests::pause(
            "rotation_prepared",
            "empty next segment synced; old CURRENT",
        );
        manifest.segments.push(next);
        if manifest.segments.len() > inner.limits.segments_max {
            return Err(full("WAL segment count exhausted"));
        }
        // Selector durability precedes all writes through the new active FD.
        if let Err(error) = write_current(&inner.dir, &manifest, inner.checkpoint_limits) {
            state.poisoned = true;
            return Err(error);
        }
        #[cfg(test)]
        super::crash_tests::pause(
            "rotation_selected",
            "new CURRENT synced; no append attempted",
        );
        file.seek(SeekFrom::Start(LOG_HEADER_BYTES))?;
        state.file = file;
        state.manifest = manifest;
        state.end = LOG_HEADER_BYTES;
        state.generation = 0;
        state.wal_bytes = bytes;
        Ok(())
    }
    fn cleanup(inner: &Inner) -> Result<()> {
        let manifest = {
            let mut state = inner
                .state
                .lock()
                .map_err(|_| unavailable("WAL lock poisoned"))?;
            Self::check_files(inner, &mut state)?;
            state.manifest.clone()
        };
        // No writer lock while checking retired bodies or deleting files.
        // The checkpoint-install guard serializes retirement/cleanup; rotation
        // can only append live descriptors and never reselect a retired file.
        let (segments, images) =
            cleanup_retired(&inner.dir, &manifest, inner.limits, inner.checkpoint_limits)?;
        let mut state = inner
            .state
            .lock()
            .map_err(|_| unavailable("WAL lock poisoned"))?;
        Self::check_files(inner, &mut state)?;
        if state.manifest.retired_segments != segments || state.manifest.retired_images != images {
            let mut current = state.manifest.clone();
            current.retired_segments = segments;
            current.retired_images = images;
            if let Err(error) = write_current(&inner.dir, &current, inner.checkpoint_limits) {
                state.poisoned = true;
                return Err(error);
            }
            state.manifest = current;
        }
        state.wal_bytes = wal_budget(&inner.dir, inner.limits, 0, 0)?;
        Ok(())
    }
}
impl MetaLog for LocalLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)
            })
            .await
            .map_err(|e| unavailable(format!("WAL health task: {e}")))?
        })
    }
    fn maintenance_policy(&self) -> Option<super::journal::MaintenancePolicy> {
        Some(self.inner.maintenance)
    }
    fn status(&self) -> MetaFuture<'_, super::journal::LogStatus> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                let checkpoint = state
                    .manifest
                    .current
                    .map_or(LogTip::EMPTY, |image| image.covered);
                let mut pending = 0u64;
                for segment in &state.manifest.segments {
                    let (tip, end) = segment
                        .sealed
                        .map_or((state.tip, state.end), |(tail, _)| (tail.tip, tail.end));
                    if tip.sequence > checkpoint.sequence {
                        pending = pending
                            .checked_add(end - LOG_HEADER_BYTES)
                            .ok_or_else(|| invalid_log("checkpoint trigger byte overflow"))?;
                    }
                }
                Ok(super::journal::LogStatus {
                    tip: state.tip,
                    checkpoint,
                    pending_bytes_upper_bound: pending,
                })
            })
            .await
            .map_err(|error| unavailable(format!("WAL status task: {error}")))?
        })
    }
    fn can_append(&self, expected: LogTip, framed_bytes: usize) -> MetaFuture<'_, bool> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if expected != state.tip {
                    return Err(unavailable("WAL admission tip changed"));
                }
                let rotate = state.end > LOG_HEADER_BYTES
                    && state
                        .end
                        .checked_add(framed_bytes as u64)
                        .is_none_or(|end| end > inner.limits.segment_bytes);
                let (extra, files) = if rotate {
                    let exists = inner.dir.join(wal_name(state.tip)?).try_exists()?;
                    (
                        if exists { 0 } else { LOG_HEADER_BYTES },
                        usize::from(!exists),
                    )
                } else {
                    (0, 0)
                };
                let charge = (framed_bytes as u64)
                    .checked_add(extra)
                    .ok_or_else(|| full("WAL admission byte overflow"))?;
                // This path performs only reads: capacity failure proves append
                // was not attempted. Never apply this classification to write IO.
                match wal_budget(&inner.dir, inner.limits, charge, files) {
                    Ok(_) => Ok(true),
                    Err(error) if error.code() == afs_error::IO_CAPACITY => Ok(false),
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(|error| unavailable(format!("WAL admission task: {error}")))?
        })
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        self.inner.checkpoint_limits
    }
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let _install = inner
                    .checkpoint_install
                    .try_lock()
                    .map_err(|_| unavailable("checkpoint install already active or poisoned"))?;
                Self::cleanup(&inner)?;
                let limits = inner.checkpoint_limits;
                let revision =
                    super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?;
                let (mut prefix, old) = {
                    let mut state = inner
                        .state
                        .lock()
                        .map_err(|_| unavailable("WAL lock poisoned"))?;
                    Self::check_files(&inner, &mut state)?;
                    if covered.sequence > state.tip.sequence {
                        return Err(invalid_log("checkpoint covers future log sequence"));
                    }
                    let mut prefix = state.manifest.segments.clone();
                    let last = prefix
                        .last_mut()
                        .ok_or_else(|| invalid_log("missing checkpoint active segment"))?;
                    last.sealed = Some((
                        Tail {
                            generation: state.generation,
                            tip: state.tip,
                            end: state.end,
                        },
                        state.revision,
                    ));
                    (prefix, state.manifest.clone())
                };
                verify_anchor(
                    &inner.dir,
                    inner.identity,
                    inner.limits,
                    &prefix,
                    covered,
                    revision,
                )?;
                let (header, reference) = image_header(inner.identity, covered, revision, &bytes);
                if let Some(current) = old.current {
                    if covered.sequence < current.covered.sequence {
                        return Err(invalid_log("checkpoint installation regresses CURRENT"));
                    }
                    if covered.sequence == current.covered.sequence {
                        if current != reference {
                            return Err(invalid_log(
                                "same checkpoint position has different state",
                            ));
                        }
                        read_image(&inner.dir, inner.identity, reference, limits)?;
                        open_file(&inner.dir.join(image_name(reference)), false)?.sync_all()?;
                        open_file(&inner.dir.join(CURRENT), false)?.sync_all()?;
                        return sync_dir(&inner.dir);
                    }
                }
                let mut previous = None;
                for candidate in old.current.into_iter().chain(old.previous) {
                    if read_image(&inner.dir, inner.identity, candidate, limits).is_ok()
                        && verify_anchor(
                            &inner.dir,
                            inner.identity,
                            inner.limits,
                            &prefix,
                            candidate.covered,
                            candidate.revision,
                        )
                        .is_ok()
                    {
                        previous = Some(candidate);
                        break;
                    }
                }
                if old.current.is_some() && previous.is_none() {
                    return Err(invalid_log(
                        "prior CURRENT recovery point invalid; originals retained",
                    ));
                }
                cleanup_unreferenced_images(&inner.dir, &old, limits, bytes.capacity())?;
                // The captured prefix is no longer needed after anchors validate.
                prefix.clear();
                quarantine_checkpoint_tmp(&inner.dir, "checkpoint.tmp", limits)?;
                let target = inner.dir.join(image_name(reference));
                let exists = target.try_exists()?;
                let extra = if exists {
                    0
                } else {
                    reference.bytes + IMAGE_HEADER as u64
                };
                checkpoint_budget(
                    &inner.dir,
                    extra + MANIFEST_MAX as u64,
                    if exists { 1 } else { 2 },
                    limits,
                )?;
                if exists {
                    read_image(&inner.dir, inner.identity, reference, limits)?;
                    open_file(&target, false)?.sync_all()?;
                } else {
                    let tmp = inner.dir.join("checkpoint.tmp");
                    let mut file = open_file(&tmp, true)?;
                    reserve_file(&file, reference.bytes + IMAGE_HEADER as u64)?;
                    file.write_all(&header)?;
                    #[cfg(test)]
                    if super::crash_tests::active("checkpoint_partial") {
                        file.write_all(&bytes[..bytes.len() / 2])?;
                        super::crash_tests::pause("checkpoint_partial", "half image body written");
                    }
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    fs::rename(tmp, &target)?;
                    sync_dir(&inner.dir)?;
                    #[cfg(test)]
                    super::crash_tests::pause(
                        "checkpoint_image_installed",
                        "image durable; old CURRENT",
                    );
                }
                {
                    let mut state = inner
                        .state
                        .lock()
                        .map_err(|_| unavailable("WAL lock poisoned"))?;
                    Self::check_files(&inner, &mut state)?;
                    let mut manifest = state.manifest.clone();
                    for image in manifest.current.into_iter().chain(manifest.previous) {
                        if image != reference
                            && Some(image) != previous
                            && !manifest.retired_images.contains(&image)
                        {
                            manifest.retired_images.push(image);
                        }
                    }
                    manifest.current = Some(reference);
                    manifest.previous = previous;
                    let oldest = previous.unwrap_or(reference).covered.sequence;
                    let remove = manifest
                        .segments
                        .iter()
                        .take_while(|segment| {
                            segment
                                .sealed
                                .is_some_and(|(tail, _)| tail.tip.sequence <= oldest)
                        })
                        .count();
                    manifest
                        .retired_segments
                        .extend(manifest.segments.drain(..remove));
                    if let Err(error) = write_current(&inner.dir, &manifest, limits) {
                        state.poisoned = true;
                        return Err(error);
                    }
                    #[cfg(test)]
                    super::crash_tests::pause(
                        "checkpoint_selected",
                        "new CURRENT durable; before retirement cleanup",
                    );
                    state.manifest = manifest;
                    state.recovered = None;
                }
                Self::cleanup(&inner)
            })
            .await
            .map_err(|error| unavailable(format!("checkpoint installer task: {error}")))?
        })
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if let Some(recovered) = state.recovered.take() {
                    return Ok(recovered);
                }
                Ok(replay(
                    &inner.dir,
                    inner.identity,
                    inner.limits,
                    inner.checkpoint_limits,
                )?
                .recovered)
            })
            .await
            .map_err(|e| unavailable(format!("WAL recovery task: {e}")))?
        })
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move {
            inner.ensure_owner()?;
            tokio::task::spawn_blocking(move || {
                inner.ensure_owner()?;
                let mut state = inner
                    .state
                    .lock()
                    .map_err(|_| unavailable("local WAL lock poisoned"))?;
                Self::check_files(&inner, &mut state)?;
                if state.tip != expected {
                    return Err(unavailable("local log compare-and-append tip changed"));
                }
                let revision = records
                    .last()
                    .map(|record| record.revision_after())
                    .ok_or_else(|| invalid_log("empty append group"))?;
                let (frames, tip, bytes) =
                    encode_batch(inner.identity, expected, records, inner.limits)?;
                let rotate = state.end > LOG_HEADER_BYTES
                    && state
                        .end
                        .checked_add(bytes as u64)
                        .is_none_or(|size| size > inner.limits.segment_bytes);
                let header_charge =
                    if rotate && !inner.dir.join(wal_name(state.tip)?).try_exists()? {
                        LOG_HEADER_BYTES
                    } else {
                        0
                    };
                let extra = (bytes as u64)
                    .checked_add(header_charge)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                if state
                    .wal_bytes
                    .checked_add(extra)
                    .is_none_or(|size| size > inner.limits.wal_max_bytes)
                {
                    return Err(full("WAL byte budget exhausted; checkpoint required"));
                }
                if rotate {
                    Self::rotate(&inner, &mut state)?;
                }
                let end = state
                    .end
                    .checked_add(bytes as u64)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                let generation = state
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_log("tail generation exhausted"))?;
                let start = state.end;
                let result = (|| {
                    state.file.seek(SeekFrom::Start(start))?;
                    for frame in frames {
                        #[cfg(test)]
                        if super::crash_tests::active("partial_second")
                            && state.file.stream_position()? > start
                        {
                            state.file.write_all(&frame[..frame.len() / 2])?;
                            super::crash_tests::pause(
                                "partial_second",
                                "one full, one partial frame",
                            );
                        }
                        state.file.write_all(&frame)?;
                    }
                    write_tail(
                        &mut state.file,
                        Tail {
                            generation,
                            tip,
                            end,
                        },
                    )?;
                    #[cfg(test)]
                    super::crash_tests::pause(
                        "after_append_before_sync",
                        "complete frames and tail, no sync yet",
                    );
                    state.file.sync_data()?;
                    Ok::<(), Error>(())
                })();
                if let Err(error) = result {
                    state.poisoned = true;
                    return Err(error);
                }
                state.tip = tip;
                state.end = end;
                state.generation = generation;
                state.revision = revision;
                state.wal_bytes = state
                    .wal_bytes
                    .checked_add(bytes as u64)
                    .ok_or_else(|| full("WAL byte budget overflow"))?;
                state.recovered = None;
                Ok(tip)
            })
            .await
            .map_err(|e| unavailable(format!("WAL append task: {e}")))?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::store::journal::{encode_frame, test_records};

    #[test]
    fn p3_lock_owner_release_is_not_delayed_by_inherited_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        // dup retains the same open-file-description lock, as a forked child
        // can do before CLOEXEC closes inherited descriptors. No unsafe fork.
        let inherited = log.inner._lock.try_clone().unwrap();
        let clone = log.clone();
        drop(log);
        assert!(
            LocalLog::open(dir.path(), LogLimits::default()).is_err(),
            "another real LocalLog owner is still alive"
        );
        drop(clone);
        let reopened = LocalLog::open(dir.path(), LogLimits::default());
        assert!(
            reopened.is_ok(),
            "last real owner release must unlock despite a non-authoritative duplicated descriptor"
        );
        let reopened = reopened.unwrap();
        drop(inherited);
        assert!(
            LocalLog::open(dir.path(), LogLimits::default()).is_err(),
            "closing a stale duplicate must not unlock the new owner"
        );
        drop(reopened);
        LocalLog::open(dir.path(), LogLimits::default()).unwrap();
    }
    #[test]
    fn p3_lock_owner_foreign_drop_does_not_unlock_parent_description() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let parent_description = log.inner._lock.try_clone().unwrap();
        Arc::get_mut(&mut log.inner).unwrap().owner_pid = std::process::id().wrapping_add(1);
        drop(log);
        assert!(
            LocalLog::open(dir.path(), LogLimits::default()).is_err(),
            "dropping an inherited handle must not unlock the parent writer"
        );
        drop(parent_description);
        LocalLog::open(dir.path(), LogLimits::default()).unwrap();
    }
    #[tokio::test]
    async fn p3_lock_owner_foreign_process_refuses_all_io_before_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let snapshot = || {
            fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    (
                        path.file_name().unwrap().to_owned(),
                        fs::read(&path).unwrap(),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let before = snapshot();
        let actual = std::process::id();
        Arc::get_mut(&mut log.inner).unwrap().owner_pid = actual.wrapping_add(1);
        let image = super::super::log::StateImage::encode(
            &super::super::state::State::default(),
            1024 * 1024,
        )
        .unwrap();
        let errors = [
            log.health().await.is_err(),
            log.status().await.is_err(),
            log.recover().await.is_err(),
            log.can_append(LogTip::EMPTY, 4096).await.is_err(),
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err(),
            log.install_checkpoint(LogTip::EMPTY, image).await.is_err(),
        ];
        Arc::get_mut(&mut log.inner).unwrap().owner_pid = actual;
        assert!(
            errors.into_iter().all(|error| error),
            "an inherited runtime handle is not a new writer authority"
        );
        assert_eq!(
            snapshot(),
            before,
            "foreign process rejection must precede any storage mutation"
        );
    }

    async fn install_test_image(
        log: &LocalLog,
        records: Vec<Record>,
        previous: LogTip,
        state: &mut super::super::state::State,
    ) -> LogTip {
        for record in &records {
            *state = state.replay_record(record).unwrap();
        }
        let tip = log.append_and_sync(previous, records).await.unwrap();
        log.install_checkpoint(
            tip,
            super::super::log::StateImage::encode(state, 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
        tip
    }

    #[test]
    fn p3_initialization_resumes_only_marked_empty_prefixes() {
        for phase in 0..6 {
            let dir = tempfile::tempdir().unwrap();
            let identity = StoreIdentity::new().unwrap();
            fs::write(dir.path().join("INITIALIZING"), []).unwrap();
            if phase == 1 {
                fs::write(
                    dir.path().join("IDENTITY.tmp"),
                    &identity_bytes(identity)[..12],
                )
                .unwrap();
            }
            if phase == 2 {
                fs::write(dir.path().join("IDENTITY.tmp"), identity_bytes(identity)).unwrap();
            }
            if phase >= 3 {
                fs::write(dir.path().join(IDENTITY), identity_bytes(identity)).unwrap();
                let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
                bytes[..STATIC_BYTES].copy_from_slice(&static_header(identity, LogTip::EMPTY));
                bytes[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
                    generation: 0,
                    tip: LogTip::EMPTY,
                    end: LOG_HEADER_BYTES,
                }));
                fs::write(
                    dir.path().join(WAL),
                    if phase == 3 {
                        &bytes[..100]
                    } else {
                        &bytes[..]
                    },
                )
                .unwrap();
            }
            if phase == 4 {
                let current = encode_current(&Current::initial(identity)).unwrap();
                fs::write(dir.path().join("CURRENT.tmp"), &current[..17]).unwrap();
            }
            if phase == 5 {
                write_current(
                    dir.path(),
                    &Current::initial(identity),
                    CheckpointLimits::default(),
                )
                .unwrap();
            }
            let log = LocalLog::open(dir.path(), LogLimits::default())
                .unwrap_or_else(|e| panic!("initialization phase {phase} must resume: {e}"));
            let found = log.identity();
            if phase >= 2 {
                assert_eq!(found, identity);
            }
            assert!(!dir.path().join("INITIALIZING").exists());
            assert_eq!(
                read_current(dir.path(), found).unwrap(),
                Some(Current::initial(found))
            );
            assert_eq!(log.inner.state.lock().unwrap().tip, LogTip::EMPTY);
            drop(log);
            assert_eq!(
                LocalLog::open(dir.path(), LogLimits::default())
                    .unwrap()
                    .identity(),
                found
            );
        }
    }

    #[tokio::test]
    async fn p3_initialization_marker_never_adopts_committed_or_foreign_data() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        log.append_and_sync(LogTip::EMPTY, test_records(1))
            .await
            .unwrap();
        drop(log);
        fs::write(dir.path().join("INITIALIZING"), []).unwrap();
        let original = fs::read(dir.path().join(WAL)).unwrap();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), original);
        assert_eq!(read_identity(dir.path()).unwrap(), identity);
        let foreign = tempfile::tempdir().unwrap();
        fs::write(foreign.path().join("INITIALIZING"), []).unwrap();
        fs::write(foreign.path().join("snapshot.json"), b"foreign input").unwrap();
        assert!(LocalLog::open(foreign.path(), LogLimits::default()).is_err());
        assert_eq!(
            fs::read(foreign.path().join("snapshot.json")).unwrap(),
            b"foreign input"
        );
    }

    #[tokio::test]
    async fn p3_orphan_image_cleanup_preserves_foreign_corrupt_and_unknown_files() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        let bytes =
            super::super::log::StateImage::encode(&super::super::state::State::default(), 65536)
                .unwrap();
        let (own_header, own_ref) =
            image_header(identity, LogTip::EMPTY, StoreRevision::ZERO, &bytes);
        fs::write(
            dir.path().join(image_name(own_ref)),
            [own_header.as_slice(), &bytes].concat(),
        )
        .unwrap();
        let (foreign_header, foreign_ref) = image_header(
            StoreIdentity::new().unwrap(),
            LogTip::EMPTY,
            StoreRevision::ZERO,
            &bytes,
        );
        let foreign = [foreign_header.as_slice(), &bytes].concat();
        fs::write(dir.path().join(image_name(foreign_ref)), &foreign).unwrap();
        let (bad_header, bad_ref) = image_header(
            identity,
            LogTip {
                sequence: 1,
                record_hash: [7; 32],
            },
            StoreRevision::ZERO,
            &bytes,
        );
        let mut bad = [bad_header.as_slice(), &bytes].concat();
        *bad.last_mut().unwrap() ^= 1;
        fs::write(dir.path().join(image_name(bad_ref)), &bad).unwrap();
        fs::write(
            dir.path().join("checkpoint-unknown.bin"),
            b"unknown original",
        )
        .unwrap();
        drop(log);
        let reopened = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert!(!dir.path().join(image_name(own_ref)).exists());
        assert_eq!(
            fs::read(dir.path().join(image_name(foreign_ref))).unwrap(),
            foreign
        );
        assert_eq!(fs::read(dir.path().join(image_name(bad_ref))).unwrap(), bad);
        assert_eq!(
            fs::read(dir.path().join("checkpoint-unknown.bin")).unwrap(),
            b"unknown original"
        );
        assert_eq!(reopened.recover().await.unwrap().tip, LogTip::EMPTY);
    }

    #[tokio::test]
    async fn p3_recovery_missing_current_refuses_acknowledged_wal() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        log.append_and_sync(LogTip::EMPTY, test_records(1))
            .await
            .unwrap();
        drop(log);
        fs::remove_file(dir.path().join(CURRENT)).unwrap();
        let original = fs::read(dir.path().join(WAL)).unwrap();
        assert!(
            LocalLog::open(dir.path(), LogLimits::default()).is_err(),
            "missing CURRENT must not silently choose the initial manifest"
        );
        assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), original);
    }
    #[tokio::test]
    async fn p3_recovery_rolled_current_refuses_unlisted_nonempty_segment() {
        let dir = tempfile::tempdir().unwrap();
        let limits = LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 4096,
            wal_max_bytes: 128 * 1024,
            segment_bytes: 4608,
            segments_max: 32,
        };
        let log = LocalLog::open(dir.path(), limits).unwrap();
        let initial = fs::read(dir.path().join(CURRENT)).unwrap();
        let mut tip = LogTip::EMPTY;
        for record in test_records(12) {
            tip = log.append_and_sync(tip, vec![record]).await.unwrap();
        }
        drop(log);
        let originals: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("wal-"))
            .map(|p| {
                let bytes = fs::read(&p).unwrap();
                (p, bytes)
            })
            .collect();
        assert!(originals.iter().any(
            |(p, bytes)| p.file_name().unwrap() != WAL && bytes.len() as u64 > LOG_HEADER_BYTES
        ));
        fs::write(dir.path().join(CURRENT), initial).unwrap();
        assert!(
            LocalLog::open(dir.path(), limits).is_err(),
            "rolled CURRENT must not hide the acknowledged suffix"
        );
        for (path, bytes) in originals {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }

    #[test]
    fn p3_maintenance_current_temporary_uses_manifest_bound_not_image_bound() {
        let dir = tempfile::tempdir().unwrap();
        let limits = CheckpointLimits {
            image_max_bytes: 256,
            capture_charge_max_bytes: 1024,
            disk_max_bytes: 256 * 1024,
            files_max: 5,
        };
        limits.validate().unwrap();
        let bytes = vec![7u8; 1024];
        fs::write(dir.path().join("CURRENT.tmp"), &bytes).unwrap();
        quarantine_checkpoint_tmp(dir.path(), "CURRENT.tmp", limits)
            .expect("CURRENT.tmp is bounded by manifest size, not a small image cap");
        let evidence = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("orphan-CURRENT.tmp-")
            })
            .unwrap();
        assert_eq!(fs::read(evidence).unwrap(), bytes);
        fs::write(dir.path().join("checkpoint.tmp"), &bytes).unwrap();
        assert!(quarantine_checkpoint_tmp(dir.path(), "checkpoint.tmp", limits).is_err());
        assert_eq!(fs::read(dir.path().join("checkpoint.tmp")).unwrap(), bytes);
    }

    #[tokio::test]
    async fn p3_segments_capacity_rejects_before_mutation_and_missing_seal_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let limits = LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 4096,
            wal_max_bytes: 128 * 1024,
            segment_bytes: 4608,
            segments_max: 2,
        };
        let log = LocalLog::open(dir.path(), limits).unwrap();
        let snapshot = || {
            fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    (
                        path.file_name().unwrap().to_owned(),
                        fs::read(&path).unwrap(),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        let mut tip = LogTip::EMPTY;
        let mut rejected = false;
        for record in test_records(24) {
            let before = snapshot();
            match log.append_and_sync(tip, vec![record]).await {
                Ok(next) => tip = next,
                Err(error) => {
                    assert!(error.to_string().contains("segment count"));
                    assert_eq!(snapshot(), before);
                    rejected = true;
                    break;
                }
            }
        }
        assert!(rejected);
        log.health().await.unwrap();
        assert_eq!(log.recover().await.unwrap().tip, tip);
        let manifest = read_current(dir.path(), log.identity()).unwrap().unwrap();
        assert!(manifest.segments[0].sealed.is_some());
        let path = dir
            .path()
            .join(wal_name(manifest.segments[0].base).unwrap());
        drop(log);
        fs::rename(&path, dir.path().join("preserved-required-segment")).unwrap();
        assert!(LocalLog::open(dir.path(), limits).is_err());
    }

    #[tokio::test]
    async fn p3_segments_rotate_reclaim_and_keep_previous_recovery_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let limits = LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 4096,
            wal_max_bytes: 128 * 1024,
            segment_bytes: 4608,
            segments_max: 32,
        };
        let log = LocalLog::open(dir.path(), limits).unwrap();
        let identity = log.identity();
        let mut state = super::super::state::State::default();
        let mut records = test_records(12).into_iter();
        let mut tip = LogTip::EMPTY;
        for _ in 0..4 {
            let record = records.next().unwrap();
            state = state.replay_record(&record).unwrap();
            tip = log.append_and_sync(tip, vec![record]).await.unwrap();
        }
        let wal_names = || {
            fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with("wal-") && name.ends_with(".log"))
                .collect::<Vec<_>>()
        };
        assert!(
            wal_names().len() > 1,
            "WAL must rotate at the fixed small segment target"
        );
        log.install_checkpoint(
            tip,
            super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
        for _ in 0..2 {
            for _ in 0..4 {
                let record = records.next().unwrap();
                state = state.replay_record(&record).unwrap();
                tip = log.append_and_sync(tip, vec![record]).await.unwrap();
            }
            log.install_checkpoint(
                tip,
                super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap(),
            )
            .await
            .unwrap();
        }
        assert!(
            !dir.path().join(WAL).exists(),
            "fully covered sealed initial segment should be reclaimed"
        );
        let manifest = read_current(dir.path(), identity).unwrap().unwrap();
        let image = dir.path().join(image_name(manifest.current.unwrap()));
        let mut corrupt = fs::read(&image).unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&image, &corrupt).unwrap();
        drop(log);
        let log = LocalLog::open(dir.path(), limits).unwrap();
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, tip);
        let image = recovered.checkpoint.unwrap();
        assert_eq!(image.covered.sequence, 8);
        let mut restored =
            super::super::log::StateImage::decode(&image.bytes, 1024 * 1024).unwrap();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            state.snapshot_bytes_with_batch(0).unwrap()
        );
        drop(log);
        let active = wal_names().into_iter().max().unwrap();
        fs::rename(
            dir.path().join(&active),
            dir.path().join("retained-missing-segment"),
        )
        .unwrap();
        assert!(
            LocalLog::open(dir.path(), limits).is_err(),
            "missing required active segment cannot be ignored"
        );
    }

    #[tokio::test]
    async fn p3_checkpoint_bad_current_image_falls_back_without_losing_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        let mut state = super::super::state::State::default();
        let mut records = test_records(3);
        let tip1 =
            install_test_image(&log, vec![records.remove(0)], LogTip::EMPTY, &mut state).await;
        let tip2 = install_test_image(&log, vec![records.remove(0)], tip1, &mut state).await;
        let tip3 = log.append_and_sync(tip2, records).await.unwrap();
        let manifest = read_current(dir.path(), identity).unwrap().unwrap();
        assert_eq!(manifest.previous.unwrap().covered, tip1);
        let current = dir.path().join(image_name(manifest.current.unwrap()));
        let mut corrupt = fs::read(&current).unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&current, &corrupt).unwrap();
        drop(log);
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, tip3);
        assert_eq!(recovered.checkpoint.unwrap().covered, tip1);
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(
            fs::read(&current).unwrap(),
            corrupt,
            "corruption evidence retained"
        );
        drop(log);
        // A previous valid image never authorizes ignoring a broken ACK chain.
        let mut wal = fs::read(dir.path().join(WAL)).unwrap();
        *wal.last_mut().unwrap() ^= 1;
        fs::write(dir.path().join(WAL), &wal).unwrap();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), wal);
    }

    #[tokio::test]
    async fn p3_checkpoint_identity_and_manifest_corruption_refuse_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        let mut state = super::super::state::State::default();
        install_test_image(&log, test_records(1), LogTip::EMPTY, &mut state).await;
        let manifest = read_current(dir.path(), identity).unwrap().unwrap();
        let image = dir.path().join(image_name(manifest.current.unwrap()));
        let original = fs::read(&image).unwrap();
        let mut foreign = original.clone();
        foreign[6] ^= 1;
        fs::write(&image, &foreign).unwrap();
        drop(log);
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        fs::write(&image, &original).unwrap();
        let current = dir.path().join(CURRENT);
        let mut broken = fs::read(&current).unwrap();
        broken[6] ^= 1;
        fs::write(&current, &broken).unwrap();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        assert_eq!(fs::read(current).unwrap(), broken);
    }

    #[test]
    fn p3_checkpoint_quarantined_temporary_is_charged_and_symlink_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let limits = CheckpointLimits {
            image_max_bytes: 256,
            capture_charge_max_bytes: 1024,
            disk_max_bytes: 256 * 1024,
            files_max: 5,
        };
        limits.validate().unwrap();
        fs::write(dir.path().join("checkpoint.tmp"), [1u8; 256]).unwrap();
        quarantine_checkpoint_tmp(dir.path(), "checkpoint.tmp", limits).unwrap();
        assert!(checkpoint_budget(dir.path(), 256 * 1024, 0, limits).is_err());
        let outside = dir.path().join("missing");
        std::os::unix::fs::symlink(&outside, dir.path().join(CURRENT)).unwrap();
        assert!(read_current(dir.path(), StoreIdentity::new().unwrap()).is_err());
    }

    #[tokio::test]
    async fn p3_local_write_failure_latches_until_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        // Real write(2) EBADF, rather than a hook returning an invented error.
        log.inner.state.lock().unwrap().file = File::open(dir.path().join(WAL)).unwrap();
        assert!(
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err()
        );
        assert!(log.inner.state.lock().unwrap().poisoned);
        assert!(log.health().await.is_err());
        assert!(log.recover().await.is_err());
        assert!(
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err()
        );
        drop(log);
        let reopened = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(reopened.recover().await.unwrap().tip, LogTip::EMPTY);
    }

    #[tokio::test]
    async fn p3_local_wal_budget_refuses_before_changing_file() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(
            dir.path(),
            LogLimits {
                record_max_bytes: 1024,
                batch_max_bytes: 4096,
                wal_max_bytes: 8192,
                ..Default::default()
            },
        )
        .unwrap();
        let mut tip = LogTip::EMPTY;
        let mut exhausted = false;
        for record in test_records(64) {
            let before = fs::read(dir.path().join(WAL)).unwrap();
            match log.append_and_sync(tip, vec![record]).await {
                Ok(next) => tip = next,
                Err(error) => {
                    assert!(error.to_string().contains("WAL byte budget"));
                    assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), before);
                    exhausted = true;
                    break;
                }
            }
        }
        assert!(exhausted);
        assert!(tip.sequence > 0);
        assert!(fs::metadata(dir.path().join(WAL)).unwrap().len() <= 8192);
        assert_eq!(log.recover().await.unwrap().tip, tip);
        log.health().await.unwrap();
    }

    #[tokio::test]
    async fn p3_local_group_reopens_exact_records_and_stable_identity() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        let original = test_records(2);
        let expected = original
            .iter()
            .map(|r| crate::meta::store::codec::encode_bounded(r, 1024 * 1024).unwrap())
            .collect::<Vec<_>>();
        let tip = log.append_and_sync(LogTip::EMPTY, original).await.unwrap();
        log.health().await.unwrap();
        drop(log);
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(log.identity(), identity);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, tip);
        assert_eq!(
            recovered
                .records
                .iter()
                .map(|r| crate::meta::store::codec::encode_bounded(r, 1024 * 1024).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn p3_unacknowledged_complete_record_replays_and_incomplete_tail_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        drop(log);
        let record = test_records(1).pop().unwrap();
        let (frame, tip) = encode_frame(identity, LogTip::EMPTY, record, 1024 * 1024).unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .open(dir.path().join(WAL))
            .unwrap();
        file.write_all(&frame).unwrap();
        file.write_all(&[4, 5, 6]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(log.recover().await.unwrap().tip, tip);
        assert_eq!(
            fs::metadata(dir.path().join(WAL)).unwrap().len(),
            LOG_HEADER_BYTES + frame.len() as u64
        );
        let evidence = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("incomplete-tail-")
            })
            .unwrap();
        assert_eq!(fs::read(evidence).unwrap(), [4, 5, 6]);
        drop(log);
        assert_eq!(
            LocalLog::open(dir.path(), LogLimits::default())
                .unwrap()
                .recover()
                .await
                .unwrap()
                .tip,
            tip
        );
    }

    #[tokio::test]
    async fn p3_acknowledged_truncation_is_not_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        log.append_and_sync(LogTip::EMPTY, test_records(1))
            .await
            .unwrap();
        drop(log);
        let path = dir.path().join(WAL);
        let file = open_file(&path, false).unwrap();
        file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let before = fs::read(&path).unwrap();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[tokio::test]
    async fn p3_complete_tail_corruption_is_not_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        log.append_and_sync(LogTip::EMPTY, test_records(1))
            .await
            .unwrap();
        drop(log);
        let path = dir.path().join(WAL);
        let mut file = open_file(&path, false).unwrap();
        file.seek(SeekFrom::Start(LOG_HEADER_BYTES + 8)).unwrap();
        file.write_all(&[255]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let before = fs::read(&path).unwrap();
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn p3_identity_mix_missing_wal_and_legacy_are_rejected() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        drop(LocalLog::open(a.path(), LogLimits::default()).unwrap());
        drop(LocalLog::open(b.path(), LogLimits::default()).unwrap());
        fs::copy(b.path().join(IDENTITY), a.path().join(IDENTITY)).unwrap();
        assert!(LocalLog::open(a.path(), LogLimits::default()).is_err());
        fs::rename(b.path().join(WAL), b.path().join("preserved-wal")).unwrap();
        assert!(LocalLog::open(b.path(), LogLimits::default()).is_err());
        for marker in ["snapshot", "wal"] {
            let old = tempfile::tempdir().unwrap();
            fs::write(old.path().join(marker), b"legacy").unwrap();
            let error = match LocalLog::open(old.path(), LogLimits::default()) {
                Err(error) => error,
                Ok(_) => panic!("old {marker} directory was accepted"),
            };
            assert_eq!(error.code(), afs_error::CONFIG_INVALID);
            assert!(error.to_string().contains("empty directory"), "{error}");
            assert!(error.to_string().contains("original binary"), "{error}");
            assert_eq!(fs::read(old.path().join(marker)).unwrap(), b"legacy");
            assert!(!old.path().join(IDENTITY).exists());
            assert!(!old.path().join(WAL).exists());
        }
    }

    #[tokio::test]
    async fn p3_live_health_detects_replaced_file_and_root() {
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        fs::rename(dir.path().join(WAL), dir.path().join("original-wal")).unwrap();
        fs::copy(dir.path().join("original-wal"), dir.path().join(WAL)).unwrap();
        assert!(log.health().await.is_err());
        assert!(
            log.append_and_sync(LogTip::EMPTY, test_records(1))
                .await
                .is_err()
        );
        let other = tempfile::tempdir().unwrap();
        drop(LocalLog::open(other.path(), LogLimits::default()).unwrap());
        fs::copy(other.path().join(IDENTITY), dir.path().join(IDENTITY)).unwrap();
        assert!(log.health().await.is_err());
    }

    #[test]
    fn p3_damaged_tail_marker_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        drop(LocalLog::open(dir.path(), LogLimits::default()).unwrap());
        let mut file = open_file(&dir.path().join(WAL), false).unwrap();
        file.seek(SeekFrom::Start(STATIC_BYTES as u64)).unwrap();
        file.write_all(&[1]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(LocalLog::open(dir.path(), LogLimits::default()).is_err());
    }
}
