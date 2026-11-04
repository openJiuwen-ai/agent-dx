//! WAL segment files: identity, static header, checked tail slots, tail preservation and WAL budget.

use super::*;

pub(super) fn open_file(path: &Path, create_new: bool) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(create_new)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?)
}

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(super) fn create_dir(path: &Path) -> Result<()> {
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

pub(super) fn identity_bytes(identity: StoreIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(90);
    bytes.extend_from_slice(&format::META_IDENTITY_HEADER);
    bytes.extend_from_slice(&identity.store_id);
    bytes.extend_from_slice(&identity.cluster_id);
    bytes.extend_from_slice(&identity.incarnation);
    let hash = *blake3::hash(&bytes).as_bytes();
    bytes.extend_from_slice(&hash);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

pub(super) fn read_identity(dir: &Path) -> Result<StoreIdentity> {
    read_identity_file(&dir.join(IDENTITY))
}

pub(super) fn read_identity_file(path: &Path) -> Result<StoreIdentity> {
    let mut file = open_file(path, false)?;
    let length = file.metadata()?.len();
    // Version first: the six-byte tag is read before the length, hash and CRC.
    if length >= 6 {
        let mut tag = [0; 6];
        file.read_exact(&mut tag)?;
        format::check_tagged_header(
            FormatKind::MetaIdentity,
            format::META_IDENTITY_MAGIC,
            path.display(),
            &tag,
        )?;
        file.seek(SeekFrom::Start(0))?;
    }
    if length != 90 {
        return Err(invalid_log("IDENTITY length"));
    }
    let mut bytes = [0; 90];
    file.read_exact(&mut bytes)?;
    if bytes[..6] != format::META_IDENTITY_HEADER
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

pub(super) fn static_header(identity: StoreIdentity, base: LogTip) -> [u8; STATIC_BYTES] {
    let mut bytes = [0; STATIC_BYTES];
    bytes[..6].copy_from_slice(&format::tagged_header(
        format::META_WAL_SEGMENT_MAGIC,
        format::version::META_WAL_SEGMENT,
    ));
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

pub(super) fn tail_bytes(tail: Tail) -> [u8; SLOT_BYTES] {
    let mut bytes = [0; SLOT_BYTES];
    bytes[..8].copy_from_slice(&tail.generation.to_be_bytes());
    bytes[8..16].copy_from_slice(&tail.tip.sequence.to_be_bytes());
    bytes[16..24].copy_from_slice(&tail.end.to_be_bytes());
    bytes[24..56].copy_from_slice(&tail.tip.record_hash);
    let crc = crc32c::crc32c(&bytes[..60]);
    bytes[60..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

pub(super) fn parse_tail(bytes: &[u8]) -> Result<Option<Tail>> {
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

pub(super) fn write_tail(file: &mut File, tail: Tail) -> Result<()> {
    file.seek(SeekFrom::Start(
        STATIC_BYTES as u64 + (tail.generation % 2) * SLOT_BYTES as u64,
    ))?;
    file.write_all(&tail_bytes(tail))?;
    Ok(())
}

pub(super) fn header(file: &mut File, identity: StoreIdentity, base: LogTip) -> Result<Tail> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; LOG_HEADER_BYTES as usize];
    file.read_exact(&mut bytes)?;
    // Version first: before the byte-exact header, identity, hash and CRC.
    format::check_tagged_header(
        FormatKind::MetaWalSegment,
        format::META_WAL_SEGMENT_MAGIC,
        wal_name(base)?,
        &bytes,
    )?;
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

pub(super) fn preserve_tail(dir: &Path, file: &mut File, start: u64, end: u64) -> Result<()> {
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

pub(super) fn wal_name(base: LogTip) -> Result<String> {
    Ok(format!(
        "wal-{:020}.log",
        base.sequence
            .checked_add(1)
            .ok_or_else(|| invalid_log("segment sequence exhausted"))?
    ))
}

pub(super) fn wal_budget(
    dir: &Path,
    limits: LocalLogOptions,
    extra_bytes: u64,
    extra_files: usize,
    retired: &[SegmentRef],
) -> Result<u64> {
    wal_budget_entries(
        fs::read_dir(dir)?,
        limits,
        extra_bytes,
        extra_files,
        retired,
        |path| fs::symlink_metadata(path),
    )
}

// Keep enumeration and accounting in the same implementation. The iterator seam
// lets regressions delete an enumerated file before stat, without timing hooks.
pub(super) fn wal_budget_entries(
    entries: impl IntoIterator<Item = std::io::Result<fs::DirEntry>>,
    limits: LocalLogOptions,
    extra_bytes: u64,
    extra_files: usize,
    retired: &[SegmentRef],
    mut metadata: impl FnMut(&Path) -> std::io::Result<fs::Metadata>,
) -> Result<u64> {
    let mut bytes = extra_bytes;
    let mut files = extra_files;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-") && name.ends_with(".log") {
            let metadata = match metadata(&entry.path()) {
                Ok(metadata) => metadata,
                Err(error) => {
                    let mut deleted_retired = false;
                    if error.kind() == std::io::ErrorKind::NotFound {
                        for segment in retired {
                            if segment.sealed.is_some() && wal_name(segment.base)? == name.as_ref()
                            {
                                deleted_retired = true;
                                break;
                            }
                        }
                    }
                    // Retirement is selected durably before cleanup unlinks in
                    // parallel with this scan. Only that exact sealed set may
                    // disappear; missing live/unknown files and other IO errors
                    // still fail closed. Present retired bytes remain charged.
                    if deleted_retired {
                        continue;
                    }
                    return Err(error.into());
                }
            };
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
