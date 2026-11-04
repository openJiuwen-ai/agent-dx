//! Checkpoint images: header, reading, budget, anchor verification, cleanup and retirement.

use super::*;

pub(super) const IMAGE_HEADER: usize = 146;

pub(super) fn image_name(image: CheckpointRef) -> String {
    let hex: String = image
        .digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("checkpoint-{:020}-{hex}.bin", image.covered.sequence)
}

pub(super) fn image_header(
    identity: StoreIdentity,
    covered: LogTip,
    revision: StoreRevision,
    bytes: &[u8],
) -> ([u8; IMAGE_HEADER], CheckpointRef) {
    let mut header = [0u8; IMAGE_HEADER];
    header[..6].copy_from_slice(&format::META_CHECKPOINT_HEADER);
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

pub(super) fn read_image(
    dir: &Path,
    identity: StoreIdentity,
    reference: CheckpointRef,
    limits: LocalCheckpointLimits,
) -> Result<RecoveredCheckpoint> {
    let name = image_name(reference);
    let read = || -> Result<RecoveredCheckpoint> {
        let mut file = open_file(&dir.join(&name), false)?;
        // Version first: the file tag is read before its length, identity,
        // hash and CRC, and before the payload's own schema.
        if file.metadata()?.len() >= 6 {
            let mut tag = [0; 6];
            file.read_exact(&mut tag)?;
            format::check_tagged_header(
                FormatKind::MetaCheckpointFile,
                format::META_CHECKPOINT_MAGIC,
                "file",
                &tag,
            )?;
            file.seek(SeekFrom::Start(0))?;
        }
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
        if actual[..6] != format::META_CHECKPOINT_HEADER
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
        if super::super::log::StateImage::validate_encoded(&bytes, limits.image_max_bytes)?
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
        log_context(
            error,
            format!("{name} sequence {}", reference.covered.sequence),
        )
    })
}

fn checkpoint_owned(name: &str) -> bool {
    name.starts_with("checkpoint-")
        || name == "checkpoint.tmp"
        || name.starts_with("orphan-checkpoint")
        || name.starts_with("orphan-CURRENT")
        || name == CURRENT
        || name == "CURRENT.tmp"
}

pub(super) fn checkpoint_budget(
    dir: &Path,
    extra_bytes: u64,
    extra_files: usize,
    limits: LocalCheckpointLimits,
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

pub(super) fn reserve_file(file: &File, length: u64) -> Result<()> {
    let length = i64::try_from(length).map_err(|_| full("checkpoint file reservation overflow"))?;
    // SAFETY: the live regular-file FD remains owned by the caller; offsets
    // are checked and nonnegative. The kernel retains no Rust pointers.
    let code = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) };
    if code != 0 {
        return Err(std::io::Error::from_raw_os_error(code).into());
    }
    Ok(())
}

pub(super) fn quarantine_checkpoint_tmp(
    dir: &Path,
    name: &str,
    limits: LocalCheckpointLimits,
) -> Result<()> {
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

pub(super) fn verify_anchor(
    dir: &Path,
    identity: StoreIdentity,
    limits: LocalLogOptions,
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
            let (record, next, _) = decode_frame(identity, tip, &bytes, limits.record_max_bytes)?;
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

/// Only objects explicitly retired by a durable CURRENT are cleanup candidates.
/// Corrupt/foreign data is retained and continues consuming its finite budget.
/// A crash after image rename but before CURRENT selection leaves a complete
/// owned image outside both selected chains. Validate its complete envelope before deletion; foreign,
/// corrupt and unrecognized files remain counted against disk limits.
pub(super) fn cleanup_unreferenced_images(
    dir: &Path,
    manifest: &Current,
    limits: LocalCheckpointLimits,
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
                    .is_none_or(|n| n > limits.validation_charge_max_bytes)
            {
                return Err(invalid_log(
                    "unreferenced image cannot be validated within the existing capture budget",
                ));
            }
            // Cleanup needs proven ownership/integrity, never adoption. Stream
            // the complete envelope without allocating/decoding another image.
            if bytes[..6] != format::META_CHECKPOINT_HEADER
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

pub(super) fn cleanup_retired(
    dir: &Path,
    manifest: &Current,
    limits: LocalLogOptions,
    checkpoint_limits: LocalCheckpointLimits,
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
            super::super::crash_tests::pause(
                "checkpoint_during_delete",
                "one retired segment removed",
            );
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
