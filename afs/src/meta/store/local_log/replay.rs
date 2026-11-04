//! Recovery replay: checkpoint selection, segment scanning and active segment completion.

use super::*;

pub(super) struct Replay {
    pub(super) recovered: RecoveredLog,
    pub(super) file: File,
    pub(super) end: u64,
    pub(super) generation: u64,
    pub(super) revision: StoreRevision,
    pub(super) manifest: Current,
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

/// First readable image of the CURRENT chain (current, then previous).
fn select_checkpoint(
    dir: &Path,
    identity: StoreIdentity,
    references: &[CheckpointRef],
    checkpoint_limits: LocalCheckpointLimits,
) -> Result<Option<RecoveredCheckpoint>> {
    let mut errors = Vec::new();
    for reference in references {
        match read_image(dir, identity, *reference, checkpoint_limits) {
            Ok(image) => return Ok(Some(image)),
            // A listed image of another version means the directory and the
            // binary do not match: refuse instead of falling back.
            Err(error) if error.code() == afs_error::STORAGE_FORMAT_INCOMPATIBLE => {
                return Err(error);
            }
            Err(error) => errors.push(error.to_string()),
        }
    }
    if !references.is_empty() {
        return Err(invalid_log(format!(
            "CURRENT has no valid checkpoint: {}",
            errors.join("; ")
        )));
    }
    Ok(None)
}

/// Replay position carried across segments.
struct ReplayCursor<'a> {
    tip: LogTip,
    /// Term of the last replayed frame; frame terms never decrease.
    pub(super) term: u64,
    /// The CURRENT term; no frame may carry a larger one.
    current_term: u64,
    pub(super) revision: StoreRevision,
    references: &'a [CheckpointRef],
    /// Whether each reference's covered position was found on the chain.
    anchors: Vec<bool>,
    pub(super) covered: u64,
    records: Vec<Record>,
}

/// One segment after its complete frames were replayed.
struct ScannedSegment {
    name: String,
    pub(super) file: File,
    declared: Tail,
    offset: u64,
    length: u64,
}

/// Replay every complete frame of one listed segment into `cursor`. Stops at
/// an incomplete trailing frame; rejects any gap, corruption or a declared
/// tail that the frames do not reach exactly.
fn replay_segment(
    dir: &Path,
    identity: StoreIdentity,
    limits: LocalLogOptions,
    segment: &SegmentRef,
    cursor: &mut ReplayCursor<'_>,
) -> Result<ScannedSegment> {
    if segment.base != cursor.tip || segment.base_revision != cursor.revision {
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
    let mut matched = declared.end == offset && declared.tip == cursor.tip;
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
        frame
            .try_reserve_exact(size)
            .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "replay frame allocation"))?;
        frame.resize(size, 0);
        frame[..8].copy_from_slice(&prefix);
        file.read_exact(&mut frame[8..])?;
        let tip = cursor.tip;
        let revision = cursor.revision;
        let (record, next, term) = decode_frame(identity, tip, &frame, limits.record_max_bytes)
            .and_then(|(record, next, found)| {
                Ok((
                    record,
                    next,
                    chain_term(cursor.term, found, cursor.current_term)?,
                ))
            })
            .map_err(|error| {
                log_context(
                    error,
                    format!("{name} offset {offset} after sequence {}", tip.sequence),
                )
            })?;
        if record.revision_before() != revision {
            return Err(invalid_log(format!(
                "{name} offset {offset} after sequence {}: business revision gap; expected {revision}, found before {} after {}",
                tip.sequence,
                record.revision_before(),
                record.revision_after()
            )));
        }
        cursor.tip = next;
        cursor.term = term;
        cursor.revision = record.revision_after();
        offset += size as u64;
        for (reference, matched) in cursor.references.iter().zip(&mut cursor.anchors) {
            if reference.covered.sequence == cursor.tip.sequence {
                *matched = reference.covered == cursor.tip && reference.revision == cursor.revision;
            }
        }
        if cursor.tip.sequence > cursor.covered {
            cursor.records.push(record);
        }
        if offset == declared.end {
            if cursor.tip != declared.tip {
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
    Ok(ScannedSegment {
        name,
        file,
        declared,
        offset,
        length,
    })
}

/// Validate the checkpoint chain against the replayed active segment, then
/// preserve and cut any incomplete tail and confirm the recovered tail slot.
/// Returns the active file, its end and its tail generation.
fn finish_active_segment(
    dir: &Path,
    scanned: ScannedSegment,
    cursor: &ReplayCursor<'_>,
    prepared_segment: Option<LogTip>,
) -> Result<(File, u64, u64)> {
    let ScannedSegment {
        mut file,
        declared,
        offset,
        length,
        ..
    } = scanned;
    let tip = cursor.tip;
    if cursor.anchors.iter().any(|matched| !matched) {
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
    Ok((file, offset, generation))
}

pub(super) fn replay(
    dir: &Path,
    identity: StoreIdentity,
    limits: LocalLogOptions,
    checkpoint_limits: LocalCheckpointLimits,
) -> Result<Replay> {
    let manifest = read_current(dir, identity)?.ok_or_else(|| {
        invalid_log("CURRENT missing from initialized Meta log; originals retained")
    })?;
    manifest.validate()?;
    wal_budget(dir, limits, 0, 0, &manifest.retired_segments)?;
    let prepared_segment = unselected_prepared_segment(dir, &manifest)?;
    let references: Vec<_> = manifest
        .current
        .into_iter()
        .chain(manifest.previous)
        .collect();
    let checkpoint = select_checkpoint(dir, identity, &references, checkpoint_limits)?;
    let tip = manifest.segments[0].base;
    let revision = manifest.segments[0].base_revision;
    let mut cursor = ReplayCursor {
        tip,
        term: 0,
        current_term: manifest.term,
        revision,
        references: &references,
        anchors: references
            .iter()
            .map(|reference| reference.covered == tip && reference.revision == revision)
            .collect(),
        covered: checkpoint
            .as_ref()
            .map_or(0, |image| image.covered.sequence),
        records: Vec::new(),
    };
    let mut active = None;
    for segment in &manifest.segments {
        let scanned = replay_segment(dir, identity, limits, segment, &mut cursor)?;
        if let Some((sealed, business)) = segment.sealed {
            if cursor.tip != sealed.tip
                || scanned.offset != sealed.end
                || cursor.revision != business
            {
                return Err(invalid_log(format!(
                    "{} seal/revision mismatch",
                    scanned.name
                )));
            }
        } else {
            active = Some(finish_active_segment(
                dir,
                scanned,
                &cursor,
                prepared_segment,
            )?);
        }
    }
    let (file, end, generation) =
        active.ok_or_else(|| invalid_log("missing active WAL segment"))?;
    // Before any later GC, confirm the root selector itself, not just data pages.
    if dir.join(CURRENT).try_exists()? {
        open_file(&dir.join(CURRENT), false)?.sync_all()?;
    }
    sync_dir(dir)?;
    let ReplayCursor {
        tip,
        revision,
        records,
        ..
    } = cursor;
    Ok(Replay {
        recovered: RecoveredLog {
            checkpoint,
            tip,
            term: manifest.term,
            records,
        },
        file,
        end,
        generation,
        revision,
        manifest,
    })
}
