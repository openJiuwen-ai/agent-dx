//! Recovery replay: checkpoint selection, segment scanning and active segment completion.

use super::*;

pub(super) struct Replay {
    pub(super) recovered: RecoveredLog,
    pub(super) file: File,
    pub(super) end: u64,
    pub(super) generation: u64,
    pub(super) revision: StoreRevision,
    pub(super) manifest: Current,
    /// Set when recovery had to drop history to make the log startable.
    pub(super) rollback: Option<TailRollback>,
}

/// What one recovery dropped, in the terms an operator needs.
///
/// It exists as data rather than only as a log line so the decision can be
/// asserted: a rollback that silently discards acknowledged records is the one
/// failure mode this whole path must not have, and a log line that is only
/// ever formatted is never checked for being emitted at all.
#[derive(Clone, Debug)]
pub(crate) struct TailRollback {
    pub(crate) segment: String,
    /// `tail_slot` when a reachable slot bounded the replay, `frame_chain` when
    /// the chain alone located the end.
    pub(crate) anchor: &'static str,
    /// Where the writer claimed to have written, which is what the loss is
    /// measured from. It may be past the file end.
    pub(crate) declared_end: u64,
    pub(crate) declared_sequence: u64,
    pub(crate) actual_len: u64,
    pub(crate) rolled_back_to: u64,
    /// Bytes that were on disk but not declared durable.
    pub(crate) dropped_bytes: u64,
    /// Records the writer declared durable that recovery could not keep. An
    /// upper bound, not a count of acknowledged writes: the slot's page can
    /// reach the disk before the frames it declares, so some of these were
    /// never acknowledged. Reads as "declared, not preserved", matching
    /// `declared_sequence`.
    pub(crate) dropped_declared_records: u64,
    pub(crate) recovered_sequence: u64,
    /// Whether the dropped bytes were copied to an `incomplete-tail-*` file.
    /// A loss that the file boundary already expressed leaves nothing to keep.
    pub(crate) preserved: bool,
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
    /// The tail slot replay used as an upper bound, when one was reachable.
    /// `None` means the frame chain alone located the end.
    declared: Option<Tail>,
    /// The newest slot on disk, reachable or not: what the writer claimed had
    /// become durable, and therefore the position lost history is measured
    /// from when a rollback drops records it had acknowledged.
    newest: Option<Tail>,
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
        return Err(corrupt_log("WAL segment base/revision gap"));
    }
    let name = wal_name(segment.base)?;
    let mut file = open_file(&dir.join(&name), false).map_err(|error| {
        corrupt_log(format!(
            "{name} offset 0 at base sequence {} required segment: {error}",
            segment.base.sequence
        ))
    })?;
    let length = file.metadata()?.len();
    // A sealed segment is immutable, so any disagreement with the manifest is
    // damage. The active segment is the only one a loss can shorten.
    //
    // The tail slot is a progress note. Under `tolerate_tail_rollback` it is
    // used only when it describes a position the file still holds; otherwise
    // `None` sends replay to the frame chain, which carries its own
    // `previous_hash` and sequence and therefore locates the last complete
    // record even when every slot points past the file end.
    let declared = match segment.sealed {
        Some(_) => Some(header(&mut file, identity, segment.base)?),
        None if limits.tolerate_tail_rollback => {
            reachable_tail(&mut file, identity, segment.base, length)?
        }
        None => Some(header(&mut file, identity, segment.base)?),
    };
    // The writer's own claim, kept for the rollback report: it is the position
    // lost history is measured from, and it can be past the file end, which is
    // exactly the case `declared` refuses to select.
    let newest = newest_tail(&mut file, identity, segment.base)?;
    if let (Some((tail, _)), Some(declared)) = (segment.sealed, declared)
        && (declared != tail || length != tail.end)
    {
        return Err(corrupt_log(format!(
            "{name} sealed/declared durable content missing or changed"
        )));
    }
    if let Some(declared) = declared
        && declared.end > length
    {
        // Without the rollback the newest slot is authoritative, so a file
        // shorter than it can only be damage.
        return Err(corrupt_log(format!(
            "{name} sealed/declared durable content missing or changed"
        )));
    }
    // Replay reads the whole file: a slot only records what was acknowledged,
    // and a complete frame past it is still a record the writer produced and
    // may have applied. `declared` therefore bounds nothing; it is checked
    // against what the frames actually reach at the end of the scan.
    let mut offset = LOG_HEADER_BYTES;
    let mut matched = declared.is_some_and(|tail| tail.end == offset && tail.tip == cursor.tip);
    while offset < length {
        if length - offset < 8 {
            break;
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut prefix = [0; 8];
        file.read_exact(&mut prefix)?;
        let size = match frame_length(&prefix, limits.record_max_bytes) {
            Ok(size) => size,
            Err(error) => {
                // A prefix that is not a legal frame length is not evidence of
                // where the data ends. Stopping would be a guess, so it stays
                // an error; a tail that was merely written short is caught by
                // the size check below instead, which does prove incompleteness.
                return Err(corrupt_log(format!("{name} offset {offset}: {error}")));
            }
        };
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
                // The frame is complete (an incomplete one broke out above), so
                // its bytes were written in full and may have been acknowledged.
                // A power loss cannot produce that shape; cutting here would
                // discard history, so it stays an error.
                log_context(
                    error,
                    format!("{name} offset {offset} after sequence {}", tip.sequence),
                )
            })?;
        if record.revision_before() != revision {
            return Err(corrupt_log(format!(
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
        if let Some(declared) = declared
            && offset == declared.end
        {
            if cursor.tip != declared.tip {
                return Err(corrupt_log(format!(
                    "{name} declared tail hash/sequence mismatch"
                )));
            }
            matched = true;
        }
        if let Some(declared) = declared
            && offset > declared.end
            && !matched
        {
            return Err(corrupt_log(format!("{name} declared tail inside record")));
        }
    }
    // With a reachable declaration the scan had to reach it exactly: a shorter
    // stop means the frames contradict what the writer claimed durable.
    // Without one the chain itself decided, and `offset` is the answer.
    if let Some(declared) = declared
        && (!matched || offset < declared.end)
    {
        return Err(corrupt_log(format!(
            "{name} declared durable content incomplete"
        )));
    }
    Ok(ScannedSegment {
        name,
        file,
        declared,
        newest,
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
) -> Result<(File, u64, u64, Option<TailRollback>)> {
    let ScannedSegment {
        name,
        mut file,
        declared,
        newest,
        offset,
        length,
    } = scanned;
    let tip = cursor.tip;
    if cursor.anchors.iter().any(|matched| !matched) {
        return Err(corrupt_log(
            "CURRENT checkpoint chain anchor missing or mismatched; no fallback can discard acknowledged history",
        ));
    }
    // A selected checkpoint is installed state, not a hint: its covered
    // position must still be reached by the replayed frames. Truncating below
    // it would keep an image describing records that no longer exist.
    if cursor.covered > tip.sequence {
        return Err(corrupt_log(
            "recovered tail precedes the selected checkpoint; truncation would discard installed state",
        ));
    }
    if prepared_segment.is_some_and(|base| base != tip) {
        return Err(corrupt_log(
            "unlisted empty WAL is not the recovered next segment",
        ));
    }
    // The writer's own claim, which is what a rollback drops history relative
    // to. `reachable_tail` picked where replay could stop; this is where the
    // writer said it had written. They differ whenever the newest slot points
    // past the file end, and the difference is acknowledged history.
    let superseded = match (declared, newest) {
        (Some(chosen), Some(latest)) => latest.generation > chosen.generation,
        (None, Some(_)) => true,
        (None, None) => true,
        (Some(_), None) => false,
    };
    let changed = offset != length || declared.is_some_and(|tail| tip != tail.tip) || superseded;
    // A rollback drops acknowledged history whenever the recovered tip is
    // behind the newest slot's, and that is true whether or not bytes remain
    // to cut: when the file happens to stop on a frame boundary the chain
    // reaches its end and `offset == length`, so the cut is invisible while
    // the loss is not. Both paths report, and both report against the newest
    // slot rather than the one replay selected, so the numbers describe the
    // records that were actually lost.
    let lost_records = newest.map_or(0, |tail| tail.tip.sequence.saturating_sub(tip.sequence));
    let cut = offset != length;
    let rollback = if cut || lost_records > 0 {
        if cut {
            // Every byte after `offset` was refused by the chain, so none of it
            // can carry an acknowledged record. Keep it as evidence, then cut.
            preserve_tail(dir, &mut file, offset, length)?;
            file.set_len(offset)?;
        }
        Some(TailRollback {
            segment: name.clone(),
            anchor: if declared.is_some() {
                "tail_slot"
            } else {
                "frame_chain"
            },
            declared_end: newest.map_or(0, |tail| tail.end),
            declared_sequence: newest.map_or(0, |tail| tail.tip.sequence),
            actual_len: length,
            rolled_back_to: offset,
            dropped_bytes: length - offset,
            dropped_declared_records: lost_records,
            recovered_sequence: tip.sequence,
            preserved: cut,
        })
    } else {
        None
    };
    let generation = if changed {
        let base = match declared {
            Some(tail) => tail.generation,
            // No slot described a reachable position, so the chain decided.
            // Start past whatever the slots still hold so the rewritten
            // declaration wins the "newest generation" comparison.
            None => newest.map_or(0, |tail| tail.generation),
        };
        let generation = base
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
        declared.map_or(0, |tail| tail.generation)
    };
    // Confirm complete but previously unacknowledged same-kernel pages. Only
    // this recovery's own writes need it: a clean segment was already made
    // durable by the writer that left it behind. `changed` already covers the
    // cut (`offset != length` is one of its terms).
    if changed {
        file.sync_all()?;
    }
    Ok((file, offset, generation, rollback))
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
    let (file, end, generation, rollback) =
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
        rollback,
    })
}
