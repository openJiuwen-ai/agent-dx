//! Staged records of the shared-service log (D-S4c-1): frames too large for
//! one atomic request are written in segments first and committed through a
//! fixed-size stub (see the `kv_log` module notes).

use afs_error::{Error, Result};

use super::super::journal::invalid_log;
#[cfg(test)]
use super::super::journal::{StoreIdentity, unavailable};
use super::{KvLog, KvService, timed};

pub(in crate::meta::store) fn part_key(
    prefix: &str,
    term: u64,
    sequence: u64,
    index: u32,
) -> String {
    format!("{prefix}/part/{term:020}/{sequence:020}/{index:06}")
}

/// `P/part/<term>/<seq>/...` → `(term, seq)`.
pub(in crate::meta::store) fn part_owner_of(prefix: &str, key: &str) -> Option<(u64, u64)> {
    let mut rest = key.strip_prefix(prefix)?.strip_prefix("/part/")?.split('/');
    Some((rest.next()?.parse().ok()?, rest.next()?.parse().ok()?))
}

/// The value stored in `P/wal/<seq>` for a staged record. A frame starts with
/// its big-endian length, so a 68-byte value starting with this magic would
/// claim a length above 1 GiB and can never be a frame.
pub(super) const STUB_MAGIC: &[u8; 8] = b"AFSKPART";
pub(super) const STUB_BYTES: usize = 8 + 8 + 8 + 4 + 8 + 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Stub {
    pub(super) term: u64,
    pub(super) sequence: u64,
    pub(super) parts: u32,
    pub(super) length: u64,
    pub(super) hash: [u8; 32],
}

pub(super) fn staged_hash(frame: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"afs.meta.kv.part.v1");
    hash.update(frame);
    *hash.finalize().as_bytes()
}

impl Stub {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(STUB_BYTES);
        bytes.extend_from_slice(STUB_MAGIC);
        bytes.extend_from_slice(&self.term.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.parts.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());
        bytes.extend_from_slice(&self.hash);
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != STUB_BYTES || &bytes[..8] != STUB_MAGIC {
            return None;
        }
        let number = |range: std::ops::Range<usize>| -> Option<u64> {
            Some(u64::from_be_bytes(bytes[range].try_into().ok()?))
        };
        let mut hash = [0; 32];
        hash.copy_from_slice(&bytes[36..68]);
        Some(Self {
            term: number(8..16)?,
            sequence: number(16..24)?,
            parts: u32::from_be_bytes(bytes[24..28].try_into().ok()?),
            length: number(28..36)?,
            hash,
        })
    }
}

/// Test-only crash points inside a staged append, by store identity.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::meta::store) enum CrashPoint {
    /// After the first segment of the first staged record.
    PartsPartial,
    /// After every segment, before the atomic append.
    PartsWritten,
    /// After the atomic append succeeded, before the handle records it.
    AfterCommit,
}
#[cfg(test)]
static CRASH: std::sync::Mutex<Vec<(StoreIdentity, CrashPoint)>> =
    std::sync::Mutex::new(Vec::new());
#[cfg(test)]
pub(in crate::meta::store) fn arm_crash(identity: StoreIdentity, point: CrashPoint) {
    if let Ok(mut armed) = CRASH.lock() {
        armed.push((identity, point));
    }
}
#[cfg(test)]
pub(super) fn crash_at(identity: StoreIdentity, point: CrashPoint) -> bool {
    CRASH.lock().is_ok_and(|mut armed| {
        armed
            .iter()
            .position(|entry| *entry == (identity, point))
            .map(|index| armed.swap_remove(index))
            .is_some()
    })
}

impl<S: KvService> KvLog<S> {
    /// The values of one atomic append. A frame larger than one segment, or
    /// one that no longer fits the service's request bound beside the frames
    /// before it, is written in segments under this writer's term first and
    /// replaced by its stub. Returns the values and the staged sequences.
    pub(super) async fn stage(
        &self,
        first: u64,
        frames: Vec<Vec<u8>>,
    ) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
        let segment = self.service.chunk_bytes().max(1);
        let bound = self
            .service
            .append_bound_bytes()
            .saturating_sub(frames.len() * STUB_BYTES);
        let mut inline = 0usize;
        let mut values = Vec::with_capacity(frames.len());
        let mut staged = Vec::new();
        for (offset, frame) in frames.into_iter().enumerate() {
            if frame.len() <= segment && inline + frame.len() <= bound {
                inline += frame.len();
                values.push(frame);
                continue;
            }
            if staged.is_empty() {
                // A superseded writer stages nothing.
                self.check_term().await?;
            }
            let sequence = first + offset as u64;
            let parts = u32::try_from(frame.len().div_ceil(segment))
                .map_err(|_| invalid_log("staged record segment count overflow"))?;
            for (index, part) in frame.chunks(segment).enumerate() {
                let index =
                    u32::try_from(index).map_err(|_| invalid_log("segment index overflow"))?;
                if let Err(error) = timed(
                    "stage",
                    self.service.put_part(self.term, sequence, index, part),
                )
                .await
                {
                    return Err(self.poison(error));
                }
                #[cfg(test)]
                if index == 0
                    && staged.is_empty()
                    && crash_at(self.identity, CrashPoint::PartsPartial)
                {
                    return Err(self.poison(unavailable("injected crash: one segment written")));
                }
            }
            values.push(
                Stub {
                    term: self.term,
                    sequence,
                    parts,
                    length: frame.len() as u64,
                    hash: staged_hash(&frame),
                }
                .encode(),
            );
            staged.push(sequence);
        }
        Ok((values, staged))
    }
}

/// Committed staged records after the checkpoint: sequence → writer term.
pub(super) type Staged = std::collections::BTreeMap<u64, u64>;

/// The frame stored at `sequence`: the value itself, or the record its
/// staged stub names, reassembled and checked. Returns the stub's term.
pub(super) async fn resolve_frame<S: KvService>(
    service: &S,
    sequence: u64,
    value: Vec<u8>,
    record_max: usize,
) -> Result<(Vec<u8>, Option<u64>)> {
    let Some(stub) = Stub::decode(&value) else {
        return Ok((value, None));
    };
    let at = |detail: String| {
        invalid_log(format!(
            "frame {sequence} (staged by term {}, {} segments): {detail}",
            stub.term, stub.parts
        ))
    };
    if stub.sequence != sequence {
        return Err(at(format!("stub names sequence {}", stub.sequence)));
    }
    let length = usize::try_from(stub.length)
        .ok()
        .filter(|length| *length <= record_max)
        .ok_or_else(|| at(format!("length {} outside the record bound", stub.length)))?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_OUT_OF_MEMORY, "staged record allocation"))?;
    for index in 0..stub.parts {
        let part = timed("recover", service.part(stub.term, sequence, index))
            .await?
            .ok_or_else(|| at(format!("segment {index} missing")))?;
        if frame.len() + part.len() > length {
            return Err(at(format!("segment {index} runs past the recorded length")));
        }
        frame.extend_from_slice(&part);
    }
    if frame.len() != length {
        return Err(at(format!(
            "segments hold {} of {length} bytes",
            frame.len()
        )));
    }
    if staged_hash(&frame) != stub.hash {
        return Err(at("segments do not match the recorded hash".into()));
    }
    Ok((frame, Some(stub.term)))
}

/// Delete segments no committed stub needs: those of covered records, and
/// unreferenced ones written by an older term (a crash before the append, a
/// fenced writer). Unreferenced segments of `term` itself may belong to an
/// append still in progress and are kept.
pub(super) async fn cleanup_parts<S: KvService>(
    service: &S,
    term: u64,
    staged: &Staged,
    covered: u64,
) -> Result<usize> {
    let mut removed = 0;
    for (owner, sequence) in timed("cleanup", service.part_owners()).await? {
        let live = sequence > covered && staged.get(&sequence) == Some(&owner);
        if !live && (owner < term || sequence <= covered) {
            timed("cleanup", service.delete_parts(owner, sequence)).await?;
            removed += 1;
        }
    }
    Ok(removed)
}
