//! Tail rollback: a damaged active WAL tail is cut back to the newest declared
//! position the file actually holds, and every other damage stays refused.
//!
//! Each case states what an operator would face and asserts the outcome plus
//! whether the directory was touched.

use super::*;
use crate::meta::store::journal::test_records;

fn options(rollback: bool) -> LocalLogOptions {
    LocalLogOptions {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 512 * 1024,
        // Large enough that seeding stays in one active segment: the cases here
        // are about tail damage, not rotation.
        segment_bytes: 256 * 1024,
        segments_max: 8,
        tolerate_tail_rollback: rollback,
    }
}

fn wal_path(dir: &Path) -> PathBuf {
    let identity = read_identity(dir).unwrap();
    let manifest = read_current(dir, identity).unwrap().expect("CURRENT");
    let active = manifest.segments.last().expect("active segment");
    dir.join(wal_name(active.base).unwrap())
}

fn file_len(dir: &Path) -> u64 {
    fs::metadata(wal_path(dir)).unwrap().len()
}

fn cut(dir: &Path, len: u64) {
    fs::OpenOptions::new()
        .write(true)
        .open(wal_path(dir))
        .unwrap()
        .set_len(len)
        .unwrap();
}

fn snapshot(dir: &Path) -> Vec<(String, u64)> {
    let mut entries = fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let len = entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            (entry.file_name().to_string_lossy().into_owned(), len)
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn slot_offset(generation: u64) -> usize {
    STATIC_BYTES + (generation % 2) as usize * SLOT_BYTES
}

/// Returns `(newest, older)` by generation, not by physical slot index: which
/// slot holds the newer generation alternates with every write, so a test that
/// relied on the index would silently depend on the fixture's parity.
fn read_slots(dir: &Path) -> (Tail, Tail) {
    let mut file = fs::File::open(wal_path(dir)).unwrap();
    let identity = read_identity(dir).unwrap();
    let (a, b) = read_tail_slots(&mut file, identity, LogTip::EMPTY).unwrap();
    let (a, b) = (a.expect("slot a"), b.expect("slot b"));
    if a.generation >= b.generation {
        (a, b)
    } else {
        (b, a)
    }
}

/// Rewrites one tail slot with a correct CRC so only its position changes.
fn write_slot(dir: &Path, generation: u64, sequence: u64, end: u64, hash: [u8; 32]) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path(dir))
        .unwrap();
    let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
    file.read_exact(&mut bytes).unwrap();
    let offset = slot_offset(generation);
    bytes[offset..offset + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
        generation,
        tip: LogTip {
            sequence,
            record_hash: hash,
        },
        end,
    }));
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
}

fn damage_byte(dir: &Path, position: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(wal_path(dir))
        .unwrap();
    file.seek(SeekFrom::Start(position)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(position)).unwrap();
    file.write_all(&[byte[0] ^ 0xFF]).unwrap();
    file.sync_all().unwrap();
}

fn append_bytes(dir: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(wal_path(dir))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

/// Builds a log with `records` durable records and drops the writer handle.
async fn seeded(records: usize) -> (tempfile::TempDir, LogTip) {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), options(false)).unwrap();
    let mut tip = LogTip::EMPTY;
    for record in test_records(records) {
        tip = log.append_and_sync(tip.at(1), vec![record]).await.unwrap();
    }
    drop(log);
    (dir, tip)
}

/// Opens the log expecting a refusal and returns the error code and text.
fn refusal(dir: &Path, rollback: bool) -> (afs_error::ErrorCode, String) {
    match LocalLog::open(dir, options(rollback)) {
        Ok(_) => panic!("expected the log to refuse"),
        Err(error) => (error.code(), error.to_string()),
    }
}

/// A shortened active tail (FE-04's `truncate -128`) rolls back to the previous
/// slot, which still describes a position the file holds.
#[tokio::test]
async fn shortened_active_tail_rolls_back_to_the_previous_slot() {
    let (dir, _tip) = seeded(6).await;
    let (newest, previous) = read_slots(dir.path());
    assert!(previous.end < newest.end, "fixture needs two distinct ends");
    let shortened = previous.end;
    cut(dir.path(), shortened);

    let log = LocalLog::open(dir.path(), options(true)).expect("rollback must open the log");
    let recovered = log.recover().await.unwrap();
    assert_eq!(
        recovered.tip.sequence, previous.tip.sequence,
        "recovered tip must be the newest reachable declaration"
    );
    assert_eq!(
        file_len(dir.path()),
        shortened,
        "the file keeps every stored byte and drops only the declared-only tail"
    );
}

/// With the rollback off the same damage still refuses, so the switch alone
/// decides whether the behaviour changes.
#[tokio::test]
async fn shortened_active_tail_still_refuses_without_rollback() {
    let (dir, _tip) = seeded(6).await;
    let (_newest, previous) = read_slots(dir.path());
    cut(dir.path(), previous.end);
    let before = snapshot(dir.path());

    let (code, text) = refusal(dir.path(), false);
    assert_eq!(code, afs_error::META_LOG_CORRUPT);
    assert!(
        text.contains("sealed/declared durable content missing or changed"),
        "{text}"
    );
    assert_eq!(
        snapshot(dir.path()),
        before,
        "a refusal must not touch bytes"
    );
}

/// A cut that lands exactly on a frame boundary leaves no bytes to drop, so
/// nothing is truncated and no evidence file appears — but the newest slot
/// still declares an acknowledged record that the file no longer holds. That
/// loss must be reported, with the numbers taken from the superseded slot and
/// not from the older one replay selected, or an external `truncate` would be
/// completely silent.
#[tokio::test]
async fn a_frame_aligned_cut_reports_the_records_it_drops() {
    let (dir, _tip) = seeded(6).await;
    let (newest, previous) = read_slots(dir.path());
    assert!(previous.end < newest.end, "fixture needs two distinct ends");
    // `previous.end` is a declared position, so it is a frame boundary: replay
    // reaches exactly the end of the shortened file.
    cut(dir.path(), previous.end);

    let log = LocalLog::open(dir.path(), options(true)).expect("rollback must open the log");
    let recovered = log.recover().await.unwrap();

    let rollback = log
        .rollback()
        .expect("a cut that drops an acknowledged record must be reported");
    assert_eq!(
        rollback.dropped_declared_records,
        newest.tip.sequence - previous.tip.sequence,
        "the reported loss must be measured from the superseded slot"
    );
    assert_eq!(
        rollback.declared_sequence, newest.tip.sequence,
        "the report must name what the writer claimed, not what replay selected"
    );
    assert_eq!(rollback.recovered_sequence, recovered.tip.sequence);
    assert_eq!(rollback.actual_len, previous.end);
    assert_eq!(
        rollback.dropped_bytes, 0,
        "a frame-aligned cut has no bytes to drop"
    );
    assert!(
        !rollback.preserved,
        "there is nothing on disk to preserve when the file already ends at the cut"
    );
    assert_eq!(file_len(dir.path()), previous.end, "the file is untouched");
}

/// A clean log must not report a rollback at all: the commit claims recovery
/// leaves intact data byte-for-byte alone, and a spurious report would make
/// every restart look like data loss.
#[tokio::test]
async fn clean_data_reports_no_rollback() {
    let (dir, tip) = seeded(6).await;
    let before = snapshot(dir.path());
    let (slot_a, slot_b) = read_slots(dir.path());
    let generation_before = slot_a.generation.max(slot_b.generation);

    let log = LocalLog::open(dir.path(), options(true)).expect("clean log must open");
    let recovered = log.recover().await.unwrap();

    assert!(
        log.rollback().is_none(),
        "intact data must not report a rollback"
    );
    assert_eq!(recovered.tip, tip, "recovery keeps every record");
    assert_eq!(snapshot(dir.path()), before, "no file may change");
    let (newest_after, _) = read_slots(dir.path());
    assert!(
        newest_after.generation == generation_before,
        "a clean open must not rewrite a tail slot"
    );
}

/// Both slots declare past the file end. The chain still locates the last
/// complete record, so recovery succeeds where a slot-only anchor would give up.
#[tokio::test]
async fn both_slots_beyond_the_file_end_recover_from_the_chain() {
    let (dir, _tip) = seeded(4).await;
    let (newest, previous) = read_slots(dir.path());
    let before = file_len(dir.path());
    // Cut below both declarations, leaving several whole frames behind.
    let cut_point = LOG_HEADER_BYTES + (before - LOG_HEADER_BYTES) / 2;
    cut(dir.path(), cut_point);
    let beyond = before + 4096;
    write_slot(
        dir.path(),
        newest.generation,
        newest.tip.sequence,
        beyond,
        newest.tip.record_hash,
    );
    write_slot(
        dir.path(),
        previous.generation,
        previous.tip.sequence,
        beyond,
        previous.tip.record_hash,
    );

    let log = LocalLog::open(dir.path(), options(true))
        .expect("the frame chain locates the end without a usable slot");
    let recovered = log.recover().await.unwrap();
    assert!(
        recovered.tip.sequence > 0,
        "at least one record must survive"
    );
    assert!(
        file_len(dir.path()) <= cut_point,
        "the file is cut at the chain end, never grown"
    );
}

/// Without the rollback the same damage is refused, so the switch still decides.
#[tokio::test]
async fn both_slots_beyond_the_file_end_refuse_without_rollback() {
    let (dir, _tip) = seeded(4).await;
    let (newest, previous) = read_slots(dir.path());
    let before = file_len(dir.path());
    let cut_point = LOG_HEADER_BYTES + (before - LOG_HEADER_BYTES) / 2;
    cut(dir.path(), cut_point);
    let beyond = before + 4096;
    write_slot(
        dir.path(),
        newest.generation,
        newest.tip.sequence,
        beyond,
        newest.tip.record_hash,
    );
    write_slot(
        dir.path(),
        previous.generation,
        previous.tip.sequence,
        beyond,
        previous.tip.record_hash,
    );
    let snapshot_before = snapshot(dir.path());

    let (code, text) = refusal(dir.path(), false);
    assert_eq!(code, afs_error::META_LOG_CORRUPT);
    assert!(text.contains("sealed/declared durable content"), "{text}");
    assert_eq!(snapshot(dir.path()), snapshot_before, "no bytes may change");
}

/// Corruption away from the tail cannot be repaired by cutting the file,
/// because frames after it may still be acknowledged.
#[tokio::test]
async fn a_flipped_byte_before_the_tail_is_refused() {
    let (dir, _tip) = seeded(8).await;
    let (newest, _previous) = read_slots(dir.path());
    let middle = LOG_HEADER_BYTES + (newest.end - LOG_HEADER_BYTES) / 2;
    damage_byte(dir.path(), middle);
    let before = snapshot(dir.path());

    let (code, _text) = refusal(dir.path(), true);
    assert_eq!(code, afs_error::META_LOG_CORRUPT);
    assert_eq!(
        snapshot(dir.path()),
        before,
        "a damaged middle must never be truncated away"
    );
}

/// Cutting the segment in half leaves whole frames behind the cut. The chain
/// still ends exactly at the last one, so recovery keeps them instead of
/// refusing: the suffix it drops was never fully written.
#[tokio::test]
async fn a_segment_cut_in_half_recovers_to_the_chain_end() {
    let (dir, _tip) = seeded(8).await;
    let half = LOG_HEADER_BYTES + (file_len(dir.path()) - LOG_HEADER_BYTES) / 2;
    cut(dir.path(), half);

    let log = LocalLog::open(dir.path(), options(true)).expect("the chain ends inside the file");
    let recovered = log.recover().await.unwrap();
    assert!(
        recovered.tip.sequence > 0 && recovered.tip.sequence < 8,
        "the surviving records are replayed"
    );
    assert!(
        file_len(dir.path()) <= half,
        "the file is cut at the chain end, never grown"
    );
}

/// The same cut without the rollback keeps the previous refusal.
#[tokio::test]
async fn a_segment_cut_in_half_refuses_without_rollback() {
    let (dir, _tip) = seeded(8).await;
    let half = LOG_HEADER_BYTES + (file_len(dir.path()) - LOG_HEADER_BYTES) / 2;
    cut(dir.path(), half);
    let before = snapshot(dir.path());

    let (code, _text) = refusal(dir.path(), false);
    assert_eq!(code, afs_error::META_LOG_CORRUPT);
    assert_eq!(snapshot(dir.path()), before);
}

/// A tail slot whose CRC fails is not evidence, so the log refuses rather than
/// guessing a position.
#[tokio::test]
async fn an_unreadable_tail_slot_is_refused() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let (dir, _tip) = seeded(4).await;
    let (newest, previous) = read_slots(dir.path());
    {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(wal_path(dir.path()))
            .unwrap();
        let mut bytes = [0u8; LOG_HEADER_BYTES as usize];
        file.read_exact(&mut bytes).unwrap();
        for generation in [newest.generation, previous.generation] {
            let offset = slot_offset(generation);
            bytes[offset + SLOT_BYTES - 1] ^= 0xFF;
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }
    let (code, text) = refusal(dir.path(), true);
    assert_eq!(
        code,
        afs_error::CONFIG_INVALID,
        "an ambiguous tail keeps the previous code"
    );
    assert!(text.contains("ambiguous tail"), "{text}");
}

/// A torn trailing frame is already handled by the pre-existing cut and must
/// keep working with the rollback on.
#[tokio::test]
async fn a_torn_trailing_frame_is_cut_with_the_rollback_on() {
    let (dir, _tip) = seeded(6).await;
    let declared = file_len(dir.path());
    let mut torn = Vec::new();
    torn.extend_from_slice(&200u32.to_be_bytes());
    torn.extend_from_slice(&[0u8; 4]);
    torn.extend_from_slice(&[0xBBu8; 40]);
    append_bytes(dir.path(), &torn);

    let log = LocalLog::open(dir.path(), options(true)).expect("torn tail is cut");
    log.recover().await.unwrap();
    assert_eq!(
        file_len(dir.path()),
        declared,
        "the undeclared suffix is cut back to the last declared byte"
    );
}

/// A prefix that is not a legal frame length says nothing about where the data
/// ends, so it is refused even with the rollback on. Cutting would be a guess.
#[tokio::test]
async fn an_invalid_length_prefix_is_refused_even_with_rollback() {
    let (dir, _tip) = seeded(6).await;
    let declared = file_len(dir.path());
    let mut junk = Vec::new();
    junk.extend_from_slice(&u32::MAX.to_be_bytes());
    junk.extend_from_slice(&[0xCCu8; 40]);
    append_bytes(dir.path(), &junk);

    for rollback in [false, true] {
        let (code, text) = refusal(dir.path(), rollback);
        assert_eq!(code, afs_error::META_LOG_CORRUPT);
        assert!(text.contains("frame length outside record bound"), "{text}");
        assert_eq!(
            file_len(dir.path()),
            declared + junk.len() as u64,
            "a refusal must leave the file exactly as it was"
        );
    }
}

/// Rolling back writes a strictly newer generation than the one it selected, so
/// reopening cannot report two equal generations with different content.
#[tokio::test]
async fn a_rolled_back_log_reopens_cleanly() {
    let (dir, _tip) = seeded(6).await;
    let (_newest, previous) = read_slots(dir.path());
    cut(dir.path(), previous.end);
    drop(LocalLog::open(dir.path(), options(true)).unwrap());
    let (reopened, _) = read_slots(dir.path());
    assert!(
        reopened.generation > previous.generation,
        "the rollback must advance past the generation it selected"
    );
    LocalLog::open(dir.path(), options(true)).expect("second open must succeed");
}
