//! One "version not supported" case per local Meta log format (R17).
//!
//! Each case bumps only the version field of a valid store and expects
//! `STORAGE_FORMAT_INCOMPATIBLE` with the found and the supported version,
//! before any CRC/hash/identity check runs (the CRC is left stale on
//! purpose where the version sits inside the CRC-covered bytes).

use super::super::journal::test_records;
use super::*;
use crate::format::version;

fn assert_incompatible(error: Error, parts: &[&str]) {
    assert_eq!(
        error.code(),
        afs_error::STORAGE_FORMAT_INCOMPATIBLE,
        "{error}"
    );
    let message = error.to_string();
    for part in parts {
        assert!(message.contains(part), "missing {part:?}: {message}");
    }
    assert!(message.contains("rebuild the data directory"), "{message}");
}

async fn store_with_one_record(dir: &Path) {
    let log = LocalLog::open(dir, LocalLogOptions::default()).unwrap();
    log.append_and_sync(LogTip::EMPTY.at(log.term()), test_records(1))
        .await
        .unwrap();
}

fn rewrite(path: &Path, edit: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut bytes = fs::read(path).unwrap();
    edit(&mut bytes);
    fs::write(path, &bytes).unwrap();
    bytes
}

fn refused(dir: &Path) -> Error {
    LocalLog::open(dir, LocalLogOptions::default())
        .err()
        .expect("a version this binary does not support must be refused")
}

#[tokio::test]
async fn wal_envelope_version_is_identified_before_the_frame_crc() {
    let dir = tempfile::tempdir().unwrap();
    store_with_one_record(dir.path()).await;
    let offset = LOG_HEADER_BYTES as usize + 8;
    let wal = rewrite(&dir.path().join(WAL), |bytes| {
        assert_eq!(u16::from(bytes[offset]), version::META_WAL_ENVELOPE);
        // The CRC still covers the old byte: a CRC-first reader would report
        // corruption.
        bytes[offset] = 3;
    });
    assert_incompatible(
        refused(dir.path()),
        &[
            "Meta WAL record envelope",
            WAL,
            "offset 4096",
            "version 3",
            "supports version 2",
        ],
    );
    assert_eq!(fs::read(dir.path().join(WAL)).unwrap(), wal, "retained");
}

#[tokio::test]
async fn wal_record_decision_schema_is_identified_before_the_full_decode() {
    let dir = tempfile::tempdir().unwrap();
    store_with_one_record(dir.path()).await;
    let identity = read_identity(dir.path()).unwrap();
    rewrite(&dir.path().join(WAL), |bytes| {
        let start = LOG_HEADER_BYTES as usize;
        let length = frame_length(&bytes[start..start + 8], 1 << 20).unwrap();
        let frame = &mut bytes[start..start + length];
        let body_end = frame.len() - 32;
        let term = read_current(dir.path(), identity).unwrap().unwrap().term;
        // Envelope fields before the record, then the `Decision` variant
        // index (0), then the decision schema.
        let head = postcard::to_allocvec(&(
            version::META_WAL_ENVELOPE,
            identity,
            1u64,
            0u64,
            term,
            [0u8; 32],
        ))
        .unwrap();
        let schema = 8 + head.len() + 1;
        assert_eq!(frame[schema - 1], 0, "Decision variant index");
        assert_eq!(u16::from(frame[schema]), version::META_RECORD_DECISION);
        frame[schema] = 2;
        // Keep the frame itself intact so only the inner version differs.
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"afs.meta.record.v2");
        hasher.update(&frame[8..body_end]);
        let hash = *hasher.finalize().as_bytes();
        frame[body_end..].copy_from_slice(&hash);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&frame[..4]), &frame[8..]);
        frame[4..8].copy_from_slice(&crc.to_be_bytes());
    });
    assert_incompatible(
        refused(dir.path()),
        &[
            "Meta WAL record decision",
            WAL,
            "version 2",
            "supports version 1",
        ],
    );
}

#[tokio::test]
async fn wal_segment_header_version_is_identified_before_the_header_hash() {
    let dir = tempfile::tempdir().unwrap();
    store_with_one_record(dir.path()).await;
    rewrite(&dir.path().join(WAL), |bytes| bytes[5] = 2);
    assert_incompatible(
        refused(dir.path()),
        &[
            "Meta WAL segment header",
            WAL,
            "version 2",
            "supports version 1",
        ],
    );
}

#[tokio::test]
async fn identity_version_is_identified_before_its_hash_and_crc() {
    let dir = tempfile::tempdir().unwrap();
    store_with_one_record(dir.path()).await;
    rewrite(&dir.path().join(IDENTITY), |bytes| bytes[5] = 4);
    assert_incompatible(
        refused(dir.path()),
        &["Meta IDENTITY", IDENTITY, "version 4", "supports version 1"],
    );
}

#[tokio::test]
async fn current_version_is_identified_before_its_hash_and_crc() {
    let dir = tempfile::tempdir().unwrap();
    store_with_one_record(dir.path()).await;
    let current = rewrite(&dir.path().join(CURRENT), |bytes| bytes[5] = 9);
    assert_incompatible(
        refused(dir.path()),
        &[
            "Meta CURRENT manifest",
            CURRENT,
            "version 9",
            "supports version 3",
        ],
    );
    assert_eq!(fs::read(dir.path().join(CURRENT)).unwrap(), current);
}

/// A listed checkpoint of another version is refused, never skipped in
/// favour of an older image.
#[tokio::test]
async fn checkpoint_file_version_is_refused_without_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::open(dir.path(), LocalLogOptions::default()).unwrap();
    let mut state = super::super::state::State::default();
    let mut tip = LogTip::EMPTY;
    for record in test_records(2) {
        state = state.replay_record(&record).unwrap();
        tip = log
            .append_and_sync(tip.at(log.term()), vec![record])
            .await
            .unwrap();
        log.install_checkpoint(
            tip,
            super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap(),
        )
        .await
        .unwrap();
    }
    let manifest = read_current(dir.path(), log.identity()).unwrap().unwrap();
    assert!(manifest.previous.is_some(), "an older image exists");
    drop(log);
    let name = image_name(manifest.current.unwrap());
    rewrite(&dir.path().join(&name), |bytes| bytes[5] = 7);
    assert_incompatible(
        refused(dir.path()),
        &[
            "Meta checkpoint file header",
            &name,
            "version 7",
            "supports version 1",
        ],
    );
}

/// The code survives Meta startup: `afs-meta` refuses the directory with
/// `STORAGE_FORMAT_INCOMPATIBLE`, not `CONFIG_INVALID`.
#[tokio::test]
async fn meta_startup_reports_the_format_code() {
    use crate::{
        config::{Cli, Config, MetaStoreBackend, Role},
        runtime::Observability,
    };
    use clap::Parser;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("meta-store");
    store_with_one_record(&root).await;
    rewrite(&root.join(IDENTITY), |bytes| bytes[5] = 2);
    let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
    cfg.meta_store = MetaStoreBackend::LocalFile;
    cfg.data_dir = dir.path().to_owned();
    cfg.dfs = true;
    let cert = dir.path().join("identity-table.der");
    fs::write(&cert, [1, 2, 3, 4]).unwrap();
    cfg.trusted_node_certs.insert("node".into(), cert.clone());
    cfg.tls_ca_certificate = Some(cert.clone());
    cfg.tls_identity_certificate = Some(cert.clone());
    cfg.tls_identity_private_key = Some(cert);
    cfg.tls_server_name = Some("afs-cluster".into());
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    cfg.grpc_listen = occupied.local_addr().unwrap();
    let error = crate::meta::run(cfg, Observability::new().unwrap())
        .await
        .unwrap_err();
    assert_incompatible(
        error.downcast_ref::<Error>().unwrap().clone(),
        &["Meta IDENTITY", "version 2", "supports version 1"],
    );
}
