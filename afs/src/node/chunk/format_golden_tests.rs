//! Golden bytes of Node-local chunk store formats: one `catalog.wal` line, a
//! `catalog.snapshot`, the CRC32C segment descriptor and a Bao sidecar.
//!
//! Inputs are fixed, so the bytes are deterministic. A changed byte is a
//! durable format change: bump the owning format version (`catalog.snapshot`
//! schema, CRC table `format`, sidecar header version) and regenerate with
//! `cargo test -p afs --lib chunk::format_golden_tests::print_chunk_goldens -- --ignored --nocapture`.

use super::outboard::{DEFAULT_OUTBOARD_BUDGET_BYTES, build_borrowed};
use super::*;
use crate::dfs::{ChunkEncoding, ContentDigest, DigestAlgorithm};
use crate::node::chunk_crc32c::CrcTable;

fn record(id: &str, revision: u64, segment_crc: Option<CrcTable>) -> LocalChunkRecord {
    LocalChunkRecord {
        chunk: ChunkObject {
            id: ChunkId::new(id),
            length: 5,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: [0x11; 32],
            },
            encoding: ChunkEncoding::Raw,
        },
        stored_length: 5,
        stored_checksum: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes: [0x22; 32],
        },
        device_id: "dev".into(),
        device_epoch: 2,
        location: PhysicalLocation::PerChunkFile {
            relative_path: format!("chunks/{id}"),
        },
        encoding: PhysicalEncoding::Raw,
        state: LocalChunkState::Durable,
        catalog_revision: revision,
        segment_crc,
    }
}

fn crc_table() -> CrcTable {
    CrcTable::build(&[0x5a; 40 * 1024]).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn snapshot_bytes() -> Vec<u8> {
    let temp = tempfile::tempdir().unwrap();
    let mut catalog = LocalCatalog {
        revision: 3,
        gc_generation: 1,
        ..LocalCatalog::default()
    };
    for record in [record("b", 3, None), record("a", 2, Some(crc_table()))] {
        catalog.records.insert(record.chunk.id.clone(), record);
    }
    checkpoint::save_checkpoint(temp.path(), &catalog).unwrap();
    std::fs::read(temp.path().join("catalog.snapshot")).unwrap()
}

fn cases() -> Vec<(&'static str, String)> {
    let txn = CatalogTxn::new(
        4,
        vec![record("a", 4, None), record("b", 4, Some(crc_table()))],
    )
    .unwrap();
    let mut line = serde_json::to_vec(&txn).unwrap();
    line.push(b'\n');
    let sidecar = build_borrowed(
        &(0..100 * 1024).map(|index| index as u8).collect::<Vec<_>>(),
        &OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES),
    )
    .outboard
    .expect("a 100 KiB body has a Bao sidecar");
    vec![
        ("catalog_wal_line", String::from_utf8(line).unwrap()),
        (
            "catalog_snapshot",
            String::from_utf8(snapshot_bytes()).unwrap(),
        ),
        (
            "crc32c_descriptor_digest",
            hex(&crc_table().descriptor_digest().bytes),
        ),
        ("bao_sidecar", hex(sidecar.encoded())),
    ]
}

#[test]
fn node_chunk_formats_match_golden() {
    let actual = cases();
    let names = actual.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let golden = GOLDEN.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    assert_eq!(names, golden, "golden table must list every case");
    for ((name, value), (_, expected)) in actual.iter().zip(GOLDEN) {
        assert_eq!(
            value, expected,
            "{name} bytes changed: this is a durable format change; bump the \
             owning format version and regenerate with print_chunk_goldens"
        );
    }
}

#[test]
fn golden_snapshot_and_wal_line_still_load() {
    let temp = tempfile::tempdir().unwrap();
    let golden = |name: &str| {
        GOLDEN
            .iter()
            .find(|(case, _)| *case == name)
            .map(|(_, value)| *value)
            .unwrap()
    };
    std::fs::write(
        temp.path().join("catalog.snapshot"),
        golden("catalog_snapshot"),
    )
    .unwrap();
    std::fs::write(temp.path().join("catalog.wal"), golden("catalog_wal_line")).unwrap();
    let catalog = LocalCatalog::recover(&temp.path().join("catalog.wal")).unwrap();
    assert_eq!(catalog.revision, 4);
    assert_eq!(catalog.gc_generation, 1);
    assert_eq!(catalog.records.len(), 2);
}

#[test]
#[ignore = "prints regenerated goldens"]
fn print_chunk_goldens() {
    for (name, value) in cases() {
        println!("    ({name:?}, {value:?}),");
    }
}

const GOLDEN: &[(&str, &str)] = &[
    (
        "catalog_wal_line",
        "{\"revision\":4,\"records\":[{\"chunk\":{\"id\":\"a\",\"length\":5,\"content_digest\":{\"algorithm\":\"Blake3\",\"bytes\":[17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]},\"encoding\":\"Raw\"},\"stored_length\":5,\"stored_checksum\":{\"algorithm\":\"Blake3\",\"bytes\":[34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34]},\"device_id\":\"dev\",\"device_epoch\":2,\"location\":{\"PerChunkFile\":{\"relative_path\":\"chunks/a\"}},\"encoding\":\"Raw\",\"state\":\"Durable\",\"catalog_revision\":4},{\"chunk\":{\"id\":\"b\",\"length\":5,\"content_digest\":{\"algorithm\":\"Blake3\",\"bytes\":[17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]},\"encoding\":\"Raw\"},\"stored_length\":5,\"stored_checksum\":{\"algorithm\":\"Blake3\",\"bytes\":[34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34]},\"device_id\":\"dev\",\"device_epoch\":2,\"location\":{\"PerChunkFile\":{\"relative_path\":\"chunks/b\"}},\"encoding\":\"Raw\",\"state\":\"Durable\",\"catalog_revision\":4,\"segment_crc\":{\"format\":1,\"length\":40960,\"segment_bytes\":32768,\"checksums\":[2742009599,3937552986]}}],\"checksum\":\"c5871d7e78e13cd62d7dd624ed912747f679ff2be6eb279d7e0f1b345b3d966b\"}\n",
    ),
    (
        "catalog_snapshot",
        "{\"schema\":1,\"revision\":3,\"gc_generation\":1,\"records\":[{\"chunk\":{\"id\":\"a\",\"length\":5,\"content_digest\":{\"algorithm\":\"Blake3\",\"bytes\":[17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]},\"encoding\":\"Raw\"},\"stored_length\":5,\"stored_checksum\":{\"algorithm\":\"Blake3\",\"bytes\":[34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34]},\"device_id\":\"dev\",\"device_epoch\":2,\"location\":{\"PerChunkFile\":{\"relative_path\":\"chunks/a\"}},\"encoding\":\"Raw\",\"state\":\"Durable\",\"catalog_revision\":2,\"segment_crc\":{\"format\":1,\"length\":40960,\"segment_bytes\":32768,\"checksums\":[2742009599,3937552986]}},{\"chunk\":{\"id\":\"b\",\"length\":5,\"content_digest\":{\"algorithm\":\"Blake3\",\"bytes\":[17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]},\"encoding\":\"Raw\"},\"stored_length\":5,\"stored_checksum\":{\"algorithm\":\"Blake3\",\"bytes\":[34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34,34]},\"device_id\":\"dev\",\"device_epoch\":2,\"location\":{\"PerChunkFile\":{\"relative_path\":\"chunks/b\"}},\"encoding\":\"Raw\",\"state\":\"Durable\",\"catalog_revision\":3}],\"checksum\":\"899d90c498ca4d8f68734dd344b04cb013dfd9333f4141cfacc4239eb84610eb\"}",
    ),
    (
        "crc32c_descriptor_digest",
        "e2cd5021a732fafe9a4edd156ff81e5354b0a9e605d327f65e7f8dc1a9002945",
    ),
    (
        "bao_sidecar",
        "41465342414f3100010005000000000000900100000000009f05d49a48b2b787c8f7197ef8d5605978cae6eb8a445b46426b155e0accce44c000000000000000e0e5b812c75bbf82f2bd640467c09172c2bdba11a084239d3b735cca87ad091ba9b18a260dfd8e6369984b1608eebd48e30fc4bde938d8240a9ecb383a0ac7bf4c3126076fbd1fbb6b09885b36119a32cc5446b05ac37302a0bf171c4869ff43c688f2e3421be1d768e45098e06e660aa3447ad2f4993ac113cbb00ad3546b41e73b9c3adedc19ced8317e9189c68c11a7e89ae6c403292fa57b4dbe23a9366dd7530aa0bfb5c946b48cb96ea1824ce63f53cf669925599a7d7a0b5821bdd150",
    ),
];
