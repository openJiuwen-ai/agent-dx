//! Golden bytes of the Peer cache entry header (`AFSPC002` + JSON header,
//! zero-padded to `MAX_HEADER_BYTES`).
//!
//! A changed byte is a format change: bump the header version and regenerate
//! with `cargo test -p afs --lib peer_cache::format_golden_tests::print_peer_cache_goldens -- --ignored --nocapture`.

use super::*;
use crate::dfs::{ChunkId, ContentDigest};

fn header() -> Header {
    Header {
        version: 2,
        namespace_id: NamespaceId::new("ns"),
        chunk: ChunkObject {
            id: ChunkId::new(format!("blake3-{}-5", "33".repeat(32))),
            length: 5,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: [0x33; 32],
            },
            encoding: ChunkEncoding::Raw,
        },
        outboard_length: 64,
    }
}

fn header_block() -> Vec<u8> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("entry");
    let mut file = File::create(&path).unwrap();
    write_header(&mut file, &header()).unwrap();
    drop(file);
    fs::read(path).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The significant prefix: magic, length and JSON header. The rest of the
/// block is zero padding, checked separately.
fn significant(block: &[u8]) -> &[u8] {
    let length = u32::from_le_bytes(block[8..12].try_into().unwrap()) as usize;
    &block[..12 + length]
}

#[test]
fn peer_cache_header_matches_golden() {
    let block = header_block();
    assert_eq!(block.len(), MAX_HEADER_BYTES);
    let prefix = significant(&block);
    assert!(block[prefix.len()..].iter().all(|byte| *byte == 0));
    assert_eq!(
        hex(prefix),
        GOLDEN_HEADER,
        "Peer cache header bytes changed: bump the header version and \
         regenerate with print_peer_cache_goldens"
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("golden");
    fs::write(&path, &block).unwrap();
    let mut file = File::open(&path).unwrap();
    let (decoded, body_start) = read_header(&mut file).unwrap();
    assert_eq!(body_start, MAX_HEADER_BYTES as u64);
    assert_eq!(decoded.version, 2);
    assert_eq!(decoded.chunk, header().chunk);
    assert_eq!(decoded.outboard_length, 64);
}

#[test]
#[ignore = "prints regenerated goldens"]
fn print_peer_cache_goldens() {
    println!(
        "const GOLDEN_HEADER: &str = {:?};",
        hex(significant(&header_block()))
    );
}

const GOLDEN_HEADER: &str = "41465350433030323f0100007b2276657273696f6e223a322c226e616d6573706163655f6964223a226e73222c226368756e6b223a7b226964223a22626c616b65332d333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333332d35222c226c656e677468223a352c22636f6e74656e745f646967657374223a7b22616c676f726974686d223a22426c616b6533222c226279746573223a5b35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35312c35315d7d2c22656e636f64696e67223a22526177227d2c226f7574626f6172645f6c656e677468223a36347d";
