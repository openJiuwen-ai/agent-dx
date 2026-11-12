use super::*;
#[test]
fn crc32c_meta_descriptor_roundtrip() {
    let chunk = crate::dfs::ChunkObject {
        id: crate::dfs::ChunkId::new("crc32c-v1-00112233445566778899aabbccddeeff"),
        length: 32769,
        content_digest: crate::dfs::ContentDigest {
            algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
            bytes: [7; 32],
        },
        encoding: crate::dfs::ChunkEncoding::Raw,
    };
    assert_eq!(
        domain_chunk_object(wire_chunk_object(chunk.clone())).unwrap(),
        chunk
    );
    assert_eq!(wire_digest_algorithm(chunk.content_digest.algorithm), 2);
    assert!(domain_digest_algorithm(99).is_err());
}
