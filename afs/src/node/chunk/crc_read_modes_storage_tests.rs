use super::*;
use crate::config::DfsReadCrcMode;
#[test]
fn crc_read_modes_storage_checks_follow_requested_mode_not_configured_mode() {
    // `Both` is the non-configurable policy requested by non-grpc nodes
    // (rdma/auto gRPC fallback) or implied by a missing header.
    for configured in [DfsReadCrcMode::Off, DfsReadCrcMode::Receiver] {
        for (requested, offset, len, detects) in [
            (DfsReadCrcMode::Both, 0, 32768, true),
            (DfsReadCrcMode::Receiver, 0, 32768, false),
            (DfsReadCrcMode::Receiver, 1, 7, true),
            (DfsReadCrcMode::Off, 0, 32768, false),
            (DfsReadCrcMode::Off, 1, 7, false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(configured);
            let item = StagedChunk::new_crc32c(OperationId::new("modes"), vec![73; 65536]).unwrap();
            store.put(item.clone()).unwrap();
            let mut changed = item.bytes().to_vec();
            changed[2] ^= 1;
            std::fs::write(temp.path().join("chunks").join(&item.chunk.id.0), &changed).unwrap();
            let result = store.read_peer_range_until(
                &item.chunk.id,
                offset,
                len,
                Instant::now() + Duration::from_secs(1),
                requested,
            );
            if detects {
                assert!(
                    result.is_err(),
                    "configured={configured:?} requested={requested:?}"
                );
            } else {
                assert_eq!(
                    result.unwrap().as_slice(),
                    &changed[offset as usize..offset as usize + len]
                );
            }
        }
    }
}
