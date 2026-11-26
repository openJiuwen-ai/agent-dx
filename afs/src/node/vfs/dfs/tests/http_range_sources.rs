use super::*;
use crate::node::read::engine::{ChunkTransfer, LocalReadAuthority, PeerReadBatch};

const PIECE: usize = 256 * 1024;

#[derive(Default)]
struct RangeSources {
    requests: Mutex<Vec<crate::dfs::DfsChunkSourcesRequest>>,
    reads: Mutex<Vec<(crate::dfs::ChunkId, u64, u64)>>,
}
impl ReadSourceProvider for RangeSources {
    fn sources_for(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> Result<DfsChunkSourcesReply> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(DfsChunkSourcesReply {
            revision: 1,
            chunks: request
                .chunk_ids
                .into_iter()
                .map(|id| ChunkSources {
                    chunk_id: id.clone(),
                    chunk: None,
                    allowed_ranges: vec![(0, PIECE as u64)],
                    sources: vec![SourceCandidate {
                        chunk_id: id,
                        copy_id: CopyId::new("http-range-copy"),
                        role: CopyRole::DurableReplica,
                        state: CopyState::Ready,
                        location: CopyLocation::Node {
                            node_id: "node-b".into(),
                            node_epoch: 1,
                            device_id: "local-0".into(),
                            device_epoch: 1,
                            catalog_revision: 1,
                        },
                        data_endpoint: Some("http://node-b".into()),
                        load_hint: 0,
                        read_grant: DfsReadGrant {
                            namespace_id: request.namespace_id.clone(),
                            file_version_id: request.file_version_id.clone(),
                            layout_root_id: request.layout_root_id.clone(),
                            caller_node_id: request.caller_id.clone(),
                            caller_node_epoch: 1,
                            expires_at_unix_ms: u64::MAX,
                            fence: 1,
                            token: "test-token".into(),
                        },
                    }],
                })
                .collect(),
        })
    }
}
impl ChunkTransfer for RangeSources {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        for item in &batch.operations {
            let op = &item.op;
            self.reads
                .lock()
                .unwrap()
                .push((op.chunk_id.clone(), op.chunk_offset, op.length));
            let index: u8 = op
                .chunk_id
                .0
                .strip_prefix("http-range-")
                .unwrap()
                .parse()
                .unwrap();
            out[op.output_offset..op.output_offset + op.length as usize].fill(index);
        }
        Ok(())
    }
}

fn sealed_range_fs(
    config: DfsReadConfig,
    chunks: usize,
) -> (
    tempfile::TempDir,
    Arc<RecordingMeta>,
    DistributedFs,
    Arc<RangeSources>,
) {
    let temp = tempfile::tempdir().unwrap();
    let meta = Arc::new(RecordingMeta::new());
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let sources = Arc::new(RangeSources::default());
    let engine = Arc::new(
        DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources.clone(),
            sources.clone(),
            config,
        )
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(60),
        ))),
    );
    meta.publish_external_version_with_receipts(
        (chunks * PIECE) as u64,
        (0..chunks)
            .map(|index| Extent {
                file_offset: (index * PIECE) as u64,
                length: PIECE as u64,
                chunk_id: crate::dfs::ChunkId::new(format!("http-range-{index}")),
                chunk_offset: 0,
            })
            .collect(),
        Vec::new(),
    );
    let source = crate::node::vfs::lazy_fill::LazyFileSpec {
        length: (chunks * PIECE) as u64,
        binding: crate::node::vfs::lazy_fill::LazyFillBinding {
            kind: "test-http-range".into(),
            payload: Vec::new(),
        },
    };
    meta.inode.lock().unwrap().xattrs.insert(
        crate::node::vfs::lazy_fill::DESCRIPTOR_XATTR.into(),
        serde_json::to_vec(&source).unwrap(),
    );
    let fs = test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta.clone(),
        local,
        engine,
    );
    (temp, meta, fs, sources)
}

#[test]
fn http_range_sources_sealed_four_mib_uses_one_lookup_and_sixteen_body_reads() {
    let (_temp, meta, fs, sources) = sealed_range_fs(DfsReadConfig::default(), 18);
    let inode = fs
        .backend_inode(&meta.inode.lock().unwrap().inode_id)
        .unwrap();
    let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let start = PIECE as u64;
    let end = 17 * PIECE as u64;
    fs.set_read_source_range(
        &context(),
        handle,
        start..end,
        Instant::now() + Duration::from_secs(10),
    )
    .unwrap();
    assert!(
        sources.requests.lock().unwrap().is_empty(),
        "installing a hint must not query Meta or read bytes"
    );
    assert!(sources.reads.lock().unwrap().is_empty());
    for index in 1..17 {
        let piece = fs
            .read_reply(&context(), handle, (index * PIECE) as u64, PIECE)
            .unwrap();
        assert_eq!(piece.len(), PIECE);
        assert!(piece.iter().all(|byte| *byte == index as u8));
    }
    fs.release(&context(), handle).unwrap();
    let requests = sources.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "one metadata request covers this 4MiB HTTP Range instead of 16 fragment requests"
    );
    let expected: BTreeSet<_> = (1..17)
        .map(|index| crate::dfs::ChunkId::new(format!("http-range-{index}")))
        .collect();
    assert_eq!(
        requests[0]
            .chunk_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected,
        "metadata preparation must not include chunks outside the HTTP Range"
    );
    let reads = sources.reads.lock().unwrap();
    assert_eq!(reads.len(), 16);
    assert_eq!(
        reads.iter().map(|(_, _, length)| length).sum::<u64>(),
        end - start,
        "metadata batching must not prefetch body bytes"
    );
    assert!(
        reads
            .iter()
            .all(|(_, offset, length)| *offset == 0 && *length == PIECE as u64)
    );
}

#[test]
fn http_range_sources_partial_boundary_chunks_keep_exact_bytes_and_range_ids() {
    let (_temp, meta, fs, sources) = sealed_range_fs(DfsReadConfig::default(), 8);
    let inode = fs
        .backend_inode(&meta.inode.lock().unwrap().inode_id)
        .unwrap();
    let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let start = PIECE as u64 + 123;
    let length = (4 * PIECE + 27) as u64;
    fs.set_read_source_range(
        &context(),
        handle,
        start..start + length,
        Instant::now() + Duration::from_secs(10),
    )
    .unwrap();
    let mut offset = start;
    while offset < start + length {
        let count = (start + length - offset).min(PIECE as u64) as usize;
        let body = fs.read_reply(&context(), handle, offset, count).unwrap();
        assert_eq!(body.len(), count);
        for (index, byte) in body.iter().enumerate() {
            assert_eq!(*byte, ((offset + index as u64) / PIECE as u64) as u8);
        }
        offset += count as u64;
    }
    fs.release(&context(), handle).unwrap();
    let requests = sources.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .chunk_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        (1..=5)
            .map(|index| crate::dfs::ChunkId::new(format!("http-range-{index}")))
            .collect()
    );
    assert_eq!(
        sources
            .reads
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, length)| length)
            .sum::<u64>(),
        length
    );
}

#[test]
fn http_range_sources_large_range_rolls_bounded_metadata_windows() {
    let (_temp, meta, fs, sources) = sealed_range_fs(
        DfsReadConfig {
            source_lookahead_chunks: 128,
            ..Default::default()
        },
        132,
    );
    let inode = fs
        .backend_inode(&meta.inode.lock().unwrap().inode_id)
        .unwrap();
    let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    fs.set_read_source_range(
        &context(),
        handle,
        PIECE as u64..131 * PIECE as u64,
        Instant::now() + Duration::from_secs(10),
    )
    .unwrap();
    for index in 1..131 {
        assert_eq!(
            fs.read_reply(&context(), handle, (index * PIECE) as u64, PIECE)
                .unwrap()[0],
            index as u8
        );
    }
    fs.release(&context(), handle).unwrap();
    let requests = sources.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].chunk_ids.len(), 128);
    assert_eq!(requests[1].chunk_ids.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.chunk_ids.len() <= 128)
    );
    let ids: BTreeSet<_> = requests
        .iter()
        .flat_map(|request| request.chunk_ids.iter().cloned())
        .collect();
    assert_eq!(
        ids,
        (1..131)
            .map(|index| crate::dfs::ChunkId::new(format!("http-range-{index}")))
            .collect()
    );
    assert_eq!(sources.reads.lock().unwrap().len(), 130);
}
