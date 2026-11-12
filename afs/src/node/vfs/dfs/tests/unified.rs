use super::*;

struct UnifiedLocalTransfer;
impl crate::node::read::engine::ChunkTransfer for UnifiedLocalTransfer {
    fn supports_async_native_read(&self) -> bool {
        true
    }
    fn read_ranges(
        &self,
        _: &crate::node::read::engine::PeerReadBatch,
        _: &mut [u8],
    ) -> Result<()> {
        Err(unavailable(
            "local unified fixture unexpectedly requested a peer",
        ))
    }
}
fn unified_local_fs(
    mode: crate::config::DfsReadCrcMode,
    cache: bool,
) -> (tempfile::TempDir, Arc<RecordingMeta>, Arc<DistributedFs>) {
    let temp = tempfile::tempdir().unwrap();
    let meta = Arc::new(RecordingMeta::new());
    let local = Arc::new(
        LocalChunkStore::open(temp.path().join("chunks"), "node-a")
            .unwrap()
            .with_peer_read_crc_mode(mode),
    );
    let mut engine = DfsReadEngine::new(
        NamespaceId::new("default"),
        "node-a".into(),
        local.clone(),
        Arc::new(crate::node::read::engine::UnimplementedReadSourceProvider),
        Arc::new(UnifiedLocalTransfer),
        crate::node::read::engine::DfsReadConfig {
            readahead_max_bytes: if cache { 16 * 1024 * 1024 } else { 0 },
            ..Default::default()
        },
    )
    .with_read_pipeline(crate::node::read::admission::ReadPipelineLimits::default())
    .unwrap();
    if cache {
        engine = engine.with_peer_cache(Arc::new(
            crate::node::read::peer_cache::DiskPeerCache::open(
                crate::node::read::peer_cache::PeerCacheOptions {
                    directory: temp.path().join("cache"),
                    capacity_bytes: 16 * 1024 * 1024,
                    namespace_id: NamespaceId::new("default"),
                },
            )
            .unwrap(),
        ));
    }
    let fs = test_dfs_new(
        NamespaceId::new("default"),
        "node-a",
        "session-a",
        meta.clone(),
        local,
        Arc::new(engine),
    );
    (temp, meta, Arc::new(fs))
}
async fn unified_reply(
    fs: &Arc<DistributedFs>,
    handle: FileHandle,
    offset: u64,
    size: usize,
) -> Result<prost::bytes::Bytes> {
    fs.clone()
        .read_reply_async(
            context(),
            handle,
            offset,
            size,
            Instant::now() + Duration::from_secs(3),
            Arc::new(()),
        )?
        .ok_or_else(|| unavailable("native shape selected a synchronous fallback"))?
        .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unified_native_crc_cache_options_preserve_fixed_open_after_metadata_observation_and_expiry()
 {
    use crate::config::DfsReadCrcMode;
    for mode in [DfsReadCrcMode::Off, DfsReadCrcMode::Receiver] {
        for cache in [false, true] {
            let (_temp, meta, fs) = unified_local_fs(mode, cache);
            let id = meta.inode.lock().unwrap().inode_id.clone();
            publish_external_version_with_bytes(&meta, &fs, "unified-old", b"old-version");
            let inode = fs.backend_inode(&id).unwrap();
            let fd = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
            assert!(fs.supports_async_reads());
            assert_eq!(
                &unified_reply(&fs, fd, 0, 64).await.unwrap()[..],
                b"old-version"
            );
            publish_external_version_with_bytes(
                &meta,
                &fs,
                "unified-new",
                b"new-version-is-longer",
            );
            let observed = meta.get_inode(&id).unwrap();
            fs.observe_inode_record(observed).unwrap();
            expire_native_read_view(&fs, fd);
            assert!(fs.supports_async_reads());
            assert_eq!(
                &unified_reply(&fs, fd, 0, 64).await.unwrap()[..],
                b"old-version"
            );
            assert!(unified_reply(&fs, fd, 11, 64).await.unwrap().is_empty());
            assert!(unified_reply(&fs, fd, 99, 0).await.unwrap().is_empty());
            fs.release(&context(), fd).unwrap();
            assert!(unified_reply(&fs, fd, 0, 64).await.is_err());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unified_native_dirty_overlay_sparse_reuse_eof_and_committed_view_match() {
    let (_temp, _meta, fs) = unified_local_fs(crate::config::DfsReadCrcMode::Off, false);
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("unified-dirty"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    // 同步 Backend 接口在生产里跑在 FUSE 线程或 spawn_blocking 上，不在异步 worker 上。
    tokio::task::block_in_place(|| {
        fs.write(&context(), created.handle, 0, b"base").unwrap();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        fs.write(&context(), created.handle, 65536, b"tail")
            .unwrap();
        fs.write(&context(), created.handle, 1, b"NEW").unwrap();
    });
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    let expected = fs.read_owned(&context(), reader, 0, 70000).unwrap();
    assert_eq!(expected.len(), 65540);
    assert_eq!(&expected[..4], b"bNEW");
    assert!(expected[4..65536].iter().all(|b| *b == 0));
    let actual = unified_reply(&fs, reader, 0, 70000).await.unwrap();
    assert_eq!(&actual[..], &expected);
    drop(actual);
    tokio::task::block_in_place(|| {
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
    });
    // Recycle a nonzero private buffer, then verify sparse regions do not leak it.
    let actual = unified_reply(&fs, reader, 0, 70000).await.unwrap();
    assert_eq!(&actual[..], &expected);
    assert_eq!(
        &unified_reply(&fs, reader, 65534, 32).await.unwrap()[..],
        &expected[65534..]
    );
    assert!(
        unified_reply(&fs, reader, 65540, 32)
            .await
            .unwrap()
            .is_empty()
    );
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}
