use super::*;

#[test]
fn p2_r1_version_layout_batch_looks_ahead_without_changing_requested_ranges() {
    const CHUNK: u64 = 4 * 1024 * 1024;
    let id = FileVersionId::new("r1-fixed-version");
    let layout_id = LayoutRootId::new("r1-fixed-layout");
    // Deliberately reversed serialization order: the existing index must
    // select later logical offsets without rebuilding or loading a layout.
    let layout = LayoutRoot {
        id: layout_id.clone(),
        file_length: 16 * CHUNK,
        inline_extents: (0..16)
            .rev()
            .map(|index| Extent {
                file_offset: index * CHUNK,
                length: CHUNK,
                chunk_id: crate::dfs::ChunkId::new(format!("r1-object-{index}")),
                chunk_offset: 0,
            })
            .collect(),
    };
    let version = FileVersion {
        id: id.clone(),
        inode_id: InodeId::new("r1-inode"),
        parent_version: None,
        length: layout.file_length,
        layout_root: layout_id,
        created_at_unix_ms: 0,
    };
    let cached = ReadOnlyVersion::checked(&id, version, layout).unwrap();
    let batch = cached.read_batch(0, 128 * 1024, 64).unwrap();
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(batch.ops[0].length, 128 * 1024);
    assert_eq!(batch.ops[0].chunk_id.0, "r1-object-0");
    assert_eq!(batch.lookahead.len(), 15);
    assert_eq!(batch.lookahead[0].0, "r1-object-1");
    assert_eq!(batch.lookahead[14].0, "r1-object-15");
    assert_eq!(batch.file_version_id.as_ref(), Some(&id));
    assert!(
        cached
            .read_batch(15 * CHUNK, 128 * 1024, 64)
            .unwrap()
            .lookahead
            .is_empty()
    );
    assert!(
        cached
            .read_batch(0, 128 * 1024, 0)
            .unwrap()
            .lookahead
            .is_empty()
    );
    let capped = cached.read_batch(0, 128 * 1024, 2).unwrap();
    assert_eq!(capped.ops, batch.ops);
    assert_eq!(capped.lookahead.len(), 2);
}

#[test]
fn read_equivalence_native_source_window_is_independent_of_first_concurrent_offset() {
    const CHUNK: u64 = 4 * 1024 * 1024;
    let id = FileVersionId::new("equivalent-query-version");
    let root = LayoutRootId::new("equivalent-query-layout");
    let layout = LayoutRoot {
        id: root.clone(),
        file_length: 256 * CHUNK,
        inline_extents: (0..256)
            .map(|index| Extent {
                file_offset: index * CHUNK,
                length: CHUNK,
                chunk_id: crate::dfs::ChunkId::new(format!("source-{index}")),
                chunk_offset: 0,
            })
            .collect(),
    };
    let version = FileVersion {
        id: id.clone(),
        inode_id: InodeId::new("query-file"),
        parent_version: None,
        length: layout.file_length,
        layout_root: root,
        created_at_unix_ms: 0,
    };
    let cached = ReadOnlyVersion::checked(&id, version, layout).unwrap();
    for (offset, first, last) in [
        (3 * CHUNK, 0, 64),
        (68 * CHUNK, 65, 129),
        (255 * CHUNK, 195, 255),
    ] {
        let batch = cached.native_read_batch(offset, 1024 * 1024, 64).unwrap();
        let original = cached.read_batch(offset, 1024 * 1024, 64).unwrap();
        assert_eq!(
            batch.ops, original.ops,
            "metadata grouping never widens payload reads"
        );
        let ids: std::collections::BTreeSet<_> = batch
            .lookahead
            .iter()
            .chain(batch.ops.iter().map(|op| &op.chunk_id))
            .map(|id| id.0.clone())
            .collect();
        let expected: std::collections::BTreeSet<_> =
            (first..=last).map(|i| format!("source-{i}")).collect();
        assert_eq!(
            ids, expected,
            "native metadata window must not depend on which concurrent offset arrives first"
        );
        assert!(batch.lookahead.len() <= 64);
    }
    assert!(
        cached
            .native_read_batch(3 * CHUNK, 1024 * 1024, 0)
            .unwrap()
            .lookahead
            .is_empty()
    );
}

#[test]
fn p2_r1_lookahead_deduplicates_fragmented_layout_without_widening_body() {
    let current = crate::dfs::ChunkId::new("current");
    let next = crate::dfs::ChunkId::new("next");
    let extents = [
        Extent {
            file_offset: 1,
            length: 1,
            chunk_id: current.clone(),
            chunk_offset: 1,
        },
        Extent {
            file_offset: 1,
            length: 1,
            chunk_id: crate::dfs::ChunkId::new(
                "x".repeat(crate::node::dfs_read::MAX_SOURCE_LOOKAHEAD_ID_BYTES + 1),
            ),
            chunk_offset: 0,
        },
        Extent {
            file_offset: 2,
            length: 1,
            chunk_id: next.clone(),
            chunk_offset: 0,
        },
        Extent {
            file_offset: 3,
            length: 1,
            chunk_id: next.clone(),
            chunk_offset: 1,
        },
    ];
    let ops = [ChunkReadOp {
        chunk_id: current,
        chunk_offset: 0,
        length: 1,
        output_offset: 0,
    }];
    assert_eq!(source_lookahead_ids(extents.iter(), &ops, 64), vec![next]);
    assert_eq!(ops[0].length, 1);
}

#[test]
fn p2_version_cache_bytes_evict_fragmented_lru_without_retiring_active_reader() {
    let (_temp, _meta, fs) = test_fs();
    let held = p2_byte_cache_version("byte-first", 24_000);
    let second = p2_byte_cache_version("byte-second", 24_000);
    let third = p2_byte_cache_version("byte-third", 24_000);
    let first_id = held.version.as_ref().unwrap().id.clone();
    let second_id = second.version.as_ref().unwrap().id.clone();
    let third_id = third.version.as_ref().unwrap().id.clone();
    let expected = held.ops(2, 3).unwrap();
    assert_eq!(expected.len(), 2);
    let weak = Arc::downgrade(&held);
    {
        let mut cache = fs.readonly_version_cache.lock().unwrap();
        cache.insert(first_id.clone(), held.clone());
        cache.insert(second_id.clone(), second);
        assert!(cache.get(&first_id).is_some());
        cache.insert(third_id.clone(), third);
        assert_eq!(cache.len(), 2, "three fragmented layouts exceed 32MiB");
        assert!(
            cache.get(&second_id).is_none(),
            "the untouched entry is oldest"
        );
        assert!(cache.get(&third_id).is_some());
        assert!(cache.get(&first_id).is_some());
        // Make the held version oldest and evict only the cache's owner.
        let fourth_id = FileVersionId::new("byte-fourth");
        cache.insert(
            fourth_id.clone(),
            p2_byte_cache_version("byte-fourth", 24_000),
        );
        assert!(cache.get(&fourth_id).is_some());
        cache.insert(
            FileVersionId::new("byte-fifth"),
            p2_byte_cache_version("byte-fifth", 24_000),
        );
        assert!(cache.get(&first_id).is_none());
    }
    assert_eq!(held.ops(2, 3).unwrap(), expected);
    assert!(
        weak.upgrade().is_some(),
        "the active reader still owns its metadata"
    );
    drop(held);
    assert!(
        weak.upgrade().is_none(),
        "the cache has no retired ownership"
    );
}

#[test]
fn p2_version_cache_bytes_counts_reserved_extent_and_index_capacity() {
    const LIMIT: usize = 32 * 1024 * 1024;
    for index_capacity in [false, true] {
        let (_temp, _meta, fs) = test_fs();
        let mut version = p2_byte_cache_version("reserved-layout", 1);
        let value = Arc::get_mut(&mut version).unwrap();
        if index_capacity {
            value
                .index
                .entries
                .reserve_exact(LIMIT / std::mem::size_of::<IndexedExtent>());
        } else {
            value
                .layout
                .inline_extents
                .reserve_exact(LIMIT / std::mem::size_of::<Extent>());
        }
        let id = version.version.as_ref().unwrap().id.clone();
        let mut cache = fs.readonly_version_cache.lock().unwrap();
        cache.insert(id.clone(), version.clone());
        assert!(
            cache.get(&id).is_none(),
            "capacity counts even when len is one"
        );
        assert_eq!(version.ops(0, 1).unwrap().len(), 1);
    }
}

#[test]
fn p2_version_cache_bytes_counts_string_capacity_and_same_key_replacement() {
    const MIB: usize = 1024 * 1024;
    let (_temp, _meta, fs) = test_fs();
    let mut inflated = p2_byte_cache_version("replacement", 1);
    Arc::get_mut(&mut inflated).unwrap().layout.inline_extents[0]
        .chunk_id
        .0
        .reserve_exact(12 * MIB);
    let compact = Arc::new(
        ReadOnlyVersion::checked(
            &inflated.version.as_ref().unwrap().id,
            inflated.version.clone().unwrap(),
            inflated.layout.clone(),
        )
        .unwrap(),
    );
    assert_eq!(inflated.layout, compact.layout);
    let id = inflated.version.as_ref().unwrap().id.clone();
    let second = p2_byte_cache_version("replacement-second", 24_000);
    let second_id = second.version.as_ref().unwrap().id.clone();
    let third = p2_byte_cache_version("replacement-third", 24_000);
    let third_id = third.version.as_ref().unwrap().id.clone();
    let mut cache = fs.readonly_version_cache.lock().unwrap();
    cache.insert(id.clone(), inflated.clone());
    cache.insert(second_id.clone(), second);
    cache.insert(id.clone(), compact);
    cache.insert(third_id.clone(), third);
    assert!(
        cache.get(&second_id).is_some(),
        "replaced capacity must be subtracted"
    );
    assert!(cache.get(&third_id).is_some());
    cache.insert(id.clone(), inflated);
    assert!(
        cache.get(&second_id).is_none(),
        "expanded replacement must be charged"
    );
    assert!(cache.get(&third_id).is_some());
    assert!(cache.get(&id).is_some());
}

#[test]
fn p2_version_cache_bytes_counts_version_layout_and_key_string_capacity() {
    const LIMIT: usize = 32 * 1024 * 1024;
    for field in [
        "version",
        "inode",
        "parent",
        "version-layout",
        "layout",
        "key",
    ] {
        let (_temp, _meta, fs) = test_fs();
        let mut value = p2_byte_cache_version("reserved-id", 1);
        let mut key = value.version.as_ref().unwrap().id.clone();
        let inner = Arc::get_mut(&mut value).unwrap();
        let version = inner.version.as_mut().unwrap();
        version.parent_version = Some(FileVersionId::new("parent"));
        let string = match field {
            "version" => &mut version.id.0,
            "inode" => &mut version.inode_id.0,
            "parent" => &mut version.parent_version.as_mut().unwrap().0,
            "version-layout" => &mut version.layout_root.0,
            "layout" => &mut inner.layout.id.0,
            "key" => &mut key.0,
            _ => unreachable!(),
        };
        string.reserve_exact(LIMIT);
        let lookup = key.clone();
        let mut cache = fs.readonly_version_cache.lock().unwrap();
        cache.insert(key, value.clone());
        assert!(
            cache.get(&lookup).is_none(),
            "reserved {field} capacity must count"
        );
        assert_eq!(value.ops(0, 1).unwrap().len(), 1);
    }
}

#[test]
fn p2_version_cache_bytes_small_limit_survives_both_configuration_orders() {
    for bytes_first in [false, true] {
        let (_temp, meta, fs) = test_fs();
        let fs = if bytes_first {
            fs.with_version_cache_bytes(1)
                .with_version_cache_capacity(2)
        } else {
            fs.with_version_cache_capacity(2)
                .with_version_cache_bytes(1)
        };
        let head = meta.publish_external_version(17);
        let before = meta.file_version_load_count();
        let first = fs.load_readonly_version(Some(&head)).unwrap();
        let second = fs.load_readonly_version(Some(&head)).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(first.layout.file_length, 17);
        assert_eq!(meta.file_version_load_count() - before, 2);
        assert_eq!(fs.readonly_version_cache.lock().unwrap().len(), 0);
    }
}

#[test]
fn p2_version_cache_bytes_oversized_version_remains_usable_without_retention() {
    let (_temp, meta, fs) = test_fs();
    let oversized = p2_byte_cache_version("oversized-source", 64_000);
    let head = meta.publish_external_version_with_receipts(
        oversized.layout.file_length,
        oversized.layout.inline_extents.clone(),
        Vec::new(),
    );
    drop(oversized);
    let before = meta.file_version_load_count();
    let first = fs.load_readonly_version(Some(&head)).unwrap();
    let second = fs.load_readonly_version(Some(&head)).unwrap();
    assert_eq!(first.ops(2, 3).unwrap().len(), 2);
    assert_eq!(first.layout, second.layout);
    assert!(
        !Arc::ptr_eq(&first, &second),
        "an overlarge version is not retained"
    );
    assert_eq!(meta.file_version_load_count() - before, 2);
    assert!(
        fs.readonly_version_cache
            .lock()
            .unwrap()
            .get(&head)
            .is_none()
    );
}

#[test]
fn p1_version_cache_reuses_multiple_immutable_heads_without_meta_reload() {
    let (_temp, meta, fs) = test_fs();
    let first = meta.publish_external_version(17);
    let second = meta.publish_external_version(23);
    let before = meta.file_version_load_count();
    for head in [&first, &second, &first, &second] {
        fs.load_readonly_version(Some(head)).unwrap();
    }
    assert_eq!(
        meta.file_version_load_count() - before,
        2,
        "alternating immutable heads must remain in the bounded cache"
    );
}

#[test]
fn p1_version_cache_hits_share_extent_allocation() {
    let (_temp, meta, fs) = test_fs();
    let head = publish_external_version_with_bytes(&meta, &fs, "p1-shared-layout", b"data");
    let first = fs.load_readonly_version(Some(&head)).unwrap();
    let second = fs.load_readonly_version(Some(&head)).unwrap();
    assert_eq!(
        first.layout.inline_extents.as_ptr(),
        second.layout.inline_extents.as_ptr(),
        "cache hits cannot deep-copy immutable layout extents"
    );
}

#[test]
fn p1_version_cache_rejects_cross_inode_version_on_consumption() {
    let (_temp, meta, fs) = test_fs();
    publish_external_version_with_bytes(&meta, &fs, "p1-wrong-inode-layout", b"data");
    let inode = meta.inode.lock().unwrap().clone();
    meta.commits
        .lock()
        .unwrap()
        .last_mut()
        .unwrap()
        .file_version
        .inode_id = InodeId::new("different-inode");
    assert!(
        fs.read_committed_inode_owned(&inode, 0, 16).is_err(),
        "an immutable cached version cannot authorize a different inode"
    );
}

#[test]
fn p1_version_cache_rejects_inconsistent_layout_identity_and_length() {
    for wrong_length in [false, true] {
        let (_temp, meta, fs) = test_fs();
        let head = meta.publish_external_version(17);
        {
            let mut commits = meta.commits.lock().unwrap();
            let commit = commits.last_mut().unwrap();
            if wrong_length {
                commit.layout_root.file_length = 16;
            } else {
                commit.layout_root.id = LayoutRootId::new("wrong-layout");
            }
        }
        assert!(
            fs.load_readonly_version(Some(&head)).is_err(),
            "immutable version/layout mismatch must be rejected before cache admission"
        );
        {
            let mut commits = meta.commits.lock().unwrap();
            let commit = commits.last_mut().unwrap();
            commit.layout_root.id = commit.file_version.layout_root.clone();
            commit.layout_root.file_length = commit.file_version.length;
        }
        assert!(
            fs.load_readonly_version(Some(&head)).is_ok(),
            "rejected metadata must not poison later cache admission"
        );
        assert_eq!(meta.file_version_load_count(), 2);
    }
}

#[test]
fn p1_version_cache_concurrent_admission_shares_canonical_arc_and_rejects_changed_value() {
    for changed in [false, true] {
        let (_temp, meta, fs) = test_fs();
        let head = meta.publish_external_version(17);
        let original = {
            let commits = meta.commits.lock().unwrap();
            let commit = commits.last().unwrap();
            (commit.file_version.clone(), commit.layout_root.clone())
        };
        let mut other = original.clone();
        if changed {
            other.0.length = 18;
            other.1.file_length = 18;
        }
        meta.file_version_replies
            .lock()
            .unwrap()
            .extend([original, other]);
        *fs.readonly_version_before_install.lock().unwrap() =
            Some(Arc::new(std::sync::Barrier::new(2)));
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| fs.load_readonly_version(Some(&head)));
            let second = scope.spawn(|| fs.load_readonly_version(Some(&head)));
            (first.join().unwrap(), second.join().unwrap())
        });
        *fs.readonly_version_before_install.lock().unwrap() = None;
        assert_eq!(
            meta.file_version_load_count(),
            2,
            "this cache does not single-flight Meta misses"
        );
        if changed {
            assert_ne!(
                first.is_ok(),
                second.is_ok(),
                "same immutable ID cannot conceal divergent metadata"
            );
        } else {
            assert!(Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
        }
    }
}

#[test]
fn p1_version_cache_lookup_open_read_reuses_one_version_rpc_per_head() {
    let (_temp, meta, fs) = test_fs();
    let original = meta.inode.lock().unwrap().clone();
    let mut files = Vec::new();
    for number in 0..100 {
        let mut inode = original.clone();
        inode.inode_id = InodeId::new(format!("p1-many-files-{number}"));
        *meta.inode.lock().unwrap() = inode;
        let bytes = format!("file-{number}").into_bytes();
        publish_external_version_with_bytes(
            &meta,
            &fs,
            &format!("p1-many-files-chunk-{number}"),
            &bytes,
        );
        files.push((meta.inode.lock().unwrap().clone(), bytes));
    }
    let before = meta.file_version_load_count();
    let opens_before = meta.get_inode_call_count();
    for _ in 0..2 {
        for (inode, bytes) in &files {
            *meta.inode.lock().unwrap() = inode.clone();
            let entry = fs
                .lookup(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("fixture-current-file"),
                )
                .unwrap();
            assert_eq!(entry.attributes.size, bytes.len() as u64);
            let reader = fs.open(&context(), entry.inode, libc::O_RDONLY).unwrap();
            assert_eq!(fs.read_owned(&context(), reader, 0, 64).unwrap(), *bytes);
            fs.release(&context(), reader).unwrap();
        }
    }
    assert_eq!(
        meta.file_version_load_count() - before,
        files.len(),
        "lookup/open/read share validated immutable metadata across files"
    );
    assert_eq!(
        meta.get_inode_call_count() - opens_before,
        2 * files.len(),
        "every open still obtains one online fresh inode"
    );
}

#[test]
fn p1_version_cache_lru_preserves_hot_entries_and_retired_arc() {
    let (_temp, meta, fs) = test_fs();
    let fs = fs.with_version_cache_capacity(2);
    let first = meta.publish_external_version(17);
    let second = meta.publish_external_version(23);
    let third = meta.publish_external_version(29);
    let held = fs.load_readonly_version(Some(&first)).unwrap();
    fs.load_readonly_version(Some(&second)).unwrap();
    assert!(Arc::ptr_eq(
        &held,
        &fs.load_readonly_version(Some(&first)).unwrap()
    ));
    fs.load_readonly_version(Some(&third)).unwrap();
    {
        let mut cache = fs.readonly_version_cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&second).is_none());
        assert!(Arc::ptr_eq(cache.get(&first).unwrap(), &held));
    }
    let before = meta.file_version_load_count();
    fs.load_readonly_version(Some(&second)).unwrap();
    fs.load_readonly_version(Some(&third)).unwrap();
    assert_eq!(meta.file_version_load_count() - before, 2);
    assert_eq!(
        held.layout.file_length, 17,
        "eviction cannot retire an in-flight metadata owner"
    );
}

#[test]
fn p1_layout_index_matches_linear_ops_for_ten_thousand_layouts() {
    let (_temp, _meta, fs) = test_fs();
    let mut seed = 0x52a9_6d71_3f08_c4e2_u64;
    let mut random = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..10_000 {
        let count = (random() % 24) as usize;
        let extents = (0..count)
            .map(|index| Extent {
                file_offset: random() % 4096,
                length: random() % 512,
                chunk_id: crate::dfs::ChunkId::new(format!("index-{case}-{index}")),
                chunk_offset: random() % 1024,
            })
            .collect();
        let layout = LayoutRoot {
            id: LayoutRootId::new("index-equivalence"),
            file_length: 4608,
            inline_extents: extents,
        };
        let index = LayoutReadIndex::new(&layout).unwrap();
        let offset = random() % 5120;
        let length = (random() % 1024) as usize;
        let expected = fs.layout_read_ops(&layout, offset, length).unwrap();
        let actual = index.ops(&layout, offset, length).unwrap();
        assert_eq!(
            actual, expected,
            "case={case} offset={offset} length={length}"
        );
    }
}

#[test]
fn p1_layout_index_preserves_overflow_and_sparse_range_checks() {
    let (_temp, _meta, fs) = test_fs();
    let mut layout = LayoutRoot {
        id: LayoutRootId::new("index-overflow"),
        file_length: u64::MAX,
        inline_extents: vec![Extent {
            file_offset: u64::MAX,
            length: 2,
            chunk_id: crate::dfs::ChunkId::new("overflow"),
            chunk_offset: 0,
        }],
    };
    assert!(fs.layout_read_ops(&layout, 0, 1).is_err());
    assert!(
        LayoutReadIndex::new(&layout).is_err(),
        "out-of-request overflow cannot be skipped by index"
    );
    layout.inline_extents[0].file_offset = 100;
    layout.inline_extents[0].length = 16;
    layout.inline_extents[0].chunk_offset = u64::MAX;
    let index = LayoutReadIndex::new(&layout).unwrap();
    assert_eq!(
        index.ops(&layout, 0, 16).unwrap(),
        fs.layout_read_ops(&layout, 0, 16).unwrap()
    );
    assert!(index.ops(&layout, 101, 1).is_err());
    assert!(fs.layout_read_ops(&layout, 101, 1).is_err());
    assert!(index.ops(&layout, u64::MAX, 2).is_err());
    assert!(fs.layout_read_ops(&layout, u64::MAX, 2).is_err());
}

#[test]
fn readonly_read_reuses_cached_file_version_for_same_head() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "cache-same-head", b"cached-view");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();

    let mut first = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut first).unwrap(), 11);
    assert_eq!(&first[..11], b"cached-view");
    assert_eq!(meta.file_version_load_count(), 1);

    let mut second = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut second).unwrap(), 11);
    assert_eq!(&second[..11], b"cached-view");
    assert_eq!(
        meta.file_version_load_count(),
        1,
        "same validated head should reuse the cached immutable version/layout"
    );

    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_expired_view_keeps_pinned_content_and_length() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "cache-old-head", b"old");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let mut old = [0; 8];
    assert_eq!(fs.read(&context(), reader, 0, &mut old).unwrap(), 3);
    assert_eq!(&old[..3], b"old");
    assert_eq!(meta.file_version_load_count(), 1);

    publish_external_version_with_bytes(&meta, &fs, "cache-new-head", b"new-content");
    expire_native_read_view(&fs, reader);
    let before = meta.get_inode_call_count();
    let mut again = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut again).unwrap(), 3);
    assert_eq!(&again[..3], b"old");
    assert_eq!(meta.file_version_load_count(), 1);
    assert_eq!(meta.get_inode_call_count(), before);

    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_expired_view_renews_without_meta() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "cache-inode-error", b"cached-view");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let mut first = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut first).unwrap(), 11);

    expire_native_read_view(&fs, reader);
    meta.fail_next_get_inode_with(unavailable("Meta must not be asked"));
    let mut second = [7; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut second).unwrap(), 11);
    assert_eq!(&second[..11], b"cached-view");
    assert!(
        fs.open(&context(), inode, libc::O_RDONLY).is_err(),
        "the injected failure is still pending for the next Meta call"
    );
    fs.release(&context(), reader).unwrap();
}

#[test]
fn readonly_native_expired_view_never_loads_another_version() {
    let (_temp, meta, fs) = test_fs();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    publish_external_version_with_bytes(&meta, &fs, "cache-old-before-error", b"old-data");
    let inode = fs.backend_inode(&inode_id).unwrap();
    let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
    let mut old = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut old).unwrap(), 8);
    assert_eq!(&old[..8], b"old-data");

    publish_external_version_with_bytes(&meta, &fs, "cache-new-error", b"new-data");
    expire_native_read_view(&fs, reader);
    let loads = meta.file_version_load_count();
    meta.fail_next_get_file_version_with(unavailable("no version load may happen"));
    let mut again = [9; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut again).unwrap(), 8);
    assert_eq!(&again[..8], b"old-data");
    assert_eq!(meta.file_version_load_count(), loads);
    assert!(
        fs.open(&context(), inode, libc::O_RDONLY).is_err(),
        "the pending failure is consumed by the next open's version load"
    );
    fs.release(&context(), reader).unwrap();
}

#[test]
fn fresh_readonly_open_refreshes_retained_committed_view_after_external_commit() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("fresh-open-external-commit.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"dfs-handover-initial")
            .unwrap(),
        20
    );
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    {
        let state = state.lock().unwrap();
        assert_eq!(state.logical_length, 20);
        assert!(!state.dirty);
        assert!(!state.metadata_dirty);
    }

    let external_version = meta.publish_external_version(18);
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();

    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(reader))
            .unwrap()
            .size,
        18
    );
    let state = state.lock().unwrap();
    assert_eq!(
        state.base_version.as_ref().map(|version| &version.id),
        Some(&external_version)
    );
    assert_eq!(state.logical_length, 18);
    assert_eq!(state.inode.head_version.as_ref(), Some(&external_version));
    drop(state);
    fs.release(&context(), reader).unwrap();
}

#[test]
fn delayed_older_readonly_observation_does_not_downgrade_clean_committed_view() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("delayed-readonly-observation.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(fs.write(&context(), created.handle, 0, b"old").unwrap(), 3);
    fs.fsync(&context(), created.handle, SyncMode::Full)
        .unwrap();
    fs.release(&context(), created.handle).unwrap();
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let older_inode = meta.inode.lock().unwrap().clone();
    let newer_version = meta.publish_external_version(8);
    let newer_inode = meta.inode.lock().unwrap().clone();

    fs.observe_inode_record(newer_inode).unwrap();
    {
        let state = state.lock().unwrap();
        assert_eq!(state.inode.head_version.as_ref(), Some(&newer_version));
        assert_eq!(state.logical_length, 8);
    }

    fs.observe_inode_record(older_inode).unwrap();
    let state = state.lock().unwrap();
    assert_eq!(state.inode.head_version.as_ref(), Some(&newer_version));
    assert_eq!(state.logical_length, 8);
}

#[test]
fn fresh_readonly_open_preserves_dirty_open_writer_after_external_commit() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("dirty-open-reader-external.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"local-dirty")
            .unwrap(),
        11
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let (old_revision, old_head, old_base, dirty_bytes) = {
        let state = state.lock().unwrap();
        assert_eq!(state.open_writers, 1);
        assert!(state.dirty);
        (
            state.inode.revision,
            state.inode.head_version.clone(),
            state
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            state.dirty_extents.dirty_data_bytes(),
        )
    };

    let external_version = meta.publish_external_version(18);
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();
    assert_eq!(
        fs.getattr(&context(), created.entry.inode, Some(reader))
            .unwrap()
            .size,
        11
    );
    let mut data = [0; 16];
    assert_eq!(fs.read(&context(), reader, 0, &mut data).unwrap(), 11);
    assert_eq!(&data[..11], b"local-dirty");

    let state = state.lock().unwrap();
    assert_eq!(state.inode.revision, old_revision);
    assert_eq!(state.inode.head_version.as_ref(), old_head.as_ref());
    assert_ne!(state.inode.head_version.as_ref(), Some(&external_version));
    assert_eq!(
        state
            .base_version
            .as_ref()
            .map(|version| version.id.clone()),
        old_base
    );
    assert!(state.dirty);
    assert_eq!(state.dirty_extents.dirty_data_bytes(), dirty_bytes);
    drop(state);
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}

#[test]
fn fresh_readonly_open_preserves_inflight_commit_after_external_commit() {
    let (_temp, meta, fs) = test_fs();
    let created = fs
        .create(
            &context(),
            fs.root_inode(),
            OsStr::new("inflight-reader-external.bin"),
            0o640,
            libc::O_RDWR,
        )
        .unwrap();
    assert_eq!(
        fs.write(&context(), created.handle, 0, b"pending").unwrap(),
        7
    );
    meta.fail_next_commit_with(unavailable("injected pending commit failure"));
    assert_eq!(
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .expect_err("injected commit must fail")
            .code(),
        afs_error::NODE_VFS_UNAVAILABLE
    );
    let inode_id = meta.inode.lock().unwrap().inode_id.clone();
    let state = fs.write_state(&inode_id).unwrap().unwrap();
    let (old_revision, old_head, pending_expected_head, pending_version) = {
        let state = state.lock().unwrap();
        let pending = match state.in_flight.as_ref().expect("commit remains in flight") {
            InFlightCommit::File(pending) => pending,
            InFlightCommit::Preparing(_) => panic!("commit should have reached Meta"),
            InFlightCommit::Metadata(_, _) => panic!("test expects data commit"),
        };
        (
            state.inode.revision,
            state.inode.head_version.clone(),
            pending.batch.commit.expected_head_version.clone(),
            pending.batch.commit.file_version.id.clone(),
        )
    };

    let external_version = meta.publish_external_version(18);
    let reader = fs
        .open(&context(), created.entry.inode, libc::O_RDONLY)
        .unwrap();

    let state = state.lock().unwrap();
    assert_eq!(state.inode.revision, old_revision);
    assert_eq!(state.inode.head_version.as_ref(), old_head.as_ref());
    assert_ne!(state.inode.head_version.as_ref(), Some(&external_version));
    match state
        .in_flight
        .as_ref()
        .expect("pending commit must be preserved")
    {
        InFlightCommit::File(pending) => {
            assert_eq!(
                pending.batch.commit.expected_head_version.as_ref(),
                pending_expected_head.as_ref()
            );
            assert_eq!(&pending.batch.commit.file_version.id, &pending_version);
        }
        InFlightCommit::Preparing(_) => {
            panic!("commit should still be exact pending file commit")
        }
        InFlightCommit::Metadata(_, _) => panic!("test expects data commit"),
    }
    drop(state);
    fs.release(&context(), reader).unwrap();
    fs.release(&context(), created.handle).unwrap();
}
