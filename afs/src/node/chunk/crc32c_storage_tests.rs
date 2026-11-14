use super::*;
fn staged() -> StagedChunk {
    StagedChunk::from_shared_crc32c(
        OperationId::new("crc-check"),
        SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(vec![73; 2 * 32768 + 13])),
    )
    .unwrap()
}
#[test]
fn crc32c_known_vector_and_tail() {
    assert_eq!(
        super::super::chunk_crc32c::checksum(b"123456789"),
        0xe3069283
    );
    for n in [0, 1, 7, 8, 31, 32768, 32781] {
        let bytes = vec![73; n];
        assert_eq!(
            super::super::chunk_crc32c::checksum(&bytes),
            crc32c::crc32c(&bytes)
        );
    }
}
#[test]
fn crc32c_unique_id_and_stable_retry() {
    let first = staged();
    let second = staged();
    assert_ne!(first.chunk.id, second.chunk.id);
    let retry = StagedChunk::from_shared_crc32c_id(
        first.operation_id.clone(),
        first.bytes.clone(),
        first.chunk.id.clone(),
    )
    .unwrap();
    assert_eq!(retry.chunk, first.chunk);
    assert!(first.chunk.valid_immutable_identity());
    assert!(first.outboard.is_none());
}

#[test]
fn crc32c_replica_receiver_preserves_identity_and_rejects_changed_body() {
    let original = staged();
    let received = StagedChunk::from_replica(
        OperationId::new("repair-attempt"),
        &original.chunk,
        original.bytes().to_vec(),
    )
    .unwrap();
    assert_eq!(received.chunk, original.chunk);
    assert_ne!(received.operation_id, original.operation_id);
    let mut damaged = original.bytes().to_vec();
    damaged[32770] ^= 1;
    assert!(
        StagedChunk::from_replica(OperationId::new("retry"), &original.chunk, damaged).is_err()
    );
}
#[test]
fn crc32c_persist_reopen_range_and_gc_without_writer_switch() {
    let temp = tempfile::tempdir().unwrap();
    let item = staged();
    let id = item.chunk.id.clone();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    store.put(item.clone()).unwrap();
    drop(store);
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    store.recover().unwrap();
    let mut out = [0; 30];
    assert_eq!(store.read_at(&id, 32760, &mut out).unwrap(), 30);
    assert_eq!(out, [73; 30]);
    assert!(
        store
            .gc_inventory()
            .unwrap()
            .iter()
            .any(|c| c == &item.chunk)
    );
    assert_eq!(store.peer_catalog_record(&id).unwrap().chunk, item.chunk);
    store
        .collect_generation(1, std::slice::from_ref(&id))
        .unwrap();
    assert!(!store.chunk_path(&id).exists());
}
#[test]
fn crc32c_peer_range_preserves_exact_grant_and_detects_boundary_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let item = staged();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    store.put(item.clone()).unwrap();
    let payload = store.read_peer_range(&item.chunk.id, 32760, 30).unwrap();
    assert_eq!(payload.as_slice(), &[73; 30]);
    drop(payload);
    // This byte is outside the returned range but in its covering segment.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(store.chunk_path(&item.chunk.id))
        .unwrap();
    file.write_at(&[99], 32759).unwrap();
    assert!(store.read_peer_range(&item.chunk.id, 32760, 30).is_err());
}

#[test]
fn crc32c_retry_adopts_synced_orphan_and_replays_durable_object() {
    let temp = tempfile::tempdir().unwrap();
    let item = staged();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let mut file = File::create(store.chunk_path(&item.chunk.id)).unwrap();
    file.write_all(item.bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    store.put(item.clone()).unwrap();
    drop(store);
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    store.recover().unwrap();
    store.put(item.clone()).unwrap();
    let mut out = [0; 20];
    assert_eq!(store.read_at(&item.chunk.id, 32760, &mut out).unwrap(), 20);
    assert_eq!(out, [73; 20]);
    assert_eq!(store.gc_inventory().unwrap().len(), 1);
}
#[test]
fn crc32c_corrupt_orphan_is_reported_for_gc_without_catalog_publication() {
    let temp = tempfile::tempdir().unwrap();
    let item = staged();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    std::fs::write(
        store.chunk_path(&item.chunk.id),
        vec![74; item.bytes().len()],
    )
    .unwrap();
    assert!(store.put(item.clone()).is_err());
    assert!(store.peer_catalog_record(&item.chunk.id).is_err());
    let inventory = store.gc_inventory().unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].id, item.chunk.id);
    assert_ne!(inventory[0].content_digest, item.chunk.content_digest);
}
#[test]
fn crc32c_missing_or_damaged_descriptor_and_body_fail_closed() {
    for damage in 0..4 {
        let temp = tempfile::tempdir().unwrap();
        let item = staged();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.put(item.clone()).unwrap();
        if damage < 2 {
            let mut catalog = store.catalog.lock().unwrap();
            let record = catalog.records.get_mut(&item.chunk.id).unwrap();
            if damage == 0 {
                record.segment_crc = None;
            } else {
                record.segment_crc.as_mut().unwrap().checksums[1] ^= 1;
            }
        } else {
            let body = std::fs::OpenOptions::new()
                .write(true)
                .open(store.chunk_path(&item.chunk.id))
                .unwrap();
            if damage == 2 {
                body.set_len(10).unwrap();
            } else {
                body.write_at(&[99], 32770).unwrap();
            }
        }
        assert!(store.read_at(&item.chunk.id, 32760, &mut [0; 30]).is_err());
    }
}
#[test]
fn crc32c_missing_body_recovery_quarantines_without_recursion() {
    let temp = tempfile::tempdir().unwrap();
    let item = staged();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    store.put(item.clone()).unwrap();
    std::fs::remove_file(store.chunk_path(&item.chunk.id)).unwrap();
    store.recover().unwrap();
    assert_eq!(
        store.catalog.lock().unwrap().records[&item.chunk.id].state,
        LocalChunkState::Quarantined
    );
}

/// R17: a persisted CRC32C descriptor of another `format` is identified at
/// startup before its shape and digest (inner layer, after the catalog
/// line's own checksum).
#[test]
fn crc_descriptor_format_is_identified_before_its_shape_and_digest() {
    let temp = tempfile::tempdir().unwrap();
    {
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        store.put(staged()).unwrap();
    }
    let path = temp.path().join("catalog.wal");
    let mut rewritten = Vec::new();
    let mut tables = 0;
    for line in fs::read_to_string(&path).unwrap().lines() {
        let txn: CatalogTxn = serde_json::from_str(line).unwrap();
        let mut records = txn.records;
        for record in &mut records {
            if let Some(table) = &mut record.segment_crc {
                assert_eq!(table.format, 1);
                table.format = 2;
                tables += 1;
            }
        }
        rewritten
            .extend(serde_json::to_vec(&CatalogTxn::new(txn.revision, records).unwrap()).unwrap());
        rewritten.push(b'\n');
    }
    assert!(tables > 0);
    fs::write(&path, rewritten).unwrap();
    let error = LocalChunkStore::open(temp.path(), "node-a")
        .and_then(|store| store.recover().map(|_| ()))
        .unwrap_err();
    assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
    for part in [
        "Node CRC32C segment descriptor",
        "version 2",
        "supports version 1",
    ] {
        assert!(error.message().contains(part), "{part}: {error}");
    }
}
