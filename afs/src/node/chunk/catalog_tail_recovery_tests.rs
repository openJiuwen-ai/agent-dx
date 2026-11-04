use super::*;

fn record(revision: u64, label: &str) -> LocalChunkRecord {
    let staged = StagedChunk::new(OperationId::new(label), label.as_bytes().to_vec());
    LocalChunkRecord {
        segment_crc: None,
        stored_length: staged.chunk.length,
        stored_checksum: staged.chunk.content_digest.clone(),
        location: PhysicalLocation::PerChunkFile {
            relative_path: format!("chunks/{}", staged.chunk.id.0),
        },
        chunk: staged.chunk,
        device_id: "local-0".into(),
        device_epoch: 1,
        encoding: PhysicalEncoding::Raw,
        state: LocalChunkState::Durable,
        catalog_revision: revision,
    }
}

fn encoded(txn: &CatalogTxn) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(txn).unwrap();
    bytes.push(b'\n');
    bytes
}

#[test]
fn catalog_tail_recovers_only_the_valid_prefix_and_can_append_again() {
    let first = CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap();
    let next = CatalogTxn::new(2, vec![record(2, "new-a"), record(2, "new-b")]).unwrap();
    let prefix = encoded(&first);
    let body = serde_json::to_string(&next).unwrap();
    let records_start = body.find("\"records\":[").unwrap() + "\"records\":[".len();
    let record_length = serde_json::to_vec(&next.records[0]).unwrap().len();
    let checksum_start = body.rfind("\"checksum\":\"").unwrap() + "\"checksum\":\"".len();
    for cut in [
        1,
        records_start + 1,
        records_start + record_length / 2,
        records_start + record_length + 1,
        checksum_start + 8,
        body.len() - 1,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.wal");
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&body.as_bytes()[..cut]);
        fs::write(&path, bytes).unwrap();
        let recovered = LocalCatalog::recover(&path).unwrap();
        assert_eq!(recovered.revision, 1, "cut {cut}");
        assert_eq!(recovered.records.len(), 1, "cut {cut}");
        assert_eq!(
            recovered.records.get(&first.records[0].chunk.id),
            Some(&first.records[0])
        );
        assert_eq!(fs::read(&path).unwrap(), prefix, "cut {cut}");
        append_catalog_txn(&path, &next, None).unwrap();
        let recovered = LocalCatalog::recover(&path).unwrap();
        assert_eq!(recovered.revision, 2);
        assert_eq!(recovered.records.len(), 3);
        for item in &next.records {
            assert_eq!(recovered.records.get(&item.chunk.id), Some(item));
        }
    }
}

#[test]
fn catalog_tail_multibyte_utf8_cut_is_incomplete_but_invalid_utf8_is_not() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("catalog.wal");
    let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
    let mut item = record(2, "unicode");
    item.device_id = "device-节点".into();
    let body = serde_json::to_string(&CatalogTxn::new(2, vec![item]).unwrap()).unwrap();
    let character = body.find('节').unwrap();
    for cut in [character + 1, character + 2] {
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&body.as_bytes()[..cut]);
        fs::write(&path, bytes).unwrap();
        assert_eq!(LocalCatalog::recover(&path).unwrap().revision, 1);
        assert_eq!(fs::read(&path).unwrap(), prefix);
    }
    // An invalid code unit is corruption, not an incomplete trailing code point.
    let mut invalid = prefix.clone();
    invalid.extend_from_slice(b"{\"revision\":2,\"records\":[{\"device_id\":\"");
    invalid.push(0xff);
    fs::write(&path, &invalid).unwrap();
    assert!(LocalCatalog::recover(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), invalid);
}

#[test]
fn catalog_tail_complete_json_without_newline_still_recovers_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("catalog.wal");
    let txn = CatalogTxn::new(1, vec![record(1, "complete")]).unwrap();
    let bytes = serde_json::to_vec(&txn).unwrap();
    fs::write(&path, &bytes).unwrap();
    let recovered = LocalCatalog::recover(&path).unwrap();
    assert_eq!(recovered.revision, 1);
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let next = CatalogTxn::new(2, vec![record(2, "after-complete")]).unwrap();
    append_catalog_txn(&path, &next, None).unwrap();
    let recovered = LocalCatalog::recover(&path).unwrap();
    assert_eq!(recovered.revision, 2);
    assert_eq!(recovered.records.len(), 2);
    let mut expected = bytes;
    expected.push(b'\n');
    expected.extend_from_slice(&encoded(&next));
    assert_eq!(fs::read(&path).unwrap(), expected);
}

#[test]
fn catalog_tail_complete_malformed_json_is_not_discarded() {
    let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
    for malformed in [
        "{]",
        "{\"revision\":2,\"records\":[],\"checksum\":\"x\",}",
        "{\"revision\":2,\"records\":[]}",
        "[]",
        "null",
    ] {
        for newline in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("catalog.wal");
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(malformed.as_bytes());
            if newline {
                bytes.push(b'\n');
            }
            fs::write(&path, &bytes).unwrap();
            assert!(
                LocalCatalog::recover(&path).is_err(),
                "{malformed:?}, newline={newline}"
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }
}

#[test]
fn catalog_tail_checksum_revision_and_non_tail_damage_remain_errors() {
    let prefix = encoded(&CatalogTxn::new(1, vec![record(1, "confirmed")]).unwrap());
    let mut bad_checksum = CatalogTxn::new(2, vec![record(2, "bad-checksum")]).unwrap();
    bad_checksum.checksum = "0".repeat(64);
    let bad_revision = CatalogTxn::new(3, vec![record(3, "revision-gap")]).unwrap();
    let bad_record = CatalogTxn::new(2, vec![record(3, "record-revision")]).unwrap();
    for txn in [bad_checksum, bad_revision, bad_record] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.wal");
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(&serde_json::to_vec(&txn).unwrap());
        fs::write(&path, &bytes).unwrap();
        assert!(LocalCatalog::recover(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    for tail in [
        b"{\"revision\":2\n".as_slice(),
        b"{\"revision\":2\n{}\n".as_slice(),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.wal");
        let mut bytes = prefix.clone();
        bytes.extend_from_slice(tail);
        fs::write(&path, &bytes).unwrap();
        assert!(LocalCatalog::recover(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn catalog_tail_recovery_does_not_remove_uncataloged_chunk_bodies() {
    let temp = tempfile::tempdir().unwrap();
    let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    let confirmed = StagedChunk::new(OperationId::new("confirmed"), b"confirmed".to_vec());
    store.put(confirmed.clone()).unwrap();
    let extra = [record(2, "orphan-a"), record(2, "orphan-b")];
    for item in &extra {
        fs::write(store.chunk_path(&item.chunk.id), item.chunk.id.0.as_bytes()).unwrap();
    }
    let prefix = fs::read(&store.catalog_path).unwrap();
    let tail = serde_json::to_vec(&CatalogTxn::new(2, extra.to_vec()).unwrap()).unwrap();
    let mut bytes = prefix.clone();
    bytes.extend_from_slice(&tail[..tail.len() / 2]);
    fs::write(&store.catalog_path, bytes).unwrap();
    drop(store);
    let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
    assert_eq!(recovered.recover().unwrap().orphan_files, 2);
    assert_eq!(fs::read(&recovered.catalog_path).unwrap(), prefix);
    let mut output = [0; 9];
    assert_eq!(
        recovered
            .read_at(&confirmed.chunk.id, 0, &mut output)
            .unwrap(),
        9
    );
    assert_eq!(&output, b"confirmed");
    for item in &extra {
        assert_eq!(
            fs::read(recovered.chunk_path(&item.chunk.id)).unwrap(),
            item.chunk.id.0.as_bytes()
        );
    }
}
