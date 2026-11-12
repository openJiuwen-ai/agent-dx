//! `catalog.snapshot`: the checksummed LocalCatalog checkpoint. It also
//! carries the durable GC progress (`gc_generation`) that fences replayed
//! deletion tasks after content is recreated.
use super::*;
#[derive(Deserialize, Serialize)]
struct Checkpoint {
    schema: u32,
    revision: u64,
    gc_generation: u64,
    records: Vec<LocalChunkRecord>,
    checksum: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    cache_intents: Vec<super::read_cache::ReadCacheIntent>,
}
/// `catalog.snapshot` schema (`FormatKind::NodeChunkCatalogSnapshot`); also
/// the first element of the checksum input.
const SCHEMA: u32 = crate::format::version::NODE_CHUNK_CATALOG_SNAPSHOT;
/// Reads only `schema`, so the version is known before the full decode.
#[derive(Deserialize)]
struct SchemaProbe {
    schema: u32,
}
fn checksum(value: &Checkpoint) -> Result<String> {
    let bytes = if value.schema == 1 {
        serde_json::to_vec(&(
            value.schema,
            value.revision,
            value.gc_generation,
            &value.records,
        ))
    } else {
        serde_json::to_vec(&(
            value.schema,
            value.revision,
            value.gc_generation,
            &value.records,
            &value.cache_intents,
        ))
    }
    .map_err(|error| invalid(error.to_string()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
pub(super) fn load_checkpoint(path: &Path) -> Result<Option<LocalCatalog>> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // Version first (B1): an unsupported schema is a format mismatch, not a
    // checksum mismatch. An unreadable probe is left to the full decode.
    if let Ok(probe) = serde_json::from_slice::<SchemaProbe>(&data)
        && probe.schema != 1
    {
        crate::format::check(
            crate::format::FormatKind::NodeChunkCatalogSnapshot,
            path.display(),
            u64::from(probe.schema),
        )?;
    }
    let value: Checkpoint = serde_json::from_slice(&data)
        .map_err(|error| invalid(format!("chunk checkpoint decode: {error}")))?;
    if !matches!(value.schema, 1 | SCHEMA)
        || (value.schema == 1
            && (!value.cache_intents.is_empty()
                || value
                    .records
                    .iter()
                    .any(|record| record.read_cache_epoch != 0)))
        || value.checksum != checksum(&value)?
    {
        return Err(invalid("chunk checkpoint checksum mismatch"));
    }
    let mut catalog = LocalCatalog {
        revision: value.revision,
        #[cfg(any(feature = "dfs", test))]
        gc_generation: value.gc_generation,
        ..LocalCatalog::default()
    };
    for record in value.records {
        if record.catalog_revision > value.revision
            || catalog.records.contains_key(&record.chunk.id)
        {
            return Err(invalid("invalid chunk checkpoint record"));
        }
        catalog.insert_record(record);
    }
    catalog.apply_cache_sync(value.cache_intents, Vec::new());
    Ok(Some(catalog))
}
#[cfg(any(feature = "dfs", test))]
pub(super) fn save_checkpoint(root: &Path, catalog: &LocalCatalog) -> Result<()> {
    let mut records = catalog.records.values().cloned().collect::<Vec<_>>();
    records.sort_by(|a, b| a.chunk.id.cmp(&b.chunk.id));
    let cache_intents = catalog.cache_intents.values().cloned().collect::<Vec<_>>();
    let schema =
        if cache_intents.is_empty() && records.iter().all(|record| record.read_cache_epoch == 0) {
            1
        } else {
            SCHEMA
        };
    let mut value = Checkpoint {
        schema,
        revision: catalog.revision,
        gc_generation: catalog.gc_generation,
        records,
        cache_intents,
        checksum: String::new(),
    };
    value.checksum = checksum(&value)?;
    let bytes = serde_json::to_vec(&value).map_err(|error| invalid(error.to_string()))?;
    let temp = root.join("catalog.snapshot.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, root.join("catalog.snapshot"))?;
    File::open(root)?.sync_all()?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    /// B1: an unsupported `catalog.snapshot` schema is a format mismatch
    /// identified before the checksum; a bad checksum keeps its message.
    #[test]
    fn snapshot_schema_is_identified_before_the_checksum() {
        let temp = tempfile::tempdir().unwrap();
        save_checkpoint(temp.path(), &LocalCatalog::default()).unwrap();
        let path = temp.path().join("catalog.snapshot");
        let original: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(original["schema"], 1);
        let mut other = original.clone();
        other["schema"] = 3.into();
        fs::write(&path, serde_json::to_vec(&other).unwrap()).unwrap();
        let error = load_checkpoint(&path).unwrap_err();
        assert_eq!(error.code(), afs_error::STORAGE_FORMAT_INCOMPATIBLE);
        for part in [
            "Node chunk catalog.snapshot",
            "catalog.snapshot",
            "version 3",
            "supports version 2",
        ] {
            assert!(error.message().contains(part), "{part}: {error}");
        }
        let mut damaged = original;
        damaged["checksum"] = "00".into();
        fs::write(&path, serde_json::to_vec(&damaged).unwrap()).unwrap();
        let error = load_checkpoint(&path).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_STORAGE_INVALID);
        assert!(error.message().contains("checksum mismatch"), "{error}");
    }
}
