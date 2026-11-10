//! The append-only local Chunk catalog and its durability helpers.

use super::*;

/// One `catalog.wal` line (`FormatKind::NodeChunkCatalogWal`). The line has
/// no version field, so its encoding is pinned by the chunk goldens only;
/// changing it bumps the registry version and must add one (format rule 4).
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CatalogTxn {
    pub(super) revision: u64,
    pub(super) records: Vec<LocalChunkRecord>,
    pub(super) checksum: String,
}

impl CatalogTxn {
    #[cfg(test)]
    pub(super) fn new(revision: u64, records: Vec<LocalChunkRecord>) -> Result<Self> {
        let checksum = catalog_checksum(revision, &records)?;
        Ok(Self {
            revision,
            records,
            checksum,
        })
    }

    fn verify(&self) -> Result<()> {
        if self.checksum != catalog_checksum(self.revision, &self.records)? {
            return Err(invalid("local chunk catalog transaction checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LocalCatalog {
    pub(super) revision: u64,
    #[cfg(any(feature = "dfs", test))]
    pub(super) gc_generation: u64,
    pub(super) records: HashMap<ChunkId, LocalChunkRecord>,
}

impl LocalCatalog {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn recover(path: &Path) -> Result<Self> {
        let mut catalog = checkpoint::load_checkpoint(&path.with_file_name("catalog.snapshot"))?
            .unwrap_or_default();
        let base_revision = catalog.revision;
        if !path.exists() {
            return Ok(catalog);
        }
        let file = File::open(path).map_err(Error::from)?;
        let mut reader = BufReader::new(file);
        let mut valid_bytes = 0_u64;
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line).map_err(Error::from)?;
            if read == 0 {
                break;
            }
            let next_offset = valid_bytes
                .checked_add(
                    u64::try_from(read).map_err(|_| invalid("local catalog is too large"))?,
                )
                .ok_or_else(|| invalid("local catalog offset overflow"))?;
            match std::str::from_utf8(&line) {
                Ok(text) if text.trim().is_empty() => {
                    valid_bytes = next_offset;
                    continue;
                }
                // A partial final code point can be a torn write, but a known
                // invalid code unit is corruption even when JSON reports EOF.
                Err(error) if error.error_len().is_some() => {
                    return Err(invalid("local chunk catalog contains invalid UTF-8"));
                }
                _ => {}
            }
            let txn: CatalogTxn = match serde_json::from_slice(&line) {
                Ok(txn) => txn,
                Err(error) if error.is_eof() && !line.ends_with(b"\n") => {
                    // read_until reaches an unterminated line only at EOF.
                    // Keep every validated transaction byte-for-byte; never
                    // discard complete malformed/checksum-invalid records.
                    let file = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(Error::from)?;
                    file.set_len(valid_bytes).map_err(Error::from)?;
                    file.sync_all().map_err(Error::from)?;
                    return Ok(catalog);
                }
                Err(error) => {
                    return Err(invalid(format!("local chunk catalog is corrupt: {error}")));
                }
            };
            txn.verify()?;
            if txn.revision <= base_revision {
                valid_bytes = next_offset;
                continue;
            }
            if txn.revision != catalog.revision.saturating_add(1) {
                return Err(invalid("local chunk catalog revision is not contiguous"));
            }
            for record in txn.records {
                if record.catalog_revision != txn.revision {
                    return Err(invalid("local chunk record has the wrong catalog revision"));
                }
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = txn.revision;
            valid_bytes = next_offset;
        }
        Ok(catalog)
    }
}

#[cfg(test)]
pub(super) fn append_catalog_txn(
    path: &Path,
    txn: &CatalogTxn,
    metrics: Option<&super::super::metrics::work::ReadWorkMetrics>,
) -> Result<()> {
    append_catalog_txn_observed(path, txn, metrics, |_| Ok(()))
}

pub(super) fn append_catalog_txn_observed(
    path: &Path,
    txn: &CatalogTxn,
    metrics: Option<&super::super::metrics::work::ReadWorkMetrics>,
    observe_capacity: impl FnOnce(usize) -> Result<()>,
) -> Result<()> {
    let mut encoded = serde_json::to_vec(txn)
        .map_err(|error| invalid(format!("cannot encode local chunk catalog: {error}")))?;
    encoded.push(b'\n');
    // Reserve room for the optional separator before measuring actual capacity.
    encoded.reserve(1);
    observe_capacity(encoded.capacity())?;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(Error::from)?;
    let length = file.metadata().map_err(Error::from)?.len();
    if length != 0 {
        let mut last = [0_u8; 1];
        if read_positioned(&file, length - 1, &mut last)? != 1 {
            return Err(invalid("local catalog changed while preparing an append"));
        }
        if last[0] != b'\n' {
            // Recovery accepts a complete final JSON transaction without its
            // newline. Preserve that transaction and separate the next one.
            encoded.insert(0, b'\n');
        }
    }
    {
        let _diagnostic = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("persist_catalog_write", encoded.len() as u64)
        });
        file.write_all(&encoded).map_err(Error::from)?;
    }
    {
        let _diagnostic = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("persist_catalog_sync", encoded.len() as u64)
        });
        file.sync_all().map_err(Error::from)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("local chunk catalog has no parent directory"))?;
    let _diagnostic =
        metrics.and_then(|metrics| metrics.coarse_diagnostic("persist_catalog_parent_sync", 0));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)
}

pub(super) fn catalog_checksum(revision: u64, records: &[LocalChunkRecord]) -> Result<String> {
    let encoded = serde_json::to_vec(&(revision, records))
        .map_err(|error| invalid(format!("cannot checksum local chunk catalog: {error}")))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

pub(super) fn sync_store_catalog(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_STORE_CATALOG_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::coded(
            afs_error::IO_OTHER,
            "injected startup catalog file barrier failure",
        ));
    }
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    STORE_CATALOG_SYNCS.with(|syncs| {
        if let Some(syncs) = syncs.borrow_mut().as_mut() {
            syncs.push(path.to_path_buf());
        }
    });
    Ok(())
}

pub(super) fn sync_store_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_STORE_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::coded(
            afs_error::IO_OTHER,
            "injected startup directory barrier failure",
        ));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    STORE_DIRECTORY_SYNCS.with(|syncs| {
        if let Some(syncs) = syncs.borrow_mut().as_mut() {
            syncs.push(path.to_path_buf());
        }
    });
    Ok(())
}
