//! Persisted outboard files beside local Chunks.

use super::*;

static OUTBOARD_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(super) fn load_outboard_file(
    path: &Path,
    chunk: &ChunkObject,
    budget: &Arc<OutboardBudget>,
) -> Result<SharedOutboard> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let length = usize::try_from(file.metadata().map_err(Error::from)?.len())
        .map_err(|_| invalid("outboard file length too large"))?;
    if length > MAX_ENCODED_OUTBOARD_BYTES {
        return Err(corrupt(&chunk.id, "oversized outboard"));
    }
    let lease = budget
        .try_reserve(ChunkOutboard::resident_charge(MAX_ENCODED_OUTBOARD_BYTES)?)
        .ok_or_else(|| {
            Error::coded(afs_error::IO_CAPACITY, "outboard metadata budget exhausted")
        })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "outboard allocation refused"))?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes).map_err(Error::from)?;
    if file.metadata().map_err(Error::from)?.len() != length as u64 {
        return Err(corrupt(&chunk.id, "outboard length changed"));
    }
    ChunkOutboard::from_encoded(chunk, bytes, lease)
}

pub(super) fn outboard_capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "outboard range working capacity exhausted",
    )
}

// A healthy sidecar is disposable metadata: compare every byte before reuse,
// without adding its optional file barrier back to a durable body ACK.
pub(super) fn outboard_file_matches(path: &Path, proof: &SharedOutboard) -> Result<bool> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    if !metadata.is_file() || metadata.len() != proof.encoded().len() as u64 {
        return Ok(false);
    }
    let mut scratch = [0; 1024];
    for expected in proof.encoded().chunks(scratch.len()) {
        let observed = &mut scratch[..expected.len()];
        file.read_exact(observed).map_err(Error::from)?;
        if observed != expected {
            return Ok(false);
        }
    }
    Ok(file.metadata().map_err(Error::from)?.len() == proof.encoded().len() as u64)
}

pub(super) fn persist_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    install_outboard_file(path, proof)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    sync_outboard_directory(parent)
}

pub(super) fn install_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    #[cfg(test)]
    OUTBOARD_INSTALL_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    let sequence = OUTBOARD_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{}.{}.tmp", std::process::id(), sequence));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(Error::from)?;
        file.write_all(proof.encoded()).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().writes += 1);
        file.sync_all().map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().file_syncs += 1);
        fs::rename(&temp, path).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().renames += 1);
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

pub(super) fn sync_outboard_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_OUTBOARD_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::from(std::io::Error::other(
            "injected outboard directory-sync failure",
        )));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().directory_syncs += 1);
    Ok(())
}
