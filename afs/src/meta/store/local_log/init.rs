//! Marked initialization of a new local log directory.

use super::*;

/// The durable empty marker precedes every initialization file. No business
/// append or listener is admitted until it is removed and the directory synced.
pub(super) fn initialize_marked(
    dir: &Path,
    limits: LocalCheckpointLimits,
) -> Result<StoreIdentity> {
    let marker = open_file(&dir.join("INITIALIZING"), false)?;
    if marker.metadata()?.len() != 0 {
        return Err(invalid_log("initialization marker is not empty"));
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if !matches!(
            name.to_str(),
            Some(
                "LOCK"
                    | "INITIALIZING"
                    | "IDENTITY"
                    | "IDENTITY.tmp"
                    | "CURRENT"
                    | "CURRENT.tmp"
                    | "wal-00000000000000000001.log"
            )
        ) || !fs::symlink_metadata(entry.path())?.is_file()
        {
            return Err(invalid_log(
                "unknown data in unfinished initialization; originals retained",
            ));
        }
    }
    let identity = if dir.join(IDENTITY).try_exists()? {
        if dir.join("IDENTITY.tmp").try_exists()? {
            return Err(invalid_log("ambiguous initialization identity files"));
        }
        read_identity(dir)?
    } else {
        // Identity is installed before any WAL or selector can be created.
        if dir.join(WAL).try_exists()?
            || dir.join(CURRENT).try_exists()?
            || dir.join("CURRENT.tmp").try_exists()?
        {
            return Err(invalid_log("initialization data precedes durable identity"));
        }
        let tmp = dir.join("IDENTITY.tmp");
        let identity = if tmp.try_exists()? {
            let mut file = open_file(&tmp, false)?;
            let length = file.metadata()?.len();
            if length == 90 {
                read_identity_file(&tmp)?
            } else {
                if length > 90 {
                    return Err(invalid_log("initial identity temporary length"));
                }
                let mut bytes = vec![0; length as usize];
                file.read_exact(&mut bytes)?;
                let n = bytes.len().min(6);
                if bytes[..n] != format::META_IDENTITY_HEADER[..n] {
                    return Err(invalid_log("foreign partial identity temporary retained"));
                }
                // The marker and absence of all data prove this is only an
                // interrupted identity write, never an acknowledged authority.
                drop(file);
                fs::remove_file(&tmp)?;
                StoreIdentity::new()?
            }
        } else {
            StoreIdentity::new()?
        };
        if !tmp.try_exists()? {
            let mut file = open_file(&tmp, true)?;
            file.write_all(&identity_bytes(identity))?;
            file.sync_all()?;
        } else {
            open_file(&tmp, false)?.sync_all()?;
        }
        fs::rename(&tmp, dir.join(IDENTITY))?;
        sync_dir(dir)?;
        identity
    };
    let mut expected = [0u8; LOG_HEADER_BYTES as usize];
    expected[..STATIC_BYTES].copy_from_slice(&static_header(identity, LogTip::EMPTY));
    expected[STATIC_BYTES..STATIC_BYTES + SLOT_BYTES].copy_from_slice(&tail_bytes(Tail {
        generation: 0,
        tip: LogTip::EMPTY,
        end: LOG_HEADER_BYTES,
    }));
    let mut wal = if dir.join(WAL).try_exists()? {
        let mut file = open_file(&dir.join(WAL), false)?;
        let length = file.metadata()?.len();
        if length > LOG_HEADER_BYTES {
            return Err(invalid_log(
                "nonempty WAL in unfinished initialization retained",
            ));
        }
        let mut prefix = vec![0; length as usize];
        file.read_exact(&mut prefix)?;
        if prefix != expected[..prefix.len()] {
            return Err(invalid_log(
                "unfinished initial WAL identity/header mismatch",
            ));
        }
        file.seek(SeekFrom::Start(0))?;
        file
    } else {
        open_file(&dir.join(WAL), true)?
    };
    let initial = Current::initial(identity);
    let current = dir.join(CURRENT);
    if current.try_exists()? && read_current(dir, identity)? != Some(initial.clone()) {
        return Err(invalid_log(
            "noninitial CURRENT under initialization marker retained",
        ));
    }
    let temporary = dir.join("CURRENT.tmp");
    if temporary.try_exists()? {
        let expected = encode_current(&initial)?;
        let mut file = open_file(&temporary, false)?;
        let length = file.metadata()?.len();
        if length > expected.len() as u64 {
            return Err(invalid_log("initial CURRENT temporary length"));
        }
        let mut prefix = vec![0; length as usize];
        file.read_exact(&mut prefix)?;
        if prefix != expected[..prefix.len()] {
            return Err(invalid_log("foreign initial CURRENT temporary retained"));
        }
        drop(file);
        fs::remove_file(&temporary)?;
    }
    wal.write_all(&expected)?;
    wal.sync_all()?;
    sync_dir(dir)?;
    #[cfg(test)]
    super::super::crash_tests::pause(
        "initialization_wal_installed",
        "identity and empty WAL durable; initialization incomplete",
    );
    if !current.try_exists()? {
        write_current(dir, &initial, limits)?;
    }
    open_file(&current, false)?.sync_all()?;
    sync_dir(dir)?;
    fs::remove_file(dir.join("INITIALIZING"))?;
    sync_dir(dir)?;
    Ok(identity)
}
