//! Private service-side key files; workload credentials belong to ADX.
use crate::{valid_identifier, CryptoError, Keyring};
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("ADX service authorization unavailable or refused")]
    Authority,
    #[error("private service file is missing, unreadable or not private")]
    File,
    #[error("invalid private service configuration")]
    Invalid,
    #[error("credential cryptography unavailable")]
    Crypto(#[from] CryptoError),
}

/// Rejects symlinks and non-private Unix files. Windows deployments must set an owner-only ACL.
pub fn read_private(path: &Path, limit: u64) -> Result<Zeroizing<Vec<u8>>, StorageError> {
    let before = fs::symlink_metadata(path).map_err(|_| StorageError::File)?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err(StorageError::File);
    }
    let file = File::open(path).map_err(|_| StorageError::File)?;
    let metadata = file.metadata().map_err(|_| StorageError::File)?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(StorageError::File);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0
            || before.dev() != metadata.dev()
            || before.ino() != metadata.ino()
        {
            return Err(StorageError::File);
        }
    }
    let mut data = Zeroizing::new(Vec::new());
    file.take(limit + 1)
        .read_to_end(&mut data)
        .map_err(|_| StorageError::File)?;
    if data.len() as u64 > limit {
        return Err(StorageError::File);
    }
    Ok(data)
}

pub fn write_private(path: &Path, data: &[u8], replace: bool) -> Result<(), StorageError> {
    let parent = path.parent().ok_or(StorageError::File)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|_| StorageError::File)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| StorageError::File)?;
    }
    temporary
        .write_all(data)
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|_| StorageError::File)?;
    if replace {
        temporary.persist(path).map_err(|_| StorageError::File)?;
    } else {
        temporary
            .persist_noclobber(path)
            .map_err(|_| StorageError::File)?;
    }
    #[cfg(unix)]
    {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|_| StorageError::File)?;
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KeyFile {
    pub active_key_id: String,
    pub keys: BTreeMap<String, String>,
}

impl KeyFile {
    /// Adds a fresh active KEK while retaining older keys for opening and rewrapping existing envelopes.
    pub fn rotate(path: &Path, id: String) -> Result<(), StorageError> {
        if !valid_identifier(&id) {
            return Err(StorageError::Invalid);
        }
        let lock_path = path.with_extension("lock");
        if lock_path.is_symlink() {
            return Err(StorageError::File);
        }
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(lock_path).map_err(|_| StorageError::File)?;
        lock.lock().map_err(|_| StorageError::File)?;
        Self::load(path)?;
        let mut config: Self = serde_json::from_slice(&read_private(path, 65_536)?)
            .map_err(|_| StorageError::Invalid)?;
        if config.keys.contains_key(&id) {
            return Err(StorageError::Invalid);
        }
        let mut key = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(key.as_mut())
            .map_err(|_| StorageError::Invalid)?;
        config
            .keys
            .insert(id.clone(), STANDARD.encode(key.as_ref()));
        config.active_key_id = id;
        let encoded =
            Zeroizing::new(serde_json::to_vec_pretty(&config).map_err(|_| StorageError::Invalid)?);
        write_private(path, &encoded, true)
    }
    pub fn load(path: &Path) -> Result<Keyring, StorageError> {
        let mut config: Self = serde_json::from_slice(&read_private(path, 65_536)?)
            .map_err(|_| StorageError::Invalid)?;
        let mut keys = BTreeMap::new();
        for (id, encoded) in &mut config.keys {
            let decoded = Zeroizing::new(
                STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|_| StorageError::Invalid)?,
            );
            keys.insert(
                id.clone(),
                decoded
                    .as_slice()
                    .try_into()
                    .map_err(|_| StorageError::Invalid)?,
            );
            zeroize::Zeroize::zeroize(encoded);
        }
        Ok(Keyring::new(config.active_key_id, keys)?)
    }
    pub fn initialize(path: &Path, id: String) -> Result<(), StorageError> {
        if !valid_identifier(&id) {
            return Err(StorageError::Invalid);
        }
        let mut key = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(key.as_mut())
            .map_err(|_| StorageError::Invalid)?;
        let config = Self {
            active_key_id: id.clone(),
            keys: BTreeMap::from([(id, STANDARD.encode(key.as_ref()))]),
        };
        let encoded =
            Zeroizing::new(serde_json::to_vec_pretty(&config).map_err(|_| StorageError::Invalid)?);
        write_private(path, &encoded, false)
    }
}
