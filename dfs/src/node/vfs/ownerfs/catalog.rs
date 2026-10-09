//! OwnerFs 本机根身份记录与启动时独占锁的合同。
//!
//! A 重启不能把“磁盘上有同名目录”当作重新获得根授权的证据。第一次创建根时
//! 必须在该目录写入并同步下方的身份记录；启动时在独占锁下扫描，再逐根与 Meta
//! 的持久记录核对。这里的默认实现把记录保存在 LocalFs 根下的 `.ownerfs-roots/`，
//! 并用一个进程生命周期 `flock` 防止两个 afs-node 同时操作同一份普通文件。

use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::ffi::{OsStrExt, OsStringExt},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

use super::root::RootId;
use crate::node::storage::{FileStore, LocalFs, StoragePath};

const CATALOG_DIR: &str = ".ownerfs-roots";
const CATALOG_LOCK: &str = ".ownerfs-root-catalog.lock";
const RECORD_VERSION: u32 = 1;
static NEXT_TEMP_RECORD: AtomicU64 = AtomicU64::new(0);

/// 一个本机候选根的持久身份；不是访问授权。
///
/// `local_prepare_id` 必须与首次 ActivateRoot 时 Meta 持久保存的值相同。
/// 如果进程恰好在 Meta 激活后、本地标记完成前崩溃，这条记录仍可供对账；
/// 只有 Meta 确认它是当前 Home 且 epoch 相同，才允许恢复。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRootRecord {
    pub id: RootId,
    pub name: OsString,
    pub epoch: u64,
    pub data_dir: StoragePath,
    pub local_prepare_id: String,
}

/// 持有本机数据目录排他锁的扫描结果。
///
/// 锁必须跨越整个 afs-node 进程生命周期；旧进程若只是卡住而未退出，新进程
/// 不能和它同时操作同一份普通文件。扫描只解码持久身份；物理目录是否存在要
/// 与 Meta 的 active/pending 状态一起判断，因为 pending 清理中可能已删除目录。
pub trait LockedLocalRootCatalog: Send + Sync {
    /// 首次创建根时先持久写入身份记录并同步目录，随后才可调 Meta ActivateRoot。
    /// 返回成功必须满足进程重启可重新扫描；它不表示 Meta 已授予访问权。
    /// 相同 root/epoch/prepare_id 重试应幂等，冲突记录必须拒绝而不能覆盖。
    fn persist_prepared_root(&self, record: &LocalRootRecord) -> Result<()>;

    /// Meta ActivateRoot 或本地准备失败后清理未进入热路径的准备记录。
    /// 删除失败时调用方应 fail-closed；留下的记录会在下次启动恢复时被 Meta 判定。
    fn remove_prepared_root(&self, record: &LocalRootRecord) -> Result<()>;

    fn scan_roots(&self) -> Result<Vec<LocalRootRecord>>;
}

/// 由 OwnerFs 实现的本机持久记录接口，不属于通用 Storage。
///
/// `lock_and_open` 成功后，调用方应一直保留返回的 guard；失败则不得挂载或
/// 接收该数据目录的请求。
pub trait LocalRootCatalog: Send + Sync {
    fn lock_and_open(&self) -> Result<Box<dyn LockedLocalRootCatalog>>;
}

/// LocalFs-backed root catalog used by production OwnerFs.
///
/// It intentionally stores only root identity and prepare evidence. It does not
/// store per-file metadata, open handles, or cache state; those are rebuilt from
/// ordinary files plus Meta grants after restart.
pub struct LocalFsRootCatalog {
    disk: Arc<LocalFs>,
}

impl LocalFsRootCatalog {
    #[must_use]
    pub fn new(disk: Arc<LocalFs>) -> Self {
        Self { disk }
    }
}

impl LocalRootCatalog for LocalFsRootCatalog {
    fn lock_and_open(&self) -> Result<Box<dyn LockedLocalRootCatalog>> {
        fs::create_dir_all(self.disk.root_path()).map_err(Error::from)?;
        let lock_path = self.disk.root_path().join(CATALOG_LOCK);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(Error::from)?;
        flock_exclusive(&lock)?;
        let locked = LockedLocalFsRootCatalog {
            disk: self.disk.clone(),
            _lock: lock,
            mutation: Mutex::new(()),
        };
        locked.ensure_dir()?;
        Ok(Box::new(locked))
    }
}

struct LockedLocalFsRootCatalog {
    disk: Arc<LocalFs>,
    _lock: File,
    mutation: Mutex<()>,
}

impl LockedLocalFsRootCatalog {
    fn ensure_dir(&self) -> Result<()> {
        let path = StoragePath::new(CATALOG_DIR).map_err(Error::from)?;
        match self.disk.mkdir(&path, 0o700) {
            Ok(()) => self.disk.sync_root().map_err(Error::from),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(Error::from(error)),
        }
    }

    fn record_path(id: &RootId) -> Result<StoragePath> {
        let filename = format!("{}.json", id.0);
        StoragePath::new(CATALOG_DIR)
            .and_then(|base| base.join_component(std::ffi::OsStr::new(&filename)))
            .map_err(Error::from)
    }

    fn read_record(&self, path: &StoragePath) -> Result<LocalRootRecord> {
        let absolute = self.disk.root_path().join(path.as_path());
        let mut bytes = Vec::new();
        File::open(absolute)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(Error::from)?;
        let wire: WireRootRecord = serde_json::from_slice(&bytes).map_err(catalog_decode_error)?;
        wire.try_into_record()
    }

    fn write_record(&self, record: &LocalRootRecord) -> Result<()> {
        let final_path = Self::record_path(&record.id)?;
        let wire = WireRootRecord::from_record(record);
        let bytes = serde_json::to_vec_pretty(&wire).map_err(catalog_encode_error)?;
        // Root/prepare IDs may contain several copies of the user name and
        // session. Keep the temporary *basename* fixed-size so an ordinary
        // workspace name cannot exceed the backing filesystem's NAME_MAX.
        // create_new also avoids overwriting a stale temp file after a crash.
        let (tmp_path, mut file) = loop {
            let serial = NEXT_TEMP_RECORD.fetch_add(1, Ordering::Relaxed);
            let name = format!(".record-{}-{serial}.tmp", std::process::id());
            let tmp_path = StoragePath::new(CATALOG_DIR)
                .and_then(|base| base.join_component(std::ffi::OsStr::new(&name)))
                .map_err(Error::from)?;
            let absolute = self.disk.root_path().join(tmp_path.as_path());
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(absolute)
            {
                Ok(file) => break (tmp_path, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(Error::from(error)),
            }
        };
        if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
            let _ = self.disk.remove_file(&tmp_path);
            return Err(Error::from(error));
        }
        drop(file);
        self.disk
            .rename(
                &tmp_path,
                &final_path,
                crate::node::storage::RenameMode::Replace,
            )
            .map_err(Error::from)?;
        self.disk.sync_root().map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_workspace_name_does_not_expand_catalog_temp_basename() {
        let temp = tempfile::tempdir().unwrap();
        let disk = Arc::new(LocalFs::open(temp.path()).unwrap());
        let catalog = LocalFsRootCatalog::new(disk.clone());
        let locked = catalog.lock_and_open().unwrap();
        let name = "bench-private-v10";
        let id = RootId(format!("root-{}", hex(name.as_bytes())));
        let data_dir = StoragePath::new(format!("{}-e1", id.0)).unwrap();
        disk.mkdir(&data_dir, 0o700).unwrap();
        let session = "12345678-1234-1234-1234-123456789abc";
        let record = LocalRootRecord {
            id: id.clone(),
            name: OsString::from(name),
            epoch: 1,
            data_dir,
            local_prepare_id: format!("node-a-afs-acc-01:{session}:{}:{session}", id.0),
        };
        locked.persist_prepared_root(&record).unwrap();
        assert_eq!(locked.scan_roots().unwrap(), vec![record]);
    }
}

impl LockedLocalRootCatalog for LockedLocalFsRootCatalog {
    fn persist_prepared_root(&self, record: &LocalRootRecord) -> Result<()> {
        let _mutation = self.mutation.lock().map_err(|_| {
            Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "root catalog lock poisoned",
            )
        })?;
        self.ensure_dir()?;
        let final_path = Self::record_path(&record.id)?;
        match self.read_record(&final_path) {
            Ok(existing) if existing == *record => return Ok(()),
            Ok(_) => {
                return Err(Error::coded(
                    afs_error::NODE_OWNER_INVALID_GRANT,
                    "conflicting local root record already exists",
                ));
            }
            Err(error) if error.code() == afs_error::IO_NOT_FOUND => {}
            Err(error) => return Err(error),
        }
        if !self
            .disk
            .metadata(&record.data_dir)
            .map_err(Error::from)?
            .is_dir()
        {
            return Err(Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "local root record points to a non-directory",
            ));
        }
        self.write_record(record)
    }

    fn remove_prepared_root(&self, record: &LocalRootRecord) -> Result<()> {
        let _mutation = self.mutation.lock().map_err(|_| {
            Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "root catalog lock poisoned",
            )
        })?;
        let path = Self::record_path(&record.id)?;
        // A delayed cleanup from an older reserve must not remove a newer
        // record for the same root name. Treat a missing file as idempotent,
        // but require the complete durable identity before deleting one.
        match self.read_record(&path) {
            Ok(existing) if existing == *record => {}
            Ok(_) => {
                return Err(Error::coded(
                    afs_error::NODE_OWNER_INVALID_GRANT,
                    "local root record changed before prepared cleanup",
                ));
            }
            Err(error) if error.code() == afs_error::IO_NOT_FOUND => return Ok(()),
            Err(error) => return Err(error),
        }
        match self.disk.remove_file(&path) {
            Ok(()) => self.disk.sync_root().map_err(Error::from),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::from(error)),
        }
    }

    fn scan_roots(&self) -> Result<Vec<LocalRootRecord>> {
        self.ensure_dir()?;
        let dir = StoragePath::new(CATALOG_DIR).map_err(Error::from)?;
        let mut records = Vec::new();
        for name in self.disk.read_dir(&dir).map_err(Error::from)? {
            let bytes = name.as_bytes();
            if !bytes.ends_with(b".json") {
                continue;
            }
            let path = dir.join_component(&name).map_err(Error::from)?;
            let record = self.read_record(&path)?;
            records.push(record);
        }
        records.sort_by(|left, right| left.id.0.cmp(&right.id.0));
        Ok(records)
    }
}

#[derive(Deserialize, Serialize)]
struct WireRootRecord {
    version: u32,
    root_id: String,
    name_hex: String,
    epoch: u64,
    data_dir: String,
    local_prepare_id: String,
}

impl WireRootRecord {
    fn from_record(record: &LocalRootRecord) -> Self {
        Self {
            version: RECORD_VERSION,
            root_id: record.id.0.clone(),
            name_hex: hex(record.name.as_bytes()),
            epoch: record.epoch,
            data_dir: record
                .data_dir
                .as_path()
                .as_os_str()
                .to_string_lossy()
                .into_owned(),
            local_prepare_id: record.local_prepare_id.clone(),
        }
    }

    fn try_into_record(self) -> Result<LocalRootRecord> {
        if self.version != RECORD_VERSION {
            return Err(Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "unsupported local root record version",
            ));
        }
        Ok(LocalRootRecord {
            id: RootId(self.root_id),
            name: OsString::from_vec(unhex(&self.name_hex)?),
            epoch: self.epoch,
            data_dir: StoragePath::new(self.data_dir).map_err(Error::from)?,
            local_prepare_id: self.local_prepare_id,
        })
    }
}

fn flock_exclusive(file: &File) -> Result<()> {
    file.try_lock().map_err(|error| {
        Error::coded(
            afs_error::IO_UNAVAILABLE,
            format!("local root catalog is already locked: {error}"),
        )
    })
}

fn catalog_decode_error(error: serde_json::Error) -> Error {
    Error::coded(
        afs_error::IO_INVALID,
        format!("invalid local root record: {error}"),
    )
}

fn catalog_encode_error(error: serde_json::Error) -> Error {
    Error::coded(
        afs_error::RUNTIME_INTERNAL,
        format!("encode local root record failed: {error}"),
    )
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn unhex(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(Error::coded(afs_error::IO_INVALID, "odd-length hex string"));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        let high = from_hex_digit(chunk[0])?;
        let low = from_hex_digit(chunk[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn from_hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::coded(afs_error::IO_INVALID, "invalid hex digit")),
    }
}
