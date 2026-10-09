//! Node 内可复用的本地 I/O 基础机制。
//!
//! OwnerFs 普通文件和诊断对象使用这里的可变文件存储机制；DFS 的不可变
//! Chunk 由独立 ChunkStore 管理，可复用底层 I/O 原则但不共享 FileStore 语义。
//! 不拥有根授权、不可变发布、目录事务或统一恢复状态机；这些由具体后端决定。
//! 不强制每次 write 都 sync，也不强制分片或 Blob 化；成功/耐久标准来自调用业务。
//! 先保持 Node 内模块，未出现实际外部调用者前不拆通用存储 crate。

use std::{
    ffi::{OsStr, OsString},
    fmt,
    fs::{self, OpenOptions},
    io::{Read, Seek, Write},
    os::unix::ffi::{OsStrExt, OsStringExt},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

pub mod localfs;

pub use localfs::LocalFs;

pub const MAX_TRANSFER_BYTES: usize = 1024 * 1024;

/// Root-relative storage path used by concrete data backends.
///
/// This validates root-relative paths before a mutable disk backend sees them.
/// A storage path is always relative to its configured root, never
/// absolute, and never contains `.`/`..`, empty components, or NUL bytes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StoragePath {
    inner: PathBuf,
}

impl StoragePath {
    /// Accepts an empty path as the storage root. Byte-level validation is used
    /// because `Path::components()` normalizes forms that this layer must reject.
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let bytes = path.as_os_str().as_bytes();
        if bytes.is_empty() {
            return Ok(Self {
                inner: PathBuf::new(),
            });
        }
        if bytes.starts_with(b"/") || bytes.ends_with(b"/") {
            return Err(invalid_path());
        }
        for component in bytes.split(|byte| *byte == b'/') {
            if component.is_empty()
                || component == b"."
                || component == b".."
                || component.contains(&0)
            {
                return Err(invalid_path());
            }
        }
        Ok(Self {
            inner: path.to_path_buf(),
        })
    }

    #[must_use]
    pub fn root() -> Self {
        Self {
            inner: PathBuf::new(),
        }
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        self.inner.as_os_str().is_empty()
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.inner
    }

    /// Return a child path below this root-confined path.
    ///
    /// OwnerFs uses this to append a FUSE name to the already authorized root
    /// path. Validation is deliberately byte-level, matching Linux path rules:
    /// no empty name, slash, NUL, `.` or `..`.
    pub fn join_component(&self, component: &OsStr) -> std::io::Result<Self> {
        let bytes = component.as_bytes();
        if bytes.is_empty()
            || bytes == b"."
            || bytes == b".."
            || bytes.contains(&b'/')
            || bytes.contains(&0)
        {
            return Err(invalid_path());
        }
        let mut joined = self.inner.clone();
        joined.push(OsString::from_vec(bytes.to_vec()));
        Self::new(joined)
    }

    /// Append another validated relative storage path.
    pub fn join_path(&self, relative: &StoragePath) -> std::io::Result<Self> {
        if relative.is_root() {
            return Ok(self.clone());
        }
        let mut joined = self.inner.clone();
        joined.push(relative.as_path());
        Self::new(joined)
    }

    fn bytes(&self) -> &[u8] {
        self.inner.as_os_str().as_bytes()
    }
}

impl TryFrom<&Path> for StoragePath {
    type Error = std::io::Error;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<PathBuf> for StoragePath {
    type Error = std::io::Error;

    fn try_from(value: PathBuf) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Linux open flags and creation mode for a backend file open.
///
/// FUSE and P2P forwarding already speak Linux file semantics, so this contract
/// keeps those bits explicit. Backends may reject flags that cannot be honored
/// safely under the configured root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenSpec {
    pub flags: i32,
    pub mode: u32,
}

impl OpenSpec {
    #[must_use]
    pub const fn new(flags: i32, mode: u32) -> Self {
        Self { flags, mode }
    }
}

/// Rename semantics requested by higher layers.
///
/// LocalFs implements `Replace`. Modes that require Linux `renameat2` atomicity
/// are kept in the common contract but may return `Unsupported` without unsafe
/// syscalls or an added safe wrapper dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenameMode {
    Replace,
    NoReplace,
    Exchange,
}

/// A file opened once by the backend.
///
/// `read_at` and `write_at` return POSIX-style short counts. `flush` is only
/// userspace buffer flushing; durable boundaries are explicit `sync_data` and
/// `sync_all`, so native close/write paths do not accidentally become fsync-heavy.
pub trait FileHandle: Send + Sync {
    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> std::io::Result<usize>;
    fn write_at(&self, offset: u64, buffer: &[u8]) -> std::io::Result<usize>;
    fn metadata(&self) -> std::io::Result<fs::Metadata>;
    fn set_len(&self, size: u64) -> std::io::Result<()>;
    fn chmod(&self, mode: u32) -> std::io::Result<()>;
    fn chown(&self, uid: Option<u32>, gid: Option<u32>) -> std::io::Result<()>;
    fn set_times(
        &self,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> std::io::Result<()>;
    fn get_xattr(&self, name: &OsStr) -> std::io::Result<Vec<u8>>;
    fn list_xattr(&self) -> std::io::Result<Vec<u8>>;
    fn set_xattr(&self, name: &OsStr, value: &[u8], flags: i32) -> std::io::Result<()>;
    fn remove_xattr(&self, name: &OsStr) -> std::io::Result<()>;
    fn flush(&self) -> std::io::Result<()>;
    fn sync_data(&self) -> std::io::Result<()>;
    fn sync_all(&self) -> std::io::Result<()>;
}

/// Open directory handle used when durability belongs to directory metadata.
///
/// Root creation, deletion, and rename recovery need an explicit directory
/// fsync point. This trait exposes that without mixing ownership state into the
/// storage layer.
pub trait DirectoryHandle: Send + Sync {
    fn metadata(&self) -> std::io::Result<fs::Metadata>;
    fn read_dir(&self) -> std::io::Result<Vec<OsString>>;
    fn sync_all(&self) -> std::io::Result<()>;
}

/// Root-confined mutable file store used by OwnerFs and diagnostics.
///
/// This layer owns local path safety and file/directory durability primitives.
/// It does not know root ownership, cache revocation, image publication, or
/// strong file identity expectations; those remain backend-specific state.
pub trait FileStore: Send + Sync {
    type File: FileHandle;
    type Directory: DirectoryHandle;

    fn open_file(&self, path: &StoragePath, spec: OpenSpec) -> std::io::Result<Self::File>;
    fn open_dir(&self, path: &StoragePath) -> std::io::Result<Self::Directory>;
    fn metadata(&self, path: &StoragePath) -> std::io::Result<fs::Metadata>;
    fn read_link(&self, path: &StoragePath) -> std::io::Result<OsString>;
    fn symlink(&self, path: &StoragePath, target: &OsStr) -> std::io::Result<()>;
    fn hard_link(&self, from: &StoragePath, to: &StoragePath) -> std::io::Result<()>;
    fn chmod(&self, path: &StoragePath, mode: u32) -> std::io::Result<()>;
    fn chown(&self, path: &StoragePath, uid: Option<u32>, gid: Option<u32>) -> std::io::Result<()>;
    fn set_times(
        &self,
        path: &StoragePath,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> std::io::Result<()>;
    fn get_xattr(&self, path: &StoragePath, name: &OsStr) -> std::io::Result<Vec<u8>>;
    fn list_xattr(&self, path: &StoragePath) -> std::io::Result<Vec<u8>>;
    fn set_xattr(
        &self,
        path: &StoragePath,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> std::io::Result<()>;
    fn remove_xattr(&self, path: &StoragePath, name: &OsStr) -> std::io::Result<()>;
    fn read_dir(&self, path: &StoragePath) -> std::io::Result<Vec<OsString>>;
    fn mkdir(&self, path: &StoragePath, mode: u32) -> std::io::Result<()>;
    fn remove_file(&self, path: &StoragePath) -> std::io::Result<()>;
    fn remove_dir(&self, path: &StoragePath) -> std::io::Result<()>;
    fn rename(&self, from: &StoragePath, to: &StoragePath, mode: RenameMode)
    -> std::io::Result<()>;
    fn sync_root(&self) -> std::io::Result<()>;
}

fn invalid_path() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "storage path must be root-relative and must not contain empty, dot, dot-dot, slash-only, or NUL components",
    )
}

#[derive(Clone, Debug)]
/// 当前最小诊断存储：共同被节点数据 RPC 与本机 SDK Handler 调用。
/// 仅支持受限单文件名和有界范围 I/O；不解析根授权、不提供 POSIX inode/句柄语义。
pub struct Storage {
    root: Arc<PathBuf>,
}

#[derive(Debug)]
pub enum StorageError {
    BadName,
    TooLarge,
    Range,
    UnsafeFileType,
    Io(std::io::Error),
    Join(tokio::task::JoinError),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadName => formatter.write_str("name must be one trusted path component"),
            Self::TooLarge => formatter.write_str("transfer exceeds 1MiB"),
            Self::Range => formatter.write_str("requested range is outside the file"),
            Self::UnsafeFileType => formatter.write_str("target is not a regular file"),
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Join(error) => write!(formatter, "blocking storage task failed: {error}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<tokio::task::JoinError> for StorageError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::Join(value)
    }
}

impl Storage {
    pub fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        fs::create_dir_all(path.as_ref())?;
        Ok(Self {
            root: Arc::new(path.as_ref().canonicalize()?),
        })
    }

    /// 读取精确长度；越过 EOF 返回错误，不补零也不返回 POSIX 式短读。
    /// 这是当前数据面诊断合同；未来完整文件 read 的语义要由业务层明确。
    pub async fn read(
        &self,
        name: &str,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, StorageError> {
        let length = usize::try_from(length).map_err(|_| StorageError::TooLarge)?;
        if length > MAX_TRANSFER_BYTES {
            return Err(StorageError::TooLarge);
        }
        let path = self.resolve(name)?;
        tokio::task::spawn_blocking(move || read_blocking(path, offset, length)).await?
    }

    /// 完成普通文件 write_all 后返回长度，不隐式 sync_data/fsync。
    /// 因此成功表示文件 I/O 完成，不承诺进程外的掉电耐久或 Blob 版本发布。
    pub async fn write(
        &self,
        name: &str,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<usize, StorageError> {
        if data.len() > MAX_TRANSFER_BYTES {
            return Err(StorageError::TooLarge);
        }
        offset
            .checked_add(u64::try_from(data.len()).map_err(|_| StorageError::Range)?)
            .ok_or(StorageError::Range)?;
        let path = self.resolve(name)?;
        // std::fs 是阻塞 API。放到 Tokio blocking pool，使慢盘等待不占异步 RPC 执行线程。
        tokio::task::spawn_blocking(move || write_blocking(path, offset, data)).await?
    }

    fn resolve(&self, name: &str) -> Result<PathBuf, StorageError> {
        if !is_simple_component(name) {
            return Err(StorageError::BadName);
        }
        Ok(self.root.join(name))
    }
}

fn read_blocking(path: PathBuf, offset: u64, length: usize) -> Result<Vec<u8>, StorageError> {
    let mut file = open_regular(&path, false)?;
    let end = offset
        .checked_add(u64::try_from(length).map_err(|_| StorageError::Range)?)
        .ok_or(StorageError::Range)?;
    if end > file.metadata()?.len() {
        return Err(StorageError::Range);
    }
    file.seek(std::io::SeekFrom::Start(offset))?;
    let mut data = vec![0; length];
    file.read_exact(&mut data)?;
    Ok(data)
}

fn write_blocking(path: PathBuf, offset: u64, data: Vec<u8>) -> Result<usize, StorageError> {
    let mut file = open_regular(&path, true)?;
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&data)?;
    Ok(data.len())
}

// 打开时拒绝符号链接，打开后检查实际 FD 类型；不靠检查路径再打开的两步检查防护。
fn open_regular(path: &Path, write: bool) -> Result<std::fs::File, StorageError> {
    let mut options = OpenOptions::new();
    options
        .read(!write)
        .write(write)
        .create(write)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(StorageError::UnsafeFileType);
    }
    Ok(file)
}

fn is_simple_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.len() <= 255
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// 本地 SDK 与节点 P2P 共用，避免同一 I/O 错误随入口改变类别。
impl From<StorageError> for afs_error::Error {
    fn from(error: StorageError) -> Self {
        use afs_error::*;
        match error {
            StorageError::BadName | StorageError::TooLarge | StorageError::Range => {
                Error::coded(NODE_STORAGE_INVALID, error.to_string())
            }
            StorageError::UnsafeFileType => {
                Error::coded(NODE_STORAGE_UNSAFE_TYPE, error.to_string())
            }
            StorageError::Io(inner) if inner.kind() == std::io::ErrorKind::NotFound => {
                Error::coded(NODE_STORAGE_NOT_FOUND, inner.to_string())
            }
            StorageError::Io(inner) => {
                let mapped = Error::from(inner);
                if mapped.code() == IO_OTHER {
                    Error::coded(NODE_STORAGE_IO, mapped.message())
                } else {
                    mapped
                }
            }
            StorageError::Join(inner) => Error::coded(NODE_STORAGE_TASK_FAILED, inner.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writes_and_reads_exact_requested_range() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new(temp.path()).unwrap();

        assert_eq!(
            storage
                .write("alpha.bin", 0, b"abcdefgh".to_vec())
                .await
                .unwrap(),
            8
        );
        assert_eq!(storage.read("alpha.bin", 2, 4).await.unwrap(), b"cdef");
    }

    #[tokio::test]
    async fn rejects_paths_outside_single_component_scope() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new(temp.path()).unwrap();

        assert!(matches!(
            storage.write("../escape", 0, b"no".to_vec()).await,
            Err(StorageError::BadName)
        ));
        assert!(matches!(
            storage.write("nested/file", 0, b"no".to_vec()).await,
            Err(StorageError::BadName)
        ));
    }

    #[tokio::test]
    async fn rejects_reads_past_end_without_padding() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new(temp.path()).unwrap();

        storage
            .write("alpha.bin", 0, b"abcd".to_vec())
            .await
            .unwrap();

        assert!(matches!(
            storage.read("alpha.bin", 2, 4).await,
            Err(StorageError::Range)
        ));
    }
}
