//! Node 内可复用的本地 I/O 基础机制。
//!
//! OwnerFs 普通文件使用这里的可变文件存储机制（诊断对象存储在
//! `node::diagnostics::probe_store`，只复用 `MAX_TRANSFER_BYTES`）；DFS 的不可变
//! Chunk 由独立 ChunkStore 管理，可复用底层 I/O 原则但不共享 FileStore 语义。
//! 不拥有根授权、不可变发布、目录事务或统一恢复状态机；这些由具体后端决定。
//! 不强制每次 write 都 sync，也不强制分片或 Blob 化；成功/耐久标准来自调用业务。
//! 先保持 Node 内模块，未出现实际外部调用者前不拆通用存储 crate。

use std::{
    ffi::{OsStr, OsString},
    fs,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
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

    /// Report whether one direct child exists, without re-walking the parent
    /// path. Backends that cannot resolve a child relative to an open directory
    /// return `Unsupported`, and callers fall back to a path-based stat.
    fn child_present(&self, _name: &std::ffi::OsStr) -> std::io::Result<bool> {
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }
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
