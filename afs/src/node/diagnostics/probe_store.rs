//! 诊断对象存储：`/v1/diagnostics` 与 UDS `LocalData` 读写的单文件名对象目录
//! （`data_dir/diagnostics`）。不是 OwnerFs/DFS 文件，不参与任何文件系统的数据面。

use std::{
    fmt, fs,
    fs::OpenOptions,
    io::{Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::node::storage::MAX_TRANSFER_BYTES;

#[derive(Clone, Debug)]
/// 当前最小诊断存储：共同被节点数据 RPC 与本机 SDK Handler 调用。
/// 仅支持受限单文件名和有界范围 I/O；不解析根授权、不提供 POSIX inode/句柄语义。
pub struct DiagnosticStorage {
    root: Arc<PathBuf>,
}

#[derive(Debug)]
pub enum DiagnosticStorageError {
    BadName,
    TooLarge,
    Range,
    UnsafeFileType,
    Io(std::io::Error),
    Join(tokio::task::JoinError),
}

impl fmt::Display for DiagnosticStorageError {
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

impl std::error::Error for DiagnosticStorageError {}

impl From<std::io::Error> for DiagnosticStorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<tokio::task::JoinError> for DiagnosticStorageError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::Join(value)
    }
}

impl DiagnosticStorage {
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
    ) -> Result<Vec<u8>, DiagnosticStorageError> {
        let length = usize::try_from(length).map_err(|_| DiagnosticStorageError::TooLarge)?;
        if length > MAX_TRANSFER_BYTES {
            return Err(DiagnosticStorageError::TooLarge);
        }
        let path = self.resolve(name)?;
        crate::node::bridge::spawn_blocking(move || read_blocking(path, offset, length)).await?
    }

    /// 完成普通文件 write_all 后返回长度，不隐式 sync_data/fsync。
    /// 因此成功表示文件 I/O 完成，不承诺进程外的掉电耐久或 Blob 版本发布。
    pub async fn write(
        &self,
        name: &str,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<usize, DiagnosticStorageError> {
        if data.len() > MAX_TRANSFER_BYTES {
            return Err(DiagnosticStorageError::TooLarge);
        }
        offset
            .checked_add(u64::try_from(data.len()).map_err(|_| DiagnosticStorageError::Range)?)
            .ok_or(DiagnosticStorageError::Range)?;
        let path = self.resolve(name)?;
        // std::fs 是阻塞 API。放到 Tokio blocking pool，使慢盘等待不占异步 RPC 执行线程。
        crate::node::bridge::spawn_blocking(move || write_blocking(path, offset, data)).await?
    }

    fn resolve(&self, name: &str) -> Result<PathBuf, DiagnosticStorageError> {
        if !is_simple_component(name) {
            return Err(DiagnosticStorageError::BadName);
        }
        Ok(self.root.join(name))
    }
}

fn read_blocking(
    path: PathBuf,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, DiagnosticStorageError> {
    let mut file = open_regular(&path, false)?;
    let end = offset
        .checked_add(u64::try_from(length).map_err(|_| DiagnosticStorageError::Range)?)
        .ok_or(DiagnosticStorageError::Range)?;
    if end > file.metadata()?.len() {
        return Err(DiagnosticStorageError::Range);
    }
    file.seek(std::io::SeekFrom::Start(offset))?;
    let mut data = vec![0; length];
    file.read_exact(&mut data)?;
    Ok(data)
}

fn write_blocking(
    path: PathBuf,
    offset: u64,
    data: Vec<u8>,
) -> Result<usize, DiagnosticStorageError> {
    let mut file = open_regular(&path, true)?;
    file.seek(std::io::SeekFrom::Start(offset))?;
    file.write_all(&data)?;
    Ok(data.len())
}

// 打开时拒绝符号链接，打开后检查实际 FD 类型；不靠检查路径再打开的两步检查防护。
fn open_regular(path: &Path, write: bool) -> Result<std::fs::File, DiagnosticStorageError> {
    let mut options = OpenOptions::new();
    options
        .read(!write)
        .write(write)
        .create(write)
        .truncate(false)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(DiagnosticStorageError::UnsafeFileType);
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
impl From<DiagnosticStorageError> for afs_error::Error {
    fn from(error: DiagnosticStorageError) -> Self {
        use afs_error::*;
        match error {
            DiagnosticStorageError::BadName
            | DiagnosticStorageError::TooLarge
            | DiagnosticStorageError::Range => {
                Error::coded(NODE_STORAGE_INVALID, error.to_string())
            }
            DiagnosticStorageError::UnsafeFileType => {
                Error::coded(NODE_STORAGE_UNSAFE_TYPE, error.to_string())
            }
            DiagnosticStorageError::Io(inner) if inner.kind() == std::io::ErrorKind::NotFound => {
                Error::coded(NODE_STORAGE_NOT_FOUND, inner.to_string())
            }
            DiagnosticStorageError::Io(inner) => {
                let mapped = Error::from(inner);
                if mapped.code() == IO_OTHER {
                    Error::coded(NODE_STORAGE_IO, mapped.message())
                } else {
                    mapped
                }
            }
            DiagnosticStorageError::Join(inner) => {
                Error::coded(NODE_STORAGE_TASK_FAILED, inner.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writes_and_reads_exact_requested_range() {
        let temp = tempfile::tempdir().unwrap();
        let storage = DiagnosticStorage::new(temp.path()).unwrap();

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
        let storage = DiagnosticStorage::new(temp.path()).unwrap();

        assert!(matches!(
            storage.write("../escape", 0, b"no".to_vec()).await,
            Err(DiagnosticStorageError::BadName)
        ));
        assert!(matches!(
            storage.write("nested/file", 0, b"no".to_vec()).await,
            Err(DiagnosticStorageError::BadName)
        ));
    }

    #[tokio::test]
    async fn rejects_reads_past_end_without_padding() {
        let temp = tempfile::tempdir().unwrap();
        let storage = DiagnosticStorage::new(temp.path()).unwrap();

        storage
            .write("alpha.bin", 0, b"abcd".to_vec())
            .await
            .unwrap();

        assert!(matches!(
            storage.read("alpha.bin", 2, 4).await,
            Err(DiagnosticStorageError::Range)
        ));
    }
}
