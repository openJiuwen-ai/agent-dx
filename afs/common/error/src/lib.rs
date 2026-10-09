//! 协议无关错误合同，复用 main 的 code + kind + message 分层。
//! code 是稳定机器身份，kind 是通用处理类别，message 只供诊断，禁止解析消息决定行为。
//! Unavailable/超时不能证明写未发生；本模块不提供自动重试策略。
#![forbid(unsafe_code)]
mod code;
mod kind;
pub use code::*;
pub use kind::ErrorKind;
use std::fmt;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    code: ErrorCode,
    kind: ErrorKind,
    message: String,
}
impl Error {
    /// 用于解码远端未知错误；保留原始 code 和 kind，不以本地目录覆盖。
    pub fn new(code: ErrorCode, kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            code,
            kind,
            message: message.into(),
        }
    }
    /// 本地已登记错误使用目录中的默认分类，避免调用点随意改分类。
    pub fn coded(code: ErrorCode, message: impl Into<String>) -> Self {
        let kind = ERROR_CATALOG
            .iter()
            .find(|entry| entry.code == code)
            .map_or(ErrorKind::Unknown, |entry| entry.kind);
        Self::new(code, kind, message)
    }
    pub fn code(&self) -> ErrorCode {
        self.code
    }
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?}: {}", self.code, self.kind, self.message)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        if let Some(code) = linux_raw_errno_code(error.raw_os_error()) {
            return Self::coded(code, error.to_string());
        }
        use std::io::ErrorKind as K;
        let code = match error.kind() {
            K::NotFound => IO_NOT_FOUND,
            K::PermissionDenied => IO_PERMISSION_DENIED,
            K::AlreadyExists => IO_ALREADY_EXISTS,
            K::InvalidInput | K::InvalidData => IO_INVALID,
            K::TimedOut => IO_TIMEOUT,
            K::WouldBlock => IO_WOULD_BLOCK,
            K::Interrupted => IO_INTERRUPTED,
            K::ConnectionRefused
            | K::ConnectionReset
            | K::ConnectionAborted
            | K::NotConnected
            | K::BrokenPipe => IO_UNAVAILABLE,
            K::StorageFull | K::QuotaExceeded => IO_CAPACITY,
            K::OutOfMemory => IO_OUT_OF_MEMORY,
            K::NotADirectory => IO_NOT_DIRECTORY,
            K::IsADirectory => IO_IS_DIRECTORY,
            K::DirectoryNotEmpty => IO_DIRECTORY_NOT_EMPTY,
            K::CrossesDevices => IO_CROSS_DEVICE,
            _ => IO_OTHER,
        };
        Self::coded(code, error.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(target_os = "linux")]
fn linux_raw_errno_code(raw: Option<i32>) -> Option<ErrorCode> {
    match raw? {
        1 => Some(IO_OPERATION_NOT_PERMITTED),
        4 => Some(IO_INTERRUPTED),
        9 => Some(IO_BAD_FILE_DESCRIPTOR),
        11 => Some(IO_WOULD_BLOCK),
        27 => Some(IO_FILE_TOO_LARGE),
        35 => Some(IO_DEADLOCK),
        36 => Some(IO_NAME_TOO_LONG),
        37 => Some(IO_NO_LOCKS),
        40 => Some(IO_TOO_MANY_SYMLINKS),
        61 => Some(IO_NO_DATA),
        95 => Some(IO_NOT_SUPPORTED),
        _ => None,
    }
}

#[cfg(not(target_os = "linux"))]
fn linux_raw_errno_code(_raw: Option<i32>) -> Option<ErrorCode> {
    None
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_matches_unique_nonzero_numbered_constants() {
        let doc: toml::Value = toml::from_str(include_str!("../../../error-codes.toml")).unwrap();
        let rows = doc["error"].as_array().unwrap();
        assert_eq!(rows.len(), ERROR_CATALOG.len());
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let entry = ERROR_CATALOG
                .iter()
                .find(|e| Some(e.name) == row["name"].as_str())
                .unwrap();
            assert!(seen.insert(entry.code));
            for field in ["component", "subsystem", "description"] {
                assert!(!row[field].as_str().unwrap().is_empty());
            }
            assert_ne!(entry.code.raw() & 0xffff, 0);
            assert_eq!(
                u32::from_str_radix(row["value"].as_str().unwrap().trim_start_matches("0x"), 16)
                    .unwrap(),
                entry.code.raw()
            );
            assert_eq!(row["kind"].as_str().unwrap(), format!("{:?}", entry.kind));
        }
    }
    #[test]
    fn unknown_code_is_preserved() {
        let e = Error::new(
            ErrorCode::from_raw(0x7f010001),
            ErrorKind::Unavailable,
            "future",
        );
        assert_eq!(e.code().raw(), 0x7f010001);
        assert_eq!(e.kind(), ErrorKind::Unavailable);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn io_keeps_linux_raw_errno_for_xattr_and_operation_permissions() {
        for (raw, code) in [
            (1, IO_OPERATION_NOT_PERMITTED),
            (4, IO_INTERRUPTED),
            (9, IO_BAD_FILE_DESCRIPTOR),
            (11, IO_WOULD_BLOCK),
            (27, IO_FILE_TOO_LARGE),
            (35, IO_DEADLOCK),
            (36, IO_NAME_TOO_LONG),
            (37, IO_NO_LOCKS),
            (40, IO_TOO_MANY_SYMLINKS),
            (61, IO_NO_DATA),
            (95, IO_NOT_SUPPORTED),
        ] {
            let e = Error::from(std::io::Error::from_raw_os_error(raw));
            assert_eq!(e.code(), code);
        }
    }

    #[test]
    fn io_keeps_permission_capacity_and_type_categories() {
        for (io, code, kind) in [
            (
                std::io::ErrorKind::PermissionDenied,
                IO_PERMISSION_DENIED,
                ErrorKind::PermissionDenied,
            ),
            (
                std::io::ErrorKind::StorageFull,
                IO_CAPACITY,
                ErrorKind::ResourceExhausted,
            ),
            (
                std::io::ErrorKind::NotADirectory,
                IO_NOT_DIRECTORY,
                ErrorKind::FailedPrecondition,
            ),
            (
                std::io::ErrorKind::CrossesDevices,
                IO_CROSS_DEVICE,
                ErrorKind::FailedPrecondition,
            ),
        ] {
            let e = Error::from(std::io::Error::from(io));
            assert_eq!(e.code(), code);
            assert_eq!(e.kind(), kind);
        }
    }
}
