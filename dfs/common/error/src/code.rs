//! 稳定机器错误身份：0xCCSSRRRR = 组件 / 子系统 / 原因。
//! 沿用 main 已有身份；删除的旧业务编号保留，不重新分配。未知未来编号原样保留。
use crate::ErrorKind;
use std::fmt;
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ErrorCode(u32);
impl ErrorCode {
    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }
    pub const fn raw(self) -> u32 {
        self.0
    }
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:08x}", self.0)
    }
}
/// 可供文档、测试和日志工具枚举；默认分类不等于自动重试策略。
pub struct CodeDefinition {
    pub name: &'static str,
    pub code: ErrorCode,
    pub kind: ErrorKind,
}
macro_rules! catalog {
    ($($name:ident = $value:literal => $kind:ident;)+) => {
        $(pub const $name: ErrorCode = ErrorCode::from_raw($value);)+
        pub const ERROR_CATALOG: &[CodeDefinition] = &[$(CodeDefinition { name: stringify!($name), code: $name, kind: ErrorKind::$kind },)+];
    }
}
catalog! {
    CLIENT_ARGUMENT_INVALID = 0x01010001 => InvalidArgument;
    CLIENT_CONNECTION_UNAVAILABLE = 0x01020001 => Unavailable;
    CLIENT_DEADLINE_EXCEEDED = 0x01020002 => DeadlineExceeded;
    CLIENT_PROTOCOL_INVALID_ERROR_DETAIL = 0x01030001 => Internal;
    CLIENT_PROTOCOL_VIOLATION = 0x01030002 => Internal;
    CLIENT_REMOTE_STATUS = 0x01030003 => Unknown;
    CLIENT_PERMISSION_DENIED = 0x01040001 => PermissionDenied;
    CLIENT_SHM_UNAVAILABLE = 0x01050001 => Unavailable;
    CLIENT_WORKER_FAILED = 0x01050002 => Internal;
    CLIENT_CLOSED = 0x01050003 => FailedPrecondition;
    NODE_TRANSFER_UNSUPPORTED = 0x02040001 => Unimplemented;
    NODE_TRANSFER_CORRUPT_DATA = 0x02040002 => DataLoss;
    NODE_TRANSFER_UNAVAILABLE = 0x02040003 => Unavailable;
    NODE_TRANSFER_INVALID = 0x02040004 => InvalidArgument;
    NODE_TRANSFER_INTERNAL = 0x02040005 => Internal;
    NODE_STORAGE_INVALID = 0x02070001 => InvalidArgument;
    NODE_STORAGE_NOT_FOUND = 0x02070002 => NotFound;
    NODE_STORAGE_UNSAFE_TYPE = 0x02070003 => FailedPrecondition;
    NODE_STORAGE_IO = 0x02070004 => Internal;
    NODE_STORAGE_TASK_FAILED = 0x02070005 => Internal;
    NODE_VFS_NOT_FOUND = 0x02080001 => NotFound;
    NODE_VFS_INVALID = 0x02080002 => InvalidArgument;
    NODE_VFS_UNIMPLEMENTED = 0x02080003 => Unimplemented;
    NODE_VFS_UNAVAILABLE = 0x02080004 => Unavailable;
    NODE_MOUNT_CONFLICT = 0x02080005 => AlreadyExists;
    NODE_OWNER_INVALID_GRANT = 0x020b0001 => FailedPrecondition;
    NODE_OWNER_GRANT_UNAVAILABLE = 0x020b0002 => Unavailable;
    NODE_OWNER_RIGHT_DENIED = 0x020b0003 => PermissionDenied;
    NODE_OWNER_STALE_ACCESS = 0x020b0004 => FailedPrecondition;
    NODE_OWNER_STALE_HANDLE = 0x020b0005 => FailedPrecondition;
    NODE_DFS_STALE_HANDLE = 0x020c0001 => FailedPrecondition;
    NODE_RDMA_SESSION_UNKNOWN = 0x02090001 => FailedPrecondition;
    NODE_RDMA_SESSION_POISONED = 0x02090002 => FailedPrecondition;
    NODE_RDMA_NOT_READY = 0x02090003 => FailedPrecondition;
    NODE_RDMA_CAPACITY = 0x02090004 => ResourceExhausted;
    NODE_RDMA_HANDSHAKE_VERSION = 0x02090005 => FailedPrecondition;
    NODE_RDMA_CLOSED = 0x02090006 => FailedPrecondition;
    NODE_SHM_INVALID = 0x020a0001 => InvalidArgument;
    NODE_SHM_UNSUPPORTED = 0x020a0002 => FailedPrecondition;
    NODE_SHM_UNAVAILABLE = 0x020a0003 => Unavailable;
    NODE_SHM_INTERNAL = 0x020a0004 => Internal;
    NODE_SHM_ACCESS_DENIED = 0x020a0005 => PermissionDenied;
    META_CATALOG_INVALID_REQUEST = 0x03010001 => InvalidArgument;
    META_DFS_CONFLICT = 0x03030001 => FailedPrecondition;
    META_DFS_LEASE_RETRY = 0x03030002 => Unavailable;
    META_DFS_REPAIR_SUPERSEDED = 0x03030003 => FailedPrecondition;
    META_STORE_UNIMPLEMENTED = 0x03020001 => Unimplemented;
    CONFIG_INVALID = 0x04010001 => InvalidArgument;
    RUNTIME_INTERNAL = 0x04020001 => Internal;
    DIAGNOSTICS_NOT_CONFIGURED = 0x04020002 => FailedPrecondition;
    METRICS_FAILED = 0x04020003 => Internal;
    IO_NOT_FOUND = 0x04030001 => NotFound;
    IO_PERMISSION_DENIED = 0x04030002 => PermissionDenied;
    IO_ALREADY_EXISTS = 0x04030003 => AlreadyExists;
    IO_INVALID = 0x04030004 => InvalidArgument;
    IO_TIMEOUT = 0x04030005 => DeadlineExceeded;
    IO_UNAVAILABLE = 0x04030006 => Unavailable;
    IO_CAPACITY = 0x04030007 => ResourceExhausted;
    IO_OTHER = 0x04030008 => Internal;
    IO_NOT_DIRECTORY = 0x04030009 => FailedPrecondition;
    IO_IS_DIRECTORY = 0x0403000a => FailedPrecondition;
    IO_OUT_OF_MEMORY = 0x0403000c => ResourceExhausted;
    IO_DIRECTORY_NOT_EMPTY = 0x0403000b => FailedPrecondition;
    IO_CROSS_DEVICE = 0x0403000d => FailedPrecondition;
    IO_BAD_FILE_DESCRIPTOR = 0x0403000e => FailedPrecondition;
    IO_NAME_TOO_LONG = 0x0403000f => OutOfRange;
    IO_NO_DATA = 0x04030010 => NotFound;
    IO_NOT_SUPPORTED = 0x04030011 => FailedPrecondition;
    IO_OPERATION_NOT_PERMITTED = 0x04030012 => PermissionDenied;
    IO_TOO_MANY_SYMLINKS = 0x04030013 => FailedPrecondition;
    IO_FILE_TOO_LARGE = 0x04030014 => OutOfRange;
    IO_INTERRUPTED = 0x04030015 => Cancelled;
    IO_WOULD_BLOCK = 0x04030016 => Unavailable;
    IO_DEADLOCK = 0x04030017 => FailedPrecondition;
    IO_NO_LOCKS = 0x04030018 => ResourceExhausted;
}
