//! 本机数据 SDK：DistributedFs 使用 typed handle / fixed FileVersion、UDS 控制与 SHM
//! 执行有界范围读写；diagnostics Storage 穿刺保留为单独的兼容入口。
//!
//! DFS SDK 是给同机 runtime / ublkd / nydusd 这类显式集成方使用的“加速 API”，
//! 不是 POSIX 自动加速器：普通进程继续通过 FUSE/POSIX 访问文件系统。
//!
//! 第一版固定边界：
//! - 控制面：gRPC over Unix Domain Socket，只发送文件身份、range、sync barrier 和 SHM grant。
//! - 数据面：真实内容放在本机 memfd，经 SCM_RIGHTS 传递 fd 后由 node 读写。
//! - 没有 gRPC payload fallback；如果不能使用 SHM，调用方应退回 POSIX。
//! - 不连接远端 Node/Meta，不封装管理面 REST，不引入 RDMA；远端数据路径由 node 内部处理。
//!
//! SDK 不初始化进程全局日志/Trace；进程所有者负责观测设施生命周期。

#![deny(unsafe_op_in_unsafe_fn)]

pub mod buffer;
pub mod connection;

pub use buffer::{DfsReadBuffer, DfsWriteBuffer};
pub use connection::{
    DfsAccess, DfsFileHandle, DfsFileRange, DfsFileVersion, DfsLocalClient, DfsLocalClientConfig,
    DfsOpenFile, DfsReadResult, DfsReadTarget, DfsSyncMode, DfsSyncResult, DfsWriteResult,
    DiagnosticLocalClient, DiagnosticLocalClientConfig, LocalClient, LocalClientConfig,
    LocalClientError, max_parallel_shm_operations,
};

/// SDK callers can inspect native machine identities without depending on Tonic.
pub use afs_error::{Error, ErrorCode, ErrorKind};
