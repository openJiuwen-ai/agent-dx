//! 本机数据 SDK：DistributedFs 使用 typed handle / fixed FileVersion、UDS 控制与 usrbio 风格的
//! 「持久 iov + 共享环」数据面；diagnostics Storage 穿刺保留为单独的兼容入口。
//!
//! DFS SDK 是给同机 runtime / ublkd / nydusd 这类显式集成方使用的“加速 API”，
//! 不是 POSIX 自动加速器：普通进程继续通过 FUSE/POSIX 访问文件系统。
//!
//! 当前固定边界：
//! - 控制面：gRPC over Unix Domain Socket，只发送文件身份、iov/环布局、注册句柄和 sync barrier。
//! - 数据面：文件内容放在一块常驻共享内存（iov）里，经共享 SQ/CQ 环提交与回收；
//!   一次 read/write 没有 RPC，也没有文件描述符传递。
//! - 没有 gRPC payload fallback；如果不能使用 SHM，调用方应退回 POSIX。
//! - 不连接远端 Node/Meta，不封装管理面 REST，不引入 RDMA；远端数据路径由 node 内部处理。
//!
//! SDK 不初始化进程全局日志/Trace；进程所有者负责观测设施生命周期。
//!
//! # 使用顺序
//!
//! ```text
//! connect → open → create_iov → create_ring → register_file
//!         → write_from / write_batch / read_into / read_batch → sync
//!         → unregister_file → close → ring.destroy → iov.destroy
//! ```
//!
//! 元数据（创建、删除、重命名）不在这里：那部分仍然走 FUSE/POSIX。SDK 只对**已存在**的
//! 路径做 `open`，然后接管它之后的所有数据读写。
//!
//! # iov 区块的生命周期（重要）
//!
//! 调用方**永远不持有区块句柄**，区块的分配与归还在 SDK 内部完成。由此有三条契约：
//!
//! 1. **成功即归还**：`write_batch` / `read_batch` 正常返回时，已有确定结果的区块已经还回
//!    iov，可以立刻被下一次调用复用。
//! 2. **不确定即作废**：等待超时、或完成项的 `userdata` 对不上任何在途请求时，SDK 无法判断
//!    那块内存是否已被 node 写过，因此**作废而不复用**。表现为 [`DfsIov::free_blocks`] 永久
//!    减少。
//! 3. **作废不可逆**：所以长时间运行时要监控 `free_blocks()`；它降到阈值以下就应销毁并重建
//!    iov（重建需要重新 `create_ring`，因为环绑定了 iov id）。
//!
//! 第 2 条是有意的取舍：宁可少几块可用内存，也不把可能被并发写过的内存交给调用方。
//!
//! # 并发模型
//!
//! - **一个环只能有一个提交者**：`DfsRing` 的方法都取 `&mut self`，编译器保证不会并发提交。
//!   需要多线程并发时，**每个线程各自 `create_ring`**（这也是 3FS 的模型）。
//! - **一条环配一块 iov**：多条环共用一个 iov 时，区块会被不同环争用，虽然不会串数据，
//!   但会让"某条环的请求在等另一条环腾块"这种耦合出现。跨线程请一并对齐 iov。
//! - 控制面（open/sync/close/register）走 gRPC，可并发调用。
//!
//! # 失败语义
//!
//! 单条 IO 失败不会拖垮整条环：失败以带原始 `userdata` 的完成项返回，环继续可用。
//! 只有控制面错误（open/register/sync）才可能让整个 `DfsRing` 进入不可用状态。

#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod connection;

pub use connection::{
    DfsAccess, DfsFileHandle, DfsIov, DfsLocalClient, DfsLocalClientConfig, DfsOpenFile,
    DfsRegistration, DfsRing, DfsSyncMode, DfsSyncResult, DiagnosticLocalClient,
    DiagnosticLocalClientConfig, LocalClient, LocalClientConfig, LocalClientError,
    max_parallel_shm_operations,
};

/// SDK callers can inspect native machine identities without depending on Tonic.
pub use afs_error::{Error, ErrorCode, ErrorKind};
