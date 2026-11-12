//! 基础诊断链（R49）：与 OwnerFs/DFS 数据面无关的显式诊断入口，照旧总是装配。
//!
//! - `probe_store`：诊断对象存储 `DiagnosticStorage`（`data_dir/diagnostics`，单文件名、
//!   有界范围读写）。
//! - `local_data`：UDS `LocalData` 服务，同机 SDK 经 memfd/FD 读写诊断对象存储。
//! - `probe`：REST `POST /v1/diagnostics` 的探测流程（Meta Ping → 对端控制 Ping →
//!   对端诊断对象写/读）。
//!
//! 传输面仍在 `rpc/`：`NodeData` gRPC 的诊断读写 handler（`rpc::data`，与 OwnerFs
//! 文件命令同一 service、共用 RDMA 会话表）和对端客户端 `DataPeerClient`
//! （`rpc::peer`）。REST 与 UDS 契约不随本模块变化。

pub(crate) mod local_data;
pub(crate) mod probe;
pub mod probe_store;

pub(in crate::node) use local_data::DiagnosticLocalDataService;
pub use probe_store::{DiagnosticStorage, DiagnosticStorageError};
