//! AFS 进程与文件后端的正式基础框架。
//!
//! afs-meta 管粗粒度权威，afs-node 同进程承载 FUSE、本地文件和 P2P。
//! 可运行接入、配置、观测与传输；完整文件业务由后续后端实施。
//! 目录合同见 docs/architecture/module-map.md；现有公共观测与 gRPC 配置实现继续保留。

#![deny(unsafe_op_in_unsafe_fn)]

pub mod dfs;
pub mod meta;
pub mod node;

pub mod config;
pub mod runtime;

pub mod error;
