//! Node 数据层读引擎。
//!
//! 文件层（`vfs/dfs`）把文件范围解析成不可变 Chunk 范围后交给这里：
//! `admission` 是异步读的准入与配额策略，`engine` 选择本地、Peer 或外部来源并取数，
//! `object_fetch`/`direct_prefetch` 调度预取，`buffer` 是读缓冲池，`peer_cache` 是可丢弃的
//! Peer 对象磁盘缓存，`activity` 记录读路径计时。

pub(super) mod activity;
pub mod admission;
pub(super) mod buffer;
#[cfg(feature = "dfs")]
pub(super) mod direct_prefetch;
#[cfg(feature = "dfs")]
pub mod engine;
#[cfg(feature = "dfs")]
pub(super) mod object_fetch;
#[cfg(feature = "dfs")]
pub mod peer_cache;
