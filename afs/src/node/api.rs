//! Node 面向本机使用者的显式 API。
//!
//! local 为高性能 SDK 的 UDS/SHM 入口；rest 为 runtime 等调用的 REST 入口。
//! 显式发布/快照属于业务操作，不能从 close/fsync 推断。两个入口调用同一份
//! 对应业务，不复制发布状态机；Meta 的管理面 API 不放到这里。

pub mod local;
pub mod rest;
