//! Shared gRPC configuration, security policy and structured error codec.
//!
//! Business services, generated protobuf clients and generated server traits
//! deliberately stay in their callers. This module applies the common
//! transport knobs and error encoding that every AFS process relation must share.
//! 调用方直接使用 Tonic 建连、选择 TCP/UDS、监听并注册生成的 service。
//! 不增加统一连接管理器、重试框架或 actor 调度层。

mod config;
mod security;

pub use config::GrpcConfig;
pub use security::{SecurityManager, TlsConfig};

/// 通用错误编解码，不注册业务 Handler，也不封装重试。
pub mod error_status;
