//! Shared gRPC connection configuration and TLS policy.
//!
//! Business protocols, routing and request lifecycles belong to their backends.
//! `grpc` 保留已有配置与 TLS 实现；`rdma` 提供单边传输与就绪探测，`shm` 提供本机缓冲区和 FD 传递。
//! 本库不注册业务 Handler，不提供文件 DataClient，也不决定失败请求是否重试。

#![deny(unsafe_code)]

#[cfg(feature = "grpc")]
mod error;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "rdma")]
pub mod rdma;
#[cfg(feature = "shm")]
pub mod shm;

#[cfg(feature = "grpc")]
pub use error::GrpcError;
#[cfg(feature = "grpc")]
pub use grpc::{GrpcConfig, SecurityManager, TlsConfig};
