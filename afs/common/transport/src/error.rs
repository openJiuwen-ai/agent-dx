//! Stable failures produced by common gRPC configuration and security code.

use std::io;

/// Connection, endpoint or TLS configuration failure.
#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("invalid gRPC endpoint `{0}`")]
    InvalidEndpoint(String),
    #[error("invalid TLS configuration: {0}")]
    InvalidTlsConfiguration(String),
    #[error("failed to read TLS file `{path}`: {source}")]
    ReadTlsFile { path: String, source: io::Error },
    #[error("gRPC transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
}
