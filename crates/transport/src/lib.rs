//! Shared transport mechanics for ADX HTTP and gRPC boundaries.

pub mod deadline;
pub mod request;
pub mod tls;

/// Select Ring explicitly when the workspace dependency graph contains more
/// than one rustls crypto provider.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
