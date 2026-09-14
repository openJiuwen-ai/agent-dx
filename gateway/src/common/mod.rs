pub mod listener;
pub mod logging;
pub mod protocol;
pub mod resource;
pub mod route;
pub mod shutdown;

/// Select one rustls provider explicitly. Some feature combinations pull both
/// rustls providers through independent clients, in which case rustls refuses
/// to guess and panics on the first TLS configuration.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
