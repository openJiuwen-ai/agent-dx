//! DigitalMateGateway business entrypoints, Jiuwen protocol adaptation and application forwarding.
pub mod application_ws;
pub mod connection;
pub mod download_config;
pub mod download_http;
pub mod download_runtime;
pub mod download_token;
pub mod driver;
pub mod entrypoint;
pub mod frontend;
mod privacy_center;
mod privacy_cipher;
mod privacy_grants;
mod privacy_http;
mod privacy_runtime;
pub mod protocol;
pub mod response;
pub mod session;
pub mod upload_http;

#[cfg(test)]
mod download_http_tests;
