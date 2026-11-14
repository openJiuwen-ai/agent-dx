//! DigitalMateGateway business entrypoints, Jiuwen protocol adaptation and application forwarding.
pub mod application_ws;
pub mod composio;
pub mod connection;
pub mod connector_owner;
mod database;
pub mod download_config;
pub mod download_http;
pub mod download_runtime;
pub mod download_token;
pub mod driver;
pub mod entrypoint;
pub mod foreground_admission;
pub mod frontend;
pub mod ingestion;
mod privacy_center;
mod privacy_cipher;
mod privacy_grants;
mod privacy_http;
mod privacy_runtime;
pub mod protocol;
pub mod response;
pub mod server_config;
pub mod session;
pub mod upload_http;

/// Cancel the HTTP connection driver on completion, error or caller cancellation.
struct ConnectionTask(tokio::task::JoinHandle<()>);

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod download_http_tests;
